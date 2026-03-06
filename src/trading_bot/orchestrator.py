print(f"Executing orchestrator.py from: {__file__}")
"""
Canonical Trading System Orchestrator

Single entry point coordinating all layers:
Layer 0: Data Ingestion → Layer 1: Alpha Models → Layer 2: Ensemble →
Layer 3: Position Sizing → Layer 4: Execution → Layer 5: Risk Management

This module replaces src/main.py, src/main_production.py, and src/trading_system/main.py
"""

import asyncio
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import threading
from prometheus_client import Counter, Gauge, Histogram, start_http_server, REGISTRY
from pydantic import BaseModel, Field

from trading_bot.utils.logging_config import configure_structlog
from trading_bot.utils.logging_config import get_logger as get_structlog_logger

# Utilities
from trading_bot.utils.runtime_env import (
    LiveTradingNotConfirmedError,
    configure_runtime,
    enforce_startup_validations,
)
from trading_bot.utils.settings import get_settings

if TYPE_CHECKING:  # pragma: no cover - imports for type checking only
    from trading_bot.ledger.ledger import TradingLedger


def load_env_file(explicit_path: Union[str, Path, None] = None) -> None:
    """Load environment variables from a .env file when present."""

    if explicit_path is not None:
        candidate = Path(explicit_path).expanduser()
        load_dotenv(candidate, override=False)
        return

    project_root = Path(__file__).resolve().parents[2]
    default_env = project_root / ".env"
    load_dotenv(default_env, override=False)

# Load shared logging configuration so that orchestrator logs match the rest of the stack.
configure_structlog()

logger = get_structlog_logger(__name__)


from trading_bot.config.schema import ExecutionMode, OrchestratorConfig, StrategyType


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Best-effort float conversion for telemetry fields."""

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed != parsed:  # NaN guard
        return default
    return parsed

# Unregister metrics to avoid errors during test collection
for metric in ['orchestrator_status', 'trading_cycles_total', 'cycle_duration_seconds', 'layer_latency_seconds', 'alpha_signals_total', 'orders_placed_total', 'risk_violations_total', 'portfolio_value_usd', 'portfolio_pnl_usd']:
    if metric in REGISTRY._names_to_collectors:
        REGISTRY.unregister(REGISTRY._names_to_collectors[metric])

# Prometheus metrics
SYSTEM_STATUS = Gauge('orchestrator_status', 'Orchestrator status', ['layer'])
CYCLE_COUNTER = Counter('trading_cycles_total', 'Total trading cycles executed')
CYCLE_DURATION = Histogram('cycle_duration_seconds', 'Trading cycle duration')
LAYER_LATENCY = Histogram('layer_latency_seconds', 'Layer processing latency', ['layer'])
SIGNAL_GENERATED = Counter('alpha_signals_total', 'Alpha signals generated', ['symbol', 'model'])
ORDERS_PLACED = Counter('orders_placed_total', 'Orders placed', ['symbol', 'side'])
RISK_VIOLATIONS = Counter('risk_violations_total', 'Risk violations', ['type'])
PORTFOLIO_VALUE = Gauge('portfolio_value_usd', 'Portfolio value in USD')
PNL_GAUGE = Gauge('portfolio_pnl_usd', 'Portfolio P&L in USD')


class KillSwitchRequest(BaseModel):
    """Request payload for activating the kill switch."""

    reason: str = Field(default="Manual activation", max_length=256)
    trigger: Optional[str] = Field(default=None, max_length=64)


class KillSwitchResetRequest(BaseModel):
    """Request payload for resetting the kill switch."""

    authorized_by: str = Field(default="operator", max_length=128)


class TradingSystemOrchestrator:
    """
    Canonical orchestrator for the multi-layer trading system.

    Coordinates all layers in sequence:
    0. Data Ingestion → 1. Alpha Models → 2. Ensemble →
    3. Position Sizing → 4. Execution → 5. Risk Management
    """

    def __init__(self, config: OrchestratorConfig, data_adapter=None):
        self.config = config
        self.settings = get_settings()
        self.is_running = False
        self.shutdown_requested = False
        self.start_time = None

        # Bind logger with context
        self.logger = logger.bind(
            mode=config.mode if isinstance(config.mode, str) else config.mode.value,
            strategy=config.strategy if isinstance(config.strategy, str) else config.strategy.value
        )

        # Layer components (initialized to None)
        self.data_ingestion = None
        self.feature_bus = None
        self.data_adapter = data_adapter  # Injected or will be created
        self.adapter_bridge = None  # For real-time connector integration
        self.alpha_models: Dict[str, Any] = {}
        self.ensemble = None
        self.position_sizing = None
        self.executors: Dict[str, Any] = {}
        self.risk_manager = None
        self.ledger: Optional[TradingLedger] = None

        # State tracking
        self.current_positions: Dict[str, float] = {}
        self.pending_orders: List[Dict] = []
        self.last_cycle_time: Optional[float] = None
        self.portfolio_value: float = config.initial_capital
        self.realized_pnl: float = 0.0
        self.total_fees_usd: float = 0.0
        self.position_entry_price: Dict[str, float] = {}
        self.position_entry_time: Dict[str, datetime] = {}
        self.symbol_realized_pnl: Dict[str, float] = defaultdict(float)
        self.symbol_fees_usd: Dict[str, float] = defaultdict(float)
        self.symbol_fill_count: Dict[str, int] = defaultdict(int)
        self.symbol_trade_count: Dict[str, int] = defaultdict(int)
        self.symbol_entry_signals: Dict[str, int] = defaultdict(int)
        self.symbol_blocked_signals: Dict[str, int] = defaultdict(int)
        self.symbol_block_reasons: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.symbol_cooldown_until: Dict[str, datetime] = {}
        self.symbol_daily_lock_date: Dict[str, date] = {}
        self.symbol_daily_loss_locked: Dict[str, bool] = defaultdict(bool)
        self.stock_market_hours_fill_count: int = 0
        self._cached_market_prices: Dict[str, float] = {}
        self._cached_market_timestamps: Dict[str, datetime] = {}
        self._last_market_data_update_at: Optional[datetime] = None

        # Kill-switch state / control plane handles
        self.kill_switch_active: bool = False
        self.kill_switch_reason: Optional[str] = None
        self.kill_switch_trigger: Optional[str] = None
        self.kill_switch_timestamp: Optional[datetime] = None
        self._control_app: Optional[FastAPI] = None
        self._control_server: Optional[uvicorn.Server] = None
        self._control_task: Optional[asyncio.Task] = None

        self.logger.info("orchestrator_initialized", config=config.dict())

    def _control_state(self) -> Dict[str, Any]:
        """Return current orchestrator control-plane state."""

        return {
            "kill_switch_active": self.kill_switch_active,
            "kill_switch_reason": self.kill_switch_reason,
            "kill_switch_trigger": self.kill_switch_trigger,
            "kill_switch_timestamp": (
                self.kill_switch_timestamp.isoformat()
                if self.kill_switch_timestamp
                else None
            ),
            "mode": self.config.mode,
            "strategy": self.config.strategy,
            "is_running": self.is_running,
            "shutdown_requested": self.shutdown_requested,
        }

    def _portfolio_snapshot_payload(self, now_utc: datetime) -> Dict[str, Any]:
        unrealized = 0.0
        positions_value = 0.0
        open_positions = 0
        for symbol, qty in self.current_positions.items():
            parsed_qty = _safe_float(qty, 0.0)
            if abs(parsed_qty) <= 1e-12:
                continue
            open_positions += 1
            market_price = _safe_float(self._cached_market_prices.get(symbol), 0.0)
            if market_price > 0:
                positions_value += parsed_qty * market_price
            unrealized += self._estimate_symbol_unrealized_pnl(symbol)
        if open_positions == 0:
            # Hard invariant: a flat book must have zero unrealized PnL.
            unrealized = 0.0
        total_value = self.config.initial_capital + self.realized_pnl + unrealized
        cash_balance = total_value - positions_value
        return {
            "timestamp": now_utc.isoformat(),
            "total_value": total_value,
            "portfolio_pnl_usd": self.realized_pnl + unrealized,
            "unrealized_pnl_usd": unrealized,
            "realized_pnl_usd": self.realized_pnl,
            "cash_balance_usd": cash_balance,
            "positions_value_usd": positions_value,
            "total_fees_usd": self.total_fees_usd,
            "positions_count": open_positions,
        }

    def _market_prices_payload(self, now_utc: datetime) -> Dict[str, Any]:
        max_age = max(self._stale_data_max_age_seconds(), 1.0)
        prices: list[Dict[str, Any]] = []
        stale_symbols: list[str] = []
        for symbol in self._get_all_symbols():
            price = _safe_float(self._cached_market_prices.get(symbol), 0.0)
            ts = self._cached_market_timestamps.get(symbol)
            age_seconds: float | None = None
            if isinstance(ts, datetime):
                age_seconds = max((now_utc - ts).total_seconds(), 0.0)
            stale = age_seconds is None or age_seconds > max_age
            if stale:
                stale_symbols.append(symbol)
            if price > 0:
                prices.append(
                    {
                        "symbol": symbol,
                        "price": price,
                        "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
                        "age_seconds": age_seconds,
                        "stale": stale,
                    }
                )
        return {
            "prices": prices,
            "stale_symbols": stale_symbols,
            "all_symbols_fresh": len(stale_symbols) == 0,
            "stale_data_max_age_seconds": max_age,
        }

    def _build_scorecards(self, now_utc: datetime) -> Dict[str, Any]:
        stock_symbols = set(self._get_stock_symbols())
        crypto_symbols = set(self._get_crypto_symbols())
        all_symbols = (
            set(self.current_positions.keys())
            | set(self.symbol_realized_pnl.keys())
            | set(self.symbol_fees_usd.keys())
            | set(self.symbol_fill_count.keys())
            | stock_symbols
            | crypto_symbols
        )

        def aggregate(symbols: set[str]) -> Dict[str, Any]:
            unrealized = sum(self._estimate_symbol_unrealized_pnl(symbol) for symbol in symbols)
            realized = sum(_safe_float(self.symbol_realized_pnl.get(symbol), 0.0) for symbol in symbols)
            fees = sum(_safe_float(self.symbol_fees_usd.get(symbol), 0.0) for symbol in symbols)
            fills = sum(int(self.symbol_fill_count.get(symbol, 0)) for symbol in symbols)
            trades = sum(int(self.symbol_trade_count.get(symbol, 0)) for symbol in symbols)
            open_positions = sum(
                1 for symbol in symbols if abs(_safe_float(self.current_positions.get(symbol), 0.0)) > 1e-12
            )
            return {
                "realized_pnl_usd": realized,
                "unrealized_pnl_usd": unrealized,
                "fees_usd": fees,
                "fills": fills,
                "trade_count": trades,
                "open_positions": open_positions,
                "symbol_count": len(symbols),
                "timestamp": now_utc.isoformat(),
            }

        stock_card = aggregate(stock_symbols)
        crypto_card = aggregate(crypto_symbols)
        portfolio_card = aggregate(all_symbols)
        return {
            "stocks": stock_card,
            "crypto": crypto_card,
            "portfolio_total": portfolio_card,
        }

    def _build_stock_day_report(self, now_utc: datetime) -> Dict[str, Any]:
        max_age = max(self._stale_data_max_age_seconds(), 1.0)
        market_hours = self._is_regular_stock_session(now_utc)
        stock_symbols = self._get_stock_symbols()
        symbols_payload: Dict[str, Any] = {}
        freshness_fail_count = 0
        after_hours_ignored_stale_count = 0
        merged_block_reasons: Dict[str, int] = defaultdict(int)

        for symbol in stock_symbols:
            price_ts = self._cached_market_timestamps.get(symbol)
            age_seconds = (
                max((now_utc - price_ts).total_seconds(), 0.0) if isinstance(price_ts, datetime) else None
            )
            is_stale = age_seconds is None or age_seconds > max_age
            if is_stale:
                if market_hours:
                    freshness_fail_count += 1
                else:
                    after_hours_ignored_stale_count += 1

            block_reasons = {
                reason: int(count)
                for reason, count in dict(self.symbol_block_reasons.get(symbol, {})).items()
                if int(count) > 0
            }
            for reason, count in block_reasons.items():
                merged_block_reasons[reason] += int(count)

            symbols_payload[symbol] = {
                "entry_signals": int(self.symbol_entry_signals.get(symbol, 0)),
                "blocked_signals": int(self.symbol_blocked_signals.get(symbol, 0)),
                "fills": int(self.symbol_fill_count.get(symbol, 0)),
                "realized_pnl_usd": _safe_float(self.symbol_realized_pnl.get(symbol), 0.0),
                "unrealized_pnl_usd": self._estimate_symbol_unrealized_pnl(symbol),
                "fees_usd": _safe_float(self.symbol_fees_usd.get(symbol), 0.0),
                "block_reasons": block_reasons,
                "quote_age_seconds": age_seconds,
                "quote_stale": is_stale,
            }

        summary = {
            "entry_signals": sum(int(self.symbol_entry_signals.get(symbol, 0)) for symbol in stock_symbols),
            "blocked_signals": sum(int(self.symbol_blocked_signals.get(symbol, 0)) for symbol in stock_symbols),
            "fills": sum(int(self.symbol_fill_count.get(symbol, 0)) for symbol in stock_symbols),
            "realized_pnl_usd": sum(_safe_float(self.symbol_realized_pnl.get(symbol), 0.0) for symbol in stock_symbols),
            "unrealized_pnl_usd": sum(self._estimate_symbol_unrealized_pnl(symbol) for symbol in stock_symbols),
            "fees_usd": sum(_safe_float(self.symbol_fees_usd.get(symbol), 0.0) for symbol in stock_symbols),
            "freshness_fail_count": freshness_fail_count,
            "market_hours_fill_count": int(self.stock_market_hours_fill_count),
            "after_hours_ignored_stale_count": after_hours_ignored_stale_count,
            "block_reasons": dict(sorted(merged_block_reasons.items())),
        }

        return {
            "date_et": self._et_now(now_utc).date().isoformat(),
            "summary": summary,
            "symbols": symbols_payload,
            "source": "orchestrator_runtime",
        }

    def _open_positions_payload(self, now_utc: datetime) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        for symbol in sorted(self.current_positions.keys()):
            qty = _safe_float(self.current_positions.get(symbol), 0.0)
            if abs(qty) <= 1e-12:
                continue
            mark_price = _safe_float(self._cached_market_prices.get(symbol), 0.0)
            entry_price = _safe_float(self.position_entry_price.get(symbol), 0.0)
            entry_time = self.position_entry_time.get(symbol)
            payload.append(
                {
                    "symbol": symbol,
                    "quantity": qty,
                    "asset_class": (
                        "crypto"
                        if self._is_crypto_symbol(symbol)
                        else ("stock" if self._is_stock_symbol(symbol) else "unknown")
                    ),
                    "mark_price": mark_price if mark_price > 0.0 else None,
                    "entry_price": entry_price if entry_price > 0.0 else None,
                    "entry_time": (
                        entry_time.isoformat() if isinstance(entry_time, datetime) else None
                    ),
                    "unrealized_pnl_usd": self._estimate_symbol_unrealized_pnl(symbol),
                    "timestamp": now_utc.isoformat(),
                }
            )
        return payload

    def _market_state_payload(self) -> Dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        price_payload = self._market_prices_payload(now_utc)
        open_positions = self._open_positions_payload(now_utc)
        return {
            "timestamp": now_utc.isoformat(),
            "all_symbols_fresh": price_payload["all_symbols_fresh"],
            "stale_symbols": price_payload["stale_symbols"],
            "last_market_data_update_at": (
                self._last_market_data_update_at.isoformat()
                if isinstance(self._last_market_data_update_at, datetime)
                else None
            ),
            "stale_data_max_age_seconds": price_payload["stale_data_max_age_seconds"],
            "prices": price_payload["prices"],
            "portfolio_snapshot": self._portfolio_snapshot_payload(now_utc),
            "stock_day_report": self._build_stock_day_report(now_utc),
            "scorecards": self._build_scorecards(now_utc),
            "open_positions_count": len(open_positions),
            "open_positions": open_positions,
            "equity_session_policy": {
                "allow_overnight": self._equities_allow_overnight(),
                "flatten_before_close_minutes": self._equities_flatten_before_close_minutes(),
                "flatten_window_active": self._is_equity_flatten_window(now_utc),
                "market_hours": self._is_regular_stock_session(now_utc),
            },
            "crypto_risk_controls": {
                "max_adverse_excursion_pct": self._crypto_max_mae_pct(),
                "max_hold_minutes": self._crypto_max_hold_minutes(),
                "daily_symbol_loss_cap_usd": self._crypto_daily_loss_cap_usd(),
                "reentry_cooldown_minutes": self._crypto_reentry_cooldown_minutes(),
            },
        }

    async def activate_kill_switch(self, reason: str, trigger: str) -> Dict[str, Any]:
        """Activate the kill switch to halt trading activity."""

        if self.kill_switch_active:
            state = self._control_state()
            state["activated"] = False
            return state

        self.kill_switch_active = True
        self.kill_switch_reason = reason
        self.kill_switch_trigger = trigger
        self.kill_switch_timestamp = datetime.now(timezone.utc)

        self.logger.critical(
            "kill_switch_activated",
            reason=reason,
            trigger=trigger,
            timestamp=self.kill_switch_timestamp.isoformat(),
        )

        RISK_VIOLATIONS.labels(type="kill_switch").inc()

        if self.risk_manager:
            try:
                self.risk_manager.emergency_stop_all()
            except Exception as exc:  # noqa: BLE001
                self.logger.error("kill_switch_risk_manager_error", error=str(exc))

        await self._cancel_open_orders()

        state = self._control_state()
        state.update({"activated": True})
        return state

    async def reset_kill_switch(self, authorized_by: str) -> Dict[str, Any]:
        """Reset kill switch and allow trading to resume."""

        if not self.kill_switch_active:
            state = self._control_state()
            state.update({"reset": False})
            return state

        self.kill_switch_active = False
        self.kill_switch_reason = None
        self.kill_switch_trigger = None
        self.kill_switch_timestamp = None

        if self.risk_manager:
            try:
                self.risk_manager.reset_emergency_stop()
            except Exception as exc:  # noqa: BLE001
                self.logger.error("kill_switch_reset_error", error=str(exc))

        self.logger.info("kill_switch_reset", authorized_by=authorized_by)

        state = self._control_state()
        state.update({"reset": True, "authorized_by": authorized_by})
        return state

    def _build_control_app(self) -> FastAPI:
        """Construct the FastAPI application for the control plane."""

        app = FastAPI(title="Trading Control API", version="1.0.0")

        @app.get("/health")
        async def health_check() -> Dict[str, Any]:
            return self._control_state()

        @app.get("/telemetry/market_state")
        async def telemetry_market_state() -> Dict[str, Any]:
            return self._market_state_payload()

        @app.post("/emergency/kill_switch")
        async def api_kill_switch(request: KillSwitchRequest) -> Dict[str, Any]:
            payload = await self.activate_kill_switch(
                reason=request.reason,
                trigger=request.trigger or "api",
            )
            return payload

        @app.post("/emergency/reset")
        async def api_reset(request: KillSwitchResetRequest) -> Dict[str, Any]:
            payload = await self.reset_kill_switch(authorized_by=request.authorized_by)
            if not payload.get("reset"):
                raise HTTPException(status_code=400, detail="Kill switch is not active")
            return payload

        return app

    async def _start_control_plane(self) -> None:
        """Start the control API if enabled in configuration."""

        if not self.config.enable_control_api or self._control_server is not None:
            return

        self._control_app = self._build_control_app()

        config = uvicorn.Config(
            self._control_app,
            host=self.config.control_api_host,
            port=self.config.control_api_port,
            log_level="info",
            loop="asyncio",
        )
        self._control_server = uvicorn.Server(config)
        self._control_task = asyncio.create_task(self._control_server.serve())
        self.logger.info(
            "control_api_started",
            host=self.config.control_api_host,
            port=self.config.control_api_port,
        )

    async def _stop_control_plane(self) -> None:
        """Stop the control API server if it is running."""

        if self._control_server is not None:
            self._control_server.should_exit = True

        if self._control_task is not None:
            try:
                await self._control_task
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("control_api_shutdown_error", error=str(exc))

        self._control_app = None
        self._control_server = None
        self._control_task = None

    async def _cancel_open_orders(self) -> None:
        """Attempt to cancel open orders across all executors."""

        if not hasattr(self, 'router'):
            return

        for executor in self.router.executors.values():
            cancel_callable = None
            for attr in ("cancel_all_orders", "cancel_all", "close_all_positions"):
                if hasattr(executor, attr):
                    cancel_callable = getattr(executor, attr)
                    break

            if cancel_callable is None:
                continue

            try:
                if asyncio.iscoroutinefunction(cancel_callable):
                    await cancel_callable()
                else:
                    await asyncio.to_thread(cancel_callable)
                self.logger.info("executor_orders_cancelled", executor=executor.__class__.__name__)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "executor_cancel_failed",
                    executor=executor.__class__.__name__,
                    error=str(exc),
                )
    async def initialize(self) -> None:
        """Initialize all system components in dependency order"""
        self.logger.info("initializing_system")

        try:
            # Initialize data adapter (if not injected)
            await self._initialize_data_adapter()
            SYSTEM_STATUS.labels(layer='data_adapter').set(1)

            # Initialize database ledger first (foundational)
            await self._initialize_ledger()
            SYSTEM_STATUS.labels(layer='database_ledger').set(1)

            # Layer 0: Data Ingestion
            await self._initialize_layer0()
            SYSTEM_STATUS.labels(layer='data_ingestion').set(1)

            # Layer 1: Alpha Models
            await self._initialize_layer1()
            SYSTEM_STATUS.labels(layer='alpha_models').set(1)

            # Layer 2: Ensemble
            await self._initialize_layer2()
            SYSTEM_STATUS.labels(layer='ensemble').set(1)

            # Layer 3: Position Sizing
            await self._initialize_layer3()
            SYSTEM_STATUS.labels(layer='position_sizing').set(1)

            # Layer 4: Execution
            await self._initialize_layer4()
            SYSTEM_STATUS.labels(layer='execution').set(1)

            # Layer 5: Risk Management
            await self._initialize_layer5()
            SYSTEM_STATUS.labels(layer='risk_management').set(1)

            # Create initial portfolio snapshot
            await self._create_portfolio_snapshot()

            self.logger.info("system_initialized_successfully")

        except Exception as e:
            self.logger.error("initialization_failed", error=str(e), exc_info=True)
            raise

    async def _initialize_data_adapter(self) -> None:
        """Initialize data adapter from config if not already injected"""
        if self.data_adapter is not None:
            self.logger.info("data_adapter_injected", adapter_type=type(self.data_adapter).__name__)
            await self.data_adapter.connect()
            return

        # Create adapter from configuration
        from trading_bot.data.adapters import (
            DataAdapterConfig,
            MockDataAdapter,
            RedisDataAdapter,
            KafkaDataAdapter,
            HistoricalReplayAdapter
        )

        adapter_config = DataAdapterConfig(
            adapter_type=self.config.data_adapter_type,
            redis_host=self.config.redis_host,
            redis_port=self.config.redis_port,
            kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,
            parquet_base_path=self.config.parquet_base_path,
            replay_speed=self.config.replay_speed,
            replay_start_date=self.config.replay_start_date,
            replay_end_date=self.config.replay_end_date
        )

        self.logger.info("creating_data_adapter", adapter_type=self.config.data_adapter_type)

        # Create appropriate adapter
        if self.config.data_adapter_type == 'mock':
            self.data_adapter = MockDataAdapter(adapter_config)
        elif self.config.data_adapter_type == 'redis':
            self.data_adapter = RedisDataAdapter(adapter_config)
        elif self.config.data_adapter_type == 'kafka':
            self.data_adapter = KafkaDataAdapter(adapter_config)
        elif self.config.data_adapter_type == 'historical':
            self.data_adapter = HistoricalReplayAdapter(adapter_config)
        else:
            self.logger.warning("unknown_adapter_type", adapter_type=self.config.data_adapter_type, falling_back_to="mock")
            self.data_adapter = MockDataAdapter(adapter_config)

        # Connect the adapter
        await self.data_adapter.connect()
        self.logger.info("data_adapter_connected", adapter_type=type(self.data_adapter).__name__)

        # Initialize AdapterBridge if enabled
        await self._initialize_adapter_bridge()

    async def _initialize_adapter_bridge(self) -> None:
        """Initialize AdapterBridge for real-time connector integration"""
        if not self.config.enable_adapter_bridge:
            self.logger.info("adapter_bridge_disabled", reason="feature_flag_off")
            return

        if not self.config.bridge_connectors:
            self.logger.info("adapter_bridge_disabled", reason="no_connectors_configured")
            return

        try:
            from trading_bot.data.bridge_factory import BridgeFactory, create_bridge_config_from_settings

            # Create bridge configuration
            bridge_config = create_bridge_config_from_settings(
                enable_bridge=True,
                mode=self.config.mode.value if hasattr(self.config.mode, 'value') else self.config.mode,
                connectors=self.config.bridge_connectors,
                symbols=self.config.bridge_symbols,
                enable_normalization=True,
                enable_metrics=self.config.enable_metrics,
                enable_fallback=True
            )

            self.logger.info(
                "creating_adapter_bridge",
                connectors=bridge_config.connectors,
                symbols=bridge_config.symbols
            )

            # Create and start bridge
            self.adapter_bridge = await BridgeFactory.create_bridge(
                bridge_config,
                self.data_adapter
            )

            if self.adapter_bridge:
                await self.adapter_bridge.start()
                self.logger.info(
                    "adapter_bridge_started",
                    num_connectors=len(bridge_config.connectors)
                )
            else:
                self.logger.warning("adapter_bridge_creation_returned_none")

        except Exception as e:
            self.logger.error(
                "adapter_bridge_initialization_failed",
                error=str(e),
                exc_info=True
            )
            # Don't fail startup - continue without bridge
            self.adapter_bridge = None

    async def _initialize_ledger(self) -> None:
        """Initialize database ledger for compliance tracking"""
        self.logger.info("initializing_database_ledger")

        from trading_bot.ledger.ledger import InMemoryLedger, TradingLedger

        use_in_memory = self.settings.features.use_in_memory_ledger
        if not use_in_memory and self.config.mode == "paper" and not self.settings.features.enable_db_in_paper:
            use_in_memory = True

        if use_in_memory:
            self.logger.info("using_in_memory_ledger", reason="paper_mode")
            self.ledger = InMemoryLedger()
        else:
            self.ledger = TradingLedger(postgres_url=self.config.postgres_url)
        await self.ledger.initialize()

        self.logger.info("ledger_initialized")

    async def _initialize_layer0(self) -> None:
        """Initialize Layer 0: Data Ingestion & Feature Bus"""
        self.logger.info("initializing_layer0_data_ingestion")

        from trading_bot.data.connectors.alpaca_connector import AlpacaConnector
        from trading_bot.data.connectors.crypto_connector import CoinbaseConnector
        from trading_bot.data.feature_bus import FeatureBus

        # Initialize feature bus
        self.feature_bus = FeatureBus(
            kafka_servers=self.config.kafka_bootstrap_servers,
            redis_host=self.config.redis_host,
            redis_port=self.config.redis_port
        )
        await self.feature_bus.start()

        if self.settings.features.disable_market_connectors:
            self.logger.info("market_connectors_skipped", reason="env_flag")
            return

        # Initialize crypto connector if needed
        crypto_symbols = self._get_crypto_symbols()
        if crypto_symbols:
            self.logger.info("initializing_coinbase_connector", symbols=crypto_symbols)
            coinbase_connector = CoinbaseConnector(
                symbols=crypto_symbols,
            )
            await coinbase_connector.start()
            self.data_ingestion = coinbase_connector

        # Initialize stock connector if needed
        stock_symbols = self._get_stock_symbols()
        if stock_symbols:
            self.logger.info("initializing_alpaca_connector", symbols=stock_symbols)
            alpaca_connector = AlpacaConnector(
                symbols=stock_symbols,
                mode=self.config.mode
            )
            await alpaca_connector.start()
            if not self.data_ingestion:
                self.data_ingestion = alpaca_connector

        self.logger.info("layer0_initialized")

    async def _initialize_layer1(self) -> None:
        """Initialize Layer 1: Alpha Signal Models"""
        self.logger.info("initializing_layer1_alpha_models")

        from trading_bot.alpha.ma_momentum import MovingAverageMomentumAlpha

        all_symbols = self._get_all_symbols()

        # Initialize baseline alpha models
        for symbol in all_symbols:
            # MA Momentum alpha
            ma_alpha = MovingAverageMomentumAlpha()
            self.alpha_models[f"ma_momentum_{symbol}"] = ma_alpha

            # Add more alpha models here as they are stabilized
            # ob_pressure_alpha = OrderBookPressureAlpha(symbol=symbol)
            # mean_rev_alpha = MeanReversionAlpha(symbol=symbol)

        self.logger.info("layer1_initialized", model_count=len(self.alpha_models))

    async def _initialize_layer2(self) -> None:
        """Initialize Layer 2: Ensemble Meta-Learner"""
        self.logger.info("initializing_layer2_ensemble")

        from trading_bot.ensemble.bandit_blender import BanditBlender
        from trading_bot.ensemble.meta_learner import MetaLearner

        if self.config.enable_bandit_blender:
            self.logger.info("using_bandit_blender")
            self.ensemble = BanditBlender(
                n_models=len(self.alpha_models),
                learning_rate=0.01
            )
        else:
            self.logger.info("using_meta_learner")
            self.ensemble = MetaLearner()
            if os.path.exists("artifacts/models/meta_learner.joblib"):
                self.ensemble.load_model("artifacts/models/meta_learner.joblib")

        self.logger.info("layer2_initialized")

    async def _initialize_layer3(self) -> None:
        """Initialize Layer 3: Position Sizing"""
        self.logger.info("initializing_layer3_position_sizing")

        from trading_bot.portfolio.allocator import EnhancedPositionSizing

        self.position_sizing = EnhancedPositionSizing(
            max_total_leverage=self.config.max_leverage,
            target_volatility=0.15
        )

        self.logger.info("layer3_initialized")

    async def _initialize_layer4(self) -> None:
        """Initialize Layer 4: Execution Engines with Smart Order Router"""
        self.logger.info("initializing_layer4_execution")

        from trading_bot.execution.adapters.alpaca_executor import AlpacaExecutor
        from trading_bot.execution.adapters.coinbase_executor import CoinbaseExecutor
        from trading_bot.execution.smart_order_router import SmartOrderRouter

        # Initialize individual executors
        coinbase_executor = CoinbaseExecutor()
        alpaca_executor = AlpacaExecutor()

        executors = {
            "coinbase": coinbase_executor,
            "alpaca": alpaca_executor,
        }

        # Initialize Smart Order Router
        self.router = SmartOrderRouter(executors=executors)

        self.logger.info("layer4_initialized", router_enabled=True, executor_count=len(executors))

    async def _initialize_layer5(self) -> None:
        """Initialize Layer 5: Risk Management"""
        self.logger.info("initializing_layer5_risk_management")

        from trading_bot.risk.manager import RiskManager, RiskLimits

        self.risk_manager = RiskManager(
            limits=RiskLimits(
                max_portfolio_var=self.config.max_portfolio_var,
                max_position_size=self.config.max_position_size,
                max_leverage=self.config.max_leverage
            ),
            initial_capital=self.config.initial_capital
        )

        self.logger.info("layer5_initialized")

    async def start(self) -> None:
        """Start the trading system"""
        self.logger.info("starting_trading_system")
        self.start_time = time.time()

        try:
            # Initialize all components
            await self.initialize()

            # Start Prometheus metrics server
            if self.config.enable_metrics:
                self._start_metrics_server_in_thread(self.config.prometheus_port)
                self.logger.info("metrics_server_started", port=self.config.prometheus_port)

            # Start control API if enabled
            await self._start_control_plane()

            # Set running flag
            self.is_running = True

            # Start main trading loop
            await self.run_trading_loop()

        except Exception as e:
            self.logger.error("startup_failed", error=str(e), exc_info=True)
            await self.shutdown()
            raise

    async def run_trading_loop(self) -> None:
        """Main trading loop coordinating all layers"""
        self.logger.info("starting_main_trading_loop")

        while self.is_running and not self.shutdown_requested:
            if self.kill_switch_active:
                self.logger.debug("kill_switch_active_skip_cycle")
                await asyncio.sleep(self.config.cycle_interval_seconds)
                continue

            cycle_start = time.time()

            try:
                # Execute one complete trading cycle
                await self.trading_cycle()

                # Update metrics
                cycle_duration = time.time() - cycle_start
                CYCLE_COUNTER.inc()
                CYCLE_DURATION.observe(cycle_duration)
                self.last_cycle_time = cycle_start

                # Sleep until next cycle
                sleep_time = max(0, self.config.cycle_interval_seconds - cycle_duration)
                await asyncio.sleep(sleep_time)

            except Exception as e:
                self.logger.error("trading_cycle_error", error=str(e), exc_info=True)
                await asyncio.sleep(5.0)  # Error backoff

    async def trading_cycle(self) -> None:
        """Execute one complete trading cycle through all layers"""

        if self.kill_switch_active:
            self.logger.debug("kill_switch_active_trading_cycle_skip")
            return

        if self.risk_manager and getattr(self.risk_manager, "emergency_stop", False):
            self.logger.warning("risk_manager_emergency_stop_active")
            return

        # Layer 0: Get latest market data and features
        with LAYER_LATENCY.labels(layer='data_ingestion').time():
            features = await self._fetch_features()
            if not features:
                self.logger.debug("no_features_available")
                return

        # Layer 1: Generate alpha signals
        with LAYER_LATENCY.labels(layer='alpha_models').time():
            signals = await self._generate_alpha_signals(features)
            if not signals:
                self.logger.debug("no_signals_generated")
                return

        # Layer 2: Combine signals via ensemble
        with LAYER_LATENCY.labels(layer='ensemble').time():
            ensemble_decision = await self._combine_signals(signals)

        # Layer 3: Calculate position sizes
        with LAYER_LATENCY.labels(layer='position_sizing').time():
            target_positions = await self._calculate_positions(ensemble_decision)
            target_positions = self._apply_runtime_guards(target_positions)

        # Layer 5: Pre-trade risk check
        with LAYER_LATENCY.labels(layer='risk_management').time():
            risk_approved = await self._check_pre_trade_risk(target_positions)

        # Layer 4: Execute trades if approved
        if risk_approved:
            with LAYER_LATENCY.labels(layer='execution').time():
                await self._execute_trades(target_positions)
        else:
            self.logger.warning("pre_trade_risk_rejected", positions=target_positions)
            RISK_VIOLATIONS.labels(type='pre_trade').inc()

    async def _fetch_features(self) -> Optional[Dict[str, Any]]:
        """Fetch latest features from feature bus or data adapter"""
        try:
            features = await self.feature_bus.get_latest_features()

            # If FeatureBus is empty but we have a data adapter, use adapter features
            # This enables bridge-mode where connectors publish directly to adapter
            if not features and self.data_adapter:
                self.logger.debug("feature_bus_empty_using_adapter_fallback")
                features = {}
                for symbol in self.config.crypto_symbols + self.config.stock_symbols:
                    try:
                        adapter_features = await self.data_adapter.get_latest_features(symbol)
                        if adapter_features and 'features' in adapter_features:
                            # Merge adapter features into unified feature dict
                            features[symbol] = adapter_features['features']
                    except Exception as e:
                        self.logger.debug("failed_to_fetch_adapter_features", symbol=symbol, error=str(e))

                # If no features from adapter either, return None
                if not features:
                    features = None

            # Always extract market prices from data adapter if available
            if self.data_adapter:
                # Extract market prices for risk manager and position sizing
                market_prices = {}
                market_timestamps: Dict[str, datetime] = {}
                now_utc = datetime.now(timezone.utc)
                for symbol in self.config.crypto_symbols + self.config.stock_symbols:
                    try:
                        adapter_features = await self.data_adapter.get_latest_features(symbol)
                        if adapter_features and 'features' in adapter_features:
                            price = adapter_features['features'].get('price')
                            if price:
                                market_prices[symbol] = float(price)
                                market_timestamps[symbol] = now_utc

                                # Update position sizing with market data
                                if hasattr(self, 'position_sizing') and hasattr(self.position_sizing, 'update_market_data'):
                                    volume = adapter_features['features'].get('volume', 0)
                                    timestamp = adapter_features.get('timestamp')
                                    self.position_sizing.update_market_data(
                                        symbol=symbol,
                                        price=price,
                                        volume=volume,
                                        timestamp=str(timestamp) if timestamp else None
                                    )
                    except Exception as e:
                        self.logger.debug("failed_to_fetch_adapter_features", symbol=symbol, error=str(e))

                # Cache market prices for risk manager
                self._cached_market_prices = market_prices
                self._cached_market_timestamps = market_timestamps
                self._last_market_data_update_at = now_utc if market_prices else self._last_market_data_update_at

            return features
        except Exception as e:
            self.logger.error("feature_fetch_failed", error=str(e))
            return None

    async def _generate_alpha_signals(self, features: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate signals from all alpha models"""
        signals = []

        for model_name, model in self.alpha_models.items():
            try:
                signal = model.generate_signal(features)
                if signal:
                    signals.append({
                        "model": model_name,
                        "signal": signal,
                        "timestamp": time.time()
                    })
                    SIGNAL_GENERATED.labels(
                        symbol=signal.get("symbol", "unknown"),
                        model=model_name
                    ).inc()
            except Exception as e:
                self.logger.error("alpha_signal_error", model=model_name, error=str(e))

        return signals

    async def _combine_signals(self, signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Combine signals using ensemble method"""
        try:
            return await self.ensemble.combine(signals)
        except Exception as e:
            self.logger.error("ensemble_combination_failed", error=str(e))
            return {}

    async def _calculate_positions(self, ensemble_decision: Dict[str, Any]) -> Dict[str, float]:
        """Calculate target positions based on ensemble decision"""
        try:
            return await self.position_sizing.calculate(
                decision=ensemble_decision,
                current_positions=self.current_positions
            )
        except Exception as e:
            self.logger.error("position_calculation_failed", error=str(e))
            return {}

    async def _check_pre_trade_risk(self, target_positions: Dict[str, float]) -> bool:
        """Check pre-trade risk constraints"""
        try:
            # Get cached market prices if available
            market_prices = getattr(self, '_cached_market_prices', None)

            result = await self.risk_manager.check_pre_trade(
                target_positions=target_positions,
                current_positions=self.current_positions,
                market_prices=market_prices
            )
            return result.approved
        except Exception as e:
            self.logger.error("risk_check_failed", error=str(e))
            return False

    def _env_float(self, key: str, default: float) -> float:
        return _safe_float(os.getenv(key, default), default)

    def _env_int(self, key: str, default: int) -> int:
        return int(_safe_float(os.getenv(key, default), float(default)))

    def _env_bool(self, key: str, default: bool) -> bool:
        raw = os.getenv(key)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _crypto_max_mae_pct(self) -> float:
        return float(
            getattr(
                self.config,
                "crypto_max_adverse_excursion_pct",
                self._env_float("TRADING_CRYPTO_MAX_MAE_PCT", 0.04),
            )
        )

    def _crypto_max_hold_minutes(self) -> int:
        return int(
            getattr(
                self.config,
                "crypto_max_hold_minutes",
                self._env_int("TRADING_CRYPTO_MAX_HOLD_MINUTES", 360),
            )
        )

    def _crypto_daily_loss_cap_usd(self) -> float:
        return float(
            getattr(
                self.config,
                "crypto_daily_symbol_loss_cap_usd",
                self._env_float("TRADING_CRYPTO_DAILY_SYMBOL_LOSS_CAP_USD", 200.0),
            )
        )

    def _crypto_reentry_cooldown_minutes(self) -> int:
        return int(
            getattr(
                self.config,
                "crypto_reentry_cooldown_minutes",
                self._env_int("TRADING_CRYPTO_REENTRY_COOLDOWN_MINUTES", 90),
            )
        )

    def _equities_allow_overnight(self) -> bool:
        return bool(
            getattr(
                self.config,
                "equities_allow_overnight",
                self._env_bool("TRADING_EQUITIES_ALLOW_OVERNIGHT", False),
            )
        )

    def _equities_flatten_before_close_minutes(self) -> int:
        return int(
            getattr(
                self.config,
                "equities_flatten_before_close_minutes",
                self._env_int("TRADING_EQUITIES_FLATTEN_BEFORE_CLOSE_MINUTES", 5),
            )
        )

    def _stale_data_max_age_seconds(self) -> float:
        return self._env_float("TRADING_STALE_DATA_MAX_AGE_SECONDS", 300.0)

    def _is_crypto_symbol(self, symbol: str) -> bool:
        return symbol in set(self._get_crypto_symbols())

    def _is_stock_symbol(self, symbol: str) -> bool:
        return symbol in set(self._get_stock_symbols())

    @staticmethod
    def _et_now(now_utc: datetime) -> datetime:
        from zoneinfo import ZoneInfo

        return now_utc.astimezone(ZoneInfo("America/New_York"))

    def _is_regular_stock_session(self, now_utc: datetime) -> bool:
        now_et = self._et_now(now_utc)
        if now_et.weekday() >= 5:
            return False
        open_time = dt_time(9, 30)
        close_time = dt_time(16, 0)
        return open_time <= now_et.time() < close_time

    def _is_equity_flatten_window(self, now_utc: datetime) -> bool:
        if self._equities_allow_overnight():
            return False

        now_et = self._et_now(now_utc)
        if now_et.weekday() >= 5:
            return True

        flatten_minutes = max(self._equities_flatten_before_close_minutes(), 0)
        close_dt = datetime.combine(now_et.date(), dt_time(16, 0), tzinfo=now_et.tzinfo)
        flatten_start = close_dt - timedelta(minutes=flatten_minutes)
        return now_et >= flatten_start

    def _record_block_reason(self, symbol: str, reason: str) -> None:
        self.symbol_blocked_signals[symbol] += 1
        self.symbol_block_reasons[symbol][reason] += 1

    def _reset_daily_loss_lock_if_needed(self, symbol: str, now_utc: datetime) -> None:
        et_date = self._et_now(now_utc).date()
        if self.symbol_daily_lock_date.get(symbol) == et_date:
            return
        self.symbol_daily_lock_date[symbol] = et_date
        self.symbol_daily_loss_locked[symbol] = False

    def _estimate_symbol_unrealized_pnl(self, symbol: str) -> float:
        qty = _safe_float(self.current_positions.get(symbol, 0.0))
        if abs(qty) <= 1e-12:
            return 0.0
        entry_price = _safe_float(self.position_entry_price.get(symbol), 0.0)
        market_price = _safe_float(self._cached_market_prices.get(symbol), entry_price)
        if entry_price <= 0.0 or market_price <= 0.0:
            return 0.0
        direction = 1.0 if qty >= 0 else -1.0
        return abs(qty) * (market_price - entry_price) * direction

    def _apply_runtime_guards(self, target_positions: Dict[str, float]) -> Dict[str, float]:
        """Apply runtime guardrails for crypto lifecycle and equity session policy."""

        now_utc = datetime.now(timezone.utc)
        adjusted: Dict[str, float] = dict(target_positions)

        max_mae_pct = max(self._crypto_max_mae_pct(), 0.0)
        max_hold_minutes = max(self._crypto_max_hold_minutes(), 1)
        daily_loss_cap = max(self._crypto_daily_loss_cap_usd(), 0.0)
        cooldown_minutes = max(self._crypto_reentry_cooldown_minutes(), 1)
        cooldown_delta = timedelta(minutes=cooldown_minutes)

        symbols = set(adjusted.keys()) | set(self.current_positions.keys()) | set(self._get_all_symbols())
        for symbol in symbols:
            self._reset_daily_loss_lock_if_needed(symbol, now_utc)

            current_qty = _safe_float(self.current_positions.get(symbol, 0.0))
            requested_target = _safe_float(adjusted.get(symbol, current_qty))
            target_qty = requested_target
            applied_reasons: list[str] = []

            if abs(requested_target) > abs(current_qty) + 1e-12:
                self.symbol_entry_signals[symbol] += 1

            if self._is_crypto_symbol(symbol) and daily_loss_cap > 0.0:
                if self.symbol_realized_pnl.get(symbol, 0.0) <= -daily_loss_cap:
                    self.symbol_daily_loss_locked[symbol] = True
                if self.symbol_daily_loss_locked.get(symbol, False):
                    if abs(target_qty) > 1e-12:
                        target_qty = 0.0
                        applied_reasons.append("daily_symbol_loss_cap")

            if self._is_crypto_symbol(symbol) and abs(current_qty) > 1e-12:
                entry_price = _safe_float(self.position_entry_price.get(symbol), 0.0)
                current_price = _safe_float(self._cached_market_prices.get(symbol), entry_price)
                if entry_price > 0 and current_price > 0:
                    direction = 1.0 if current_qty >= 0 else -1.0
                    pnl_pct = ((current_price - entry_price) / entry_price) * direction
                    if pnl_pct <= -max_mae_pct:
                        target_qty = 0.0
                        self.symbol_cooldown_until[symbol] = now_utc + cooldown_delta
                        applied_reasons.append("max_adverse_excursion_stop")

                opened_at = self.position_entry_time.get(symbol)
                if isinstance(opened_at, datetime):
                    hold_minutes = max((now_utc - opened_at).total_seconds() / 60.0, 0.0)
                    if hold_minutes >= float(max_hold_minutes):
                        target_qty = 0.0
                        self.symbol_cooldown_until[symbol] = now_utc + cooldown_delta
                        applied_reasons.append("max_hold_time_stop")

            cooldown_until = self.symbol_cooldown_until.get(symbol)
            if (
                self._is_crypto_symbol(symbol)
                and isinstance(cooldown_until, datetime)
                and now_utc < cooldown_until
                and abs(current_qty) <= 1e-12
                and abs(target_qty) > 1e-12
            ):
                target_qty = 0.0
                applied_reasons.append("cooldown_active")

            if self._is_stock_symbol(symbol) and self._is_equity_flatten_window(now_utc):
                if abs(current_qty) > 1e-12 or abs(target_qty) > 1e-12:
                    target_qty = 0.0
                    applied_reasons.append("equity_session_flatten")

            if applied_reasons:
                # Deduplicate reasons while preserving ordering.
                deduped = list(dict.fromkeys(applied_reasons))
                for reason in deduped:
                    self._record_block_reason(symbol, reason)
                self.logger.info(
                    "runtime_guard_applied",
                    symbol=symbol,
                    current_position=current_qty,
                    requested_target=requested_target,
                    adjusted_target=target_qty,
                    reasons=deduped,
                )

            adjusted[symbol] = target_qty

        return adjusted

    async def _execute_trades(self, target_positions: Dict[str, float]) -> None:
        """Execute trades to reach target positions using the Smart Order Router."""
        self.logger.info("executing_trades_with_sor", target_positions=target_positions)
        import uuid
        from decimal import Decimal
        from trading_bot.execution.smart_order_router import OrderUrgency

        for symbol, target_size in target_positions.items():
            current_size = self.current_positions.get(symbol, 0.0)
            delta = target_size - current_size

            if abs(delta) < 0.001:  # Ignore tiny differences
                continue

            side = "buy" if delta > 0 else "sell"
            quantity = Decimal(str(abs(delta)))

            try:
                # 1. Route the order to get an execution plan
                plan = await self.router.route_order(
                    symbol=symbol,
                    side=side,
                    size=quantity,
                    urgency=OrderUrgency.MODERATE, # or determine urgency based on context
                )

                if not plan.fragments:
                    self.logger.warning("sor_no_fragments", symbol=symbol, size=quantity)
                    continue

                # 2. Execute the plan
                execution_results = await self.router.execute_plan(plan)

                # 3. Update state based on execution results
                filled_quantity = execution_results.get("total_filled", Decimal("0"))
                if filled_quantity > 0:
                    filled_qty = float(filled_quantity)
                    fill_price = _safe_float(execution_results.get("avg_fill_price"), 0.0)
                    fee_paid = _safe_float(execution_results.get("total_fees"), 0.0)
                    if fee_paid < 0:
                        fee_paid = 0.0
                    self.total_fees_usd += fee_paid
                    self.symbol_fees_usd[symbol] += fee_paid

                    # Update local state
                    prev_qty = _safe_float(self.current_positions.get(symbol, 0.0))
                    prev_entry = _safe_float(self.position_entry_price.get(symbol), fill_price)
                    if side == "buy":
                        next_qty = prev_qty + filled_qty
                    else:
                        next_qty = prev_qty - filled_qty
                    self.current_positions[symbol] = next_qty

                    realized_delta = 0.0
                    closed_qty = 0.0
                    if abs(prev_qty) > 1e-12 and (prev_qty * next_qty <= 0 or abs(next_qty) < abs(prev_qty)):
                        closed_qty = min(abs(prev_qty), filled_qty)
                        direction = 1.0 if prev_qty >= 0 else -1.0
                        if fill_price > 0 and prev_entry > 0:
                            realized_delta = closed_qty * (fill_price - prev_entry) * direction

                    if realized_delta:
                        self.realized_pnl += realized_delta
                        self.symbol_realized_pnl[symbol] += realized_delta

                    if fee_paid:
                        self.realized_pnl -= fee_paid
                        self.symbol_realized_pnl[symbol] -= fee_paid

                    if abs(next_qty) <= 1e-12:
                        self.current_positions[symbol] = 0.0
                        self.position_entry_price.pop(symbol, None)
                        self.position_entry_time.pop(symbol, None)
                    else:
                        same_direction = prev_qty == 0 or (prev_qty > 0) == (next_qty > 0)
                        increasing = abs(next_qty) > abs(prev_qty)
                        if fill_price > 0 and same_direction and increasing:
                            base_qty = abs(prev_qty)
                            added_qty = max(abs(next_qty) - base_qty, 0.0)
                            denom = base_qty + added_qty
                            if denom > 0:
                                weighted = ((prev_entry * base_qty) + (fill_price * added_qty)) / denom
                                self.position_entry_price[symbol] = weighted
                            else:
                                self.position_entry_price[symbol] = fill_price
                            self.position_entry_time[symbol] = datetime.now(timezone.utc)
                        elif fill_price > 0 and abs(next_qty) > 1e-12 and not same_direction:
                            # Position flipped; reset cost basis on the new side.
                            self.position_entry_price[symbol] = fill_price
                            self.position_entry_time[symbol] = datetime.now(timezone.utc)

                    self.symbol_trade_count[symbol] += 1
                    self.symbol_fill_count[symbol] += 1
                    if self._is_stock_symbol(symbol) and self._is_regular_stock_session(datetime.now(timezone.utc)):
                        self.stock_market_hours_fill_count += 1

                    ORDERS_PLACED.labels(symbol=symbol, side=side).inc()
                    self.logger.info(
                        "sor_trade_executed",
                        symbol=symbol,
                        side=side,
                        filled_quantity=filled_qty,
                        fill_price=fill_price,
                        realized_delta=realized_delta,
                        fee_paid=fee_paid,
                    )

                    if self.ledger:
                        order_id = plan.original_order_id
                        await self.ledger.create_order(
                            order_id=order_id,
                            client_order_id=order_id,
                            symbol=symbol,
                            side=side,
                            order_type="market", # The plan can have different order types
                            quantity=quantity,
                            metadata={"strategy": self.config.strategy, "sor_plan": plan.original_order_id}
                        )
                        await self.ledger.update_order_status(
                            order_id=order_id,
                            status="filled" if execution_results["success_rate"] > 0.9 else "partially_filled",
                            filled_quantity=filled_quantity,
                            average_fill_price=execution_results.get("avg_fill_price", Decimal("0"))
                        )

                    await self._create_portfolio_snapshot()

            except Exception as e:
                self.logger.error("sor_execution_failed", symbol=symbol, error=str(e))

    async def _create_portfolio_snapshot(self) -> None:
        """Create a portfolio snapshot for compliance tracking"""
        if not self.ledger:
            return

        try:
            from decimal import Decimal

            unrealized_pnl = 0.0
            positions_value = 0.0
            for symbol, qty in self.current_positions.items():
                parsed_qty = _safe_float(qty, 0.0)
                if abs(parsed_qty) <= 1e-12:
                    continue
                market_price = _safe_float(self._cached_market_prices.get(symbol), 0.0)
                if market_price > 0:
                    positions_value += parsed_qty * market_price
                unrealized_pnl += self._estimate_symbol_unrealized_pnl(symbol)

            # Consistent accounting: total value = starting capital + realized + unrealized.
            total_value = self.config.initial_capital + self.realized_pnl + unrealized_pnl
            cash_balance = total_value - positions_value

            # Update Prometheus metrics
            PORTFOLIO_VALUE.set(total_value)
            PNL_GAUGE.set(self.realized_pnl + unrealized_pnl)

            # Update local tracking
            self.portfolio_value = total_value

            # Create snapshot in ledger
            await self.ledger.create_portfolio_snapshot(
                total_value=Decimal(str(total_value)),
                cash_balance=Decimal(str(cash_balance)),
                positions_value=Decimal(str(positions_value)),
                unrealized_pnl=Decimal(str(unrealized_pnl)),
                realized_pnl=Decimal(str(self.realized_pnl)),
                positions={k: v for k, v in self.current_positions.items()},
                metadata={
                    "mode": self.config.mode,
                    "strategy": self.config.strategy,
                    "cycle_time": self.last_cycle_time,
                    "total_fees_usd": self.total_fees_usd,
                }
            )

            self.logger.debug(
                "portfolio_snapshot_created",
                total_value=total_value,
                realized_pnl=self.realized_pnl,
                unrealized_pnl=unrealized_pnl,
                positions_count=len(self.current_positions)
            )

        except Exception as e:
            self.logger.error("snapshot_creation_failed", error=str(e))

    async def shutdown(self) -> None:
        """Gracefully shutdown the trading system"""
        self.logger.info("initiating_shutdown")
        self.shutdown_requested = True
        self.is_running = False

        await self._stop_control_plane()

        # Create final portfolio snapshot
        if self.ledger:
            await self._create_portfolio_snapshot()

        # Shutdown layers in reverse order
        if self.risk_manager:
            await self._safe_shutdown(self.risk_manager, "risk_manager")

        if hasattr(self, 'router'):
            for executor in self.router.executors.values():
                await self._safe_shutdown(executor, "executor")
        else:
            for executor in set(self.executors.values()):
                await self._safe_shutdown(executor, "executor")

        if self.position_sizing:
            await self._safe_shutdown(self.position_sizing, "position_sizing")

        if self.ensemble:
            await self._safe_shutdown(self.ensemble, "ensemble")

        for model in self.alpha_models.values():
            await self._safe_shutdown(model, "alpha_model")

        if self.feature_bus:
            await self._safe_shutdown(self.feature_bus, "feature_bus")

        if self.data_ingestion:
            await self._safe_shutdown(self.data_ingestion, "data_ingestion")

        # Shutdown adapter bridge
        if self.adapter_bridge:
            await self._safe_shutdown(self.adapter_bridge, "adapter_bridge")

        # Shutdown data adapter
        if self.data_adapter:
            await self._safe_shutdown(self.data_adapter, "data_adapter")

        # Shutdown ledger last
        if self.ledger:
            await self._safe_shutdown(self.ledger, "ledger")

        # Update metrics
        for layer in ['database_ledger', 'data_ingestion', 'alpha_models', 'ensemble', 'position_sizing', 'execution', 'risk_management']:
            SYSTEM_STATUS.labels(layer=layer).set(0)

        self.logger.info("shutdown_complete")

    async def _safe_shutdown(self, component: Any, name: str) -> None:
        """Safely shutdown a component"""
        try:
            if hasattr(component, 'shutdown'):
                if asyncio.iscoroutinefunction(component.shutdown):
                    await component.shutdown()
                else:
                    component.shutdown()
            elif hasattr(component, 'stop'):
                if asyncio.iscoroutinefunction(component.stop):
                    await component.stop()
                else:
                    component.stop()
            self.logger.info("component_shutdown", component=name)
        except Exception as e:
            self.logger.error("component_shutdown_failed", component=name, error=str(e))

    def _get_crypto_symbols(self) -> List[str]:
        """Get crypto symbols based on strategy"""
        if self.config.strategy in [StrategyType.CRYPTO_MOMENTUM, StrategyType.CRYPTO_SCALPING, StrategyType.MULTI_ASSET]:
            return self.config.crypto_symbols
        return []

    def _get_stock_symbols(self) -> List[str]:
        """Get stock symbols based on strategy"""
        if self.config.strategy in [StrategyType.STOCKS_INTRADAY, StrategyType.STOCKS_SWING, StrategyType.MULTI_ASSET]:
            return self.config.stock_symbols
        return []

    def _start_metrics_server_in_thread(self, port: int) -> None:
        """Starts the Prometheus metrics HTTP server in a separate thread."""
        def start_server():
            start_http_server(port)
        
        metrics_thread = threading.Thread(target=start_server, daemon=True)
        metrics_thread.start()
        self.logger.info("prometheus_exporter_thread_started", port=port)

    def _get_all_symbols(self) -> List[str]:
        """Get all symbols"""
        return self._get_crypto_symbols() + self._get_stock_symbols()


def setup_signal_handlers(orchestrator: TradingSystemOrchestrator) -> None:
    """Setup signal handlers for graceful shutdown"""

    def signal_handler(signum, frame):
        logger.info("signal_received", signal=signum)
        asyncio.create_task(orchestrator.shutdown())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def _build_arg_parser() -> "argparse.ArgumentParser":
    import argparse

    parser = argparse.ArgumentParser(description="Canonical Trading System Orchestrator")
    parser.add_argument("--mode", type=str, default="paper", choices=["paper", "live", "backtest"], help="Execution mode for the trading stack")
    parser.add_argument("--strategy", type=str, default="crypto_momentum", help="Strategy identifier to load")
    parser.add_argument("--live", action="store_true", help="Enable live trading (requires confirmation)")
    parser.add_argument("--config", type=str, help="Path to configuration file")

    # Control-plane configuration
    parser.add_argument("--control-host", type=str, default="0.0.0.0", help="Host interface for the control API (when running the orchestrator)")
    parser.add_argument("--control-port", type=int, default=9001, help="Port for the control API")
    parser.add_argument("--enable-control-api", action="store_true", help="Enable the control API server")
    parser.add_argument("--no-control-api", action="store_true", help="Disable the control API server")
    parser.add_argument("--enable-metrics", action="store_true", help="Enable Prometheus metrics server")

    # CLI control commands
    parser.add_argument("--target-host", type=str, default=None, help="Target host when issuing control commands (defaults to 127.0.0.1)")
    parser.add_argument("--target-port", type=int, default=None, help="Target port when issuing control commands (defaults to --control-port)")
    parser.add_argument("--kill-switch", action="store_true", help="Activate the kill switch via the control API and exit")
    parser.add_argument("--kill-reason", type=str, default="Manual CLI activation", help="Reason recorded when activating the kill switch")
    parser.add_argument("--reset-kill-switch", action="store_true", help="Reset the kill switch via the control API and exit")
    parser.add_argument("--authorized-by", type=str, default="cli", help="Identifier recorded when resetting the kill switch")
    parser.add_argument("--status", action="store_true", help="Query the control API health endpoint and exit")

    return parser


async def _run_control_command(args) -> None:
    import json

    import httpx

    target_host = args.target_host or "127.0.0.1"
    target_port = args.target_port or args.control_port
    base_url = f"http://{target_host}:{target_port}"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            if args.kill_switch:
                response = await client.post(
                    f"{base_url}/emergency/kill_switch",
                    json={"reason": args.kill_reason, "trigger": "cli"},
                )
            elif args.reset_kill_switch:
                response = await client.post(
                    f"{base_url}/emergency/reset",
                    json={"authorized_by": args.authorized_by},
                )
            else:  # status
                response = await client.get(f"{base_url}/health")

        payload = response.json()
        print(json.dumps(payload, indent=2))

        if response.status_code >= 400:
            raise SystemExit(1)

    except httpx.RequestError as exc:
        print(f"❌ Failed to reach control API at {base_url}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


async def _run_orchestrator(args) -> None:
    # Load environment
    load_env_file()

    requested_live = args.live or args.mode == "live"
    if requested_live:
        try:
            runtime = configure_runtime(requested_live)
            await enforce_startup_validations(runtime.exec_mode)
        except LiveTradingNotConfirmedError as exc:
            logger.error("live_trading_not_confirmed", error=str(exc))
            raise SystemExit(1) from exc

    # Build orchestrator configuration
    config = OrchestratorConfig(
        mode=ExecutionMode(args.mode),
        strategy=StrategyType(args.strategy),
        control_api_host=args.control_host,
        control_api_port=args.control_port,
        enable_control_api=not args.no_control_api,
    )

    orchestrator = TradingSystemOrchestrator(config)
    setup_signal_handlers(orchestrator)

    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
    except Exception as exc:  # noqa: BLE001
        logger.error("fatal_error", error=str(exc), exc_info=True)
        raise SystemExit(1) from exc
    finally:
        await orchestrator.shutdown()


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    command_flags = [args.kill_switch, args.reset_kill_switch, args.status]
    if sum(bool(flag) for flag in command_flags) > 1:
        parser.error("Specify at most one of --kill-switch, --reset-kill-switch, or --status")

    if any(command_flags):
        asyncio.run(_run_control_command(args))
        return

    asyncio.run(_run_orchestrator(args))


async def async_main() -> None:
    """Backward-compatible async entry point."""

    parser = _build_arg_parser()
    args = parser.parse_args()
    await _run_orchestrator(args)


if __name__ == "__main__":
    main()
