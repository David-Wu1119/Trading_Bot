"""Unit tests for the trading desk frontend control panel."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from trading_bot.frontend.app import DashboardConfig, call_control_api, create_app

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def frontend_config(tmp_path):
    return DashboardConfig(
        data_dir=tmp_path / "frontend-data",
        scratchpad_dir=tmp_path / "scratchpad",
        control_api_base_url="http://control.invalid",
        audit_dir=tmp_path / "audit",
        econ_dir=tmp_path / "econ",
        ramp_dir=tmp_path / "ramp",
        metrics_url=None,
    )


@pytest.fixture
def frontend_client(frontend_config):
    app = create_app(frontend_config)
    with TestClient(app) as client:
        yield client


def test_health_and_index(frontend_client):
    health = frontend_client.get("/health")
    assert health.status_code == 200
    payload = health.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "trading-desk-frontend"

    index = frontend_client.get("/")
    assert index.status_code == 200
    assert "Trading Desk Control" in index.text


def test_mission_lifecycle_and_scratchpad(frontend_client):
    created = frontend_client.post(
        "/api/missions",
        json={
            "title": "Scale BTC momentum checks",
            "symbol": "BTC-USD",
            "priority": "high",
            "objective": "Increase signal confidence before market open.",
        },
    )
    assert created.status_code == 200
    mission = created.json()["mission"]
    assert mission["status"] == "planned"

    updated = frontend_client.patch(
        f"/api/missions/{mission['id']}",
        json={"status": "in_progress", "notes": "Running preflight now"},
    )
    assert updated.status_code == 200
    assert updated.json()["mission"]["status"] == "in_progress"

    listed = frontend_client.get("/api/missions")
    assert listed.status_code == 200
    missions = listed.json()["missions"]
    assert len(missions) == 1
    assert missions[0]["id"] == mission["id"]

    scratchpad = frontend_client.get("/api/scratchpad?limit=50")
    assert scratchpad.status_code == 200
    entries = scratchpad.json()["entries"]
    event_types = {entry["type"] for entry in entries}
    assert "mission_created" in event_types
    assert "mission_updated" in event_types


def test_update_unknown_mission_returns_404(frontend_client):
    response = frontend_client.patch(
        "/api/missions/mission-does-not-exist",
        json={"status": "done"},
    )
    assert response.status_code == 404
    assert "Mission not found" in response.json()["detail"]


def test_system_status_with_control_api_reachable(frontend_client, monkeypatch):
    async def fake_call(_config, method: str, endpoint: str, payload=None):
        assert method == "GET"
        assert endpoint == "/health"
        assert payload is None
        return {"kill_switch_active": False, "is_running": True}

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response = frontend_client.get("/api/system/status")
    assert response.status_code == 200
    body = response.json()
    assert body["control_api"]["reachable"] is True
    assert body["control_api"]["state"]["is_running"] is True


def test_control_actions_proxy_and_audit(frontend_client, monkeypatch):
    calls: list[tuple[str, str, dict[str, Any]]] = []

    async def fake_call(_config, method: str, endpoint: str, payload=None):
        calls.append((method, endpoint, payload or {}))
        if endpoint == "/emergency/kill_switch":
            return {"activated": True, "kill_switch_active": True}
        if endpoint == "/emergency/reset":
            return {"reset": True, "kill_switch_active": False}
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    kill = frontend_client.post("/api/control/kill", json={"reason": "test halt"})
    assert kill.status_code == 200
    assert kill.json()["result"]["activated"] is True

    reset = frontend_client.post("/api/control/reset", json={"authorized_by": "tester"})
    assert reset.status_code == 200
    assert reset.json()["result"]["reset"] is True

    assert calls == [
        (
            "POST",
            "/emergency/kill_switch",
            {"reason": "test halt", "trigger": "frontend"},
        ),
        (
            "POST",
            "/emergency/reset",
            {"authorized_by": "tester"},
        ),
    ]

    scratchpad = frontend_client.get("/api/scratchpad?limit=20")
    entries = scratchpad.json()["entries"]
    types = [entry["type"] for entry in entries]
    assert "kill_switch_activated" in types
    assert "kill_switch_reset" in types


def test_control_api_errors_are_passed_through(frontend_client, monkeypatch):
    async def fake_call(_config, _method: str, _endpoint: str, payload=None):
        raise HTTPException(status_code=502, detail="control plane unavailable")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response = frontend_client.post("/api/control/kill", json={"reason": "halt"})
    assert response.status_code == 502
    assert response.json()["detail"] == "control plane unavailable"


def test_notifications_status_and_test_when_disabled(frontend_client):
    status = frontend_client.get("/api/notifications/status")
    assert status.status_code == 200
    payload = status.json()
    assert payload["channels"]["enabled"] is False

    test_resp = frontend_client.post(
        "/api/notifications/test",
        json={"message": "test message"},
    )
    assert test_resp.status_code == 200
    result = test_resp.json()["result"]
    assert result["sent"] is False
    assert result["reason"] == "notifications_disabled"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_telemetry_overview_defaults_when_no_artifacts(frontend_client):
    response = frontend_client.get("/api/telemetry/overview")
    assert response.status_code == 200
    payload = response.json()

    assert payload["economics"] is None
    assert payload["ramp"] is None
    assert payload["exposure"]["summary"]["total"] == 0
    assert payload["metrics"]["configured"] is False
    assert payload["advisor"]["portfolio_posture"] in {
        "NEUTRAL",
        "DEFENSIVE",
        "OFFENSIVE",
        "RISK_OFF",
    }


def test_market_telemetry_prefers_control_api_state(frontend_client, monkeypatch):
    async def fake_call(_config, method: str, endpoint: str, payload=None):
        assert method == "GET"
        if endpoint == "/telemetry/market_state":
            return {
                "all_symbols_fresh": True,
                "stale_symbols": [],
                "last_market_data_update_at": "2026-02-19T00:00:00+00:00",
                "stale_data_max_age_seconds": 15.0,
                "prices": [
                    {
                        "symbol": "BTC-USD",
                        "price": 52000.12,
                        "timestamp": "2026-02-19T00:00:00+00:00",
                        "age_seconds": 0.8,
                        "stale": False,
                    }
                ],
                "portfolio_snapshot": {
                    "timestamp": "2026-02-19T00:00:00+00:00",
                    "total_value": 100100.0,
                    "portfolio_pnl_usd": 100.0,
                    "unrealized_pnl_usd": 40.0,
                    "realized_pnl_usd": 60.0,
                    "cash_balance_usd": 90000.0,
                    "positions_value_usd": 10100.0,
                    "total_fees_usd": 2.0,
                    "positions_count": 1,
                },
            }
        raise HTTPException(status_code=502, detail="unexpected endpoint")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response = frontend_client.get("/api/telemetry/market")
    assert response.status_code == 200
    payload = response.json()
    assert payload["price_count"] == 1
    assert payload["prices"][0]["symbol"] == "BTC-USD"
    assert payload["prices"][0]["source"] == "control_api_market_state"
    assert payload["all_symbols_fresh"] is True
    assert payload["live_pnl"]["source"] == "control_api_snapshot"
    assert payload["live_pnl"]["portfolio_pnl_usd"] == 100.0


def test_telemetry_advisor_uses_market_state(frontend_client, monkeypatch):
    async def fake_call(_config, method: str, endpoint: str, payload=None):
        assert method == "GET"
        if endpoint == "/telemetry/market_state":
            return {
                "all_symbols_fresh": True,
                "stale_symbols": [],
                "last_market_data_update_at": "2026-02-19T00:00:00+00:00",
                "stale_data_max_age_seconds": 15.0,
                "prices": [
                    {
                        "symbol": "ETH-USD",
                        "price": 2200.0,
                        "timestamp": "2026-02-19T00:00:00+00:00",
                        "age_seconds": 2.0,
                        "stale": False,
                    },
                    {
                        "symbol": "BTC-USD",
                        "price": 53000.0,
                        "timestamp": "2026-02-19T00:00:00+00:00",
                        "age_seconds": 1.5,
                        "stale": False,
                    },
                ],
                "portfolio_snapshot": {
                    "timestamp": "2026-02-19T00:00:00+00:00",
                    "total_value": 101000.0,
                    "portfolio_pnl_usd": 200.0,
                    "unrealized_pnl_usd": 80.0,
                    "realized_pnl_usd": 120.0,
                    "cash_balance_usd": 88000.0,
                    "positions_value_usd": 13000.0,
                    "total_fees_usd": 5.0,
                    "positions_count": 2,
                },
            }
        raise HTTPException(status_code=502, detail="unexpected endpoint")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response = frontend_client.get("/api/telemetry/advisor?symbol_limit=2")
    assert response.status_code == 200
    payload = response.json()
    assert payload["portfolio_action"] in {
        "SELECTIVE_ADD",
        "HOLD_AND_MONITOR",
        "REDUCE_EXPOSURE",
        "PAUSE_NEW_RISK",
    }
    assert len(payload["recommendations"]) == 2
    assert payload["recommendations"][0]["symbol"] in {"ETH-USD", "BTC-USD"}
    assert "acceptance_gates" in payload
    assert isinstance(payload["acceptance_gates"]["overall_pass"], bool)
    assert "quality" in payload
    assert payload["quality"]["evaluated"] >= 0
    assert "provenance" in payload["recommendations"][0]

    history_response = frontend_client.get("/api/telemetry/advisor_history?limit=10")
    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert history_payload["count"] >= 1
    assert history_payload["latest"]["portfolio_posture"] in {
        "OFFENSIVE",
        "NEUTRAL",
        "DEFENSIVE",
        "RISK_OFF",
    }


def test_telemetry_advisor_quality_endpoint(frontend_client, monkeypatch):
    async def fake_call(_config, method: str, endpoint: str, payload=None):
        assert method == "GET"
        if endpoint == "/telemetry/market_state":
            return {
                "all_symbols_fresh": True,
                "stale_symbols": [],
                "last_market_data_update_at": "2026-02-19T00:00:00+00:00",
                "stale_data_max_age_seconds": 15.0,
                "prices": [
                    {
                        "symbol": "ETH-USD",
                        "price": 2200.0,
                        "timestamp": "2026-02-19T00:00:00+00:00",
                        "age_seconds": 2.0,
                        "stale": False,
                    }
                ],
                "portfolio_snapshot": {
                    "timestamp": "2026-02-19T00:00:00+00:00",
                    "total_value": 101000.0,
                    "portfolio_pnl_usd": 200.0,
                    "unrealized_pnl_usd": 80.0,
                    "realized_pnl_usd": 120.0,
                    "cash_balance_usd": 88000.0,
                    "positions_value_usd": 13000.0,
                    "total_fees_usd": 5.0,
                    "positions_count": 2,
                },
            }
        raise HTTPException(status_code=502, detail="unexpected endpoint")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response = frontend_client.get("/api/telemetry/advisor_quality?symbol_limit=2")
    assert response.status_code == 200
    payload = response.json()
    assert payload["portfolio_posture"] in {"OFFENSIVE", "NEUTRAL", "DEFENSIVE", "RISK_OFF"}
    assert "quality" in payload
    assert payload["quality"]["methodology"].startswith("Directional alignment")


def test_telemetry_advisor_locks_recommendations_when_gates_fail(frontend_client, monkeypatch):
    async def fake_call(_config, method: str, endpoint: str, payload=None):
        assert method == "GET"
        if endpoint == "/telemetry/market_state":
            return {
                "all_symbols_fresh": False,
                "stale_symbols": ["ETH-USD"],
                "last_market_data_update_at": "2026-02-19T00:00:00+00:00",
                "stale_data_max_age_seconds": 15.0,
                "prices": [
                    {
                        "symbol": "ETH-USD",
                        "price": 2200.0,
                        "timestamp": "2026-02-19T00:00:00+00:00",
                        "age_seconds": 140.0,
                        "stale": True,
                    }
                ],
                "portfolio_snapshot": {
                    "timestamp": "2026-02-19T00:00:00+00:00",
                    "total_value": 98000.0,
                    "portfolio_pnl_usd": -1800.0,
                    "unrealized_pnl_usd": -1200.0,
                    "realized_pnl_usd": -600.0,
                    "cash_balance_usd": 88000.0,
                    "positions_value_usd": 10000.0,
                    "total_fees_usd": 8.0,
                    "positions_count": 1,
                },
            }
        raise HTTPException(status_code=502, detail="unexpected endpoint")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response = frontend_client.get("/api/telemetry/advisor?symbol_limit=1")
    assert response.status_code == 200
    payload = response.json()
    assert payload["decision_locked"] is True
    assert payload["locked_recommendation_count"] >= 1
    assert payload["recommendations"][0]["recommendation"] == "HOLD_LOCKED"
    assert payload["recommendations"][0]["raw_recommendation"] in {"BUY_BIAS", "HOLD", "REDUCE", "AVOID"}

    scratchpad = frontend_client.get("/api/scratchpad?limit=50")
    assert scratchpad.status_code == 200
    entries = scratchpad.json()["entries"]
    event_types = {entry["type"] for entry in entries}
    assert "advisor_decision_locked" in event_types


def test_telemetry_advisor_history_throttled(frontend_client, monkeypatch):
    async def fake_call(_config, method: str, endpoint: str, payload=None):
        assert method == "GET"
        if endpoint == "/telemetry/market_state":
            return {
                "all_symbols_fresh": True,
                "stale_symbols": [],
                "last_market_data_update_at": "2026-02-19T00:00:00+00:00",
                "stale_data_max_age_seconds": 15.0,
                "prices": [
                    {
                        "symbol": "ETH-USD",
                        "price": 2200.0,
                        "timestamp": "2026-02-19T00:00:00+00:00",
                        "age_seconds": 2.0,
                        "stale": False,
                    }
                ],
                "portfolio_snapshot": {
                    "timestamp": "2026-02-19T00:00:00+00:00",
                    "total_value": 101000.0,
                    "portfolio_pnl_usd": 200.0,
                    "unrealized_pnl_usd": 80.0,
                    "realized_pnl_usd": 120.0,
                    "cash_balance_usd": 88000.0,
                    "positions_value_usd": 13000.0,
                    "total_fees_usd": 5.0,
                    "positions_count": 2,
                },
            }
        raise HTTPException(status_code=502, detail="unexpected endpoint")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    response_1 = frontend_client.get("/api/telemetry/advisor")
    response_2 = frontend_client.get("/api/telemetry/advisor")
    assert response_1.status_code == 200
    assert response_2.status_code == 200

    history_response = frontend_client.get("/api/telemetry/advisor_history?limit=10")
    assert history_response.status_code == 200
    history_payload = history_response.json()
    # Writes are throttled by advisor_history_min_interval_seconds.
    assert history_payload["count"] == 1


def test_telemetry_overview_reads_local_artifacts(frontend_client, frontend_config):
    _write_json(
        frontend_config.audit_dir / "2026-02-12T13_00_00.000000+00_00_exposure_check.json",
        {
            "timestamp": "2026-02-12T13:00:00+00:00",
            "allowed": False,
            "reason": "risk blocked",
            "operator": "exposure_limiter",
            "order_request": {
                "symbol": "BTC-USD",
                "side": "buy",
                "venue": "coinbase",
                "notional_usd": 1500,
                "influence_pct": 5.0,
            },
        },
    )
    _write_json(
        frontend_config.audit_dir / "2026-02-12T13_05_00.000000+00_00_exposure_check.json",
        {
            "timestamp": "2026-02-12T13:05:00+00:00",
            "allowed": True,
            "reason": "",
            "operator": "exposure_limiter",
            "order_request": {
                "symbol": "ETH-USD",
                "side": "sell",
                "venue": "coinbase",
                "notional_usd": 2000,
                "influence_pct": 4.0,
            },
        },
    )
    _write_json(
        frontend_config.econ_dir / "20260212_130500Z" / "econ_close.json",
        {
            "timestamp": "2026-02-12T13:05:00+00:00",
            "date": "2026-02-12",
            "portfolio": {
                "gross_pnl_usd": 320.0,
                "net_pnl_final_usd": 240.0,
                "total_fees_usd": 40.0,
                "total_infra_cost_usd": 40.0,
                "cost_ratio": 0.25,
                "net_margin_pct": 12.3,
            },
            "assets": {
                "BTC-USD": {
                    "net_pnl_usd": 180.0,
                    "gross_pnl_usd": 210.0,
                    "fill_count": 3,
                    "slippage_bps_p95": 24,
                },
                "ETH-USD": {
                    "net_pnl_usd": 60.0,
                    "gross_pnl_usd": 110.0,
                    "fill_count": 2,
                    "slippage_bps_p95": 28,
                },
            },
        },
    )
    _write_json(
        frontend_config.ramp_dir / "20260212_130510Z" / "decision.json",
        {
            "timestamp": "2026-02-12T13:05:10+00:00",
            "decision": "NO_RAMP",
            "reasons": ["Cost ratio above cap"],
            "checks": {
                "kri_status": {
                    "metrics": {
                        "entropy": 1.11,
                        "qspread_ratio": 1.5,
                        "heartbeat_age": 30,
                        "daily_drawdown_pct": 0.8,
                    }
                },
                "cost_gates": {
                    "cost_ratio": 0.58,
                    "cost_ratio_cap": 0.30,
                    "overall_pass": False,
                },
            },
        },
    )

    response = frontend_client.get("/api/telemetry/overview?exposure_limit=50&include_events=10")
    assert response.status_code == 200
    payload = response.json()

    assert payload["economics"]["portfolio"]["net_pnl_final_usd"] == 240.0
    assert payload["ramp"]["decision"] == "NO_RAMP"
    assert payload["exposure"]["summary"]["total"] == 2
    assert payload["exposure"]["summary"]["allowed"] == 1
    assert payload["exposure"]["summary"]["blocked"] == 1
    assert payload["exposure"]["events"][0]["symbol"] in {"BTC-USD", "ETH-USD"}


def test_telemetry_overview_parses_metrics(monkeypatch, tmp_path):
    metrics_text = """
# HELP portfolio_value_usd Portfolio value in USD
portfolio_value_usd 123456.7
portfolio_pnl_usd -321.5
trading_cycles_total 12
orders_placed_total{symbol="BTC-USD",side="buy"} 4
orders_placed_total{symbol="ETH-USD",side="sell"} 2
risk_violations_total{type="kill_switch"} 1
risk_violations_total{type="var"} 3
""".strip()

    class FakeResponse:
        text = metrics_text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

        async def get(self, url):
            return FakeResponse()

    monkeypatch.setattr("trading_bot.frontend.app.httpx.AsyncClient", FakeClient)

    config = DashboardConfig(
        data_dir=tmp_path / "frontend-data",
        scratchpad_dir=tmp_path / "scratchpad",
        control_api_base_url="http://control.invalid",
        audit_dir=tmp_path / "audit",
        econ_dir=tmp_path / "econ",
        ramp_dir=tmp_path / "ramp",
        metrics_url="http://127.0.0.1:9108/metrics",
    )

    app = create_app(config)
    with TestClient(app) as client:
        response = client.get("/api/telemetry/overview")
        assert response.status_code == 200
        payload = response.json()["metrics"]
        assert payload["configured"] is True
        assert payload["reachable"] is True
        assert payload["metrics"]["portfolio_value_usd"] == 123456.7
        assert payload["metrics"]["portfolio_pnl_usd"] == -321.5
        assert payload["metrics"]["orders_placed_total"] == 6.0
        assert payload["metrics"]["kill_switch_violations"] == 1.0


def test_notifications_test_can_deliver_when_enabled(monkeypatch, tmp_path):
    config = DashboardConfig(
        data_dir=tmp_path / "frontend-data",
        scratchpad_dir=tmp_path / "scratchpad",
        control_api_base_url="http://control.invalid",
        audit_dir=tmp_path / "audit",
        econ_dir=tmp_path / "econ",
        ramp_dir=tmp_path / "ramp",
        metrics_url=None,
        advisor_notify_enabled=True,
        advisor_wechat_webhook_url="https://example.invalid/wechat",
        advisor_notify_min_interval_seconds=1,
    )
    app = create_app(config)

    async def fake_wechat(_self, _title: str, _body: str):
        return True

    monkeypatch.setattr("trading_bot.frontend.app.TelemetryService._send_wechat_notification", fake_wechat)

    with TestClient(app) as client:
        response = client.post(
            "/api/notifications/test",
            json={"message": "advisor test"},
        )
        assert response.status_code == 200
        result = response.json()["result"]
        assert result["sent"] is True
        assert result["delivery"]["wechat"] is True


@pytest.mark.asyncio
async def test_call_control_api_converts_network_errors(frontend_config, monkeypatch):
    class BrokenClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

        async def request(self, *args, **kwargs):
            raise httpx.ConnectError("refused")

    monkeypatch.setattr("trading_bot.frontend.app.httpx.AsyncClient", BrokenClient)

    with pytest.raises(HTTPException) as exc_info:
        await call_control_api(frontend_config, "GET", "/health")

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail["message"] == "control_api_unreachable"
