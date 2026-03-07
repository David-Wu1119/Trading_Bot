#!/usr/bin/env python3
"""Run and capture ETH-enabled vs ETH-disabled paper trading A/B sessions."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_MIN_CRYPTO_FILLS_FOR_ETH_ON = 3
DEFAULT_MIN_TOTAL_FILLS = 3
DEFAULT_MIN_STOCK_FILLS = 0
DEFAULT_MIN_MARKET_HOURS_CHECKPOINTS = 0
MARKET_TZ = ZoneInfo("America/New_York")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_slug() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


def _market_date_slug(ts: datetime) -> str:
    return ts.astimezone(MARKET_TZ).date().isoformat()


def _load_env_lines(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"env file does not exist: {path}")
    return path.read_text(encoding="utf-8").splitlines()


def _upsert_env_lines(lines: list[str], updates: dict[str, str]) -> list[str]:
    remaining = dict(updates)
    rendered: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            rendered.append(line)
            continue

        key, _value = line.split("=", 1)
        key = key.strip()
        if key in remaining:
            rendered.append(f"{key}={remaining.pop(key)}")
        else:
            rendered.append(line)

    for key, value in remaining.items():
        rendered.append(f"{key}={value}")
    return rendered


def _write_env_file(path: Path, lines: list[str]) -> None:
    content = "\n".join(lines)
    if not content.endswith("\n"):
        content += "\n"
    path.write_text(content, encoding="utf-8")


def _backup_file(path: Path) -> Path:
    backup = path.parent / f"{path.name}.bak.{_timestamp_slug()}"
    backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup


def _run_command(command: list[str]) -> None:
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            "command failed: "
            + " ".join(command)
            + f"\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _fetch_json(url: str) -> dict[str, Any]:
    req = Request(url=url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urlopen(req, timeout=5) as resp:
            data = resp.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"request failed [{exc.code}] {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"request failed {url}: {exc}") from exc

    try:
        payload = json.loads(data)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON from {url}: {data[:200]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"unexpected payload type from {url}: {type(payload).__name__}"
        )
    return payload


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _update_latest_summary_links(
    *,
    capture_dir: Path,
    summary_artifact: Path,
) -> dict[str, str]:
    """
    Maintain stable latest-summary references for shell consumers.

    Produces:
      - close_summary_latest.json (symlink when supported, otherwise copied file)
      - close_summary_latest_path.txt (absolute path pointer)
    """
    latest_json = capture_dir / "close_summary_latest.json"
    latest_path_txt = capture_dir / "close_summary_latest_path.txt"
    mode = "symlink"

    latest_path_txt.write_text(str(summary_artifact) + "\n", encoding="utf-8")

    if latest_json.exists() or latest_json.is_symlink():
        latest_json.unlink()

    try:
        latest_json.symlink_to(summary_artifact.name)
    except OSError:
        # Some filesystems or environments disallow symlinks; copy as deterministic fallback.
        shutil.copyfile(summary_artifact, latest_json)
        mode = "copied_file"

    return {
        "summary_latest_json": str(latest_json),
        "summary_latest_path_pointer": str(latest_path_txt),
        "summary_latest_mode": mode,
    }


def _extract_position_details(
    *,
    market_payload: dict[str, Any],
    stock_day_report: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract explicit open-position details from market telemetry."""
    direct_positions = market_payload.get("open_positions")
    if isinstance(direct_positions, list):
        sanitized: list[dict[str, Any]] = []
        for row in direct_positions:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            sanitized.append(
                {
                    "symbol": symbol,
                    "quantity": _safe_float(row.get("quantity")),
                    "asset_class": str(row.get("asset_class") or "").strip() or None,
                    "source": "market.open_positions",
                }
            )
        if sanitized:
            return sanitized
    return []


def _load_previous_arm_summary(
    *,
    artifact_root: Path,
    arm: str,
    current_date_slug: str,
) -> tuple[dict[str, Any] | None, Path | None]:
    if not artifact_root.exists():
        return None, None

    date_dirs = sorted(
        (
            path
            for path in artifact_root.iterdir()
            if path.is_dir() and path.name < current_date_slug
        ),
        key=lambda path: path.name,
        reverse=True,
    )

    for date_dir in date_dirs:
        arm_dir = date_dir / arm
        if not arm_dir.exists():
            continue

        candidates = [arm_dir / "close_summary_latest.json"]
        candidates.extend(sorted(arm_dir.glob("close_summary_*.json"), reverse=True))

        for candidate in candidates:
            if not candidate.exists():
                continue
            payload = _read_json_dict(candidate)
            if payload is not None:
                return payload, candidate.resolve()

    return None, None


def _build_delta_metric(
    *,
    current_value: float,
    baseline_value: float,
    allow_negative: bool,
) -> tuple[float, bool]:
    delta = current_value - baseline_value
    if not allow_negative and delta < 0:
        return current_value, True
    return delta, False


def _build_session_delta_scorecard(
    *,
    current_scorecard: dict[str, Any],
    previous_scorecard: dict[str, Any] | None,
    realized_override: float | None = None,
    fees_override: float | None = None,
    fills_override: int | None = None,
    trade_count_override: int | None = None,
) -> dict[str, Any]:
    baseline = previous_scorecard if isinstance(previous_scorecard, dict) else {}

    realized_delta, realized_reset = _build_delta_metric(
        current_value=_safe_float(current_scorecard.get("realized_pnl_usd")),
        baseline_value=_safe_float(baseline.get("realized_pnl_usd")),
        allow_negative=True,
    )
    fees_delta, fees_reset = _build_delta_metric(
        current_value=_safe_float(current_scorecard.get("fees_usd")),
        baseline_value=_safe_float(baseline.get("fees_usd")),
        allow_negative=False,
    )
    fills_delta_float, fills_reset = _build_delta_metric(
        current_value=float(_safe_int(current_scorecard.get("fills"))),
        baseline_value=float(_safe_int(baseline.get("fills"))),
        allow_negative=False,
    )
    trades_delta_float, trades_reset = _build_delta_metric(
        current_value=float(_safe_int(current_scorecard.get("trade_count"))),
        baseline_value=float(_safe_int(baseline.get("trade_count"))),
        allow_negative=False,
    )

    fills_delta = int(round(fills_delta_float))
    trades_delta = int(round(trades_delta_float))
    reset_fields: list[str] = []
    if realized_reset:
        reset_fields.append("realized_pnl_usd")
    if fees_reset:
        reset_fields.append("fees_usd")
    if fills_reset:
        reset_fields.append("fills")
    if trades_reset:
        reset_fields.append("trade_count")

    effective_fills = fills_override if fills_override is not None else fills_delta
    effective_trade_count = (
        trade_count_override if trade_count_override is not None else trades_delta
    )

    return {
        "realized_pnl_usd": (
            realized_override if realized_override is not None else realized_delta
        ),
        "unrealized_pnl_usd": _safe_float(current_scorecard.get("unrealized_pnl_usd")),
        "fees_usd": fees_override if fees_override is not None else fees_delta,
        "fills": effective_fills,
        "trade_count": effective_trade_count,
        "open_positions": _safe_int(current_scorecard.get("open_positions")),
        "baseline_available": previous_scorecard is not None,
        "baseline_reset_fields": reset_fields,
        "trade_count_semantics": (
            "session_trade_count"
            if trade_count_override is not None
            else "scorecard_trade_count_delta"
        ),
    }


def _combine_session_scorecards(
    *,
    stocks_session: dict[str, Any],
    crypto_session: dict[str, Any],
) -> dict[str, Any]:
    return {
        "realized_pnl_usd": _safe_float(stocks_session.get("realized_pnl_usd"))
        + _safe_float(crypto_session.get("realized_pnl_usd")),
        "unrealized_pnl_usd": _safe_float(stocks_session.get("unrealized_pnl_usd"))
        + _safe_float(crypto_session.get("unrealized_pnl_usd")),
        "fees_usd": _safe_float(stocks_session.get("fees_usd"))
        + _safe_float(crypto_session.get("fees_usd")),
        "fills": _safe_int(stocks_session.get("fills"))
        + _safe_int(crypto_session.get("fills")),
        "trade_count": _safe_int(stocks_session.get("trade_count"))
        + _safe_int(crypto_session.get("trade_count")),
        "open_positions": _safe_int(stocks_session.get("open_positions"))
        + _safe_int(crypto_session.get("open_positions")),
        "baseline_available": bool(stocks_session.get("baseline_available"))
        or bool(crypto_session.get("baseline_available")),
        "baseline_reset_fields": list(
            dict.fromkeys(
                [
                    *(
                        stocks_session.get("baseline_reset_fields")
                        if isinstance(stocks_session.get("baseline_reset_fields"), list)
                        else []
                    ),
                    *(
                        crypto_session.get("baseline_reset_fields")
                        if isinstance(crypto_session.get("baseline_reset_fields"), list)
                        else []
                    ),
                ]
            )
        ),
    }


def _build_acceptance_gates_payload(advisor_gates: Any) -> dict[str, Any]:
    if isinstance(advisor_gates, dict):
        return {
            "available": True,
            "reason": None,
            "overall_pass": advisor_gates.get("overall_pass"),
            "critical_failures": (
                advisor_gates.get("critical_failures")
                if isinstance(advisor_gates.get("critical_failures"), list)
                else []
            ),
            "gates": advisor_gates.get("gates") if isinstance(advisor_gates.get("gates"), list) else [],
        }
    return {
        "available": False,
        "reason": "advisor_payload_missing_acceptance_gates",
        "overall_pass": None,
        "critical_failures": [],
        "gates": [],
    }


def _build_advisor_state_payload(
    *,
    advisor_payload: Any,
    advisor_key_metrics: Any,
) -> dict[str, Any]:
    if isinstance(advisor_payload, dict):
        return {
            "available": True,
            "reason": None,
            "portfolio_posture": advisor_payload.get("portfolio_posture"),
            "decision_locked": advisor_payload.get("decision_locked"),
            "ramp_decision": (
                advisor_key_metrics.get("ramp_decision")
                if isinstance(advisor_key_metrics, dict)
                else None
            ),
            "approval_rate": (
                advisor_key_metrics.get("approval_rate")
                if isinstance(advisor_key_metrics, dict)
                else None
            ),
        }
    return {
        "available": False,
        "reason": "advisor_payload_unavailable",
        "portfolio_posture": None,
        "decision_locked": None,
        "ramp_decision": None,
        "approval_rate": None,
    }


def _build_alignment_payload(
    *,
    market_alignment: Any,
    advisor_alignment: Any,
) -> dict[str, Any]:
    alignment = market_alignment if isinstance(market_alignment, dict) else advisor_alignment
    if isinstance(alignment, dict):
        return {
            "available": True,
            "reason": None,
            "source": (
                "market.advisor_execution_alignment"
                if isinstance(market_alignment, dict)
                else "advisor.execution_alignment"
            ),
            "advisor_signal_count": alignment.get("advisor_signal_count"),
            "executed_trade_count": alignment.get("executed_trade_count"),
            "signal_execution_match_count": alignment.get("signal_execution_match_count"),
            "signal_execution_utilization_rate": alignment.get(
                "signal_execution_utilization_rate"
            ),
            "median_execution_lag_seconds": alignment.get(
                "median_execution_lag_seconds"
            ),
            "unmatched_advisor_signal_count": alignment.get(
                "unmatched_advisor_signal_count"
            ),
            "executed_without_advisor_signal_count": alignment.get(
                "executed_without_advisor_signal_count"
            ),
        }
    return {
        "available": False,
        "reason": "alignment_unavailable_from_market_and_advisor",
        "source": None,
        "advisor_signal_count": None,
        "executed_trade_count": None,
        "signal_execution_match_count": None,
        "signal_execution_utilization_rate": None,
        "median_execution_lag_seconds": None,
        "unmatched_advisor_signal_count": None,
        "executed_without_advisor_signal_count": None,
    }


def _build_telemetry_validity(
    *,
    market_payload: dict[str, Any],
    portfolio_scorecard: dict[str, Any],
    position_details: list[dict[str, Any]],
) -> dict[str, Any]:
    live_pnl = (
        market_payload.get("live_pnl") if isinstance(market_payload, dict) else None
    )
    scorecard_open_positions = _safe_int(portfolio_scorecard.get("open_positions"))
    live_open_positions = _safe_int(
        live_pnl.get("positions_count") if isinstance(live_pnl, dict) else 0
    )
    open_positions_count = max(scorecard_open_positions, live_open_positions)

    details_count = len(position_details)
    errors: list[str] = []
    if open_positions_count > 0 and details_count == 0:
        errors.append("open_positions_without_position_details")
    if open_positions_count == 0 and details_count > 0:
        errors.append("position_details_present_when_open_positions_zero")

    return {
        "is_consistent": len(errors) == 0,
        "errors": errors,
        "open_positions_count": open_positions_count,
        "position_details_count": details_count,
        "position_details": position_details,
        "position_details_source": (
            position_details[0].get("source")
            if position_details and isinstance(position_details[0], dict)
            else None
        ),
    }


def _build_ab_validity(
    *,
    arm: str,
    crypto_scorecard: dict[str, Any],
    min_crypto_fills_for_eth_on: int,
) -> dict[str, Any]:
    crypto_fills = _safe_int(crypto_scorecard.get("fills"))
    reasons: list[str] = []

    is_valid_arm_day = True
    if arm == "eth_on" and crypto_fills < min_crypto_fills_for_eth_on:
        is_valid_arm_day = False
        reasons.append("crypto_underparticipation")

    return {
        "arm": arm,
        "is_valid_arm_day": is_valid_arm_day,  # Backward-compatible alias.
        "is_valid": is_valid_arm_day,
        "min_crypto_fills_required": min_crypto_fills_for_eth_on,
        "crypto_fills": crypto_fills,
        "reasons": reasons,
    }


def _extract_market_hours_checkpoint_count(
    rubric_payload: dict[str, Any] | None
) -> int | None:
    if not isinstance(rubric_payload, dict):
        return None

    runtime = rubric_payload.get("runtime_integrity")
    if isinstance(runtime, dict):
        for key in (
            "market_hours_checkpoint_count",
            "checkpoint_count_market_hours",
            "snapshot_count",
            "checkpoints_written",
            "samples",
        ):
            if key in runtime:
                return _safe_int(runtime.get(key), 0)
    return None


def _build_session_validity(
    *,
    portfolio_session_scorecard: dict[str, Any],
    stock_session_scorecard: dict[str, Any],
    rubric_payload: dict[str, Any] | None,
    min_total_fills: int,
    min_stock_fills: int,
    min_market_hours_checkpoints: int,
) -> dict[str, Any]:
    total_fills = _safe_int(portfolio_session_scorecard.get("fills"))
    stock_fills = _safe_int(stock_session_scorecard.get("fills"))
    market_hours_checkpoint_count = _extract_market_hours_checkpoint_count(
        rubric_payload
    )

    reasons: list[str] = []
    if min_total_fills > 0 and total_fills < min_total_fills:
        reasons.append("insufficient_total_fills")
    if min_stock_fills > 0 and stock_fills < min_stock_fills:
        reasons.append("insufficient_stock_fills")

    if min_market_hours_checkpoints > 0:
        if market_hours_checkpoint_count is None:
            reasons.append("market_hours_checkpoints_unavailable")
        elif market_hours_checkpoint_count < min_market_hours_checkpoints:
            reasons.append("insufficient_market_hours_checkpoints")

    is_valid_session = len(reasons) == 0
    return {
        "is_valid_session": is_valid_session,  # Backward-compatible alias.
        "is_valid": is_valid_session,
        "min_total_fills_required": min_total_fills,
        "min_stock_fills_required": min_stock_fills,
        "min_market_hours_checkpoints_required": min_market_hours_checkpoints,
        "total_fills": total_fills,
        "stock_fills": stock_fills,
        "market_hours_checkpoint_count": market_hours_checkpoint_count,
        "reasons": reasons,
    }


def _build_overall_validity(
    *,
    ab_validity: dict[str, Any],
    session_validity: dict[str, Any],
    telemetry_validity: dict[str, Any],
) -> dict[str, Any]:
    arm_valid = bool(ab_validity.get("is_valid", False))
    session_valid = bool(session_validity.get("is_valid", False))
    telemetry_valid = bool(telemetry_validity.get("is_consistent", False))

    reasons: list[str] = []
    reasons.extend(
        [
            str(reason)
            for reason in ab_validity.get("reasons", [])
            if isinstance(reason, str)
        ]
    )
    reasons.extend(
        [
            str(reason)
            for reason in session_validity.get("reasons", [])
            if isinstance(reason, str)
        ]
    )
    if not telemetry_valid:
        reasons.append("telemetry_inconsistent")

    deduped_reasons = list(dict.fromkeys(reasons))
    return {
        "arm_valid": arm_valid,
        "session_valid": session_valid,
        "telemetry_valid": telemetry_valid,
        "overall_valid": arm_valid and session_valid and telemetry_valid,
        "reasons": deduped_reasons,
    }


def _start_arm(
    *,
    arm: str,
    env_file: Path,
    state_dir: Path,
    service: str,
    restart: bool,
    reset_state: bool,
    use_sudo: bool,
) -> dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / f"paper_state_{arm}.json"
    if reset_state and state_file.exists():
        state_file.unlink()

    backup = _backup_file(env_file)
    lines = _load_env_lines(env_file)
    crypto_symbols = "BTC-USD,ETH-USD" if arm == "eth_on" else "BTC-USD"

    updates = {
        "PAPER_ENGINE_CRYPTO_SYMBOLS": crypto_symbols,
        "PAPER_ENGINE_STATE_FILE": str(state_file),
        "PAPER_ENGINE_AB_ARM": arm,
    }
    updated_lines = _upsert_env_lines(lines, updates)
    _write_env_file(env_file, updated_lines)

    if restart:
        command = ["systemctl", "restart", service]
        if use_sudo:
            command.insert(0, "sudo")
        _run_command(command)

    return {
        "arm": arm,
        "env_file": str(env_file),
        "env_backup": str(backup),
        "state_file": str(state_file),
        "crypto_symbols": crypto_symbols,
        "service": service,
        "service_restarted": restart,
        "timestamp": _utc_now().isoformat(),
    }


def _capture_arm(
    *,
    arm: str,
    frontend_base_url: str,
    artifact_root: Path,
    rubric_dir: Path,
    min_crypto_fills_for_eth_on: int,
    min_total_fills: int,
    min_stock_fills: int,
    min_market_hours_checkpoints: int,
) -> dict[str, Any]:
    now = _utc_now()
    date_slug = _market_date_slug(now)
    capture_dir = artifact_root / date_slug / arm
    capture_dir.mkdir(parents=True, exist_ok=True)

    market = _fetch_json(f"{frontend_base_url}/api/telemetry/market")
    stocks_day = _fetch_json(f"{frontend_base_url}/api/telemetry/stocks/day")
    advisor = _fetch_json(f"{frontend_base_url}/api/telemetry/advisor")

    rubric_file = rubric_dir / f"rubric_{date_slug}.json"
    rubric_payload: dict[str, Any] | None = None
    if rubric_file.exists():
        try:
            parsed = json.loads(rubric_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            rubric_payload = parsed

    payload = {
        "timestamp": now.isoformat(),
        "arm": arm,
        "frontend_base_url": frontend_base_url,
        "market": market,
        "stocks_day": stocks_day,
        "advisor": advisor,
        "rubric": rubric_payload,
        "rubric_file": str(rubric_file),
    }

    artifact = capture_dir / f"capture_{_timestamp_slug()}.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    advisor_gates = advisor.get("acceptance_gates") if isinstance(advisor, dict) else {}
    advisor_key_metrics = (
        advisor.get("key_metrics") if isinstance(advisor, dict) else {}
    )
    advisor_alignment = (
        advisor.get("execution_alignment") if isinstance(advisor, dict) else {}
    )
    market_scorecards = market.get("scorecards") if isinstance(market, dict) else {}
    market_alignment = (
        market.get("advisor_execution_alignment") if isinstance(market, dict) else {}
    )
    market_stock_day = (
        market.get("stock_day_report") if isinstance(market, dict) else {}
    )
    portfolio_scorecard = (
        market_scorecards.get("portfolio_total")
        if isinstance(market_scorecards, dict)
        and isinstance(market_scorecards.get("portfolio_total"), dict)
        else {}
    )
    stock_scorecard = (
        market_scorecards.get("stocks")
        if isinstance(market_scorecards, dict)
        and isinstance(market_scorecards.get("stocks"), dict)
        else {}
    )
    crypto_scorecard = (
        market_scorecards.get("crypto")
        if isinstance(market_scorecards, dict)
        and isinstance(market_scorecards.get("crypto"), dict)
        else {}
    )
    position_details = _extract_position_details(
        market_payload=market,
        stock_day_report=market_stock_day,
    )
    previous_summary, previous_summary_path = _load_previous_arm_summary(
        artifact_root=artifact_root,
        arm=arm,
        current_date_slug=date_slug,
    )
    previous_stocks = (
        previous_summary.get("stocks")
        if isinstance(previous_summary, dict)
        and isinstance(previous_summary.get("stocks"), dict)
        else None
    )
    previous_crypto = (
        previous_summary.get("crypto")
        if isinstance(previous_summary, dict)
        and isinstance(previous_summary.get("crypto"), dict)
        else None
    )
    market_stock_day_summary = (
        market_stock_day.get("summary")
        if isinstance(market_stock_day, dict)
        and isinstance(market_stock_day.get("summary"), dict)
        else {}
    )
    stocks_session = _build_session_delta_scorecard(
        current_scorecard=stock_scorecard,
        previous_scorecard=previous_stocks,
        realized_override=_safe_float(market_stock_day_summary.get("realized_pnl_usd")),
        fees_override=_safe_float(market_stock_day_summary.get("fees_usd")),
        fills_override=_safe_int(market_stock_day_summary.get("fills")),
        trade_count_override=_safe_int(market_stock_day_summary.get("fills")),
    )
    crypto_session = _build_session_delta_scorecard(
        current_scorecard=crypto_scorecard,
        previous_scorecard=previous_crypto,
    )
    portfolio_session = _combine_session_scorecards(
        stocks_session=stocks_session,
        crypto_session=crypto_session,
    )
    telemetry_validity = _build_telemetry_validity(
        market_payload=market,
        portfolio_scorecard=portfolio_scorecard,
        position_details=position_details,
    )
    ab_validity = _build_ab_validity(
        arm=arm,
        crypto_scorecard=crypto_scorecard,
        min_crypto_fills_for_eth_on=min_crypto_fills_for_eth_on,
    )
    session_validity = _build_session_validity(
        portfolio_session_scorecard=portfolio_session,
        stock_session_scorecard=stocks_session,
        rubric_payload=rubric_payload,
        min_total_fills=min_total_fills,
        min_stock_fills=min_stock_fills,
        min_market_hours_checkpoints=min_market_hours_checkpoints,
    )
    validity = _build_overall_validity(
        ab_validity=ab_validity,
        session_validity=session_validity,
        telemetry_validity=telemetry_validity,
    )
    summary_payload = {
        "timestamp": now.isoformat(),
        "arm": arm,
        "artifact": str(artifact),
        "validity": validity,
        "ab_validity": ab_validity,
        "session_validity": session_validity,
        "telemetry_validity": telemetry_validity,
        "acceptance_gates": _build_acceptance_gates_payload(advisor_gates),
        "advisor_state": _build_advisor_state_payload(
            advisor_payload=advisor,
            advisor_key_metrics=advisor_key_metrics,
        ),
        "market_freshness": {
            "all_symbols_fresh": (
                market.get("all_symbols_fresh") if isinstance(market, dict) else None
            ),
            "stale_symbols": (
                market.get("stale_symbols")
                if isinstance(market, dict)
                and isinstance(market.get("stale_symbols"), list)
                else []
            ),
            "last_market_data_update_at": (
                market.get("last_market_data_update_at")
                if isinstance(market, dict)
                else None
            ),
        },
        "portfolio": (portfolio_scorecard),
        "stocks": (stock_scorecard),
        "crypto": (crypto_scorecard),
        "session_delta": {
            "baseline_source": (
                str(previous_summary_path)
                if previous_summary_path is not None
                else None
            ),
            "baseline_timestamp": (
                previous_summary.get("timestamp")
                if isinstance(previous_summary, dict)
                else None
            ),
            "portfolio": portfolio_session,
            "stocks": stocks_session,
            "crypto": crypto_session,
        },
        "alignment": _build_alignment_payload(
            market_alignment=market_alignment,
            advisor_alignment=advisor_alignment,
        ),
        "stock_day_summary": (
            market_stock_day.get("summary")
            if isinstance(market_stock_day, dict)
            and isinstance(market_stock_day.get("summary"), dict)
            else {}
        ),
        "equity_session_policy": (
            market.get("equity_session_policy") if isinstance(market, dict) else {}
        ),
        "crypto_risk_controls": (
            market.get("crypto_risk_controls") if isinstance(market, dict) else {}
        ),
    }
    summary_artifact = capture_dir / f"close_summary_{_timestamp_slug()}.json"
    summary_artifact.write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    latest_refs = _update_latest_summary_links(
        capture_dir=capture_dir,
        summary_artifact=summary_artifact,
    )

    return {
        "arm": arm,
        "artifact": str(artifact),
        "summary_artifact": str(summary_artifact),
        **latest_refs,
        "date": date_slug,
        "timestamp": now.isoformat(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ETH enabled vs disabled A/B tooling for paper trading runtime"
    )

    repo_root = Path(__file__).resolve().parents[1]
    default_env = repo_root / ".env.paper-engine"
    default_state = repo_root / "runtime" / "ab_state"
    default_artifact = repo_root / "artifacts" / "ab_runs"
    default_rubric = repo_root / "artifacts" / "telemetry_watch"

    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser(
        "start", help="switch environment to selected A/B arm"
    )
    start_parser.add_argument("--arm", choices=["eth_on", "eth_off"], required=True)
    start_parser.add_argument("--env-file", type=Path, default=default_env)
    start_parser.add_argument("--state-dir", type=Path, default=default_state)
    start_parser.add_argument("--service", default="trading-paper-engine.service")
    start_parser.add_argument("--no-restart", action="store_true")
    start_parser.add_argument("--reset-state", action="store_true")
    start_parser.add_argument("--no-sudo", action="store_true")

    capture_parser = subparsers.add_parser(
        "capture", help="capture telemetry artifact for selected arm"
    )
    capture_parser.add_argument("--arm", choices=["eth_on", "eth_off"], required=True)
    capture_parser.add_argument("--frontend-base-url", default="http://127.0.0.1:8088")
    capture_parser.add_argument("--artifact-root", type=Path, default=default_artifact)
    capture_parser.add_argument("--rubric-dir", type=Path, default=default_rubric)
    capture_parser.add_argument(
        "--min-crypto-fills-for-eth-on",
        type=int,
        default=DEFAULT_MIN_CRYPTO_FILLS_FOR_ETH_ON,
        help="Minimum crypto fills required to treat an eth_on session as A/B-valid",
    )
    capture_parser.add_argument(
        "--min-total-fills",
        type=int,
        default=DEFAULT_MIN_TOTAL_FILLS,
        help="Minimum total fills required to treat a session as strategy-valid",
    )
    capture_parser.add_argument(
        "--min-stock-fills",
        type=int,
        default=DEFAULT_MIN_STOCK_FILLS,
        help="Minimum stock fills required to treat a session as strategy-valid",
    )
    capture_parser.add_argument(
        "--min-market-hours-checkpoints",
        type=int,
        default=DEFAULT_MIN_MARKET_HOURS_CHECKPOINTS,
        help="Optional minimum market-hours checkpoints in rubric runtime_integrity (0 disables)",
    )

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.command == "start":
            result = _start_arm(
                arm=args.arm,
                env_file=args.env_file.resolve(),
                state_dir=args.state_dir.resolve(),
                service=args.service,
                restart=not args.no_restart,
                reset_state=bool(args.reset_state),
                use_sudo=not args.no_sudo,
            )
        else:
            result = _capture_arm(
                arm=args.arm,
                frontend_base_url=args.frontend_base_url.rstrip("/"),
                artifact_root=args.artifact_root.resolve(),
                rubric_dir=args.rubric_dir.resolve(),
                min_crypto_fills_for_eth_on=max(
                    int(args.min_crypto_fills_for_eth_on), 0
                ),
                min_total_fills=max(int(args.min_total_fills), 0),
                min_stock_fills=max(int(args.min_stock_fills), 0),
                min_market_hours_checkpoints=max(
                    int(args.min_market_hours_checkpoints), 0
                ),
            )
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        return 1

    print(json.dumps({"ok": True, "result": result}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
