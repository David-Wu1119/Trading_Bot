"""Smoke-level HTTP contract checks for release-critical frontend endpoints."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from trading_bot.frontend.app import DashboardConfig, create_app


def test_live_path_http_contract(tmp_path, monkeypatch) -> None:
    async def fake_call(_config, method: str, endpoint: str, payload=None) -> dict[str, Any]:
        assert method == "GET"
        assert payload is None
        if endpoint == "/health":
            return {"kill_switch_active": False, "is_running": True}
        if endpoint == "/telemetry/market_state":
            return {
                "all_symbols_fresh": True,
                "stale_symbols": [],
                "last_market_data_update_at": "2026-03-02T21:05:03+00:00",
                "stale_data_max_age_seconds": 0.5,
                "prices": [],
                "trades": [],
                "stock_day_report": {
                    "date_et": "2026-03-02",
                    "summary": {
                        "entry_signals": 0,
                        "blocked_signals": 0,
                        "fills": 0,
                        "realized_pnl_usd": 0.0,
                        "unrealized_pnl_usd": 0.0,
                        "fees_usd": 0.0,
                        "freshness_fail_count": 0,
                        "market_hours_fill_count": 0,
                        "after_hours_ignored_stale_count": 0,
                    },
                    "symbols": {},
                },
            }
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    monkeypatch.setattr("trading_bot.frontend.app.call_control_api", fake_call)

    config = DashboardConfig(
        data_dir=tmp_path / "frontend-data",
        scratchpad_dir=tmp_path / "scratchpad",
        control_api_base_url="http://control.invalid",
        control_timeout_seconds=0.05,
        audit_dir=tmp_path / "audit",
        econ_dir=tmp_path / "econ",
        ramp_dir=tmp_path / "ramp",
        metrics_url=None,
    )
    app = create_app(config)

    with TestClient(app) as client:
        status = client.get("/api/system/status")
        assert status.status_code == 200
        status_payload = status.json()
        assert status_payload["control_api"]["reachable"] is True
        assert isinstance(status_payload["missions"]["counts"], dict)

        stock_day = client.get("/api/telemetry/stocks/day")
        assert stock_day.status_code == 200
        stock_payload = stock_day.json()
        assert stock_payload["source"] in {"control_api_stock_day_report", "frontend_fallback"}
        assert isinstance(stock_payload["summary"], dict)
        assert isinstance(stock_payload["symbols"], dict)
        assert "date_et" in stock_payload
