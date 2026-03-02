"""Smoke-level HTTP contract checks for release-critical dashboard endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from api.promql_proxy import app


def test_live_path_http_contract() -> None:
    with TestClient(app) as client:
        root = client.get("/")
        assert root.status_code == 200
        root_payload = root.json()
        assert root_payload["service"] == "Trading System PromQL Proxy"
        assert "/api/queries" in root_payload["endpoints"]

        queries = client.get("/api/queries")
        assert queries.status_code == 200
        queries_payload = queries.json()
        assert isinstance(queries_payload.get("queries"), dict)
        assert "ab_gate_status" in queries_payload["queries"]
        assert isinstance(queries_payload.get("total"), int)
