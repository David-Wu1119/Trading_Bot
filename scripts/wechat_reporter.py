#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
STATE_FILE = Path("/home/linuxuser/trading-bot-app/data/frontend/wechat_report_state.json")


def _http_json(url: str, method: str = "GET", payload: dict[str, Any] | None = None, timeout: float = 8.0) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url=url, data=data, headers=headers, method=method)
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body.decode("utf-8"))


def _load_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_report(mode: str, market: dict[str, Any], advisor: dict[str, Any]) -> str:
    now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    now_et = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")

    live = market.get("live_pnl") if isinstance(market.get("live_pnl"), dict) else {}
    pnl = float(live.get("portfolio_pnl_usd") or 0.0)
    value = float(live.get("portfolio_value_usd") or 0.0)
    positions = int(float(live.get("positions_count") or 0.0))
    fees = float(live.get("total_fees_usd") or 0.0)
    unrealized = float(live.get("unrealized_pnl_usd") or 0.0)
    realized = float(live.get("realized_pnl_usd") or 0.0)

    trade_count = int(market.get("trade_count") or 0)
    trade_ledger_total = int(market.get("trade_ledger_total") or trade_count)
    prices = market.get("prices") if isinstance(market.get("prices"), list) else []
    open_positions = market.get("open_positions") if isinstance(market.get("open_positions"), list) else []

    top_lines = []
    for row in prices[:5]:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "?")
        price = row.get("price")
        age = row.get("age_seconds")
        try:
            price_s = f"{float(price):.2f}"
        except Exception:
            price_s = "n/a"
        try:
            age_s = f"{float(age):.1f}s"
        except Exception:
            age_s = "n/a"
        top_lines.append(f"- {sym}: ${price_s} (age {age_s})")

    position_lines = []
    for row in open_positions[:8]:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "?")
        qty = _safe_float(row.get("quantity"))
        mark_price = _safe_float(row.get("mark_price"))
        entry_price = _safe_float(row.get("entry_price"))
        market_value = _safe_float(row.get("market_value_usd"))
        if market_value <= 0.0 and abs(qty) > 1e-12 and mark_price > 0.0:
            market_value = abs(qty) * mark_price
        upnl = _safe_float(row.get("unrealized_pnl_usd"))
        upnl_pct_raw = row.get("unrealized_pnl_pct")
        if isinstance(upnl_pct_raw, (int, float)):
            upnl_pct_s = f"{float(upnl_pct_raw) * 100:+.2f}%"
        elif abs(qty) > 1e-12 and entry_price > 0.0:
            cost_basis = abs(qty) * entry_price
            upnl_pct = (upnl / cost_basis) if cost_basis > 0.0 else 0.0
            upnl_pct_s = f"{upnl_pct:+.2%}"
        else:
            upnl_pct_s = "n/a"
        position_lines.append(
            f"- {symbol}: qty {qty:.6f} | MV ${market_value:,.2f} | U-PnL ${upnl:,.2f} ({upnl_pct_s})"
        )

    posture = str(advisor.get("portfolio_posture") or "UNKNOWN")
    lock = bool(advisor.get("decision_locked", False))
    quality = advisor.get("quality") if isinstance(advisor.get("quality"), dict) else {}
    score = quality.get("score")
    score_s = "n/a"
    if isinstance(score, (int, float)):
        score_s = str(int(score))

    title = "[1H Report] Trading Bot Update"
    if mode == "close":
        title = "[Market Close] Trading Bot EOD Update"

    lines = [
        title,
        f"Time: {now_et} / {now_utc}",
        f"Portfolio Value: ${value:,.2f}",
        f"PnL: ${pnl:,.2f}",
        f"Unrealized PnL: ${unrealized:,.2f} | Realized PnL: ${realized:,.2f}",
        f"Open Positions: {positions}",
        f"Total Fees: ${fees:,.2f}",
        f"Recent Trade Events: {trade_count} | Ledger Total: {trade_ledger_total}",
        f"Advisor Posture: {posture} | Locked: {lock} | Score: {score_s}",
        "Prices:",
    ]
    lines.extend(top_lines or ["- n/a"])
    lines.append("Open Position PnL:")
    lines.extend(position_lines or ["- none"])
    return "\n".join(lines)


def _send_wechat(webhook: str, content: str) -> None:
    payload = {
        "msgtype": "text",
        "text": {"content": content},
    }
    _http_json(webhook, method="POST", payload=payload, timeout=10.0)


def _should_send_close_now() -> tuple[bool, str]:
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:
        return False, "weekend"
    # Must run around 16:05 ET (allow 0-20 minutes past to tolerate timer delays)
    if not (now_et.hour == 16 and 0 <= now_et.minute <= 20):
        return False, f"outside_close_window_{now_et.strftime('%H:%M')}"

    state = _load_state()
    today = now_et.strftime("%Y-%m-%d")
    if state.get("last_close_sent_date") == today:
        return False, "already_sent_today"
    return True, today


def main() -> int:
    parser = argparse.ArgumentParser(description="Send WeChat trading reports")
    parser.add_argument("--mode", choices=["interval", "close"], required=True)
    parser.add_argument("--api-base", default=os.getenv("WECHAT_REPORT_API_BASE", "http://127.0.0.1"))
    parser.add_argument("--webhook", default=os.getenv("TRADING_ADVISOR_NOTIFY_WECHAT_WEBHOOK", ""))
    args = parser.parse_args()

    webhook = args.webhook.strip()
    if not webhook:
        print("Missing TRADING_ADVISOR_NOTIFY_WECHAT_WEBHOOK", file=sys.stderr)
        return 2

    if args.mode == "close":
        ok, token = _should_send_close_now()
        if not ok:
            print(f"Skip close report: {token}")
            return 0

    try:
        market = _http_json(f"{args.api_base.rstrip('/')}/api/telemetry/market?trade_limit=8&symbol_limit=8")
        advisor = _http_json(f"{args.api_base.rstrip('/')}/api/telemetry/advisor?symbol_limit=8")
        report = _format_report(args.mode, market, advisor)
        _send_wechat(webhook, report)

        if args.mode == "close":
            state = _load_state()
            now_et = datetime.now(ET).strftime("%Y-%m-%d")
            state["last_close_sent_date"] = now_et
            state["last_close_sent_at"] = datetime.now(UTC).isoformat()
            _save_state(state)

        print("WeChat report sent")
        return 0
    except HTTPError as exc:
        print(f"HTTPError: {exc}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"URLError: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
