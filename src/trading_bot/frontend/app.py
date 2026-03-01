"""Modern trading control frontend API + static UI (Grafana alternative)."""

from __future__ import annotations

import asyncio
from collections import deque
import json
import os
import re
import smtplib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _read_jsonl_lines(file_path: Path) -> list[str]:
    try:
        return file_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []


def _parse_jsonl_entry(line: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


@dataclass
class DashboardConfig:
    """Runtime configuration for the control frontend."""

    data_dir: Path
    scratchpad_dir: Path
    control_api_base_url: str
    control_timeout_seconds: float = 2.0
    audit_dir: Path = Path("artifacts/audit")
    econ_dir: Path = Path("artifacts/econ")
    ramp_dir: Path = Path("artifacts/ramp")
    orchestrator_log_path: Path = Path("logs/demo/orchestrator.log")
    metrics_url: str | None = None
    telemetry_lookback: int = 120
    advisor_history_enabled: bool = True
    advisor_history_min_interval_seconds: int = 60
    advisor_notify_enabled: bool = False
    advisor_notify_min_interval_seconds: int = 300
    advisor_notify_on_lock_transition: bool = True
    advisor_notify_on_actionable: bool = True
    advisor_notify_confidence_threshold: float = 0.75
    advisor_wechat_webhook_url: str | None = None
    advisor_email_to: str | None = None
    advisor_email_from: str | None = None
    advisor_smtp_host: str | None = None
    advisor_smtp_port: int = 587
    advisor_smtp_user: str | None = None
    advisor_smtp_pass: str | None = None
    advisor_smtp_starttls: bool = True


def _default_config() -> DashboardConfig:
    data_dir = Path(
        os.getenv("TRADING_DASHBOARD_DATA_DIR", "data/frontend")
    ).resolve()
    scratchpad_dir = Path(
        os.getenv("TRADING_DASHBOARD_SCRATCHPAD_DIR", ".tradingdesk/scratchpad")
    ).resolve()
    control_api_base_url = os.getenv("CONTROL_API_BASE_URL", "http://127.0.0.1:9001")
    audit_dir = Path(os.getenv("TRADING_DASHBOARD_AUDIT_DIR", "artifacts/audit")).resolve()
    econ_dir = Path(os.getenv("TRADING_DASHBOARD_ECON_DIR", "artifacts/econ")).resolve()
    ramp_dir = Path(os.getenv("TRADING_DASHBOARD_RAMP_DIR", "artifacts/ramp")).resolve()
    orchestrator_log_path = Path(
        os.getenv("TRADING_DASHBOARD_ORCH_LOG_PATH", "logs/demo/orchestrator.log")
    ).resolve()

    raw_metrics_url = os.getenv("TRADING_METRICS_URL", "").strip()
    metrics_url = raw_metrics_url or None

    telemetry_lookback = int(os.getenv("TRADING_TELEMETRY_LOOKBACK", "120"))
    telemetry_lookback = min(max(telemetry_lookback, 20), 500)

    raw_advisor_history_enabled = os.getenv("TRADING_ADVISOR_HISTORY_ENABLED", "1").strip().lower()
    advisor_history_enabled = raw_advisor_history_enabled not in {"0", "false", "no", "off"}

    advisor_history_min_interval_seconds = int(
        os.getenv("TRADING_ADVISOR_HISTORY_MIN_INTERVAL", "60")
    )
    advisor_history_min_interval_seconds = min(
        max(advisor_history_min_interval_seconds, 5),
        3600,
    )

    raw_notify_enabled = os.getenv("TRADING_ADVISOR_NOTIFY_ENABLED", "0").strip().lower()
    advisor_notify_enabled = raw_notify_enabled in {"1", "true", "yes", "on"}
    advisor_notify_min_interval_seconds = int(
        os.getenv("TRADING_ADVISOR_NOTIFY_MIN_INTERVAL", "300")
    )
    advisor_notify_min_interval_seconds = min(
        max(advisor_notify_min_interval_seconds, 10),
        86400,
    )
    raw_notify_lock_transition = os.getenv(
        "TRADING_ADVISOR_NOTIFY_ON_LOCK_TRANSITION",
        "1",
    ).strip().lower()
    advisor_notify_on_lock_transition = raw_notify_lock_transition not in {"0", "false", "no", "off"}
    raw_notify_actionable = os.getenv("TRADING_ADVISOR_NOTIFY_ON_ACTIONABLE", "1").strip().lower()
    advisor_notify_on_actionable = raw_notify_actionable not in {"0", "false", "no", "off"}
    advisor_notify_confidence_threshold = _safe_float(
        os.getenv("TRADING_ADVISOR_NOTIFY_CONFIDENCE_THRESHOLD", "0.75"),
        0.75,
    )
    advisor_notify_confidence_threshold = min(max(advisor_notify_confidence_threshold, 0.1), 0.99)

    advisor_wechat_webhook_url = os.getenv("TRADING_ADVISOR_NOTIFY_WECHAT_WEBHOOK", "").strip() or None
    advisor_email_to = os.getenv("TRADING_ADVISOR_NOTIFY_EMAIL_TO", "").strip() or None
    advisor_email_from = os.getenv("TRADING_ADVISOR_NOTIFY_EMAIL_FROM", "").strip() or None
    advisor_smtp_host = os.getenv("TRADING_ADVISOR_NOTIFY_SMTP_HOST", "").strip() or None
    advisor_smtp_port = int(os.getenv("TRADING_ADVISOR_NOTIFY_SMTP_PORT", "587"))
    advisor_smtp_port = min(max(advisor_smtp_port, 1), 65535)
    advisor_smtp_user = os.getenv("TRADING_ADVISOR_NOTIFY_SMTP_USER", "").strip() or None
    advisor_smtp_pass = os.getenv("TRADING_ADVISOR_NOTIFY_SMTP_PASS", "").strip() or None
    raw_smtp_starttls = os.getenv("TRADING_ADVISOR_NOTIFY_SMTP_STARTTLS", "1").strip().lower()
    advisor_smtp_starttls = raw_smtp_starttls not in {"0", "false", "no", "off"}

    return DashboardConfig(
        data_dir=data_dir,
        scratchpad_dir=scratchpad_dir,
        control_api_base_url=control_api_base_url.rstrip("/"),
        audit_dir=audit_dir,
        econ_dir=econ_dir,
        ramp_dir=ramp_dir,
        orchestrator_log_path=orchestrator_log_path,
        metrics_url=metrics_url,
        telemetry_lookback=telemetry_lookback,
        advisor_history_enabled=advisor_history_enabled,
        advisor_history_min_interval_seconds=advisor_history_min_interval_seconds,
        advisor_notify_enabled=advisor_notify_enabled,
        advisor_notify_min_interval_seconds=advisor_notify_min_interval_seconds,
        advisor_notify_on_lock_transition=advisor_notify_on_lock_transition,
        advisor_notify_on_actionable=advisor_notify_on_actionable,
        advisor_notify_confidence_threshold=advisor_notify_confidence_threshold,
        advisor_wechat_webhook_url=advisor_wechat_webhook_url,
        advisor_email_to=advisor_email_to,
        advisor_email_from=advisor_email_from,
        advisor_smtp_host=advisor_smtp_host,
        advisor_smtp_port=advisor_smtp_port,
        advisor_smtp_user=advisor_smtp_user,
        advisor_smtp_pass=advisor_smtp_pass,
        advisor_smtp_starttls=advisor_smtp_starttls,
    )


class MissionCreate(BaseModel):
    """Payload for creating a mission."""

    title: str = Field(min_length=3, max_length=200)
    symbol: str | None = Field(default=None, max_length=32)
    objective: str | None = Field(default=None, max_length=1000)
    priority: Literal["low", "medium", "high"] = "medium"

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        title = value.strip()
        if len(title) < 3:
            raise ValueError("title must contain at least 3 non-space characters")
        return title

    @field_validator("symbol", "objective")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class MissionUpdate(BaseModel):
    """Payload for updating mission state."""

    status: Literal["planned", "in_progress", "blocked", "done"] | None = None
    objective: str | None = Field(default=None, max_length=1000)
    notes: str | None = Field(default=None, max_length=2000)
    priority: Literal["low", "medium", "high"] | None = None

    @field_validator("objective", "notes")
    @classmethod
    def normalize_update_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip()


class MissionRecord(BaseModel):
    """Stored mission record."""

    id: str
    title: str
    status: Literal["planned", "in_progress", "blocked", "done"] = "planned"
    symbol: str | None = None
    objective: str = ""
    notes: str = ""
    priority: Literal["low", "medium", "high"] = "medium"
    created_at: str
    updated_at: str


class KillSwitchRequest(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


class ResetRequest(BaseModel):
    authorized_by: str = Field(min_length=2, max_length=120)


class NotificationTestRequest(BaseModel):
    message: str = Field(
        default="Trading advisor test notification.",
        min_length=3,
        max_length=1000,
    )


class MissionStore:
    """Simple JSON-backed mission storage for planning board."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self._write([])

    def _read(self) -> list[dict[str, Any]]:
        try:
            with self.file_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError, TypeError):
            return []

        if not isinstance(payload, list):
            return []

        return [item for item in payload if isinstance(item, dict)]

    def _write(self, items: list[dict[str, Any]]) -> None:
        temp_path = self.file_path.with_suffix(".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(items, handle, indent=2)
        temp_path.replace(self.file_path)

    def list(self) -> list[MissionRecord]:
        items = [MissionRecord.model_validate(x) for x in self._read()]
        return sorted(items, key=lambda item: item.updated_at, reverse=True)

    def create(self, payload: MissionCreate) -> MissionRecord:
        now = _utc_now_iso()
        mission = MissionRecord(
            id=f"mission-{uuid4().hex[:10]}",
            title=payload.title,
            symbol=payload.symbol,
            objective=payload.objective or "",
            priority=payload.priority,
            created_at=now,
            updated_at=now,
        )
        items = self._read()
        items.append(mission.model_dump())
        self._write(items)
        return mission

    def update(self, mission_id: str, payload: MissionUpdate) -> MissionRecord:
        items = self._read()
        for item in items:
            if item.get("id") != mission_id:
                continue
            if payload.status is not None:
                item["status"] = payload.status
            if payload.objective is not None:
                item["objective"] = payload.objective
            if payload.notes is not None:
                item["notes"] = payload.notes
            if payload.priority is not None:
                item["priority"] = payload.priority
            item["updated_at"] = _utc_now_iso()
            updated = MissionRecord.model_validate(item)
            self._write(items)
            return updated
        raise KeyError(mission_id)


def _scratchpad_file(config: DashboardConfig) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    config.scratchpad_dir.mkdir(parents=True, exist_ok=True)
    return config.scratchpad_dir / f"{stamp}.jsonl"


def append_scratchpad(config: DashboardConfig, event_type: str, payload: dict[str, Any]) -> None:
    """Append execution trace events (Dexter-style scratchpad)."""

    entry = {
        "timestamp": _utc_now_iso(),
        "type": event_type,
        "payload": payload,
        "source": "frontend",
    }
    path = _scratchpad_file(config)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True))
        handle.write("\n")


def read_scratchpad(config: DashboardConfig, limit: int = 120) -> list[dict[str, Any]]:
    """Read most recent scratchpad entries."""

    if not config.scratchpad_dir.exists():
        return []

    entries: list[dict[str, Any]] = []
    files = sorted(config.scratchpad_dir.glob("*.jsonl"), reverse=True)
    for file_path in files:
        lines = _read_jsonl_lines(file_path)
        for line in reversed(lines):
            entry = _parse_jsonl_entry(line)
            if entry is None:
                continue
            entries.append(entry)
            if len(entries) >= limit:
                return entries
    return entries


async def call_control_api(
    config: DashboardConfig,
    method: Literal["GET", "POST"],
    endpoint: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Proxy call to orchestrator control API."""

    url = f"{config.control_api_base_url}{endpoint}"
    timeout = httpx.Timeout(config.control_timeout_seconds)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, url, json=payload)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={"message": "control_api_unreachable", "error": str(exc)},
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=502,
            detail={"message": "control_api_unreachable", "error": str(exc)},
        ) from exc

    try:
        body = response.json()
    except json.JSONDecodeError:
        body = {"raw": response.text}

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail={"status": response.status_code, "body": body})
    return body


_PROM_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{[^}]*\})?\s+(?P<value>[-+0-9.eEInfNa]+)$"
)
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')
_LIVE_PRICE_RE = re.compile(
    r"Latest:\s+(?P<symbol>[A-Z0-9-]+)\s+@\s+\$(?P<price>[0-9]+(?:\.[0-9]+)?)"
)
_LOG_TS_FORMAT = "%Y-%m-%d %H:%M:%S,%f"


class TelemetryService:
    """Collects trading telemetry from local artifacts + optional metrics endpoint."""

    def __init__(self, config: DashboardConfig):
        self.config = config
        self._last_advisor_history_write_at: datetime | None = None
        self._last_advisor_lock_state: bool | None = None
        self._last_notification_at: dict[str, datetime] = {}
        self._last_actionable_signature: str | None = None

    def notification_channels(self) -> dict[str, bool]:
        wechat_ready = bool(self.config.advisor_wechat_webhook_url)
        email_ready = bool(
            self.config.advisor_email_to
            and self.config.advisor_email_from
            and self.config.advisor_smtp_host
        )
        return {
            "enabled": bool(self.config.advisor_notify_enabled),
            "wechat": wechat_ready,
            "email": email_ready,
        }

    def _notification_allowed(self, key: str) -> bool:
        min_interval = max(self.config.advisor_notify_min_interval_seconds, 10)
        now = datetime.now(timezone.utc)
        last = self._last_notification_at.get(key)
        if last is not None and (now - last).total_seconds() < min_interval:
            return False
        self._last_notification_at[key] = now
        return True

    async def _send_wechat_notification(self, title: str, body: str) -> bool:
        webhook = self.config.advisor_wechat_webhook_url
        if not webhook:
            return False

        payload = {
            "msgtype": "text",
            "text": {"content": f"{title}\n{body}"},
        }
        timeout = httpx.Timeout(3.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(webhook, json=payload)
                response.raise_for_status()
            return True
        except httpx.HTTPError:
            return False

    def _send_email_notification_sync(self, subject: str, body: str) -> bool:
        to_address = self.config.advisor_email_to
        from_address = self.config.advisor_email_from
        host = self.config.advisor_smtp_host
        if not to_address or not from_address or not host:
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_address
        msg["To"] = to_address
        msg.set_content(body)

        try:
            with smtplib.SMTP(host, self.config.advisor_smtp_port, timeout=5) as smtp:
                if self.config.advisor_smtp_starttls:
                    smtp.starttls()
                if self.config.advisor_smtp_user and self.config.advisor_smtp_pass:
                    smtp.login(
                        self.config.advisor_smtp_user,
                        self.config.advisor_smtp_pass,
                    )
                smtp.send_message(msg)
            return True
        except OSError:
            return False

    async def _send_email_notification(self, subject: str, body: str) -> bool:
        return await asyncio.to_thread(self._send_email_notification_sync, subject, body)

    async def _dispatch_notification(
        self,
        *,
        key: str,
        title: str,
        body: str,
        force: bool = False,
    ) -> dict[str, Any]:
        channels = self.notification_channels()
        if not channels["enabled"] and not force:
            return {"sent": False, "reason": "notifications_disabled", "wechat": False, "email": False}
        if not force and not self._notification_allowed(key):
            return {"sent": False, "reason": "throttled", "wechat": False, "email": False}

        wechat_sent = await self._send_wechat_notification(title, body) if channels["wechat"] else False
        email_sent = await self._send_email_notification(title, body) if channels["email"] else False

        sent = bool(wechat_sent or email_sent)
        reason = "ok" if sent else "no_channel_or_delivery_failed"
        return {
            "sent": sent,
            "reason": reason,
            "wechat": wechat_sent,
            "email": email_sent,
        }

    async def test_notification(self, message: str) -> dict[str, Any]:
        channels = self.notification_channels()
        if not channels["enabled"]:
            return {
                "sent": False,
                "reason": "notifications_disabled",
                "channels": channels,
            }
        result = await self._dispatch_notification(
            key="manual_test",
            title="[Trading Advisor] Manual Notification Test",
            body=message,
            force=True,
        )
        return {
            "sent": bool(result.get("sent", False)),
            "reason": result.get("reason", "unknown"),
            "channels": channels,
            "delivery": result,
        }

    def _glob_files(self, base: Path, pattern: str) -> list[Path]:
        if not base.exists() or not base.is_dir():
            return []
        try:
            return [item for item in base.glob(pattern) if item.is_file()]
        except OSError:
            return []

    def _read_log_tail(self, max_lines: int = 2000) -> list[str]:
        path = self.config.orchestrator_log_path
        if not path.exists() or not path.is_file():
            return []
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                return [line.rstrip("\n") for line in deque(handle, maxlen=max_lines)]
        except OSError:
            return []

    @staticmethod
    def _parse_log_timestamp(line: str) -> datetime | None:
        if len(line) < 23:
            return None
        raw = line[:23]
        try:
            local_tz = datetime.now().astimezone().tzinfo
            parsed = datetime.strptime(raw, _LOG_TS_FORMAT)
            if local_tz is not None:
                parsed = parsed.replace(tzinfo=local_tz)
                return parsed.astimezone(timezone.utc)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _extract_structured_event(line: str) -> dict[str, Any] | None:
        brace_idx = line.find("{")
        if brace_idx < 0:
            return None
        try:
            payload = json.loads(line[brace_idx:])
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _latest_json(self, base: Path, patterns: list[str]) -> tuple[Path, dict[str, Any]] | None:
        files: list[Path] = []
        for pattern in patterns:
            files.extend(self._glob_files(base, pattern))
        if not files:
            return None

        latest = max(files, key=_safe_mtime)
        payload = _read_json_dict(latest)
        if payload is None:
            return None
        return latest, payload

    def _recent_exposure_events(self, limit: int) -> list[dict[str, Any]]:
        files = self._glob_files(self.config.audit_dir, "*_exposure_check.json")
        files.sort(key=_safe_mtime, reverse=True)

        events: list[dict[str, Any]] = []
        for path in files[:limit]:
            payload = _read_json_dict(path)
            if payload is None:
                continue
            order_request = payload.get("order_request")
            if not isinstance(order_request, dict):
                order_request = {}

            event = {
                "timestamp": payload.get("timestamp") or datetime.fromtimestamp(
                    _safe_mtime(path), tz=timezone.utc
                ).isoformat(),
                "symbol": str(order_request.get("symbol") or "UNKNOWN"),
                "side": str(order_request.get("side") or "n/a"),
                "venue": str(order_request.get("venue") or "n/a"),
                "notional_usd": _safe_float(order_request.get("notional_usd")),
                "influence_pct": _safe_float(order_request.get("influence_pct")),
                "allowed": bool(payload.get("allowed", False)),
                "reason": str(payload.get("reason") or ""),
                "operator": str(payload.get("operator") or "unknown"),
            }
            events.append(event)

        return events

    @staticmethod
    def _summarize_exposure(events: list[dict[str, Any]]) -> dict[str, Any]:
        total = len(events)
        allowed = sum(1 for event in events if event["allowed"])
        blocked = total - allowed
        approval_rate = (allowed / total) if total else 0.0
        notional_total = sum(_safe_float(event["notional_usd"]) for event in events)

        symbol_stats: dict[str, dict[str, Any]] = {}
        for event in events:
            symbol = str(event["symbol"])
            stats = symbol_stats.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "count": 0,
                    "allowed": 0,
                    "blocked": 0,
                    "notional_usd": 0.0,
                },
            )
            stats["count"] += 1
            if event["allowed"]:
                stats["allowed"] += 1
            else:
                stats["blocked"] += 1
            stats["notional_usd"] += _safe_float(event["notional_usd"])

        top_symbols = sorted(
            symbol_stats.values(),
            key=lambda item: (item["count"], item["notional_usd"]),
            reverse=True,
        )[:6]

        return {
            "total": total,
            "allowed": allowed,
            "blocked": blocked,
            "approval_rate": approval_rate,
            "notional_usd": notional_total,
            "top_symbols": top_symbols,
            "latest": events[0] if events else None,
        }

    def _advisor_history_file(self) -> Path:
        return self.config.data_dir / "advisor_quality_history.jsonl"

    def _advisor_lock_audit_file(self) -> Path:
        return self.config.audit_dir / "advisor_lock_events.jsonl"

    def _record_advisor_lock_transition(
        self,
        *,
        decision_locked: bool,
        acceptance_gates: dict[str, Any],
        recommendation_count: int,
        locked_recommendation_count: int,
    ) -> dict[str, Any] | None:
        previous = self._last_advisor_lock_state
        if previous is not None and previous == decision_locked:
            return None

        event_type = "advisor_decision_locked" if decision_locked else "advisor_decision_unlocked"
        critical_failures = acceptance_gates.get("critical_failures", [])
        details = {
            "decision_locked": decision_locked,
            "critical_failures": critical_failures if isinstance(critical_failures, list) else [],
            "recommendation_count": recommendation_count,
            "locked_recommendation_count": locked_recommendation_count,
            "overall_pass": bool(acceptance_gates.get("overall_pass", False)),
        }

        append_scratchpad(self.config, event_type, details)

        audit_file = self._advisor_lock_audit_file()
        audit_file.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp": _utc_now_iso(),
            "event": event_type,
            **details,
        }
        try:
            with audit_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, ensure_ascii=True))
                handle.write("\n")
        except OSError:
            pass

        self._last_advisor_lock_state = decision_locked
        return {
            "event_type": event_type,
            "details": details,
        }

    def _maybe_append_advisor_history(self, advisor_payload: dict[str, Any]) -> None:
        if not self.config.advisor_history_enabled:
            return

        now = datetime.now(timezone.utc)
        min_interval = max(self.config.advisor_history_min_interval_seconds, 5)
        if self._last_advisor_history_write_at is not None:
            elapsed = (now - self._last_advisor_history_write_at).total_seconds()
            if elapsed < min_interval:
                return

        quality = advisor_payload.get("quality")
        quality_dict = quality if isinstance(quality, dict) else {}
        gates = advisor_payload.get("acceptance_gates")
        gates_dict = gates if isinstance(gates, dict) else {}

        recommendations_raw = advisor_payload.get("recommendations")
        recommendations = recommendations_raw if isinstance(recommendations_raw, list) else []
        top_rows: list[dict[str, Any]] = []
        for row in recommendations[:5]:
            if not isinstance(row, dict):
                continue
            top_rows.append(
                {
                    "symbol": str(row.get("symbol") or "UNKNOWN"),
                    "recommendation": str(row.get("recommendation") or "HOLD"),
                    "confidence": _safe_float(row.get("confidence"), 0.0),
                    "score": _safe_float(row.get("score"), 0.0),
                    "trend_pct": (
                        _safe_float(row.get("trend_pct"))
                        if row.get("trend_pct") is not None
                        else None
                    ),
                }
            )

        entry = {
            "timestamp": _utc_now_iso(),
            "portfolio_posture": str(advisor_payload.get("portfolio_posture") or "UNKNOWN"),
            "portfolio_action": str(advisor_payload.get("portfolio_action") or "HOLD_AND_MONITOR"),
            "posture_score": _safe_float(advisor_payload.get("posture_score"), 0.0),
            "decision_locked": bool(advisor_payload.get("decision_locked", False)),
            "gates_overall_pass": bool(gates_dict.get("overall_pass", False)),
            "critical_failures": gates_dict.get("critical_failures", []),
            "evaluated": int(_safe_float(quality_dict.get("evaluated"), 0.0)),
            "hit_rate": (
                _safe_float(quality_dict.get("hit_rate"))
                if quality_dict.get("hit_rate") is not None
                else None
            ),
            "weighted_hit_rate": (
                _safe_float(quality_dict.get("weighted_hit_rate"))
                if quality_dict.get("weighted_hit_rate") is not None
                else None
            ),
            "top_recommendations": top_rows,
        }

        history_file = self._advisor_history_file()
        history_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with history_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, ensure_ascii=True))
                handle.write("\n")
            self._last_advisor_history_write_at = now
        except OSError:
            return

    def advisor_history(self, limit: int = 120) -> dict[str, Any]:
        safe_limit = min(max(limit, 1), 1000)
        history_file = self._advisor_history_file()

        entries: list[dict[str, Any]] = []
        for line in reversed(_read_jsonl_lines(history_file)):
            parsed = _parse_jsonl_entry(line)
            if parsed is None:
                continue
            entries.append(parsed)
            if len(entries) >= safe_limit:
                break

        hit_rates = [
            _safe_float(item.get("hit_rate"))
            for item in entries
            if item.get("hit_rate") is not None
        ]
        weighted_hit_rates = [
            _safe_float(item.get("weighted_hit_rate"))
            for item in entries
            if item.get("weighted_hit_rate") is not None
        ]

        return {
            "timestamp": _utc_now_iso(),
            "count": len(entries),
            "latest": entries[0] if entries else None,
            "entries": entries,
            "summary": {
                "avg_hit_rate": (sum(hit_rates) / len(hit_rates)) if hit_rates else None,
                "avg_weighted_hit_rate": (
                    sum(weighted_hit_rates) / len(weighted_hit_rates)
                )
                if weighted_hit_rates
                else None,
            },
            "source_file": str(history_file),
        }

    def _collect_price_samples_from_logs(
        self, lines: list[str], max_samples_per_symbol: int
    ) -> dict[str, deque[tuple[datetime | None, float]]]:
        samples: dict[str, deque[tuple[datetime | None, float]]] = {}
        safe_max = min(max(max_samples_per_symbol, 20), 1000)

        for line in lines:
            match = _LIVE_PRICE_RE.search(line)
            if not match:
                continue

            symbol = match.group("symbol")
            price = _safe_float(match.group("price"))
            if price <= 0:
                continue

            ts = self._parse_log_timestamp(line)
            bucket = samples.setdefault(symbol, deque(maxlen=safe_max))
            bucket.append((ts, price))

        return samples

    @staticmethod
    def _compute_price_trends(
        samples: dict[str, deque[tuple[datetime | None, float]]]
    ) -> dict[str, dict[str, Any]]:
        trends: dict[str, dict[str, Any]] = {}
        for symbol, rows in samples.items():
            if len(rows) < 2:
                continue

            first_ts, first_price = rows[0]
            last_ts, last_price = rows[-1]
            if first_price <= 0:
                continue

            trend_pct = ((last_price - first_price) / first_price) * 100.0
            window_seconds: float | None = None
            if first_ts is not None and last_ts is not None:
                window_seconds = max((last_ts - first_ts).total_seconds(), 0.0)

            trends[symbol] = {
                "trend_pct": trend_pct,
                "window_seconds": window_seconds,
                "samples": len(rows),
                "start_price": first_price,
                "end_price": last_price,
            }

        return trends

    def economics_summary(self) -> dict[str, Any] | None:
        latest = self._latest_json(self.config.econ_dir, ["*/econ_close.json", "**/econ_close.json"])
        if latest is None:
            return None

        path, payload = latest
        portfolio = payload.get("portfolio") if isinstance(payload.get("portfolio"), dict) else {}
        assets_raw = payload.get("assets") if isinstance(payload.get("assets"), dict) else {}

        top_assets: list[dict[str, Any]] = []
        for symbol, values in assets_raw.items():
            if not isinstance(values, dict):
                continue
            top_assets.append(
                {
                    "symbol": str(symbol),
                    "net_pnl_usd": _safe_float(values.get("net_pnl_usd")),
                    "gross_pnl_usd": _safe_float(values.get("gross_pnl_usd")),
                    "fill_count": int(_safe_float(values.get("fill_count"), 0.0)),
                    "slippage_bps_p95": _safe_float(values.get("slippage_bps_p95")),
                }
            )

        top_assets.sort(key=lambda item: item["net_pnl_usd"], reverse=True)

        return {
            "timestamp": payload.get("timestamp") or datetime.fromtimestamp(
                _safe_mtime(path), tz=timezone.utc
            ).isoformat(),
            "date": payload.get("date"),
            "portfolio": {
                "gross_pnl_usd": _safe_float(portfolio.get("gross_pnl_usd")),
                "net_pnl_final_usd": _safe_float(portfolio.get("net_pnl_final_usd")),
                "total_fees_usd": _safe_float(portfolio.get("total_fees_usd")),
                "total_infra_cost_usd": _safe_float(portfolio.get("total_infra_cost_usd")),
                "cost_ratio": _safe_float(portfolio.get("cost_ratio")),
                "net_margin_pct": _safe_float(portfolio.get("net_margin_pct")),
            },
            "top_assets": top_assets[:8],
            "source_file": str(path),
        }

    async def market_summary(self, trade_limit: int = 40, symbol_limit: int = 16) -> dict[str, Any]:
        lines = self._read_log_tail(max_lines=max(1000, trade_limit * 80))
        now = datetime.now(timezone.utc)
        price_samples = self._collect_price_samples_from_logs(
            lines,
            max_samples_per_symbol=self.config.telemetry_lookback,
        )
        live_pnl: dict[str, Any] | None = None
        control_market_state: dict[str, Any] | None = None
        control_market_error: str | None = None

        try:
            control_payload = await call_control_api(self.config, "GET", "/telemetry/market_state")
            if isinstance(control_payload, dict):
                control_market_state = control_payload
        except HTTPException as exc:
            control_market_error = str(exc.detail)

        if control_market_state:
            snapshot = (
                control_market_state.get("portfolio_snapshot")
                if isinstance(control_market_state.get("portfolio_snapshot"), dict)
                else None
            )
            if snapshot:
                live_pnl = {
                    "timestamp": str(snapshot.get("timestamp") or _utc_now_iso()),
                    "portfolio_value_usd": _safe_float(snapshot.get("total_value")),
                    "portfolio_pnl_usd": _safe_float(snapshot.get("portfolio_pnl_usd")),
                    "unrealized_pnl_usd": _safe_float(snapshot.get("unrealized_pnl_usd")),
                    "realized_pnl_usd": _safe_float(snapshot.get("realized_pnl_usd")),
                    "cash_balance_usd": _safe_float(snapshot.get("cash_balance_usd")),
                    "positions_value_usd": _safe_float(snapshot.get("positions_value_usd")),
                    "total_fees_usd": _safe_float(snapshot.get("total_fees_usd")),
                    "positions_count": int(_safe_float(snapshot.get("positions_count"), 0.0)),
                    "source": "control_api_snapshot",
                }

        if live_pnl is None:
            for line in reversed(lines):
                payload = self._extract_structured_event(line)
                if not payload:
                    continue
                if str(payload.get("event") or "") != "portfolio_snapshot_created":
                    continue
                ts = payload.get("timestamp")
                if not ts:
                    parsed_ts = self._parse_log_timestamp(line)
                    ts = parsed_ts.isoformat() if parsed_ts else _utc_now_iso()
                live_pnl = {
                    "timestamp": ts,
                    "portfolio_value_usd": _safe_float(payload.get("total_value")),
                    "portfolio_pnl_usd": _safe_float(payload.get("portfolio_pnl_usd")),
                    "unrealized_pnl_usd": _safe_float(payload.get("unrealized_pnl_usd")),
                    "realized_pnl_usd": _safe_float(payload.get("realized_pnl_usd")),
                    "cash_balance_usd": _safe_float(payload.get("cash_balance_usd")),
                    "positions_value_usd": _safe_float(payload.get("positions_value_usd")),
                    "total_fees_usd": _safe_float(payload.get("total_fees_usd")),
                    "positions_count": int(_safe_float(payload.get("positions_count"), 0.0)),
                    "source": "orchestrator_snapshot",
                }
                break

        prices_by_symbol: dict[str, dict[str, Any]] = {}
        if control_market_state:
            price_rows = control_market_state.get("prices")
            if isinstance(price_rows, list):
                for row in price_rows:
                    if not isinstance(row, dict):
                        continue
                    symbol = str(row.get("symbol") or "").strip()
                    if not symbol or symbol in prices_by_symbol:
                        continue
                    price = _safe_float(row.get("price"))
                    if price <= 0:
                        continue
                    age_seconds = row.get("age_seconds")
                    parsed_age = _safe_float(age_seconds) if age_seconds is not None else None
                    prices_by_symbol[symbol] = {
                        "symbol": symbol,
                        "price": price,
                        "timestamp": row.get("timestamp"),
                        "age_seconds": parsed_age,
                        "source": "control_api_market_state",
                        "stale": bool(row.get("stale", False)),
                    }
                    if len(prices_by_symbol) >= symbol_limit:
                        break

        for line in reversed(lines):
            match = _LIVE_PRICE_RE.search(line)
            if not match:
                continue
            symbol = match.group("symbol")
            if symbol in prices_by_symbol:
                continue
            ts = self._parse_log_timestamp(line)
            age_seconds = (
                max((now - ts).total_seconds(), 0.0) if ts is not None else None
            )
            prices_by_symbol[symbol] = {
                "symbol": symbol,
                "price": _safe_float(match.group("price")),
                "timestamp": ts.isoformat() if ts is not None else None,
                "age_seconds": age_seconds,
                "source": "connector_stream",
                "stale": False,
            }
            if len(prices_by_symbol) >= symbol_limit:
                break

        price_trends = self._compute_price_trends(price_samples)

        trades: list[dict[str, Any]] = []
        for line in reversed(lines):
            payload = self._extract_structured_event(line)
            if not payload:
                continue

            event_name = str(payload.get("event") or "")
            if not event_name:
                continue

            ts = payload.get("timestamp")
            if not ts:
                parsed_ts = self._parse_log_timestamp(line)
                ts = parsed_ts.isoformat() if parsed_ts else _utc_now_iso()
            parsed_trade_ts: datetime | None = None
            if isinstance(ts, str):
                try:
                    parsed_trade_ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except ValueError:
                    parsed_trade_ts = None
            if parsed_trade_ts is not None and (now - parsed_trade_ts).total_seconds() > 86400:
                continue

            if event_name == "sor_trade_executed":
                trades.append(
                    {
                        "timestamp": ts,
                        "symbol": str(payload.get("symbol") or "UNKNOWN"),
                        "side": str(payload.get("side") or "n/a").upper(),
                        "quantity": _safe_float(payload.get("filled_quantity")),
                        "status": "EXECUTED",
                        "reason": "",
                        "source": "execution",
                    }
                )
            elif event_name == "sor_execution_failed":
                trades.append(
                    {
                        "timestamp": ts,
                        "symbol": str(payload.get("symbol") or "UNKNOWN"),
                        "side": str(payload.get("side") or "n/a").upper(),
                        "quantity": _safe_float(payload.get("quantity")),
                        "status": "FAILED",
                        "reason": str(payload.get("error") or "execution failed"),
                        "source": "execution",
                    }
                )
            elif event_name == "pre_trade_risk_rejected":
                positions = payload.get("positions")
                if isinstance(positions, dict):
                    for symbol, amount in positions.items():
                        raw_amount = _safe_float(amount)
                        trades.append(
                            {
                                "timestamp": ts,
                                "symbol": str(symbol),
                                "side": "BUY" if raw_amount >= 0 else "SELL",
                                "quantity": abs(raw_amount),
                                "status": "BLOCKED",
                                "reason": "pre-trade risk rejected",
                                "source": "risk",
                            }
                        )
                        if len(trades) >= trade_limit:
                            break

            if len(trades) >= trade_limit:
                break

        if not trades:
            # Fallback for quiet sessions: surface recent exposure checks as trade-intent events.
            exposure_events = self._recent_exposure_events(limit=trade_limit)
            for event in exposure_events[:trade_limit]:
                raw_ts = str(event.get("timestamp") or "")
                parsed_event_ts: datetime | None = None
                if raw_ts:
                    try:
                        parsed_event_ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                    except ValueError:
                        parsed_event_ts = None

                # Avoid surfacing stale historical artifacts as "current" trade activity.
                if parsed_event_ts is not None and (now - parsed_event_ts).total_seconds() > 86400:
                    continue

                allowed = bool(event.get("allowed", False))
                trades.append(
                    {
                        "timestamp": str(event.get("timestamp") or _utc_now_iso()),
                        "symbol": str(event.get("symbol") or "UNKNOWN"),
                        "side": str(event.get("side") or "n/a").upper(),
                        "quantity": _safe_float(event.get("notional_usd")),
                        "status": "CHECKED" if allowed else "BLOCKED",
                        "reason": str(
                            event.get("reason")
                            or ("exposure approved" if allowed else "exposure blocked")
                        ),
                        "source": "exposure_audit",
                    }
                )

        prices = sorted(prices_by_symbol.values(), key=lambda item: item["symbol"])
        return {
            "timestamp": _utc_now_iso(),
            "prices": prices,
            "price_count": len(prices),
            "price_trends": price_trends,
            "trades": trades[:trade_limit],
            "trade_count": len(trades[:trade_limit]),
            "live_pnl": live_pnl,
            "all_symbols_fresh": bool(control_market_state.get("all_symbols_fresh", False))
            if control_market_state
            else None,
            "stale_symbols": control_market_state.get("stale_symbols", [])
            if control_market_state
            else [],
            "last_market_data_update_at": control_market_state.get("last_market_data_update_at")
            if control_market_state
            else None,
            "stale_data_max_age_seconds": _safe_float(
                control_market_state.get("stale_data_max_age_seconds")
            )
            if control_market_state
            else None,
            "control_market_state_error": control_market_error,
            "source_log": str(self.config.orchestrator_log_path),
        }

    @staticmethod
    def _portfolio_posture_from_score(score: int) -> tuple[str, str]:
        if score <= -3:
            return "RISK_OFF", "PAUSE_NEW_RISK"
        if score <= -1:
            return "DEFENSIVE", "REDUCE_EXPOSURE"
        if score <= 1:
            return "NEUTRAL", "HOLD_AND_MONITOR"
        return "OFFENSIVE", "SELECTIVE_ADD"

    @staticmethod
    def _build_advisor_acceptance_gates(
        *,
        approval_rate: float,
        exposure_total: int,
        market_payload: dict[str, Any],
        live_pnl: dict[str, Any] | None,
        ramp_decision: str,
        economics_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        gates: list[dict[str, Any]] = []

        approval_pass = approval_rate >= 0.60 if exposure_total >= 5 else True
        gates.append(
            {
                "name": "approval_rate",
                "actual": approval_rate,
                "threshold": ">= 0.60 (or <5 samples)",
                "passed": approval_pass,
                "critical": True,
                "details": {"samples": exposure_total},
            }
        )

        stale_symbols = market_payload.get("stale_symbols", [])
        stale_count = len(stale_symbols) if isinstance(stale_symbols, list) else 0
        stale_pass = stale_count == 0
        gates.append(
            {
                "name": "fresh_market_data",
                "actual": stale_count,
                "threshold": "== 0 stale symbols",
                "passed": stale_pass,
                "critical": True,
                "details": stale_symbols[:5] if isinstance(stale_symbols, list) else [],
            }
        )

        ramp_pass = "NO" not in ramp_decision
        gates.append(
            {
                "name": "ramp_decision",
                "actual": ramp_decision,
                "threshold": "must not be NO_*",
                "passed": ramp_pass,
                "critical": True,
            }
        )

        drawdown_ratio: float | None = None
        drawdown_pass = True
        if isinstance(live_pnl, dict):
            portfolio_value = _safe_float(live_pnl.get("portfolio_value_usd"))
            portfolio_pnl = _safe_float(live_pnl.get("portfolio_pnl_usd"))
            if portfolio_value > 0:
                drawdown_ratio = portfolio_pnl / portfolio_value
                drawdown_pass = drawdown_ratio > -0.015
        gates.append(
            {
                "name": "live_drawdown",
                "actual": drawdown_ratio,
                "threshold": "> -1.5%",
                "passed": drawdown_pass,
                "critical": True,
            }
        )

        cost_ratio: float | None = None
        cost_pass = True
        if isinstance(economics_payload, dict):
            portfolio = economics_payload.get("portfolio")
            if isinstance(portfolio, dict):
                cost_ratio = _safe_float(portfolio.get("cost_ratio"))
                cost_pass = cost_ratio <= 0.40 if cost_ratio > 0 else True
        gates.append(
            {
                "name": "cost_ratio",
                "actual": cost_ratio,
                "threshold": "<= 40%",
                "passed": cost_pass,
                "critical": False,
            }
        )

        critical_failures = [gate["name"] for gate in gates if gate["critical"] and not gate["passed"]]
        overall_pass = len(critical_failures) == 0
        return {
            "overall_pass": overall_pass,
            "critical_failures": critical_failures,
            "gates": gates,
        }

    @staticmethod
    def _trend_direction(trend_pct: float | None, threshold: float = 0.10) -> int:
        if trend_pct is None:
            return 0
        if trend_pct >= threshold:
            return 1
        if trend_pct <= -threshold:
            return -1
        return 0

    @staticmethod
    def _advice_direction(recommendation: str) -> int:
        normalized = recommendation.upper()
        if normalized == "BUY_BIAS":
            return 1
        if normalized in {"REDUCE", "AVOID"}:
            return -1
        return 0

    def _advisor_quality_from_recommendations(
        self, recommendations: list[dict[str, Any]]
    ) -> dict[str, Any]:
        evaluated = 0
        hits = 0
        weighted_total = 0.0
        weighted_hits = 0.0
        by_symbol: list[dict[str, Any]] = []

        for item in recommendations:
            if not isinstance(item, dict):
                continue

            recommendation = str(item.get("recommendation") or "HOLD")
            expected = self._advice_direction(recommendation)
            realized = self._trend_direction(
                _safe_float(item.get("trend_pct")) if item.get("trend_pct") is not None else None
            )
            confidence = _safe_float(item.get("confidence"), 0.0)

            aligned = None
            if expected != 0 and realized != 0:
                evaluated += 1
                aligned = expected == realized
                if aligned:
                    hits += 1
                weighted_total += max(confidence, 0.0)
                if aligned:
                    weighted_hits += max(confidence, 0.0)

            by_symbol.append(
                {
                    "symbol": str(item.get("symbol") or "UNKNOWN"),
                    "recommendation": recommendation,
                    "expected_direction": expected,
                    "realized_direction": realized,
                    "aligned": aligned,
                }
            )

        hit_rate = (hits / evaluated) if evaluated else None
        weighted_hit_rate = (weighted_hits / weighted_total) if weighted_total > 0 else None
        return {
            "evaluated": evaluated,
            "hits": hits,
            "hit_rate": hit_rate,
            "weighted_hit_rate": weighted_hit_rate,
            "baseline_hit_rate": 0.50 if evaluated else None,
            "methodology": "Directional alignment of recommendation vs short-horizon trend; telemetry proxy.",
            "by_symbol": by_symbol,
        }

    async def advisor_summary(
        self,
        symbol_limit: int = 10,
        market: dict[str, Any] | None = None,
        exposure_summary: dict[str, Any] | None = None,
        ramp: dict[str, Any] | None = None,
        economics: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        safe_symbol_limit = min(max(symbol_limit, 1), 50)
        market_payload = market or await self.market_summary(
            trade_limit=60,
            symbol_limit=max(safe_symbol_limit, 16),
        )
        summary = exposure_summary or self.exposure(limit=120, include_events=1)["summary"]
        ramp_payload = ramp or self.ramp_summary()
        economics_payload = economics or self.economics_summary()

        posture_score = 0
        posture_reasons: list[str] = []

        ramp_decision = str(
            ramp_payload["decision"] if isinstance(ramp_payload, dict) and "decision" in ramp_payload else "UNKNOWN"
        ).upper()
        if "NO" in ramp_decision:
            posture_score -= 2
            posture_reasons.append(f"Ramp gate says {ramp_decision}.")
        elif "GO" in ramp_decision or "RAMP" in ramp_decision:
            posture_score += 1
            posture_reasons.append(f"Ramp gate says {ramp_decision}.")

        approval_rate = _safe_float(summary.get("approval_rate"))
        if int(_safe_float(summary.get("total"), 0.0)) >= 5:
            if approval_rate < 0.50:
                posture_score -= 1
                posture_reasons.append("Risk approval rate is below 50%.")
            elif approval_rate >= 0.80:
                posture_score += 1
                posture_reasons.append("Risk approval rate is above 80%.")

        stale_symbols = market_payload.get("stale_symbols", [])
        if stale_symbols:
            posture_score -= 2
            posture_reasons.append(f"Stale market data symbols: {', '.join(stale_symbols[:5])}.")

        live_pnl = market_payload.get("live_pnl")
        if isinstance(live_pnl, dict):
            portfolio_value = _safe_float(live_pnl.get("portfolio_value_usd"))
            portfolio_pnl = _safe_float(live_pnl.get("portfolio_pnl_usd"))
            if portfolio_value > 0:
                pnl_ratio = portfolio_pnl / portfolio_value
                if pnl_ratio <= -0.02:
                    posture_score -= 2
                    posture_reasons.append("Live drawdown is worse than -2%.")
                elif pnl_ratio <= -0.01:
                    posture_score -= 1
                    posture_reasons.append("Live drawdown is worse than -1%.")
                elif pnl_ratio >= 0.01:
                    posture_score += 1
                    posture_reasons.append("Live P&L is above +1%.")

        acceptance_gates = self._build_advisor_acceptance_gates(
            approval_rate=approval_rate,
            exposure_total=int(_safe_float(summary.get("total"), 0.0)),
            market_payload=market_payload,
            live_pnl=live_pnl if isinstance(live_pnl, dict) else None,
            ramp_decision=ramp_decision,
            economics_payload=economics_payload if isinstance(economics_payload, dict) else None,
        )
        if not acceptance_gates["overall_pass"]:
            posture_score -= 1
            posture_reasons.append(
                "Acceptance gates failed: "
                + ", ".join(acceptance_gates.get("critical_failures", [])[:5])
            )

        posture, portfolio_action = self._portfolio_posture_from_score(posture_score)

        blocked_symbols = {
            str(item.get("symbol"))
            for item in summary.get("top_symbols", [])
            if isinstance(item, dict) and int(_safe_float(item.get("blocked"), 0.0)) > 0
        }
        price_trends = (
            market_payload.get("price_trends", {})
            if isinstance(market_payload.get("price_trends"), dict)
            else {}
        )

        recommendations: list[dict[str, Any]] = []
        for row in (market_payload.get("prices", []) or [])[:safe_symbol_limit]:
            if not isinstance(row, dict):
                continue

            symbol = str(row.get("symbol") or "UNKNOWN")
            symbol_score = posture_score
            rationale: list[str] = []
            contributions: dict[str, float] = {
                "posture_bias": float(posture_score),
                "freshness_penalty": 0.0,
                "trend_factor": 0.0,
                "exposure_penalty": 0.0,
                "gate_penalty": 0.0,
            }

            stale = bool(row.get("stale", False))
            age_seconds = row.get("age_seconds")
            parsed_age = _safe_float(age_seconds) if age_seconds is not None else None
            if stale:
                symbol_score -= 2
                contributions["freshness_penalty"] = -2.0
                rationale.append("Price feed stale.")
            elif parsed_age is not None and parsed_age > 60:
                symbol_score -= 1
                contributions["freshness_penalty"] = -1.0
                rationale.append("Price age above 60s.")

            trend = price_trends.get(symbol) if isinstance(price_trends.get(symbol), dict) else {}
            trend_pct = trend.get("trend_pct")
            trend_pct_value = _safe_float(trend_pct) if trend_pct is not None else None
            if trend_pct_value is not None:
                if trend_pct_value >= 0.30:
                    symbol_score += 1
                    contributions["trend_factor"] = 1.0
                    rationale.append(f"Short-term trend +{trend_pct_value:.2f}%.")
                elif trend_pct_value <= -0.30:
                    symbol_score -= 1
                    contributions["trend_factor"] = -1.0
                    rationale.append(f"Short-term trend {trend_pct_value:.2f}%.")

            if symbol in blocked_symbols:
                symbol_score -= 1
                contributions["exposure_penalty"] = -1.0
                rationale.append("Recent exposure checks blocked for symbol.")

            if not acceptance_gates["overall_pass"]:
                symbol_score -= 1
                contributions["gate_penalty"] = -1.0
                rationale.append("Portfolio acceptance gates are not fully passing.")

            if symbol_score >= 2:
                recommendation = "BUY_BIAS"
                tone = "bullish"
            elif symbol_score >= 0:
                recommendation = "HOLD"
                tone = "neutral"
            elif symbol_score >= -2:
                recommendation = "REDUCE"
                tone = "cautious"
            else:
                recommendation = "AVOID"
                tone = "defensive"

            confidence = 0.45 + min(abs(symbol_score) * 0.10, 0.40)
            confidence_components: dict[str, float] = {
                "base": 0.45,
                "score_bonus": min(abs(symbol_score) * 0.10, 0.40),
                "missing_trend_penalty": 0.0,
                "stale_penalty": 0.0,
                "gate_penalty": 0.0,
            }
            if trend_pct_value is None:
                confidence -= 0.05
                confidence_components["missing_trend_penalty"] = -0.05
            if stale:
                confidence -= 0.10
                confidence_components["stale_penalty"] = -0.10
            if not acceptance_gates["overall_pass"]:
                confidence -= 0.05
                confidence_components["gate_penalty"] = -0.05
            confidence = min(max(confidence, 0.25), 0.95)

            recommendations.append(
                {
                    "symbol": symbol,
                    "price": _safe_float(row.get("price")),
                    "age_seconds": parsed_age,
                    "stale": stale,
                    "recommendation": recommendation,
                    "raw_recommendation": recommendation,
                    "tone": tone,
                    "score": symbol_score,
                    "confidence": confidence,
                    "trend_pct": trend_pct_value,
                    "trend_window_seconds": trend.get("window_seconds"),
                    "rationale": rationale or ["No strong symbol-specific override."],
                    "provenance": {
                        "contributions": contributions,
                        "confidence_components": confidence_components,
                    },
                }
            )

        decision_locked = not acceptance_gates["overall_pass"]
        locked_recommendation_count = 0
        if decision_locked:
            for item in recommendations:
                if not isinstance(item, dict):
                    continue
                recommendation = str(item.get("recommendation") or "HOLD")
                if recommendation == "HOLD":
                    continue
                item["raw_recommendation"] = recommendation
                item["recommendation"] = "HOLD_LOCKED"
                item["tone"] = "defensive"
                rationale = item.get("rationale")
                if isinstance(rationale, list):
                    rationale.append("Recommendation locked by acceptance gate failure.")
                item["lock_applied"] = True
                locked_recommendation_count += 1

        recommendations.sort(key=lambda item: (item["score"], item["confidence"]), reverse=True)
        quality = self._advisor_quality_from_recommendations(recommendations)

        lock_transition = self._record_advisor_lock_transition(
            decision_locked=decision_locked,
            acceptance_gates=acceptance_gates,
            recommendation_count=len(recommendations),
            locked_recommendation_count=locked_recommendation_count,
        )

        notifications: list[dict[str, Any]] = []
        if (
            self.config.advisor_notify_on_lock_transition
            and isinstance(lock_transition, dict)
            and self.config.advisor_notify_enabled
        ):
            transition_event = str(lock_transition.get("event_type") or "advisor_decision_locked")
            details = lock_transition.get("details")
            details_dict = details if isinstance(details, dict) else {}
            failures = details_dict.get("critical_failures")
            failure_str = ", ".join(failures[:5]) if isinstance(failures, list) else ""
            title = (
                "[Trading Advisor] Decision LOCKED"
                if transition_event == "advisor_decision_locked"
                else "[Trading Advisor] Decision UNLOCKED"
            )
            body = (
                f"Posture: {posture}\n"
                f"Action: {portfolio_action}\n"
                f"Critical failures: {failure_str or 'none'}\n"
                f"Locked recommendations: {locked_recommendation_count}/{len(recommendations)}"
            )
            notif_result = await self._dispatch_notification(
                key=f"lock_transition:{transition_event}",
                title=title,
                body=body,
            )
            notifications.append(
                {
                    "type": "lock_transition",
                    "event": transition_event,
                    **notif_result,
                }
            )

        if self.config.advisor_notify_on_actionable and self.config.advisor_notify_enabled and not decision_locked:
            actionable = next(
                (
                    item
                    for item in recommendations
                    if isinstance(item, dict)
                    and str(item.get("recommendation") or "") in {"BUY_BIAS", "REDUCE", "AVOID"}
                    and _safe_float(item.get("confidence"), 0.0)
                    >= self.config.advisor_notify_confidence_threshold
                ),
                None,
            )
            if isinstance(actionable, dict):
                actionable_signature = (
                    f"{actionable.get('symbol')}|{actionable.get('recommendation')}|"
                    f"{_safe_float(actionable.get('score'), 0.0):.2f}"
                )
                if actionable_signature != self._last_actionable_signature:
                    top_reason = (
                        actionable.get("rationale", [""])
                        if isinstance(actionable.get("rationale"), list)
                        else [""]
                    )[0]
                    body = (
                        f"Symbol: {actionable.get('symbol')}\n"
                        f"Recommendation: {actionable.get('recommendation')}\n"
                        f"Confidence: {_safe_float(actionable.get('confidence'), 0.0):.2f}\n"
                        f"Score: {_safe_float(actionable.get('score'), 0.0):.2f}\n"
                        f"Reason: {top_reason or 'n/a'}"
                    )
                    notif_result = await self._dispatch_notification(
                        key="actionable_advice",
                        title="[Trading Advisor] Actionable Signal",
                        body=body,
                    )
                    notifications.append(
                        {
                            "type": "actionable",
                            "event": "actionable_signal",
                            **notif_result,
                        }
                    )
                    if notif_result.get("sent"):
                        self._last_actionable_signature = actionable_signature

        key_metrics = {
            "approval_rate": approval_rate,
            "blocked_checks": int(_safe_float(summary.get("blocked"), 0.0)),
            "ramp_decision": ramp_decision,
            "all_symbols_fresh": bool(market_payload.get("all_symbols_fresh", False)),
            "portfolio_pnl_usd": (
                _safe_float(live_pnl.get("portfolio_pnl_usd")) if isinstance(live_pnl, dict) else None
            ),
            "econ_net_pnl_usd": (
                _safe_float(
                    economics_payload.get("portfolio", {}).get("net_pnl_final_usd")
                )
                if isinstance(economics_payload, dict)
                else None
            ),
        }

        response_payload = {
            "timestamp": _utc_now_iso(),
            "portfolio_posture": posture,
            "portfolio_action": portfolio_action,
            "posture_score": posture_score,
            "posture_reasons": posture_reasons or ["No critical guardrail signals."],
            "key_metrics": key_metrics,
            "acceptance_gates": acceptance_gates,
            "decision_locked": decision_locked,
            "locked_recommendation_count": locked_recommendation_count,
            "recommendations": recommendations[:safe_symbol_limit],
            "quality": quality,
            "notifications": notifications,
            "disclaimer": "System-generated operational guidance; not investment advice.",
        }
        self._maybe_append_advisor_history(response_payload)
        return response_payload

    def ramp_summary(self) -> dict[str, Any] | None:
        latest = self._latest_json(self.config.ramp_dir, ["*/decision.json", "**/decision.json"])
        if latest is None:
            return None

        path, payload = latest
        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        kri_status = checks.get("kri_status") if isinstance(checks.get("kri_status"), dict) else {}
        kri_metrics = kri_status.get("metrics") if isinstance(kri_status.get("metrics"), dict) else {}
        cost_gates = checks.get("cost_gates") if isinstance(checks.get("cost_gates"), dict) else {}

        reasons = payload.get("reasons")
        if not isinstance(reasons, list):
            reasons = []

        return {
            "timestamp": payload.get("timestamp") or datetime.fromtimestamp(
                _safe_mtime(path), tz=timezone.utc
            ).isoformat(),
            "decision": str(payload.get("decision") or "UNKNOWN"),
            "reasons": [str(reason) for reason in reasons][:8],
            "kri": {
                "entropy": _safe_float(kri_metrics.get("entropy")),
                "qspread_ratio": _safe_float(kri_metrics.get("qspread_ratio")),
                "heartbeat_age": _safe_float(kri_metrics.get("heartbeat_age")),
                "daily_drawdown_pct": _safe_float(kri_metrics.get("daily_drawdown_pct")),
            },
            "cost_gates": {
                "cost_ratio": _safe_float(cost_gates.get("cost_ratio")),
                "cost_ratio_cap": _safe_float(cost_gates.get("cost_ratio_cap")),
                "overall_pass": bool(cost_gates.get("overall_pass", False)),
            },
            "source_file": str(path),
        }

    @staticmethod
    def _parse_metric_labels(raw_labels: str | None) -> dict[str, str]:
        if not raw_labels:
            return {}
        return {match.group(1): match.group(2) for match in _LABEL_RE.finditer(raw_labels)}

    @staticmethod
    def _parse_metric_value(raw_value: str) -> float | None:
        lowered = raw_value.lower()
        if lowered in {"nan", "+inf", "-inf", "inf"}:
            return None
        try:
            return float(raw_value)
        except ValueError:
            return None

    def _summarize_prometheus_metrics(self, text: str) -> dict[str, Any]:
        sample_map: dict[str, list[tuple[dict[str, str], float]]] = {}

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            match = _PROM_LINE_RE.match(stripped)
            if not match:
                continue

            value = self._parse_metric_value(match.group("value"))
            if value is None:
                continue

            name = match.group("name")
            labels = self._parse_metric_labels(match.group("labels"))
            sample_map.setdefault(name, []).append((labels, value))

        def first(metric_name: str) -> float | None:
            samples = sample_map.get(metric_name, [])
            if not samples:
                return None
            return samples[0][1]

        def total(metric_name: str) -> float:
            return sum(value for _labels, value in sample_map.get(metric_name, []))

        kill_switch_violations = 0.0
        for labels, value in sample_map.get("risk_violations_total", []):
            if labels.get("type") == "kill_switch":
                kill_switch_violations += value

        return {
            "portfolio_value_usd": first("portfolio_value_usd"),
            "portfolio_pnl_usd": first("portfolio_pnl_usd"),
            "trading_cycles_total": first("trading_cycles_total"),
            "orders_placed_total": total("orders_placed_total"),
            "risk_violations_total": total("risk_violations_total"),
            "kill_switch_violations": kill_switch_violations,
        }

    async def metrics_summary(self) -> dict[str, Any]:
        if not self.config.metrics_url:
            return {
                "configured": False,
                "reachable": False,
                "metrics": {},
                "error": None,
            }

        timeout = httpx.Timeout(1.5)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(self.config.metrics_url)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            return {
                "configured": True,
                "reachable": False,
                "metrics": {},
                "error": str(exc),
            }

        return {
            "configured": True,
            "reachable": True,
            "metrics": self._summarize_prometheus_metrics(response.text),
            "error": None,
        }

    def exposure(self, limit: int = 120, include_events: int = 30) -> dict[str, Any]:
        safe_limit = min(max(limit, 5), 500)
        events = self._recent_exposure_events(safe_limit)
        summary = self._summarize_exposure(events)
        return {
            "summary": summary,
            "events": events[: max(1, include_events)],
            "lookback": safe_limit,
        }

    async def overview(self, exposure_limit: int = 120, include_events: int = 30) -> dict[str, Any]:
        exposure = self.exposure(limit=exposure_limit, include_events=include_events)
        economics = self.economics_summary()
        ramp = self.ramp_summary()
        market = await self.market_summary()
        metrics = await self.metrics_summary()
        advisor = await self.advisor_summary(
            symbol_limit=10,
            market=market,
            exposure_summary=exposure["summary"],
            ramp=ramp,
            economics=economics,
        )

        return {
            "timestamp": _utc_now_iso(),
            "economics": economics,
            "ramp": ramp,
            "market": market,
            "exposure": exposure,
            "metrics": metrics,
            "advisor": advisor,
        }


def create_app(config: DashboardConfig | None = None) -> FastAPI:
    """Create FastAPI app for control frontend."""

    runtime = config or _default_config()
    runtime.data_dir.mkdir(parents=True, exist_ok=True)
    runtime.scratchpad_dir.mkdir(parents=True, exist_ok=True)
    mission_store = MissionStore(runtime.data_dir / "missions.json")
    telemetry = TelemetryService(runtime)

    app = FastAPI(title="Trading Desk Control Panel", version="2.0.0")
    app.state.config = runtime
    app.state.mission_store = mission_store
    app.state.telemetry = telemetry

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "timestamp": _utc_now_iso(),
            "service": "trading-desk-frontend",
            "version": "2.0.0",
        }

    @app.get("/api/system/status")
    async def system_status() -> dict[str, Any]:
        missions = mission_store.list()
        status_counts = {"planned": 0, "in_progress": 0, "blocked": 0, "done": 0}
        for mission in missions:
            status_counts[mission.status] += 1

        control = {"reachable": False, "state": None, "error": None}
        try:
            control_state = await call_control_api(runtime, "GET", "/health")
            control["reachable"] = True
            control["state"] = control_state
        except HTTPException as exc:
            control["error"] = str(exc.detail)

        return {
            "timestamp": _utc_now_iso(),
            "control_api": control,
            "missions": {
                "total": len(missions),
                "counts": status_counts,
                "latest": missions[0].model_dump() if missions else None,
            },
            "scratchpad_entries": len(read_scratchpad(runtime, limit=500)),
        }

    @app.get("/api/telemetry/overview")
    async def telemetry_overview(
        exposure_limit: int = 120,
        include_events: int = 30,
    ) -> dict[str, Any]:
        safe_exposure_limit = min(max(exposure_limit, 5), 500)
        safe_include_events = min(max(include_events, 1), 200)
        return await telemetry.overview(
            exposure_limit=safe_exposure_limit,
            include_events=safe_include_events,
        )

    @app.get("/api/telemetry/exposure")
    async def telemetry_exposure(limit: int = 120, include_events: int = 60) -> dict[str, Any]:
        safe_limit = min(max(limit, 5), 500)
        safe_include_events = min(max(include_events, 1), 300)
        return telemetry.exposure(limit=safe_limit, include_events=safe_include_events)

    @app.get("/api/telemetry/market")
    async def telemetry_market(trade_limit: int = 40, symbol_limit: int = 16) -> dict[str, Any]:
        safe_trade_limit = min(max(trade_limit, 1), 200)
        safe_symbol_limit = min(max(symbol_limit, 1), 50)
        return await telemetry.market_summary(
            trade_limit=safe_trade_limit,
            symbol_limit=safe_symbol_limit,
        )

    @app.get("/api/telemetry/advisor")
    async def telemetry_advisor(symbol_limit: int = 10) -> dict[str, Any]:
        safe_symbol_limit = min(max(symbol_limit, 1), 50)
        return await telemetry.advisor_summary(symbol_limit=safe_symbol_limit)

    @app.get("/api/telemetry/advisor_quality")
    async def telemetry_advisor_quality(symbol_limit: int = 10) -> dict[str, Any]:
        safe_symbol_limit = min(max(symbol_limit, 1), 50)
        advisor = await telemetry.advisor_summary(symbol_limit=safe_symbol_limit)
        quality = advisor.get("quality", {})
        return {
            "timestamp": _utc_now_iso(),
            "portfolio_posture": advisor.get("portfolio_posture"),
            "decision_locked": bool(advisor.get("decision_locked", False)),
            "quality": quality if isinstance(quality, dict) else {},
        }

    @app.get("/api/telemetry/advisor_history")
    async def telemetry_advisor_history(limit: int = 120) -> dict[str, Any]:
        safe_limit = min(max(limit, 1), 1000)
        return telemetry.advisor_history(limit=safe_limit)

    @app.get("/api/missions")
    async def list_missions() -> dict[str, Any]:
        missions = [item.model_dump() for item in mission_store.list()]
        return {"missions": missions}

    @app.post("/api/missions")
    async def create_mission(payload: MissionCreate) -> dict[str, Any]:
        mission = mission_store.create(payload)
        append_scratchpad(runtime, "mission_created", mission.model_dump())
        return {"mission": mission.model_dump()}

    @app.patch("/api/missions/{mission_id}")
    async def update_mission(mission_id: str, payload: MissionUpdate) -> dict[str, Any]:
        try:
            mission = mission_store.update(mission_id, payload)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Mission not found: {mission_id}") from exc
        append_scratchpad(runtime, "mission_updated", mission.model_dump())
        return {"mission": mission.model_dump()}

    @app.get("/api/scratchpad")
    async def scratchpad(limit: int = 120) -> dict[str, Any]:
        safe_limit = min(max(limit, 1), 500)
        return {"entries": read_scratchpad(runtime, limit=safe_limit)}

    @app.get("/api/notifications/status")
    async def notifications_status() -> dict[str, Any]:
        return {
            "timestamp": _utc_now_iso(),
            "channels": telemetry.notification_channels(),
            "min_interval_seconds": runtime.advisor_notify_min_interval_seconds,
            "on_lock_transition": runtime.advisor_notify_on_lock_transition,
            "on_actionable": runtime.advisor_notify_on_actionable,
            "confidence_threshold": runtime.advisor_notify_confidence_threshold,
        }

    @app.post("/api/notifications/test")
    async def notifications_test(payload: NotificationTestRequest) -> dict[str, Any]:
        result = await telemetry.test_notification(payload.message)
        append_scratchpad(
            runtime,
            "advisor_notification_test",
            {"request": payload.model_dump(), "result": result},
        )
        return {
            "timestamp": _utc_now_iso(),
            "result": result,
        }

    @app.post("/api/control/kill")
    async def activate_kill_switch(payload: KillSwitchRequest) -> dict[str, Any]:
        response = await call_control_api(
            runtime,
            "POST",
            "/emergency/kill_switch",
            payload={"reason": payload.reason, "trigger": "frontend"},
        )
        append_scratchpad(
            runtime,
            "kill_switch_activated",
            {"request": payload.model_dump(), "response": response},
        )
        return {"result": response}

    @app.post("/api/control/reset")
    async def reset_kill_switch(payload: ResetRequest) -> dict[str, Any]:
        response = await call_control_api(
            runtime,
            "POST",
            "/emergency/reset",
            payload={"authorized_by": payload.authorized_by},
        )
        append_scratchpad(
            runtime,
            "kill_switch_reset",
            {"request": payload.model_dump(), "response": response},
        )
        return {"result": response}

    @app.get("/api/insights")
    async def insights() -> dict[str, Any]:
        missions = mission_store.list()
        top = [m.model_dump() for m in missions if m.status in {"planned", "in_progress"}][:5]
        blocked = [m.model_dump() for m in missions if m.status == "blocked"][:5]
        exposure = telemetry.exposure(limit=80, include_events=5)
        ramp = telemetry.ramp_summary()

        return {
            "timestamp": _utc_now_iso(),
            "active_focus": top,
            "blocked": blocked,
            "guardrails": {
                "kill_switch_required_for_hard_stop": True,
                "live_mode_requires_explicit_confirmation": True,
                "audit_trace_enabled": True,
            },
            "risk_posture": {
                "approval_rate": exposure["summary"]["approval_rate"],
                "blocked_checks": exposure["summary"]["blocked"],
                "latest_ramp_decision": ramp["decision"] if ramp else None,
            },
        }

    return app
