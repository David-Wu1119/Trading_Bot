# Advisor Notifications + SuperNotes Publish Runbook

## 1) Enable Advisor Notifications (WeChat + Gmail)

The advisor notification system is disabled by default.

Set environment variables:

```bash
export TRADING_ADVISOR_NOTIFY_ENABLED=1
export TRADING_ADVISOR_NOTIFY_MIN_INTERVAL=300
export TRADING_ADVISOR_NOTIFY_ON_LOCK_TRANSITION=1
export TRADING_ADVISOR_NOTIFY_ON_ACTIONABLE=1
export TRADING_ADVISOR_NOTIFY_CONFIDENCE_THRESHOLD=0.75

# WeChat Work (webhook bot)
export TRADING_ADVISOR_NOTIFY_WECHAT_WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=..."

# Gmail-compatible SMTP
export TRADING_ADVISOR_NOTIFY_EMAIL_TO="you@example.com"
export TRADING_ADVISOR_NOTIFY_EMAIL_FROM="bot@example.com"
export TRADING_ADVISOR_NOTIFY_SMTP_HOST="smtp.gmail.com"
export TRADING_ADVISOR_NOTIFY_SMTP_PORT=587
export TRADING_ADVISOR_NOTIFY_SMTP_USER="bot@example.com"
export TRADING_ADVISOR_NOTIFY_SMTP_PASS="your_app_password"
export TRADING_ADVISOR_NOTIFY_SMTP_STARTTLS=1
```

Validate config:

```bash
curl -s http://127.0.0.1:8088/api/notifications/status | jq
```

Send a test message:

```bash
curl -s -X POST http://127.0.0.1:8088/api/notifications/test \
  -H 'Content-Type: application/json' \
  -d '{"message":"Advisor notification test"}' | jq
```

## 2) Publish Clean Copy to SuperNotes/project

Use script:

```bash
scripts/publish_clean_copy_to_supernotes.sh \
  --repo https://github.com/David-Wu1119/SuperNotes.git \
  --subdir project \
  --branch codex/project-sync-$(date +%Y%m%d-%H%M%S) \
  --message "Add clean trading project snapshot"
```

The script:
- clones target repo
- syncs a filtered clean copy using `scripts/clean_copy_excludes.txt`
- commits
- pushes branch

## 3) Notes

- Advisor quality history is stored at:
  - `data/frontend/advisor_quality_history.jsonl`
- Advisor lock transition audit is stored at:
  - `artifacts/audit/advisor_lock_events.jsonl`
