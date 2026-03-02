#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    PYTHON_BIN="python3"
  fi
fi

echo "[production-gate] using python: $PYTHON_BIN"
"$PYTHON_BIN" --version

echo "[production-gate] running focused release tests"
"$PYTHON_BIN" -m pytest -q \
  tests/unit/test_frontend_app.py \
  tests/unit/test_acceptance_gate_checks.py \
  tests/unit/test_orchestrator.py::test_control_api_endpoints \
  tests/smoke/test_live_path_http_contract.py

echo "[production-gate] validating acceptance/preflight script contracts"
"$PYTHON_BIN" - <<'PY'
from scripts.go_nogo_check import GoNoGoChecker
from scripts.preflight_release_check import PreflightChecker

assert hasattr(GoNoGoChecker, "check_alpha_acceptance_gates")
assert hasattr(PreflightChecker, "check_alpha_acceptance_gates")
print("script_contract_ok")
PY

echo "[production-gate] validating script CLI surfaces"
"$PYTHON_BIN" scripts/preflight_release_check.py --help >/dev/null
"$PYTHON_BIN" scripts/go_nogo_check.py --help >/dev/null
"$PYTHON_BIN" scripts/live_readiness_summary.py --help >/dev/null

echo "[production-gate] PASS"
