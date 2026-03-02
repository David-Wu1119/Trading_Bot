#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    PYTHON_BIN="python"
  fi
fi

echo "[production-gate] using python: $PYTHON_BIN"
"$PYTHON_BIN" --version

echo "[production-gate] running focused release tests"
"$PYTHON_BIN" -m pytest -q \
  tests/test_preflight_release_check.py \
  tests/test_go_nogo_check.py \
  tests/smoke/test_live_path_http_contract.py

echo "[production-gate] validating preflight/go-no-go script contracts"
"$PYTHON_BIN" - <<'PY'
from scripts.go_nogo_check import GoNoGoChecker
from scripts.preflight_release_check import PreflightChecker

assert hasattr(GoNoGoChecker, "run_all_checks")
assert hasattr(PreflightChecker, "run_all_checks")
print("script_contract_ok")
PY

echo "[production-gate] validating script CLI surfaces"
"$PYTHON_BIN" scripts/preflight_release_check.py --help >/dev/null
"$PYTHON_BIN" scripts/go_nogo_check.py --help >/dev/null

echo "[production-gate] PASS"
