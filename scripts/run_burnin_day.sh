#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
STRATEGY="${STRATEGY:-momentum}"
CONFIG_PROFILE_VALUE="${CONFIG_PROFILE:-paper}"
GIT_COMMIT="$(git rev-parse --short HEAD)"

echo "strategy=${STRATEGY} config_profile=${CONFIG_PROFILE_VALUE} git_commit=${GIT_COMMIT}"

echo "[1/4] Checking Alpaca trading and data auth"
.venv/bin/python scripts/check_alpaca_auth.py --symbol SPY

echo "[2/4] Running paper trading cycle with strategy=${STRATEGY}"
.venv/bin/python -m app.main --strategy "$STRATEGY" paper

echo "[3/4] Reconciling broker state"
.venv/bin/python -m app.main reconcile

echo "[4/5] Generating 1-day burn-in report"
.venv/bin/python scripts/burnin_report.py --days 1

echo "[5/5] Printing operator report"
.venv/bin/python -m app.main report

echo "Burn-in daily workflow completed for strategy=${STRATEGY}"
