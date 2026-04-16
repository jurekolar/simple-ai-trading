#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/4] Checking Alpaca trading and data auth"
.venv/bin/python scripts/check_alpaca_auth.py --symbol SPY

echo "[2/4] Running paper trading cycle"
.venv/bin/python -m app.main paper

echo "[3/4] Reconciling broker state"
.venv/bin/python -m app.main reconcile

echo "[4/4] Generating 1-day burn-in report"
.venv/bin/python scripts/burnin_report.py --days 1

echo "Burn-in daily workflow completed"
