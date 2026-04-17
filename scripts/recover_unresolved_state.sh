#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/4] Preflight validation"
.venv/bin/python -m app.main --strategy "${STRATEGY:-breakout}" preflight || true

echo "[2/4] Reconcile broker state"
.venv/bin/python -m app.main reconcile

echo "[3/4] Print latest operator report"
.venv/bin/python -m app.main report

echo "[4/4] Review unresolved state in the burn-in report"
.venv/bin/python scripts/burnin_report.py --days 1
