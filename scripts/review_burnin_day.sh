#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs/burnin
STAMP="$(date +%Y-%m-%d)"
LOG_FILE="logs/burnin/${STAMP}.log"

{
  echo "===== Burn-In Review ${STAMP} ====="
  echo
  echo "[1/2] 1-day summary"
  .venv/bin/python scripts/burnin_report.py --days 1
  echo
  echo "[2/2] 7-day summary"
  .venv/bin/python scripts/burnin_report.py --days 7
  echo
} | tee -a "$LOG_FILE"

echo "Burn-in review written to ${LOG_FILE}"
