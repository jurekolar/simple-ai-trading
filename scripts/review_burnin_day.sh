#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs/burnin
STAMP="$(date +%Y-%m-%d)"
LOG_FILE="logs/burnin/${STAMP}.log"
GIT_COMMIT="$(git rev-parse --short HEAD)"
CONFIG_PROFILE_VALUE="${CONFIG_PROFILE:-paper}"

{
  echo "===== Burn-In Review ${STAMP} ====="
  echo "Config profile: ${CONFIG_PROFILE_VALUE}"
  echo "Git commit: ${GIT_COMMIT}"
  echo
  echo "[1/3] 1-day summary"
  .venv/bin/python scripts/burnin_report.py --days 1
  echo
  echo "[2/3] 7-day summary"
  .venv/bin/python scripts/burnin_report.py --days 7
  echo
  echo "[3/3] operator report"
  .venv/bin/python -m app.main report
  echo
} | tee -a "$LOG_FILE"

echo "Burn-in review written to ${LOG_FILE}"
