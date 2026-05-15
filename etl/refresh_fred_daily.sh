#!/usr/bin/env bash
set -euo pipefail

source ~/.bashrc

cd "$(dirname "$0")/.."

LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/fred_daily_$(date +%Y%m%d).log"

{
  echo "=== FRED daily refresh: $(date -Iseconds) ==="
  python etl/fetch_fred_rates.py --latest
  echo "=== Exit: $? ==="
} >>"$LOG_FILE" 2>&1
