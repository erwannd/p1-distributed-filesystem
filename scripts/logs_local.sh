#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
LOG_DIR="$ROOT/logs"

if [ ! -d "$LOG_DIR" ] || [ -z "$(ls -A $LOG_DIR/*.log 2>/dev/null)" ]; then
    echo "[logs_local.sh] No log files found in $LOG_DIR"
    exit 1
fi

tail -f "$LOG_DIR"/*.log