#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
PID_DIR="$ROOT/pids"

if [ ! -d "$PID_DIR" ]; then
    echo "[stop_local.sh] No pids directory found, nothing to stop"
    exit 0
fi

STOPPED=0
for PID_FILE in "$PID_DIR"/*.pid; do
    [ -f "$PID_FILE" ] || continue  # skip if no .pid files exist

    PID=$(cat "$PID_FILE")
    NAME=$(basename "$PID_FILE" .pid)

    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "[stop_local.sh] Stopped $NAME (PID $PID)"
        STOPPED=$((STOPPED + 1))
    else
        echo "[stop_local.sh] $NAME (PID $PID) already stopped"
    fi
    rm "$PID_FILE"
done

echo "[stop_local.sh] Stopped $STOPPED component(s)"