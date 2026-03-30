#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
OUTPUT_DIR="/bigdata/students/$(whoami)"
PID_DIR="$OUTPUT_DIR/pids"

# -----------------------------------------------
# 1. Stop Controller (local)
# -----------------------------------------------
if [ -f "$PID_DIR/controller.pid" ]; then
    PID=$(cat "$PID_DIR/controller.pid")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "[stop_orion.sh] Controller stopped (PID $PID)"
    else
        echo "[stop_orion.sh] Controller already stopped"
    fi
    rm "$PID_DIR/controller.pid"
fi

# -----------------------------------------------
# 2. Stop Storage Nodes (remote via SSH)
# -----------------------------------------------
if [ ! -f "$PID_DIR/remote_nodes.txt" ]; then
    echo "[stop_orion.sh] No remote nodes file found"
    exit 0
fi

while IFS=: read -r NODE_HOST PID_FILE; do
    echo "[stop_orion.sh] Stopping storage node on $NODE_HOST..."
    ssh "$NODE_HOST" "
        if [ -f '$PID_FILE' ]; then
            PID=\$(cat '$PID_FILE')
            if kill -0 \$PID 2>/dev/null; then
                kill \$PID
                echo '[stop_orion.sh] Stopped (PID '\$PID')'
            else
                echo '[stop_orion.sh] Already stopped'
            fi
            rm '$PID_FILE'
        else
            echo '[stop_orion.sh] PID file not found: $PID_FILE'
        fi
    " &
done < "$PID_DIR/remote_nodes.txt"

wait
rm -f "$PID_DIR/remote_nodes.txt"
echo "[stop_orion.sh] All storage nodes stopped"
