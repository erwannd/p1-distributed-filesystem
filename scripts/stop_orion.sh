#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CONFIG="$ROOT/config.orion.json"
OUTPUT_DIR="/bigdata/students/$(whoami)"
PID_DIR="$OUTPUT_DIR/pids"
NODE_COUNT=$(jq '.storage.nodes | length' "$CONFIG")

# -----------------------------------------------
# 1. Stop Controller (local)
# -----------------------------------------------
controller_stopped=0

if [ -f "$PID_DIR/controller.pid" ]; then
    PID=$(cat "$PID_DIR/controller.pid")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "[stop_orion.sh] Controller stopped (PID $PID)"
        controller_stopped=1
    else
        echo "[stop_orion.sh] Controller already stopped"
    fi
    rm "$PID_DIR/controller.pid"
fi

for PID in $(lsof -tiTCP:10000 -sTCP:LISTEN 2>/dev/null || true); do
    COMM=$(ps -p "$PID" -o comm= 2>/dev/null | tr -d '[:space:]')
    if [ "$COMM" = "controller" ]; then
        kill "$PID"
        echo "[stop_orion.sh] Stopped leftover controller listener on port 10000 (PID $PID)"
        controller_stopped=1
    fi
done

if [ "$controller_stopped" -eq 0 ]; then
    echo "[stop_orion.sh] No controller process found on port 10000"
fi

# -----------------------------------------------
# 2. Stop Storage Nodes (remote via SSH)
# -----------------------------------------------
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_HOST=$(jq -r ".storage.nodes[$i].host" "$CONFIG")
    NODE_PORT=$(jq -r ".storage.nodes[$i].port" "$CONFIG")
    NODE_PID_FILE="$PID_DIR/storage_${NODE_HOST}_${NODE_PORT}.pid"

    echo "[stop_orion.sh] Stopping storage node on $NODE_HOST:$NODE_PORT..."
    ssh "$NODE_HOST" "
        stopped=0

        if [ -f '$NODE_PID_FILE' ]; then
            PID=\$(cat '$NODE_PID_FILE')
            if kill -0 \$PID 2>/dev/null; then
                kill \$PID
                echo '[stop_orion.sh] Stopped via PID file (PID '\$PID')'
                stopped=1
            else
                echo '[stop_orion.sh] Stale PID file: $NODE_PID_FILE'
            fi
            rm -f '$NODE_PID_FILE'
        fi

        for PID in \$(lsof -tiTCP:$NODE_PORT -sTCP:LISTEN 2>/dev/null || true); do
            COMM=\$(ps -p \$PID -o comm= 2>/dev/null | tr -d '[:space:]')
            if [ \"\$COMM\" = 'storage' ]; then
                kill \$PID
                echo '[stop_orion.sh] Stopped leftover listener on port $NODE_PORT (PID '\$PID')'
                stopped=1
            fi
        done

        if [ \$stopped -eq 0 ]; then
            echo '[stop_orion.sh] No storage process found on $NODE_HOST:$NODE_PORT'
        fi
    " &
done

wait
rm -f "$PID_DIR/remote_nodes.txt"
echo "[stop_orion.sh] All storage nodes stopped"
