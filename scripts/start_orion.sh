#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CONFIG="$ROOT/config.orion.json"
BIN_DIR="$ROOT/bin"

OUTPUT_DIR="/bigdata/students/$(whoami)"
LOG_DIR="$OUTPUT_DIR/logs"
PID_DIR="$OUTPUT_DIR/pids"

CONTROLLER_HOST=$(jq -r '.controller.host' "$CONFIG")
CONTROLLER_PORT=$(jq -r '.controller.port' "$CONFIG")
NODE_COUNT=$(jq '.storage.nodes | length' "$CONFIG")

mkdir -p "$LOG_DIR" "$PID_DIR"
: > "$PID_DIR/remote_nodes.txt"

echo "[start_orion.sh] Building binaries..."
make -C "$ROOT" all

echo "[start_orion.sh] Starting Controller on $CONTROLLER_HOST:$CONTROLLER_PORT..."
"$BIN_DIR/controller" --config "$CONFIG" \
    > "$LOG_DIR/controller.log" 2>&1 &
echo $! > "$PID_DIR/controller.pid"

sleep 1

for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_HOST=$(jq -r ".storage.nodes[$i].host" "$CONFIG")
    NODE_PORT=$(jq -r ".storage.nodes[$i].port" "$CONFIG")

    echo "[start_orion.sh] Starting Storage Node on $NODE_HOST:$NODE_PORT..."

    ssh -fn "$NODE_HOST" "
        mkdir -p '$LOG_DIR' '$PID_DIR'
        '$BIN_DIR/storage' \
            --config '$CONFIG' \
            --port $NODE_PORT \
            > '$LOG_DIR/storage_${NODE_HOST}_${NODE_PORT}.log' 2>&1 &
        echo \$! > '$PID_DIR/storage_${NODE_HOST}_${NODE_PORT}.pid'
    "

    echo "$NODE_HOST:$PID_DIR/storage_${NODE_HOST}_${NODE_PORT}.pid" \
        >> "$PID_DIR/remote_nodes.txt"
done
