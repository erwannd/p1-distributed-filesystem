#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CONFIG="$ROOT/config.local.json"
BIN_DIR="$ROOT/bin"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/pids"

# Parse config with jq
CONTROLLER_PORT=$(jq -r '.controller.port' "$CONFIG")
BASE_DIR=$(jq -r '.storage.baseOutputDir' "$CONFIG")
NODE_COUNT=$(jq '.storage.nodes | length' "$CONFIG")

mkdir -p "$LOG_DIR" "$PID_DIR" "$ROOT/tmp/downloads"

# Build binaries first
echo "[start_local.sh] Building binaries..."
make -C "$ROOT" all

# Start Controller
echo "[start_local.sh] Starting Controller on port $CONTROLLER_PORT..."
"$BIN_DIR/controller" --config "$CONFIG" \
    > "$LOG_DIR/controller.log" 2>&1 &
echo $! > "$PID_DIR/controller.pid"
echo "[start_local.sh] Controller started (PID $(cat $PID_DIR/controller.pid))"

# Give Controller time to start
sleep 1

# Start Storage Nodes
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_PORT=$(jq -r ".storage.nodes[$i].port" "$CONFIG")

    echo "[start_local.sh] Starting Storage Node on port $NODE_PORT..."
    "$BIN_DIR/storage" \
        --config "$CONFIG" \
        --port "$NODE_PORT" \
        > "$LOG_DIR/storage_$NODE_PORT.log" 2>&1 &
    echo $! > "$PID_DIR/storage_$NODE_PORT.pid"
    echo "[start_local.sh] Storage Node $NODE_PORT started (PID $(cat $PID_DIR/storage_$NODE_PORT.pid))"
done

echo ""
echo "[start_local.sh] All components started!"
echo "[start_local.sh] Run 'make logs' to watch logs"
echo "[start_local.sh] Run 'make stop' to stop all components"