#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CONFIG="$ROOT/config.orion.json"

OUTPUT_DIR="/bigdata/students/$(whoami)"
LOG_DIR="$OUTPUT_DIR/logs"
PID_DIR="$OUTPUT_DIR/pids"

SNAPSHOT_PATH=$(jq -r '.controller.snapshotPath' "$CONFIG")
STORAGE_BASE=$(jq -r '.storage.baseOutputDir' "$CONFIG")
DOWNLOAD_DIR=$(jq -r '.client.outputDir' "$CONFIG")
NODE_COUNT=$(jq '.storage.nodes | length' "$CONFIG")

echo "[clean_orion.sh] Stopping Orion processes first..."
"$SCRIPT_DIR/stop_orion.sh" || true

echo "[clean_orion.sh] Removing controller snapshot artifacts..."
rm -f "$SNAPSHOT_PATH" "${SNAPSHOT_PATH}.tmp"

echo "[clean_orion.sh] Removing controller runtime files..."
rm -f "$LOG_DIR/controller.log" \
      "$PID_DIR/controller.pid" \
      "$PID_DIR/remote_nodes.txt"

echo "[clean_orion.sh] Removing client download directory..."
rm -rf "$DOWNLOAD_DIR"

for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_HOST=$(jq -r ".storage.nodes[$i].host" "$CONFIG")
    NODE_PORT=$(jq -r ".storage.nodes[$i].port" "$CONFIG")
    NODE_LOG="$LOG_DIR/storage_${NODE_HOST}_${NODE_PORT}.log"
    NODE_PID="$PID_DIR/storage_${NODE_HOST}_${NODE_PORT}.pid"
    NODE_STORAGE_DIR="$STORAGE_BASE/$NODE_HOST/node_${NODE_PORT}"

    echo "[clean_orion.sh] Cleaning $NODE_HOST:$NODE_PORT..."
    ssh "$NODE_HOST" "
        rm -f '$NODE_LOG' '$NODE_PID'
        rm -rf '$NODE_STORAGE_DIR'
        rmdir '$STORAGE_BASE/$NODE_HOST' 2>/dev/null || true
        rmdir '$LOG_DIR' 2>/dev/null || true
        rmdir '$PID_DIR' 2>/dev/null || true
    "
done

echo "[clean_orion.sh] Removing empty local runtime directories when possible..."
rmdir "$LOG_DIR" 2>/dev/null || true
rmdir "$PID_DIR" 2>/dev/null || true
rmdir "$(dirname "$SNAPSHOT_PATH")" 2>/dev/null || true
rmdir "$(dirname "$DOWNLOAD_DIR")" 2>/dev/null || true
rmdir "$STORAGE_BASE" 2>/dev/null || true

echo "[clean_orion.sh] Orion runtime artifacts removed"
