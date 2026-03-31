#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CONFIG="$ROOT/config.orion.json"
OUTPUT_DIR="/bigdata/students/$(whoami)"
LOG_DIR="$OUTPUT_DIR/logs"

mkdir -p "$LOG_DIR"

NODE_COUNT=$(jq '.storage.nodes | length' "$CONFIG")

# Tail controller log locally
tail -f "$LOG_DIR/controller.log" &

# Tail each storage node log via SSH
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_HOST=$(jq -r ".storage.nodes[$i].host" "$CONFIG")
    NODE_PORT=$(jq -r ".storage.nodes[$i].port" "$CONFIG")

    ssh "$NODE_HOST" \
        "tail -f '$LOG_DIR/storage_${NODE_HOST}_${NODE_PORT}.log'" \
        2>/dev/null &
done

# Wait for Ctrl+C
trap "kill 0" SIGINT
wait
