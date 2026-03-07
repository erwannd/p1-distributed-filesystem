#!/usr/bin/env bash

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PATH="$PATH:$(go env GOPATH)/bin:$HOME/go/bin"

protoc \
  --proto_path="$ROOT/proto" \
  --go_out="$ROOT/messages" \
  --go_opt=paths=source_relative \
  "$ROOT/proto/dfs.proto"