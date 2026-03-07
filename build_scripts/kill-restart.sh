#!/bin/bash

PID=$(lsof -ti:8001 -sTCP:LISTEN)

if [ -z "$PID" ]; then
    echo "No process found listening on port 8001"
    exit 1
fi

echo "Killing server 1 (PID: $PID) on port 8001"
kill -9 $PID
echo "Server 1 killed, restarting in 5 seconds..."
sleep 5

echo "Restarting server 1..."
RUST_LOG=info \
SERVER_CONFIG_FILE=./server-1-config.toml \
CLUSTER_CONFIG_FILE=./cluster-config.toml \
cargo run --manifest-path="../Cargo.toml" --bin server &

echo "Server 1 restarted (PID: $!)"