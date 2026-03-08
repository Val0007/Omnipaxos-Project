#!/bin/bash

kill_port() {
    local PORT=$1
    local PID=$(lsof -ti :$PORT -sTCP:LISTEN)
    if [ -n "$PID" ]; then
        kill -9 $PID 2>/dev/null
    fi
}

while true; do
    SERVER=$((RANDOM % 3 + 1))
    PORT=$((8000 + SERVER))
    echo "Killing server $SERVER (port $PORT)"

    kill_port $PORT

    echo "Waiting 3 seconds before restart..."
    sleep 3

    echo "Restarting server $SERVER"
    RUST_LOG=info \
        SERVER_CONFIG_FILE=./server-${SERVER}-config.toml \
        CLUSTER_CONFIG_FILE=./cluster-config.toml \
        ../target/debug/server >> ./logs/server-${SERVER}-nemesis.log 2>&1 &

    echo "Server $SERVER restarted (PID $!)"
done