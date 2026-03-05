#!/bin/bash

kill_port() {
    local PORT=$1
    local PID=$(lsof -ti :$PORT)
    if [ -n "$PID" ]; then
        kill -9 $PID 2>/dev/null
    fi
}

while true; do
    sleep $((RANDOM % 10 + 5))

    SERVER=$((RANDOM % 3 + 1))
    PORT=$((8000 + SERVER))
    echo "Killing server $SERVER (port $PORT)"

    kill_port $PORT

    # Wait for port to be free
    echo "Waiting for port $PORT to be free..."
    for i in $(seq 1 10); do
        sleep 0.5
        lsof -i :$PORT > /dev/null 2>&1 || break
    done

    echo "Restarting server $SERVER"
    RUST_LOG=info \
        SERVER_CONFIG_FILE=./server-${SERVER}-config.toml \
        CLUSTER_CONFIG_FILE=./cluster-config.toml \
        ../target/debug/server >> ./logs/server-${SERVER}-nemesis.log 2>&1 &

    echo "Server $SERVER restarted (PID $!)"
done