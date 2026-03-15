bash#!/bin/bash

rust_log="info"
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

echo "Starting gateway only (servers are in Docker)..."

# Wait for Docker servers to be ready
echo "Waiting for Docker servers to be ready..."
sleep 3

# Run only the gateway
RUST_LOG=$rust_log cargo run --manifest-path="../Cargo.toml" --bin gateway &

wait