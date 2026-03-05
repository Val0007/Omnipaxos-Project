#!/bin/bash

cluster_size=3
rust_log="${RUST_LOG:-info}"

# Nemesis knobs (can be overridden by env before running script).
nemesis_mode="${NEMESIS_MODE:-alternate}"              # leader_isolation | split_half | alternate
nemesis_start_delay_ms="${NEMESIS_START_DELAY_MS:-5000}"
nemesis_interval_ms="${NEMESIS_INTERVAL_MS:-12000}"
nemesis_active_ms="${NEMESIS_ACTIVE_MS:-7000}"

interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

cluster_config_path="./cluster-config.toml"
for ((i = 1; i <= cluster_size; i++)); do
    server_config_path="./server-${i}-config.toml"
    RUST_LOG=$rust_log \
    SERVER_CONFIG_FILE=$server_config_path \
    CLUSTER_CONFIG_FILE=$cluster_config_path \
    OMNIPAXOS_NEMESIS_ENABLED=true \
    OMNIPAXOS_NEMESIS_MODE=$nemesis_mode \
    OMNIPAXOS_NEMESIS_START_DELAY_MS=$nemesis_start_delay_ms \
    OMNIPAXOS_NEMESIS_INTERVAL_MS=$nemesis_interval_ms \
    OMNIPAXOS_NEMESIS_ACTIVE_MS=$nemesis_active_ms \
    cargo run --manifest-path="../Cargo.toml" --bin server &
done

sleep 3
RUST_LOG=$rust_log cargo run --manifest-path="../Cargo.toml" --bin gateway &

echo "Partition nemesis enabled:"
echo "  mode=$nemesis_mode"
echo "  start_delay_ms=$nemesis_start_delay_ms"
echo "  interval_ms=$nemesis_interval_ms"
echo "  active_ms=$nemesis_active_ms"

wait
