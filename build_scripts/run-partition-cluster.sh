#!/bin/bash

NETWORK=$(docker network ls | grep omnipaxos | awk '{print $2}' | head -1)
echo "Using Docker network: $NETWORK"

partition() {
    echo ""
    echo "========================================="
    echo "=== PARTITION MODE: Split Half        ==="
    echo "=== s3 cut off from s1 and s2         ==="
    echo "=== Topology: [s1] ←→ [s2]  |  [s3]  ==="
    echo "========================================="
    docker network disconnect $NETWORK s3
    echo "  → s3 disconnected from Docker network"
}

heal() {
    echo ""
    echo "=== HEALING all partitions ==="
    docker network connect $NETWORK s3 2>/dev/null
    echo "  ✓ s3 reconnected — cluster fully connected"
    echo ""
}

trap heal EXIT

echo "========================================="
echo "=== OmniPaxos Partition Nemesis START ==="
echo "=== Using Docker network disconnect    ==="
echo "=== Partition duration: 7s             ==="
echo "=== Interval: 5-14s                    ==="
echo "========================================="

while true; do
    WAIT=$((RANDOM % 10 + 5))
    echo "--- Sleeping ${WAIT}s before next partition ---"
    sleep $WAIT
    partition
    echo "Partition active, sleeping 7s..."
    sleep 7
    heal
    echo "--- Cluster healed ---"
done