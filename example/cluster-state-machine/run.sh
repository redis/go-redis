#!/bin/bash

# Quick run script for cluster state machine example
# Usage: ./run.sh [mode]
# Modes: basic, advanced, detect, all

# Default cluster addresses (ports 16600-16605)
ADDRS="localhost:16600,localhost:16601,localhost:16602,localhost:16603,localhost:16604,localhost:16605"

# Get mode from argument or use default
MODE="${1:-basic}"

echo "=== Running Cluster State Machine Example ==="
echo "Cluster addresses: $ADDRS"
echo "Mode: $MODE"
echo ""

# Check if cluster is reachable
echo "Checking cluster connectivity..."
if command -v redis-cli &> /dev/null; then
    for port in 16600 16601 16602; do
        if redis-cli -p $port ping &> /dev/null; then
            echo "✓ Port $port is reachable"
        else
            echo "✗ Port $port is NOT reachable"
            echo ""
            echo "Make sure your Redis cluster is running on ports 16600-16605"
            echo "Check with: docker ps | grep redis"
            exit 1
        fi
    done
    echo ""
else
    echo "⚠ redis-cli not found, skipping connectivity check"
    echo ""
fi

# Run the example
echo "Running tests..."
echo ""
go run *.go -addrs="$ADDRS" -mode="$MODE"

echo ""
echo "=== Done ==="

