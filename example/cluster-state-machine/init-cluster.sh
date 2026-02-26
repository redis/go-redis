#!/bin/bash

# Script to initialize Redis cluster on ports 16600-16605

echo "=== Initializing Redis Cluster ==="
echo ""

# Check if redis-cli is available
if ! command -v redis-cli &> /dev/null; then
    echo "❌ redis-cli not found. Please install redis-tools."
    exit 1
fi

# Check connectivity
echo "Checking connectivity to all nodes..."
for port in 16600 16601 16602 16603 16604 16605; do
    if redis-cli -p $port ping &> /dev/null; then
        echo "✓ Port $port is reachable"
    else
        echo "❌ Port $port is NOT reachable"
        echo ""
        echo "Make sure all Redis nodes are running:"
        echo "  docker ps | grep redis"
        exit 1
    fi
done

echo ""
echo "Creating cluster with 3 masters and 3 replicas..."
echo ""
echo "This will configure:"
echo "  - Masters: 16600, 16601, 16602"
echo "  - Replicas: 16603, 16604, 16605"
echo ""

# Create the cluster
redis-cli --cluster create \
  localhost:16600 localhost:16601 localhost:16602 \
  localhost:16603 localhost:16604 localhost:16605 \
  --cluster-replicas 1 \
  --cluster-yes

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Cluster created successfully!"
    echo ""
    echo "Verifying cluster state..."
    sleep 2
    
    CLUSTER_STATE=$(redis-cli -p 16600 CLUSTER INFO | grep cluster_state | cut -d: -f2 | tr -d '\r')
    
    if [ "$CLUSTER_STATE" = "ok" ]; then
        echo "✓ Cluster state: OK"
        echo ""
        echo "Cluster is ready! You can now run the example:"
        echo "  ./run.sh basic"
    else
        echo "⚠ Cluster state: $CLUSTER_STATE"
        echo "You may need to wait a few seconds for the cluster to stabilize."
    fi
else
    echo ""
    echo "❌ Failed to create cluster"
    echo ""
    echo "Troubleshooting:"
    echo "1. Make sure all nodes are empty (no data)"
    echo "2. Try resetting the nodes:"
    echo "   for port in 16600 16601 16602 16603 16604 16605; do"
    echo "     redis-cli -p \$port FLUSHALL"
    echo "     redis-cli -p \$port CLUSTER RESET"
    echo "   done"
    echo "3. Then run this script again"
    exit 1
fi

