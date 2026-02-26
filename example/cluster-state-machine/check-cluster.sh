#!/bin/bash

# Script to check Redis cluster health on ports 16600-16605

echo "=== Redis Cluster Health Check ==="
echo ""

# Check if redis-cli is available
if ! command -v redis-cli &> /dev/null; then
    echo "❌ redis-cli not found. Please install redis-tools."
    exit 1
fi

# Check each port
echo "Checking connectivity to cluster nodes..."
REACHABLE_PORTS=()
for port in 16600 16601 16602 16603 16604 16605; do
    if redis-cli -p $port ping &> /dev/null; then
        echo "✓ Port $port is reachable"
        REACHABLE_PORTS+=($port)
    else
        echo "✗ Port $port is NOT reachable"
    fi
done

echo ""

if [ ${#REACHABLE_PORTS[@]} -eq 0 ]; then
    echo "❌ No cluster nodes are reachable!"
    echo ""
    echo "Solutions:"
    echo "1. Check if Docker containers are running:"
    echo "   docker ps | grep redis"
    echo ""
    echo "2. Start the cluster:"
    echo "   docker-compose up -d"
    exit 1
fi

# Check cluster state on first reachable port
PORT=${REACHABLE_PORTS[0]}
echo "Checking cluster state on port $PORT..."
echo ""

CLUSTER_STATE=$(redis-cli -p $PORT CLUSTER INFO 2>/dev/null | grep cluster_state | cut -d: -f2 | tr -d '\r')

if [ "$CLUSTER_STATE" = "ok" ]; then
    echo "✓ Cluster state: OK"
else
    echo "❌ Cluster state: $CLUSTER_STATE"
    echo ""
    echo "The cluster is not in OK state. This causes 'CLUSTERDOWN Hash slot not served' errors."
    echo ""
    echo "Cluster Info:"
    redis-cli -p $PORT CLUSTER INFO
    echo ""
    echo "Cluster Nodes:"
    redis-cli -p $PORT CLUSTER NODES
    echo ""
    echo "Solutions:"
    echo ""
    echo "1. Check if all hash slots are assigned:"
    echo "   redis-cli -p $PORT CLUSTER SLOTS"
    echo ""
    echo "2. If cluster was never initialized, create it:"
    echo "   redis-cli --cluster create \\"
    echo "     localhost:16600 localhost:16601 localhost:16602 \\"
    echo "     localhost:16603 localhost:16604 localhost:16605 \\"
    echo "     --cluster-replicas 1 --cluster-yes"
    echo ""
    echo "3. If cluster is in failed state, try fixing it:"
    echo "   redis-cli --cluster fix localhost:$PORT"
    echo ""
    echo "4. If nothing works, reset and recreate:"
    echo "   docker-compose down -v"
    echo "   docker-compose up -d"
    echo "   # Wait a few seconds, then create cluster"
    exit 1
fi

# Check slot coverage
echo ""
echo "Checking hash slot coverage..."
SLOTS_OUTPUT=$(redis-cli -p $PORT CLUSTER SLOTS 2>/dev/null)

if [ -z "$SLOTS_OUTPUT" ]; then
    echo "❌ No hash slots assigned!"
    echo ""
    echo "The cluster needs to be initialized. Run:"
    echo "  redis-cli --cluster create \\"
    echo "    localhost:16600 localhost:16601 localhost:16602 \\"
    echo "    localhost:16603 localhost:16604 localhost:16605 \\"
    echo "    --cluster-replicas 1 --cluster-yes"
    exit 1
else
    echo "✓ Hash slots are assigned"
fi

# Show cluster nodes
echo ""
echo "Cluster Nodes:"
redis-cli -p $PORT CLUSTER NODES

echo ""
echo "=== Cluster is healthy and ready! ==="
echo ""
echo "You can now run the example:"
echo "  ./run.sh basic"

