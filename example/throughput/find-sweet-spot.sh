#!/bin/bash

# Script to find the optimal configuration for maximum throughput
# Usage: ./find-sweet-spot.sh [host] [port]

HOST=${1:-localhost}
PORT=${2:-6379}
DURATION=5s

echo "=== Finding Sweet Spot for Redis Throughput ==="
echo "Host: $HOST:$PORT"
echo "Test duration: $DURATION per configuration"
echo ""

# Build the tool if not already built
if [ ! -f ./throughput ]; then
    echo "Building throughput tool..."
    go build
    echo ""
fi

# Test different configurations
declare -a RESULTS=()

test_config() {
    local workers=$1
    local pipeline_size=$2
    local pipeline_delay=$3
    
    echo -n "Testing: workers=$workers, batch=$pipeline_size, delay=$pipeline_delay ... "
    
    # Run the test and extract throughput
    output=$(./throughput -host "$HOST" -port "$PORT" -duration "$DURATION" \
        -workers "$workers" -autopipeline \
        -pipeline-size "$pipeline_size" -pipeline-delay "$pipeline_delay" \
        -report-interval 999s 2>&1)
    
    throughput=$(echo "$output" | grep "Throughput:" | tail -1 | awk '{print $2}')
    
    if [ -n "$throughput" ]; then
        echo "$throughput ops/sec"
        RESULTS+=("$throughput|$workers|$pipeline_size|$pipeline_delay")
    else
        echo "FAILED"
    fi
}

echo "Phase 1: Testing different worker counts (batch=500, delay=0)"
echo "=============================================================="
for workers in 100 200 300 500 800 1000; do
    test_config $workers 500 0
done
echo ""

echo "Phase 2: Testing different batch sizes (workers=500, delay=0)"
echo "=============================================================="
for batch in 100 200 500 1000 2000; do
    test_config 500 $batch 0
done
echo ""

echo "Phase 3: Testing different delays (workers=500, batch=1000)"
echo "=============================================================="
for delay in 0 10us 50us 100us 500us; do
    test_config 500 1000 $delay
done
echo ""

# Find the best configuration
echo "=== Results Summary ==="
echo ""
printf "%-15s %-10s %-12s %-15s\n" "Throughput" "Workers" "Batch Size" "Delay"
printf "%-15s %-10s %-12s %-15s\n" "----------" "-------" "----------" "-----"

best_throughput=0
best_config=""

for result in "${RESULTS[@]}"; do
    IFS='|' read -r throughput workers batch delay <<< "$result"
    printf "%-15s %-10s %-12s %-15s\n" "$throughput" "$workers" "$batch" "$delay"
    
    # Compare throughput (remove commas if any)
    throughput_num=$(echo "$throughput" | tr -d ',')
    if (( $(echo "$throughput_num > $best_throughput" | bc -l) )); then
        best_throughput=$throughput_num
        best_config="workers=$workers, batch=$batch, delay=$delay"
    fi
done

echo ""
echo "=== Best Configuration ==="
echo "Throughput: $best_throughput ops/sec"
echo "Config: $best_config"
echo ""
echo "To run with best config:"
echo "./throughput -host $HOST -port $PORT -autopipeline \\"
echo "  $(echo $best_config | sed 's/, / -/g' | sed 's/=/ /g' | sed 's/workers/-workers/; s/batch/-pipeline-size/; s/delay/-pipeline-delay/')"

