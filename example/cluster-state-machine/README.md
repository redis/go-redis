# Redis Cluster State Machine Example

This example demonstrates the connection state machine behavior in the Redis cluster client under high concurrency.

## What This Example Shows

1. **Basic Concurrent Operations**: Multiple goroutines performing SET operations concurrently
2. **High Concurrency Stress Test**: Limited pool size with many concurrent goroutines to stress the state machine
3. **Connection Pool Behavior**: Monitoring connection reuse and state transitions over time
4. **Mixed Read/Write Workload**: Realistic workload with both reads and writes

## Connection State Machine

The connection state machine manages connection lifecycle:

```
CREATED → INITIALIZING → IDLE ⇄ IN_USE
                           ↓
                       UNUSABLE (handoff/reauth)
                           ↓
                        IDLE/CLOSED
```

### States

- **CREATED**: Connection just created, not yet initialized
- **INITIALIZING**: Connection initialization in progress
- **IDLE**: Connection initialized and idle in pool, ready to be acquired
- **IN_USE**: Connection actively processing a command
- **UNUSABLE**: Connection temporarily unusable (handoff, reauth, etc.)
- **CLOSED**: Connection closed

## Running the Example

### Prerequisites

You need a Redis cluster running.

**Option 1: Use existing Docker cluster**

If you already have a Redis cluster running in Docker:

```bash
# Find your cluster ports
docker ps | grep redis

# Note the ports (e.g., 16600, 16601, 16602, etc.)
```

**Option 2: Start a new cluster**

```bash
# From the go-redis root directory
docker-compose up -d

# This will start a cluster on ports 16600-16605
```

### Run the Example

**Quick Start (using run.sh script):**

```bash
cd example/cluster-state-machine

# Run basic tests (default, uses ports 16600-16605)
./run.sh

# Run specific mode
./run.sh basic
./run.sh advanced
./run.sh detect
./run.sh all
```

**Manual Run:**

```bash
cd example/cluster-state-machine

# Run with default addresses (localhost:6379)
go run *.go

# Run with Docker cluster addresses (ports 16600-16605)
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602,localhost:16603,localhost:16604,localhost:16605"

# Or use a subset of nodes
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602"

# Specify test mode
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=basic
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=advanced
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=detect
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=all
```

**Available flags:**
- `-addrs`: Comma-separated Redis addresses (default: "localhost:6379")
- `-mode`: Test mode - basic, advanced, detect, or all (default: "basic")

## What to Look For

### Normal Behavior

- High success rate (>99%)
- Low latency (typically <10ms)
- Few or no pool timeouts
- Efficient connection reuse

### Potential Issues

If you see:

1. **High pool timeout rate**: Pool size may be too small for the workload
2. **High failure rate**: Could indicate connection state machine issues
3. **Increasing latency**: May indicate connection contention or state transition delays
4. **Many pool timeouts in Example 2**: This is expected due to intentionally small pool size

## Understanding the Metrics

- **Total Operations**: Total number of Redis operations attempted
- **Successful**: Operations that completed successfully
- **Failed**: Operations that failed (excluding timeouts)
- **Timeouts**: Operations that timed out
- **Pool Timeouts**: Number of times we couldn't acquire a connection from the pool
- **Avg Latency**: Average latency for successful operations

## Tuning Parameters

You can modify these parameters in the code to experiment:

- `PoolSize`: Number of connections per cluster node
- `PoolTimeout`: How long to wait for a connection from the pool
- `MinIdleConns`: Minimum number of idle connections to maintain
- `numGoroutines`: Number of concurrent goroutines
- `opsPerGoroutine`: Number of operations per goroutine

## Test Modes

### Basic Mode (default)

Runs 4 examples demonstrating normal usage patterns:
1. **Basic Concurrent Operations**: 50 goroutines, 100 ops each
2. **High Concurrency Stress**: 100 goroutines with small pool (5 connections)
3. **Connection Pool Behavior**: 20 workers running for 5 seconds
4. **Mixed Read/Write**: 30 goroutines with 70/30 read/write ratio

### Advanced Mode

Includes detailed latency distribution and state monitoring:
1. **Extreme Contention**: 200 goroutines with pool size of 2
2. **Rapid Cycles**: 50 goroutines doing rapid-fire operations
3. **Long-Running Operations**: Pipeline operations with delays
4. **Concurrent Pipelines**: Multiple pipelines executing simultaneously

### Detect Mode

Runs tests specifically designed to expose concurrency issues:
1. **Thundering Herd**: All goroutines start simultaneously
2. **Bursty Traffic**: Alternating high/low load patterns
3. **Connection Churn**: Rapidly creating and closing clients
4. **Race Condition Detection**: Mixed operations with high contention

## Common Scenarios

### Scenario 1: Pool Exhaustion

Example 2 intentionally creates pool exhaustion by using a small pool (5 connections) with many goroutines (100). This tests:
- Connection state machine under contention
- Pool timeout handling
- Connection reuse efficiency

### Scenario 2: Sustained Load

Example 3 runs workers continuously for 5 seconds, testing:
- Connection lifecycle management
- State transitions over time
- Connection health checks

### Scenario 3: Mixed Workload

Example 4 uses a realistic 70/30 read/write ratio, testing:
- State machine with different operation types
- Connection reuse patterns
- Concurrent read/write handling

## Debugging Tips

If you encounter issues:

1. **Enable debug logging**: Set `REDIS_DEBUG=1` environment variable
2. **Reduce concurrency**: Lower `numGoroutines` to isolate issues
3. **Increase pool size**: If seeing many pool timeouts
4. **Check cluster health**: Ensure all cluster nodes are responsive
5. **Monitor connection states**: Add logging to track state transitions

## Expected Output

```
=== Redis Cluster State Machine Example ===

Example 1: Basic Concurrent Operations
---------------------------------------
✓ Completed 5000 operations from 50 goroutines in 1.2s
  Throughput: 4166 ops/sec

=== Metrics ===
Total Operations:    5000
Successful:          5000 (100.00%)
Failed:              0 (0.00%)
Timeouts:            0 (0.00%)
Pool Timeouts:       0
Avg Latency:         2.4ms

Example 2: High Concurrency Stress Test
----------------------------------------
✓ Completed stress test in 3.5s
  Throughput: 1428 ops/sec

=== Metrics ===
Total Operations:    5000
Successful:          4950 (99.00%)
Failed:              0 (0.00%)
Timeouts:            50 (1.00%)
Pool Timeouts:       50
Avg Latency:         8.2ms

...
```

## Related Files

- `internal/pool/conn_state.go`: Connection state machine implementation
- `internal/pool/pool.go`: Connection pool implementation
- `internal/pool/conn.go`: Connection implementation with state machine
- `osscluster.go`: Cluster client implementation

