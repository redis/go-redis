# Quick Start Guide

## Running Against Your Docker Cluster

### Step 0: Initialize the Cluster (First Time Only)

If this is the first time running the cluster, you need to initialize it:

```bash
cd example/cluster-state-machine

# Check cluster health
./check-cluster.sh

# If cluster is not initialized (state: fail), initialize it
./init-cluster.sh

# Verify cluster is ready
./check-cluster.sh
```

**Expected output:**
```
✓ Cluster state: OK
✓ Hash slots are assigned
```

### Step 1: Find Your Cluster Ports

```bash
# List running Redis containers
docker ps | grep redis

# Example output:
# CONTAINER ID   IMAGE         PORTS                      NAMES
# abc123def456   redis:latest  0.0.0.0:16600->6379/tcp   redis-node-1
# def456ghi789   redis:latest  0.0.0.0:16601->6379/tcp   redis-node-2
# ghi789jkl012   redis:latest  0.0.0.0:16602->6379/tcp   redis-node-3
# jkl012mno345   redis:latest  0.0.0.0:16603->6379/tcp   redis-node-4
# mno345pqr678   redis:latest  0.0.0.0:16604->6379/tcp   redis-node-5
# pqr678stu901   redis:latest  0.0.0.0:16605->6379/tcp   redis-node-6
```

### Step 2: Run the Example

```bash
cd example/cluster-state-machine

# Basic test (default) - using all 6 nodes
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602,localhost:16603,localhost:16604,localhost:16605"

# Or use just the master nodes (typically first 3)
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602"

# Advanced monitoring
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=advanced

# Issue detection
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=detect

# Run all tests
go run *.go -addrs="localhost:16600,localhost:16601,localhost:16602" -mode=all
```

### Step 3: Interpret Results

**Good Results:**
```
✓ Completed in 2.5s
  Total operations: 5000
  Successful: 5000 (100.00%)
  Failed: 0 (0.00%)
  Pool timeouts: 0 (0.00%)
  Average latency: 2.3ms
  Throughput: 2000 ops/sec
```

**Warning Signs:**
```
⚠️  Completed in 15.2s
  Total operations: 5000
  Successful: 4750 (95.00%)
  Failed: 250 (5.00%)
  Pool timeouts: 150 (3.00%)
  Average latency: 45.7ms
  Throughput: 328 ops/sec
```

## Common Issues

### Issue: "CLUSTERDOWN Hash slot not served"

**Problem:** Cluster is not initialized or in failed state

**Solution:**
```bash
cd example/cluster-state-machine

# Check cluster health
./check-cluster.sh

# If cluster state is "fail", initialize it
./init-cluster.sh

# Wait a few seconds and verify
sleep 3
./check-cluster.sh
```

### Issue: "connection refused"

**Problem:** Can't connect to Redis cluster

**Solution:**
```bash
# Check if cluster is running
docker ps | grep redis

# Check if ports are correct
docker port <container-name>

# Try connecting with redis-cli
redis-cli -c -p 16600 ping

# Or test each node
for port in 16600 16601 16602 16603 16604 16605; do
  echo "Testing port $port..."
  redis-cli -c -p $port ping
done
```

### Issue: "pool timeout" errors

**Problem:** Too many concurrent requests for pool size

**Solutions:**
1. Increase pool size in the example code
2. Reduce number of concurrent goroutines
3. Check if cluster is overloaded

### Issue: "Connection state changed by hook to UNUSABLE"

**Problem:** Maintenance notifications hook is marking connections for handoff

**This is normal** if:
- You see it occasionally (<1% of operations)
- Operations still succeed
- No performance degradation

**This is a problem** if:
- You see it very frequently (>10% of operations)
- Many operations are failing
- High latency

**Solution:**
- Maintenance notifications are disabled in the example by default
- If you're still seeing this, check if you have streaming auth enabled
- Increase pool size to handle UNUSABLE connections

## Understanding the Logs

### Normal Logs

```
redis: 2025/10/27 18:10:57 pool.go:691: Connection state changed by hook to IDLE, pooling as-is
```
This is informational - the hook changed the state before the pool could transition it.

### Error Logs

```
redis: 2025/10/27 18:10:58 pool.go:393: redis: connection pool: failed to dial after 5 attempts: dial tcp :7000: connect: connection refused
```
This means the cluster is not reachable. Check your Docker containers and ports.

```
redis: 2025/10/27 18:10:59 pool.go:621: redis: connection pool: failed to get a usable connection after 5 attempts
```
This means all connections in the pool are UNUSABLE. This could indicate:
- Handoff operations are stuck
- Re-auth operations are failing
- Connections are in bad state

## Debugging Tips

### Enable Verbose Logging

Set the log level to see more details:

```go
// In your test code
redis.SetLogger(redis.NewLogger(os.Stderr))
```

### Monitor Pool Stats

Add this to the example to see pool statistics:

```go
stats := client.PoolStats()
fmt.Printf("Pool Stats:\n")
fmt.Printf("  Hits: %d\n", stats.Hits)
fmt.Printf("  Misses: %d\n", stats.Misses)
fmt.Printf("  Timeouts: %d\n", stats.Timeouts)
fmt.Printf("  TotalConns: %d\n", stats.TotalConns)
fmt.Printf("  IdleConns: %d\n", stats.IdleConns)
fmt.Printf("  StaleConns: %d\n", stats.StaleConns)
```

### Check Cluster Health

```bash
# Connect to cluster
redis-cli -c -p 16600

# Check cluster info
CLUSTER INFO

# Check cluster nodes
CLUSTER NODES

# Check if all slots are covered
CLUSTER SLOTS

# Check cluster state
CLUSTER INFO | grep cluster_state
```

## Performance Tuning

### For High Throughput

```go
PoolSize:     20,
MinIdleConns: 5,
PoolTimeout:  2 * time.Second,
```

### For Bursty Traffic

```go
PoolSize:     30,
MinIdleConns: 10,
PoolTimeout:  5 * time.Second,
```

### For Low Latency

```go
PoolSize:     15,
MinIdleConns: 5,
PoolTimeout:  1 * time.Second,
ReadTimeout:  1 * time.Second,
WriteTimeout: 1 * time.Second,
```

## Next Steps

1. Run the basic test to establish a baseline
2. Run the advanced test to see latency distribution
3. Run the detect test to find potential issues
4. Adjust pool size and timeouts based on results
5. Test with your actual workload patterns

For more details, see:
- [README.md](README.md) - Full documentation
- [POTENTIAL_ISSUES.md](POTENTIAL_ISSUES.md) - Detailed issue analysis

