# Potential Concurrency Issues with State Machine

This document outlines potential concurrency issues that may occur when using the cluster client with the connection state machine under high load.

## Overview

The connection state machine manages connection lifecycle through atomic state transitions:

```
CREATED → INITIALIZING → IDLE ⇄ IN_USE
                           ↓
                       UNUSABLE (handoff/reauth)
                           ↓
                        IDLE/CLOSED
```

## Potential Issues

### 1. Race Conditions in State Transitions

**Scenario**: Multiple goroutines trying to acquire the same connection simultaneously.

**What happens**:
- Thread A: Reads connection state as IDLE
- Thread B: Reads connection state as IDLE (before A transitions it)
- Thread A: Attempts IDLE → IN_USE transition (succeeds via CAS)
- Thread B: Attempts IDLE → IN_USE transition (fails via CAS)

**Current mitigation**: The code uses Compare-And-Swap (CAS) operations in `TryAcquire()` to ensure only one goroutine can successfully transition the connection. The losing goroutine will get a different connection or create a new one.

**Test**: Run `go run *.go -mode=detect` and look for the "Race Condition Detection" test results.

### 2. Pool Exhaustion Under High Concurrency

**Scenario**: Many goroutines competing for a small pool of connections.

**What happens**:
- All connections are IN_USE
- New requests wait for a connection to become available
- If pool timeout is too short, requests fail with pool timeout errors
- If pool timeout is too long, requests queue up and latency increases

**Current mitigation**: 
- Semaphore-based connection limiting with FIFO fairness
- Configurable pool timeout
- Pool size can be tuned per workload

**Test**: Run Example 2 or the "Extreme Contention" test to see this in action.

### 3. State Machine Deadlock (Theoretical)

**Scenario**: A connection gets stuck in an intermediate state.

**What could happen**:
- Connection transitions to UNUSABLE for handoff/reauth
- Background operation fails or hangs
- Connection never transitions back to IDLE
- Connection is stuck in pool but unusable

**Current mitigation**:
- Connections in UNUSABLE state are placed at the end of the idle queue
- Pool's `popIdle()` tries multiple connections (up to `popAttempts`)
- Health checks remove stale connections
- Timeouts on all operations

**Test**: The "Connection Churn" test exercises rapid state transitions.

### 4. Thundering Herd on Pool Initialization

**Scenario**: Many goroutines start simultaneously with an empty pool.

**What happens**:
- All goroutines call Get() at the same time
- Pool is empty, so all create new connections
- Potential to exceed pool size temporarily
- High initial latency spike

**Current mitigation**:
- Semaphore limits concurrent connection creation
- Pool size checks before creating connections
- MinIdleConns can pre-warm the pool

**Test**: Run the "Thundering Herd" test to see this behavior.

### 5. Connection Reuse Inefficiency

**Scenario**: Connections are not reused efficiently under bursty load.

**What happens**:
- Burst of requests creates many connections
- Burst ends, connections become idle
- Next burst might create new connections instead of reusing idle ones
- Pool size grows unnecessarily

**Current mitigation**:
- LIFO (default) or FIFO pool ordering
- MaxIdleConns limits idle connection count
- Idle connection health checks

**Test**: Run the "Bursty Traffic" test to observe this pattern.

## How to Identify Issues

### Symptoms of State Machine Issues

1. **High pool timeout rate**: More than 1-2% of operations timing out
2. **Increasing latency**: Average latency growing over time
3. **Error bursts**: Multiple errors occurring in quick succession
4. **Slow operations**: Operations taking >100ms consistently

### Using the Example App

```bash
# Run all tests
go run *.go -mode=all

# Focus on issue detection
go run *.go -mode=detect

# Advanced monitoring with latency distribution
go run *.go -mode=advanced
```

### What to Look For

**Good indicators**:
- Success rate >99%
- Average latency <10ms
- No pool timeouts (or very few)
- Latency distribution: most operations in 0-5ms range

**Warning signs**:
- Success rate <95%
- Average latency >50ms
- Pool timeouts >1% of operations
- Many operations in >50ms latency bucket
- Error bursts detected

## Recommendations

### For Production Use

1. **Size the pool appropriately**:
   - Start with `PoolSize = 10 * number of cluster nodes`
   - Monitor pool timeout rate
   - Increase if seeing >1% pool timeouts

2. **Set reasonable timeouts**:
   - `PoolTimeout`: 3-5 seconds (time to wait for a connection)
   - `ReadTimeout`: 3 seconds (time to read response)
   - `WriteTimeout`: 3 seconds (time to write command)

3. **Use MinIdleConns for steady load**:
   - Set to 20-30% of PoolSize
   - Pre-warms the pool
   - Reduces initial latency spikes

4. **Monitor metrics**:
   - Track pool timeout rate
   - Monitor average latency
   - Alert on error bursts

### Tuning for Different Workloads

**High throughput, low latency**:
```go
PoolSize:     20,
MinIdleConns: 5,
PoolTimeout:  2 * time.Second,
```

**Bursty traffic**:
```go
PoolSize:     30,
MinIdleConns: 10,
PoolTimeout:  5 * time.Second,
```

**Low traffic, resource constrained**:
```go
PoolSize:     5,
MinIdleConns: 0,
PoolTimeout:  3 * time.Second,
```

## Debugging Log Messages

### "Connection state changed by hook to IDLE/UNUSABLE, pooling as-is"

This message appears when the connection state is not IN_USE when `putConn()` tries to release it.

**What's happening**:
1. Connection is being returned to pool
2. Pool tries to transition IN_USE → IDLE
3. Transition fails because connection is already in a different state (IDLE or UNUSABLE)
4. Pool logs this message and pools the connection as-is

**Possible causes**:

1. **Hook changed state to UNUSABLE** (normal for handoff/reauth):
   - Maintenance notifications hook marks connection for handoff
   - Re-auth hook marks connection for re-authentication
   - Connection is pooled in UNUSABLE state for background processing

2. **Connection already in IDLE state** (potential issue):
   - Connection was released twice
   - Connection was never properly acquired
   - Race condition in connection lifecycle

**This is normal** when you see it occasionally (<1% of operations) with state=UNUSABLE.

**This indicates a problem** when:
- You see it on **every operation** or very frequently (>10%)
- The state is IDLE (not UNUSABLE)
- Pool timeout rate is high
- Operations are failing

**How to investigate**:
1. Check which state the connection is in (IDLE vs UNUSABLE)
2. If UNUSABLE: Check if handoff/reauth is completing
3. If IDLE: There may be a bug in connection lifecycle management

**How to reduce log verbosity**:
The example has maintenance notifications disabled but hooks may still be registered.
To completely silence these logs, you can set a custom logger that filters them out.

## Known Limitations

1. **No connection state visibility**: Can't easily inspect connection states from outside
2. **No per-node pool metrics**: Pool stats are aggregated across all nodes
3. **Limited backpressure**: No built-in circuit breaker or rate limiting
4. **Hook state transitions**: Hooks can change connection state during OnPut, which may cause confusion

## Testing Recommendations

1. **Load test before production**: Use this example app to test your specific workload
2. **Test failure scenarios**: Simulate node failures, network issues
3. **Monitor in staging**: Run with production-like load in staging first
4. **Gradual rollout**: Deploy to a subset of traffic first

## Further Reading

- `internal/pool/conn_state.go`: State machine implementation
- `internal/pool/pool.go`: Connection pool implementation
- `internal/pool/conn.go`: Connection with state machine
- `internal/semaphore.go`: Semaphore for connection limiting

