# Hitless Upgrades

This package provides hitless upgrade functionality for Redis clients, enabling seamless connection migration during Redis server upgrades, failovers, or cluster topology changes without dropping active connections.

## How It Works

The hitless upgrade system integrates with go-redis through pool hooks:

1. **Push Notifications**: Redis sends RESP3 push notifications for topology changes
2. **Connection Marking**: Affected connections are marked for handoff
3. **Pool Integration**: Marked connections are queued for handoff when returned to pool
4. **Async Migration**: Worker goroutines perform connection handoffs in background
5. **Connection Replacement**: Old connections are replaced with new ones to target endpoints

### Push Notification Types
- `MOVING` - Connection handoff to new endpoint
- `MIGRATING` - Apply relaxed timeouts during migration
- `MIGRATED` - Clear relaxed timeouts after migration
- `FAILING_OVER` - Apply relaxed timeouts during failover
- `FAILED_OVER` - Clear relaxed timeouts after failover

## Configuration Examples

### Basic Setup
```go
import "github.com/redis/go-redis/v9"

opt := &redis.Options{
    Addr:     "localhost:6379",
    Protocol: 3, // RESP3 required for push notifications
    HitlessUpgrades: &redis.HitlessUpgradeConfig{
        Enabled: true,
    },
}

client := redis.NewClient(opt)
defer client.Close()
```

### Advanced Configuration
```go
import "github.com/redis/go-redis/v9/hitless"

opt := &redis.Options{
    Addr:     "localhost:6379",
    Protocol: 3,
    HitlessUpgrades: &redis.HitlessUpgradeConfig{
        Enabled: true,
        Config: &hitless.Config{
            MaxRetries:           3,
            RetryDelay:          time.Second,
            HandoffTimeout:      30 * time.Second,
            RelaxedTimeout:      10 * time.Second,
            PostHandoffDuration: 5 * time.Second,
            LogLevel:            1,
            MaxWorkers:          10,
        },
    },
}

client := redis.NewClient(opt)
defer client.Close()
```

### Cluster Client
```go
cluster := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs:    []string{"localhost:7000", "localhost:7001", "localhost:7002"},
    Protocol: 3,
    HitlessUpgrades: &redis.HitlessUpgradeConfig{
        Enabled: true,
    },
})
defer cluster.Close()
```

## Hook Examples

### Custom Pool Hook
```go
package main

import (
    "context"
    "log"

    "github.com/redis/go-redis/v9/internal/pool"
)

// Custom hook that logs connection events
type LoggingHook struct {
    name string
}

func (lh *LoggingHook) OnGet(ctx context.Context, conn *pool.Conn, isNewConn bool) error {
    log.Printf("Hook %s: Getting connection %d (new: %v)", lh.name, conn.GetID(), isNewConn)
    return nil
}

func (lh *LoggingHook) OnPut(ctx context.Context, conn *pool.Conn) (shouldPool bool, shouldRemove bool, err error) {
    log.Printf("Hook %s: Putting connection %d back to pool", lh.name, conn.GetID())
    return true, false, nil // Pool the connection, don't remove it
}

func main() {
    // Create pool with custom hook
    opt := &pool.Options{
        Dialer: func(ctx context.Context) (net.Conn, error) {
            return net.Dial("tcp", "localhost:6379")
        },
        PoolSize: 10,
    }

    connPool := pool.NewConnPool(opt)
    defer connPool.Close()

    // Add custom hook
    loggingHook := &LoggingHook{name: "MyLogger"}
    connPool.AddPoolHook(loggingHook)

    // Use the pool - hooks will be called automatically
    ctx := context.Background()
    conn, err := connPool.Get(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // Do something with connection...

    connPool.Put(ctx, conn)
}
```

### Multiple Hooks
```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/go-redis/v9/internal/pool"
)

// Metrics hook
type MetricsHook struct {
    getCount int64
    putCount int64
}

func (mh *MetricsHook) OnGet(ctx context.Context, conn *pool.Conn, isNewConn bool) error {
    mh.getCount++
    return nil
}

func (mh *MetricsHook) OnPut(ctx context.Context, conn *pool.Conn) (shouldPool bool, shouldRemove bool, err error) {
    mh.putCount++
    return true, false, nil
}

// Validation hook
type ValidationHook struct{}

func (vh *ValidationHook) OnGet(ctx context.Context, conn *pool.Conn, isNewConn bool) error {
    if !conn.IsUsable() {
        return errors.New("connection not usable")
    }
    return nil
}

func (vh *ValidationHook) OnPut(ctx context.Context, conn *pool.Conn) (shouldPool bool, shouldRemove bool, err error) {
    // Check if connection has errors
    if conn.HasBufferedData() {
        return false, true, nil // Don't pool, remove connection
    }
    return true, false, nil
}

func main() {
    opt := &pool.Options{
        Dialer: func(ctx context.Context) (net.Conn, error) {
            return net.Dial("tcp", "localhost:6379")
        },
        PoolSize: 10,
    }

    connPool := pool.NewConnPool(opt)
    defer connPool.Close()

    // Add multiple hooks - they execute in order
    metricsHook := &MetricsHook{}
    validationHook := &ValidationHook{}

    connPool.AddPoolHook(metricsHook)
    connPool.AddPoolHook(validationHook)

    // Hooks will be called in the order they were added
    ctx := context.Background()
    conn, err := connPool.Get(ctx) // Both OnGet methods called
    if err != nil {
        log.Fatal(err)
    }

    connPool.Put(ctx, conn) // Both OnPut methods called

    log.Printf("Metrics: Get=%d, Put=%d", metricsHook.getCount, metricsHook.putCount)
}
```
