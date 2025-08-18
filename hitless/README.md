# Hitless Upgrades

Seamless Redis connection handoffs during topology changes without interrupting operations.

## Quick Start

```go
import "github.com/redis/go-redis/v9/hitless"

opt := &redis.Options{
    Addr:     "localhost:6379",
    Protocol: 3, // RESP3 required
    HitlessUpgrades: &redis.HitlessUpgradeConfig{
        Mode: hitless.MaintNotificationsEnabled, // or MaintNotificationsAuto
    },
}
client := redis.NewClient(opt)
```

## Modes

- **`MaintNotificationsDisabled`**: Hitless upgrades are completely disabled
- **`MaintNotificationsEnabled`**: Hitless upgrades are forcefully enabled (fails if server doesn't support it)
- **`MaintNotificationsAuto`**: Hitless upgrades are enabled if server supports it (default)

## Configuration

```go
import "github.com/redis/go-redis/v9/hitless"

Config: &hitless.Config{
    Mode:                       hitless.MaintNotificationsAuto, // Notification mode
    MaxHandoffRetries:           3,  // Retry failed handoffs
    HandoffTimeout:             15 * time.Second, // Handoff operation timeout
    RelaxedTimeout:             10 * time.Second, // Extended timeout during migrations
    PostHandoffRelaxedDuration: 20 * time.Second, // Keep relaxed timeout after handoff
    LogLevel:                   1,  // 0=errors, 1=warnings, 2=info, 3=debug
    MaxWorkers:                 15, // Concurrent handoff workers
    HandoffQueueSize:           50, // Handoff request queue size
}
```

### Worker Scaling
- **Auto-calculated**: `min(10, PoolSize/3)` - scales with pool size, capped at 10
- **Explicit values**: `max(10, set_value)` - enforces minimum 10 workers
- **On-demand**: Workers created when needed, cleaned up when idle

### Queue Sizing
- **Auto-calculated**: `10 × MaxWorkers`, capped by pool size
- **Always capped**: Queue size never exceeds pool size

## Metrics Hook Example

A metrics collection hook is available in `example_hooks.go` that demonstrates how to monitor hitless upgrade operations:

```go
import "github.com/redis/go-redis/v9/hitless"

metricsHook := hitless.NewMetricsHook()
// Use with your monitoring system
```

The metrics hook tracks:
- Handoff success/failure rates
- Handoff duration
- Queue depth
- Worker utilization
- Connection lifecycle events

## Requirements

- **RESP3 Protocol**: Required for push notifications
