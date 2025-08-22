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

## Notification Hooks

Notification hooks allow you to monitor and customize hitless upgrade operations. The `NotificationHook` interface provides pre and post processing hooks:

```go
type NotificationHook interface {
    PreHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}) ([]interface{}, bool)
    PostHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}, result error)
}
```

### Example: Metrics Collection Hook

A metrics collection hook is available in `example_hooks.go`:

```go
import "github.com/redis/go-redis/v9/hitless"

metricsHook := hitless.NewMetricsHook()
manager.AddNotificationHook(metricsHook)

// Access metrics
metrics := metricsHook.GetMetrics()
```

### Example: Custom Logging Hook

```go
type CustomHook struct{}

func (h *CustomHook) PreHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}) ([]interface{}, bool) {
    // Log notification with connection details
    if conn, ok := notificationCtx.Conn.(*pool.Conn); ok {
        log.Printf("Processing %s on connection %d", notificationType, conn.GetID())
    }
    return notification, true // Continue processing
}

func (h *CustomHook) PostHook(ctx context.Context, notificationCtx push.NotificationHandlerContext, notificationType string, notification []interface{}, result error) {
    if result != nil {
        log.Printf("Failed to process %s: %v", notificationType, result)
    }
}
```

The notification context provides access to:
- **Client**: The Redis client instance
- **Pool**: The connection pool
- **Conn**: The specific connection that received the notification
- **IsBlocking**: Whether the notification was received on a blocking connection

Hooks can track:
- Handoff success/failure rates
- Processing duration
- Connection-specific metrics
- Custom business logic

## Requirements

- **RESP3 Protocol**: Required for push notifications
