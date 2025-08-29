# Hitless Upgrades

Seamless Redis connection handoffs during cluster changes without dropping connections.

## Quick Start

```go
client := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Protocol: 3, // RESP3 required
    HitlessUpgrades: &hitless.Config{
        Mode: hitless.MaintNotificationsEnabled,
    },
})
```

## Modes

- **`MaintNotificationsDisabled`** - Hitless upgrades disabled
- **`MaintNotificationsEnabled`** - Forcefully enabled (fails if server doesn't support)
- **`MaintNotificationsAuto`** - Auto-detect server support (default)

## Configuration

```go
&hitless.Config{
    Mode:                       hitless.MaintNotificationsAuto,
    EndpointType:               hitless.EndpointTypeAuto,
    RelaxedTimeout:             10 * time.Second,
    HandoffTimeout:             15 * time.Second,
    MaxHandoffRetries:          3,
    MaxWorkers:                 0,    // Auto-calculated
    HandoffQueueSize:           0,    // Auto-calculated
    PostHandoffRelaxedDuration: 0,    // 2 * RelaxedTimeout
    LogLevel:                   logging.LogLevelError,
}
```

### Endpoint Types

- **`EndpointTypeAuto`** - Auto-detect based on connection (default)
- **`EndpointTypeInternalIP`** - Internal IP address
- **`EndpointTypeInternalFQDN`** - Internal FQDN
- **`EndpointTypeExternalIP`** - External IP address
- **`EndpointTypeExternalFQDN`** - External FQDN
- **`EndpointTypeNone`** - No endpoint (reconnect with current config)

### Auto-Scaling

**Workers**: `min(PoolSize/2, max(10, PoolSize/3))` when auto-calculated
**Queue**: `max(20×Workers, PoolSize)` capped by `MaxActiveConns+1` or `5×PoolSize`

**Examples:**
- Pool 100: 33 workers, 660 queue (capped at 500)
- Pool 100 + MaxActiveConns 150: 33 workers, 151 queue

## How It Works

1. Redis sends push notifications about cluster changes
2. Client creates new connections to updated endpoints
3. Active operations transfer to new connections
4. Old connections close gracefully

## Supported Notifications

- `MOVING` - Slot moving to new node
- `MIGRATING` - Slot in migration state
- `MIGRATED` - Migration completed
- `FAILING_OVER` - Node failing over
- `FAILED_OVER` - Failover completed

## Hooks (Optional)

Monitor and customize hitless operations:

```go
type NotificationHook interface {
    PreHook(ctx, notificationCtx, notificationType, notification) ([]interface{}, bool)
    PostHook(ctx, notificationCtx, notificationType, notification, result)
}

// Add custom hook
manager.AddNotificationHook(&MyHook{})
```

### Metrics Hook Example

```go
// Create metrics hook
metricsHook := hitless.NewMetricsHook()
manager.AddNotificationHook(metricsHook)

// Access collected metrics
metrics := metricsHook.GetMetrics()
fmt.Printf("Notification counts: %v\n", metrics["notification_counts"])
fmt.Printf("Processing times: %v\n", metrics["processing_times"])
fmt.Printf("Error counts: %v\n", metrics["error_counts"])
```
