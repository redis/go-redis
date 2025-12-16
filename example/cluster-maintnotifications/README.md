# Cluster Maintenance Notifications Example

This example demonstrates how to use maintenance notifications with Redis Cluster to handle slot migrations seamlessly.

## Overview

When Redis Cluster performs slot migrations (e.g., during resharding or node rebalancing), the client can receive push notifications to:

1. **SMIGRATING** - Indicates slots are being migrated (relaxes timeouts)
2. **SMIGRATED** - Indicates slot migration completed (triggers cluster state reload)

This example shows how to:
- Enable maintnotifications for ClusterClient
- Track SMIGRATING and SMIGRATED notifications
- Monitor cluster state reloads
- Continue operations seamlessly during migrations

## Prerequisites

You need a running Redis Cluster with at least 3 master nodes.

## Running the Example

### Basic Usage

```bash
# Run with default cluster addresses (localhost:7000, 7001, 7002)
go run main.go

# Or specify custom cluster addresses
REDIS_CLUSTER_ADDRS="localhost:7000,localhost:7001,localhost:7002" go run main.go
```

### Expected Output

```
2024/12/02 10:00:00 Connected to Redis cluster with maintnotifications enabled
2024/12/02 10:00:00 The client will automatically handle SMIGRATING and SMIGRATED notifications
2024/12/02 10:00:00 Press Ctrl+C to exit
2024/12/02 10:00:10 Completed 10 operations
2024/12/02 10:00:20 Completed 20 operations
```

## Testing Slot Migration

To see maintnotifications in action, trigger a slot migration:

### Using cae-resp-proxy (Recommended for Testing)

```bash
# Terminal 1: Start the proxy
docker run -d \
  -p 7000:6379 -p 8000:3000 \
  -e TARGET_HOST=host.docker.internal \
  -e TARGET_PORT=6379 \
  redislabs/client-resp-proxy:latest

# Terminal 2: Run the example (connecting through proxy)
REDIS_CLUSTER_ADDRS="localhost:7000" go run main.go

# Terminal 3: Inject SMIGRATING notification
curl -X POST "http://localhost:8000/send-to-all-clients?encoding=raw" \
  --data-binary ">3\r\n\$10\r\nSMIGRATING\r\n:12345\r\n\$4\r\n1000\r\n"

# Inject SMIGRATED notification (new format)
curl -X POST "http://localhost:8000/send-to-all-clients?encoding=raw" \
  --data-binary ">4\r\n\$9\r\nSMIGRATED\r\n:12346\r\n:1\r\n*1\r\n\$20\r\n127.0.0.1:6380 1000\r\n"
```

Expected output after injection:
```
2024/12/02 10:01:00 SMIGRATING notification received: SeqID=12345, Slots=[1000]
2024/12/02 10:01:05 SMIGRATED notification received (reload #1): SeqID=12346, Endpoints=[127.0.0.1:6380 1000]
```

### Using Real Cluster Migration

```bash
# Reshard slots from one node to another
redis-cli --cluster reshard 127.0.0.1:7000 \
  --cluster-from <source-node-id> \
  --cluster-to <target-node-id> \
  --cluster-slots 100 \
  --cluster-yes
```

During resharding, you should see SMIGRATING and SMIGRATED notifications in the example output.

## Code Walkthrough

### 1. Enable Maintnotifications

```go
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs:    []string{"localhost:7000", "localhost:7001", "localhost:7002"},
    Protocol: 3, // RESP3 required for push notifications
    
    MaintNotificationsConfig: &maintnotifications.Config{
        Mode:           maintnotifications.ModeEnabled,
        RelaxedTimeout: 30 * time.Second,
    },
})
```

### 2. Track Notifications with Custom Hook

```go
client.OnNewNode(func(nodeClient *redis.Client) {
    manager := nodeClient.GetMaintNotificationsManager()
    if manager != nil {
        hook := &notificationTracker{
            onSMigrated: func(seqID int64, endpoints []string) {
                log.Printf("SMIGRATED: SeqID=%d, Endpoints=%v",
                    seqID, endpoints)
            },
            onSMigrating: func(seqID int64, slots []string) {
                log.Printf("SMIGRATING: SeqID=%d, Slots=%v", seqID, slots)
            },
        }
        manager.AddNotificationHook(hook)
    }
})
```

### 3. Perform Operations

The client automatically handles notifications in the background. Your application code continues to work normally:

```go
client.Set(ctx, "key", "value", 0)
client.Get(ctx, "key")
```

## Key Features Demonstrated

### Automatic Timeout Relaxation

When SMIGRATING is received:
- Read/write timeouts are automatically relaxed to `RelaxedTimeout`
- Prevents false failures during slot migration
- Automatically restored when migration completes

### Automatic Cluster State Reload

When SMIGRATED is received:
- Cluster state is automatically reloaded
- Client learns about new slot ownership
- Subsequent commands are routed to correct nodes
- Deduplication ensures reload happens only once per SeqID

### Seamless Operation

- No application code changes needed during migration
- Operations continue without interruption
- Automatic retry and redirection handled by client

## Configuration Options

```go
MaintNotificationsConfig: &maintnotifications.Config{
    // Mode: auto (default), enabled, or disabled
    Mode: maintnotifications.ModeEnabled,
    
    // Timeout to use during migration (default: 10s)
    RelaxedTimeout: 30 * time.Second,
    
    // Duration to keep relaxed timeout after handoff (default: 5s)
    PostHandoffRelaxedDuration: 5 * time.Second,
    
    // Circuit breaker settings
    CircuitBreakerFailureThreshold: 5,
    CircuitBreakerResetTimeout:     60 * time.Second,
}
```

## Monitoring

The example tracks:
- Number of SMIGRATING notifications received
- Number of SMIGRATED notifications received
- Number of cluster state reloads triggered
- Operation success/failure rates

You can extend this to:
- Export metrics to Prometheus
- Log to structured logging system
- Alert on excessive migration activity

## Troubleshooting

### No Notifications Received

1. Verify RESP3 is enabled (`Protocol: 3`)
2. Check Redis version supports push notifications (Redis 6.0+)
3. Verify maintnotifications mode is not disabled
4. Check Redis cluster is actually performing migrations

### Operations Failing During Migration

1. Increase `RelaxedTimeout` value
2. Check network connectivity
3. Verify cluster has sufficient resources
4. Review Redis logs for errors

### Excessive Cluster State Reloads

1. Check for rapid slot migrations
2. Verify deduplication is working (same SeqID should reload once)
3. Consider adding cooldown period in custom hook

## Related Examples

- [Disable Maintnotifications](../disable-maintnotifications/) - How to disable the feature
- [Maintnotifications PubSub](../maintnotifiations-pubsub/) - Using with Pub/Sub

## References

- [Maintenance Notifications Documentation](../../maintnotifications/README.md)
- [Redis Cluster Specification](https://redis.io/topics/cluster-spec)
- [RESP3 Protocol](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md)

