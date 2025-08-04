# Hitless Upgrades Package

This package provides hitless upgrade functionality for Redis clients, enabling seamless cluster maintenance operations without dropping connections or failing commands. The system automatically handles Redis cluster topology changes through push notifications and intelligent connection management.

## 🚀 Quick Start

```go
client := redis.NewClient(&redis.Options{
    Addr:            "localhost:6379",
    Protocol:        3, // Required for push notifications
    HitlessUpgrades: true,
    // Optional: customize configuration
    HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
        RelaxedTimeout:             30 * time.Second, // Extended timeout during migrations
        HandoffTimeout:             15 * time.Second, // Max time for connection handoff
        PostHandoffRelaxedDuration: 10 * time.Second, // Keep relaxed timeout after handoff
        LogLevel:                   1,                // Warning level logging
    },
})

// That's it! Hitless upgrades now work automatically
result, err := client.Get(ctx, "key") // Seamlessly handles cluster changes
```

## 📋 Supported Client Types

Hitless upgrades are supported by the following client types:

- ✅ **`redis.Client`** - Standard Redis client
- ✅ **`redis.ClusterClient`** - Redis Cluster client
- ✅ **`redis.SentinelClient`** - Redis Sentinel client
- ❌ **`redis.RingClient`** - Not supported (no hitless upgrade integration)

All supported clients require **Protocol: 3 (RESP3)** for push notification support.

## 🔄 How Hitless Upgrades Work

The hitless upgrade system provides seamless Redis cluster maintenance through push notifications and connection-level management:

### **Architecture Overview**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Redis Client  │    │  Hitless Manager │    │ Connection Pool │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Push Notif   │ │    │ │MOVING Op     │ │    │ │Connection   │ │
│ │Processor    │ │────┤ │Tracker       │ │    │ │Processor    │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Command      │ │    │ │Notification  │ │────┤ │Handoff      │ │
│ │Execution    │ │    │ │Handler       │ │    │ │Workers      │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### **Push Notification Types**

The system handles the following Redis push notifications:

- **`MOVING`** - Connection handoff to new endpoint (per-connection)
- **`MIGRATING`** - Slot migration in progress (applies relaxed timeouts)
- **`MIGRATED`** - Slot migration completed (clears relaxed timeouts)
- **`FAILING_OVER`** - Failover in progress (applies relaxed timeouts)
- **`FAILED_OVER`** - Failover completed (clears relaxed timeouts)

### **Operation Flow**

#### **1. 🏗️ Initialization**
```go
// When HitlessUpgrades: true is set
client := redis.NewClient(&redis.Options{
    HitlessUpgrades: true,
    Protocol:        3, // RESP3 required for push notifications
})

// Internally, the client:
// 1. Creates HitlessManager with default configuration
// 2. Registers push notification handlers for all upgrade events
// 3. Creates ConnectionProcessor for handoff management
// 4. Integrates with existing connection pool
```

#### **2. 📡 Push Notification Handling**
```go
// Redis server sends push notifications during cluster operations:
// Format: ["MOVING", seqNum, timeS, endpoint]
// Format: ["MIGRATING", slot]
// Format: ["MIGRATED", slot]
// Format: ["FAILING_OVER", node]
// Format: ["FAILED_OVER", node]

// Example MOVING notification flow:
// 1. Redis sends: ["MOVING", "12345", "30", "10.0.0.2:6379"]
// 2. Push processor routes to hitless notification handler
// 3. Handler marks specific connection for handoff
// 4. Background workers process the handoff asynchronously
```

#### **3. 🔄 Connection Management**
```go
// MOVING notification handling (per-connection):
// ├── Parse sequence ID, timeout, and new endpoint
// ├── Mark specific connection for handoff
// ├── Queue handoff request for background processing
// └── Track operation with composite key (seqID + connID)

// MIGRATING/FAILING_OVER notification handling:
// ├── Apply relaxed timeouts to the receiving connection
// ├── Allow commands to continue with extended timeouts
// └── Connection-specific timeout management

// MIGRATED/FAILED_OVER notification handling:
// ├── Clear relaxed timeouts from the receiving connection
// ├── Resume normal timeout behavior
// └── Per-connection state cleanup
```

#### **4. 🔀 Connection Handoff Process**
```go
// Background handoff workers process requests:

func (processor *RedisConnectionProcessor) processHandoffRequest(request HandoffRequest) {
    // 1. Create new connection to target endpoint
    newConn, err := processor.dialNewConnection(request.NewEndpoint)
    if err != nil {
        // Handoff failed, remove connection from pool
        processor.pool.Remove(request.Conn)
        return err
    }

    // 2. Replace connection in pool atomically
    // The pool handles connection state transfer internally
    err = processor.pool.ReplaceConnection(request.Conn, newConn)
    if err != nil {
        newConn.Close()
        return err
    }

    // 3. Apply post-handoff relaxed timeout to new connection
    newConn.SetRelaxedTimeoutWithDeadline(
        config.RelaxedTimeout,
        config.RelaxedTimeout,
        time.Now().Add(config.PostHandoffRelaxedDuration),
    )

    // 4. Notify hitless manager of completion
    processor.hitlessManager.CompleteOperationWithConnID(seqID, connID)

    // 5. Old connection is closed by pool replacement
}
```

#### **5. ⏱️ Timeout Management**
```go
// Timeout hierarchy (highest priority first):

// 1. Context Deadline (if set and shorter)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
result := client.Get(ctx, "key") // Uses 5s even during MIGRATING

// 2. Relaxed Timeout (during MIGRATING/FAILING_OVER on specific connection)
ctx := context.Background() // No deadline
result := client.Get(ctx, "key") // Uses RelaxedTimeout (30s) if connection has relaxed timeout

// 3. Normal Client Timeout (default)
ctx := context.Background() // No deadline, normal state
result := client.Get(ctx, "key") // Uses client ReadTimeout (5s)

// Note: Relaxed timeouts are applied per-connection, not globally
// Only connections that receive MIGRATING/FAILING_OVER notifications get relaxed timeouts
```

#### **6. 🎯 Command Execution Flow**
```go
// Every command goes through this flow:

client.Get(ctx, "key")
// ↓
// 1. Get connection from pool
conn := pool.Get(ctx)
// ↓
// 2. Connection processor processes pending notifications
processor.ProcessConnectionOnGet(ctx, conn)
// ↓
// 3. Execute command (timeout determined by connection state)
// - If connection has relaxed timeout: uses RelaxedTimeout
// - Otherwise: uses normal client timeout
// - Context deadline always takes precedence if shorter
result := conn.ExecuteCommand(ctx, cmd)
// ↓
// 4. Return connection to pool
// - Check if connection is marked for handoff
// - Queue handoff if needed
processor.ProcessConnectionOnPut(ctx, conn)
// ↓
// 5. Return result to application
return result
```

## 🏗️ Component Architecture

### **HitlessManager**
- **MOVING operation tracking** with composite keys (seqID + connID)
- **Push notification handling** for all upgrade events
- **Operation deduplication** to handle duplicate notifications
- **Configuration management** with sensible defaults

### **RedisConnectionProcessor**
- **Connection handoff management** with background workers
- **Dynamic worker scaling** based on load (min/max workers)
- **Queue management** for handoff requests with timeout handling
- **Pool integration** for connection replacement

### **Push Notification System**
- **Automatic handler registration** for upgrade events
- **RESP3 protocol parsing** of Redis push notifications
- **Per-connection event routing** to appropriate handlers
- **Protected handler registration** (cannot be overwritten)

### **Connection Pool Integration**
- **Existing pool architecture** with processor integration
- **Connection marking** for handoff operations
- **Atomic connection replacement** during handoffs
- **Per-connection timeout management**

## ⚙️ Configuration Options

```go
type HitlessUpgradeConfig struct {
    // Core settings
    Enabled      bool   // Enable hitless upgrades (default: false)
    EndpointType string // Endpoint type: "internal-ip", "internal-fqdn",
                        // "external-ip", "external-fqdn", "none", or "" (auto-detect)

    // Timeout settings
    RelaxedTimeout             time.Duration // Timeout during MIGRATING/FAILING_OVER (default: 30s)
    RelaxedTimeoutDuration     time.Duration // Legacy alias for RelaxedTimeout (default: 30s)
    HandoffTimeout             time.Duration // Max time for connection handoff (default: 15s)
    PostHandoffRelaxedDuration time.Duration // Keep relaxed timeout after handoff (default: 10s)

    // Worker settings (auto-calculated based on pool size if 0)
    MinWorkers       int // Minimum handoff workers (default: max(1, poolSize/20))
    MaxWorkers       int // Maximum handoff workers (default: max(MinWorkers*4, poolSize/5))
    HandoffQueueSize int // Handoff request queue size (default: MaxWorkers*10, capped by poolSize)

    // Advanced settings
    ScaleDownDelay time.Duration // Delay before scaling down workers (default: 2s)
    LogLevel       int          // 0=errors, 1=warnings, 2=info, 3=debug (default: 1)
}
```

## 🎯 Usage Examples

### **Basic Usage (Recommended)**
```go
// Minimal configuration - uses sensible defaults
client := redis.NewClient(&redis.Options{
    Addr:            "localhost:6379",
    Protocol:        3, // Required for push notifications
    HitlessUpgrades: true,
})
```

### **Custom Configuration**
```go
client := redis.NewClient(&redis.Options{
    Addr:            "localhost:6379",
    Protocol:        3,
    HitlessUpgrades: true,
    HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
        RelaxedTimeout:             45 * time.Second, // Longer timeout for slow operations
        HandoffTimeout:             20 * time.Second, // More time for handoffs
        PostHandoffRelaxedDuration: 15 * time.Second, // Extended post-handoff period
        LogLevel:                   2,                // Info level logging
    },
})
```

### **Cluster Client**
```go
clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs:           []string{"localhost:7000", "localhost:7001", "localhost:7002"},
    Protocol:        3,
    HitlessUpgrades: true,
    // Configuration options same as regular client
})
```

### **Sentinel Client**
```go
sentinelClient := redis.NewSentinelClient(&redis.SentinelOptions{
    MasterName:      "mymaster",
    SentinelAddrs:   []string{"localhost:26379"},
    Protocol:        3,
    HitlessUpgrades: true,
})
```

## 🔧 Automatic Operation

Once configured, hitless upgrades work completely automatically:

- ✅ **No manual registration** - Push handlers are registered automatically
- ✅ **No state management** - MOVING operations are tracked automatically
- ✅ **No timeout management** - Relaxed timeouts are applied per-connection
- ✅ **No handoff coordination** - Connection handoffs happen transparently
- ✅ **No cleanup required** - Resources and workers are managed automatically

Your application code remains unchanged - just enable the feature and it works!

## 📋 Requirements

### **Protocol Requirements**
- **RESP3 Protocol Required**: Must set `Protocol: 3` in client options
- **Redis Version**: Redis 6.0+ (for RESP3 and push notification support)
- **Push Notifications**: Server must support push notifications

### **Client Support**
- ✅ `redis.Client` - Full support
- ✅ `redis.ClusterClient` - Full support
- ✅ `redis.SentinelClient` - Full support
- ❌ `redis.RingClient` - Not supported

### **Network Requirements**
- **Endpoint Connectivity**: Client must be able to connect to new endpoints provided in MOVING notifications
- **TLS Compatibility**: Auto-detects appropriate endpoint type based on TLS configuration

## ⚠️ Current Limitations

### **Implementation Scope**
- **Connection-Level Operations**: Handoffs and timeouts are managed per-connection, not pool-wide
- **Single Pool Architecture**: No dual-pool implementation (contrary to some documentation)
- **MOVING Operations Only**: Only MOVING notifications trigger connection handoffs
- **No Slot Tracking**: MIGRATING/MIGRATED notifications only affect timeouts, not routing

### **Configuration Constraints**
- **Auto-Calculated Defaults**: Many settings are calculated based on pool size and cannot be overridden
- **Worker Scaling**: Dynamic worker scaling is based on simple heuristics
- **Queue Management**: Handoff queue has timeout-based overflow handling

### **Error Handling**
- **Handoff Failures**: Failed handoffs result in connection removal from pool
- **Notification Errors**: Invalid notifications are logged but don't stop processing
- **Timeout Handling**: Queue timeouts (5s) may drop handoff requests under extreme load

## 🔍 Implementation Details

### **Operation Tracking**
- **Composite Keys**: MOVING operations tracked with `(seqID, connID)` to handle duplicates
- **Deduplication**: Duplicate MOVING notifications for same operation are ignored
- **Connection Marking**: Connections marked for handoff with target endpoint and sequence ID

### **Worker Management**
- **Dynamic Scaling**: Workers scale between MinWorkers and MaxWorkers based on load
- **Scale-Down Delay**: 2-second delay before checking if workers should be scaled down
- **Graceful Shutdown**: Workers complete current handoffs before shutting down

### **Timeout Behavior**
- **Per-Connection**: Relaxed timeouts applied only to connections receiving notifications
- **Deadline Management**: Automatic timeout clearing with configurable post-handoff duration
- **Context Priority**: Context deadlines always take precedence over relaxed timeouts

### **Push Notification Integration**
- **Protected Handlers**: Hitless upgrade handlers cannot be overwritten by application code
- **Automatic Registration**: All upgrade event handlers registered during initialization
- **Error Isolation**: Handler errors don't affect other notification processing

## 📊 Monitoring and Troubleshooting

### **Logging Levels**
```go
// Configure logging verbosity
HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
    LogLevel: 2, // 0=errors, 1=warnings, 2=info, 3=debug
}
```

- **Level 0 (Errors)**: Only critical errors (handoff failures, configuration errors)
- **Level 1 (Warnings)**: Default level, includes warnings and errors
- **Level 2 (Info)**: Handoff operations, worker scaling, operation tracking
- **Level 3 (Debug)**: Detailed notification processing, connection state changes

### **Common Issues**

#### **"RESP3 protocol required" Error**
```go
// ❌ Wrong - will log error and disable hitless upgrades
client := redis.NewClient(&redis.Options{
    Protocol:        2, // RESP2 doesn't support push notifications
    HitlessUpgrades: true,
})

// ✅ Correct - enables hitless upgrades
client := redis.NewClient(&redis.Options{
    Protocol:        3, // RESP3 required
    HitlessUpgrades: true,
})
```

#### **Handoff Queue Timeouts**
- **Symptom**: "handoff queue timeout after 5 seconds" errors
- **Cause**: Queue overflow under high load
- **Solution**: Increase `MaxWorkers` or `HandoffQueueSize` in configuration

#### **Failed Connection Handoffs**
- **Symptom**: Connections removed from pool during MOVING operations
- **Cause**: Network connectivity issues to new endpoints
- **Solution**: Verify network connectivity and endpoint reachability

### **Performance Considerations**
- **Worker Count**: Balance between responsiveness and resource usage
- **Queue Size**: Size based on expected burst load and worker capacity
- **Timeout Values**: Balance between resilience and responsiveness
- **Pool Size Impact**: Worker defaults scale with pool size automatically

## 🚀 Getting Started

1. **Enable RESP3**: Set `Protocol: 3` in your client options
2. **Enable Feature**: Set `HitlessUpgrades: true`
3. **Optional Config**: Customize `HitlessUpgradeConfig` if needed
4. **Test**: Verify with Redis cluster maintenance operations

The system will automatically handle all Redis cluster upgrade notifications without requiring any changes to your application code.
