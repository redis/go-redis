# Hitless Upgrades Package

This package provides hitless upgrade functionality for Redis clients, enabling seamless cluster maintenance operations without dropping connections or failing commands. The system automatically handles Redis cluster topology changes through push notifications and intelligent connection management.

## 🚀 Quick Start

```go
client := redis.NewClient(&redis.Options{
    Addr:            "localhost:6379",
    Protocol:        3, // Required for push notifications
    HitlessUpgrades: true,
    HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
        Enabled:                    true,
        RelaxedTimeout:             30 * time.Second, // Extended timeout during migrations
        HandoffTimeout:             15 * time.Second, // Max time for connection handoff
        HandoffWorkers:             10,               // Concurrent handoff workers
        HandoffQueueSize:           100,              // Handoff request queue size
        PostHandoffRelaxedDuration: 10 * time.Second, // Keep relaxed timeout after handoff
        LogLevel:                   2,                // Info level logging
    },
})

// That's it! Hitless upgrades now work automatically
result, err := client.Get(ctx, "key") // Seamlessly handles cluster changes
```

## 🔄 How Hitless Upgrades Work

The hitless upgrade system provides seamless Redis cluster maintenance through a sophisticated flow that automatically handles topology changes:

### **Architecture Overview**

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Redis Client  │    │  Hitless Manager │    │ Connection Pool │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Push Notif   │ │    │ │State Tracker │ │    │ │Processor    │ │
│ │Processor    │ │────┤ │              │ │    │ │             │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Command      │ │    │ │Handoff       │ │────┤ │Connection   │ │
│ │Execution    │ │    │ │Manager       │ │    │ │Handoff      │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### **Complete Flow Walkthrough**

#### **1. 🏗️ Initialization Phase**
```go
// When HitlessUpgrades: true is set
client := redis.NewClient(&redis.Options{
    HitlessUpgrades: true,
    Protocol:        3, // RESP3 required for push notifications
})

// Internally, the client:
// 1. Creates HitlessManager with configuration
// 2. Manager creates ConnectionProcessor with itself attached
// 3. Pool is created with the processor
// 4. Push notification handlers are registered automatically
```

#### **2. 📡 Push Notification Reception**
```go
// Redis server sends push notifications during cluster operations:
// - MOVING: Node is being moved to new endpoint
// - MIGRATING: Slot migration in progress
// - MIGRATED: Slot migration completed
// - FAILING_OVER: Failover in progress
// - FAILED_OVER: Failover completed

// Example notification flow:
// 1. Redis sends: ["MOVING", "10.0.0.1:6379", "10.0.0.2:6379", "12345"]
// 2. Push processor receives and parses notification
// 3. Hitless manager processes the state change
// 4. Connection processor handles the transition
```

#### **3. 🔄 State Transition Management**
```go
// The hitless manager tracks state transitions:

// MOVING notification received
// ├── Mark connection for handoff to new endpoint
// ├── Apply relaxed timeouts to affected connection
// └── Queue handoff request for background processing

// MIGRATING notification received
// ├── Apply relaxed timeouts to connection
// ├── Allow commands to continue with extended timeouts
// └── Track operation sequence for completion

// MIGRATED notification received
// ├── Clear relaxed timeouts
// ├── Mark operation as completed
// └── Resume normal timeout behavior
```

#### **4. 🔀 Connection Handoff Process**
```go
// Background handoff workers process requests:

func (processor *RedisConnectionProcessor) handleHandoff(request HandoffRequest) {
    // 1. Create new connection to target endpoint
    newConn, err := processor.dialNewConnection(request.NewEndpoint)
    if err != nil {
        return err // Handoff failed, connection continues on old endpoint
    }

    // 2. Transfer connection state
    newConn.CopyStateFrom(request.OldConnection)

    // 3. Replace connection in pool atomically
    pool.ReplaceConnection(request.OldConnection, newConn)

    // 4. Apply relaxed timeout to new connection for post-handoff period
    newConn.SetRelaxedTimeout(config.RelaxedTimeout, config.RelaxedTimeout)

    // 5. Schedule automatic timeout clearing after configured duration
    go func() {
        time.Sleep(config.PostHandoffRelaxedDuration) // Default: 10s
        newConn.ClearRelaxedTimeout()
    }()

    // 6. Notify hitless manager of completion
    processor.hitlessManager.CompleteOperationWithConnID(seqID, connID)

    // 7. Close old connection gracefully
    request.OldConnection.Close()
}
```

#### **5. ⏱️ Timeout Management**
```go
// Timeout hierarchy (highest priority first):

// 1. Context Deadline (if set and shorter)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
result := client.Get(ctx, "key") // Uses 5s even during MIGRATING

// 2. Relaxed Timeout (during MIGRATING/FAILING_OVER)
ctx := context.Background() // No deadline
result := client.Get(ctx, "key") // Uses RelaxedTimeout (30s) during migration

// 3. Normal Client Timeout (default)
ctx := context.Background() // No deadline, normal state
result := client.Get(ctx, "key") // Uses client ReadTimeout (5s)
```

#### **6. 🎯 Command Execution Flow**
```go
// Every command goes through this flow:

client.Get(ctx, "key")
// ↓
// 1. Get connection from pool
conn := pool.Get(ctx)
// ↓
// 2. Connection processor checks state
processor.ProcessConnectionOnGet(ctx, conn)
// ↓
// 3. Apply appropriate timeout based on connection state
if conn.IsInMigratingState() {
    timeout = relaxedTimeout // Extended timeout
} else {
    timeout = normalTimeout  // Regular timeout
}
// ↓
// 4. Execute command with calculated timeout
result := conn.ExecuteCommand(ctx, cmd, timeout)
// ↓
// 5. Return connection to pool
processor.ProcessConnectionOnPut(ctx, conn)
// ↓
// 6. Return result to application
return result
```

## 🏗️ Component Architecture

### **HitlessManager**
- **Central coordinator** for all hitless upgrade operations
- **State tracking** for ongoing migrations and failovers
- **Operation sequencing** to ensure proper completion order
- **Configuration management** with field-by-field defaults

### **ConnectionProcessor**
- **Connection lifecycle management** during handoffs
- **Background worker pool** for concurrent handoff processing
- **Queue management** for handoff requests with configurable size
- **Timeout application** per connection based on state

### **Push Notification System**
- **Automatic registration** of cluster state change handlers
- **Protocol parsing** of Redis push notifications
- **Event routing** to appropriate handlers
- **Error handling** and retry logic

### **Connection Pool Integration**
- **Seamless integration** with existing pool architecture
- **Atomic connection replacement** during handoffs
- **State preservation** across connection transitions
- **Resource cleanup** for old connections

## ⚙️ Configuration Options

```go
type HitlessUpgradeConfig struct {
    Enabled                    bool          // Enable hitless upgrades
    EndpointType               string        // "internal-ip" or auto-detect
    RelaxedTimeout             time.Duration // Extended timeout during migrations (30s)
    RelaxedTimeoutDuration     time.Duration // Same as RelaxedTimeout (for compatibility)
    HandoffTimeout             time.Duration // Max time for connection handoff (15s)
    HandoffWorkers             int           // Concurrent handoff workers (10)
    HandoffQueueSize           int           // Handoff request queue size (100)
    PostHandoffRelaxedDuration time.Duration // Keep relaxed timeout after handoff (10s)
    LogLevel                   int           // 0=errors, 1=warnings, 2=info, 3=debug
}
```

## 🎯 Automatic Operation

Once configured, hitless upgrades work completely automatically:

- ✅ **No manual registration** - Push handlers are registered automatically
- ✅ **No state management** - Connection states are tracked automatically
- ✅ **No timeout management** - Relaxed timeouts are applied automatically
- ✅ **No handoff coordination** - Connection handoffs happen transparently
- ✅ **No cleanup required** - Resources are managed automatically

Your application code remains unchanged - just enable the feature and it works!
