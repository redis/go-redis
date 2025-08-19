# GitHub Copilot Instructions for go-redis

This file provides context and guidelines for GitHub Copilot when working with the go-redis codebase.

## Project Overview

go-redis is a Redis client for Go with support for:
- Redis Standalone, Cluster, Sentinel, and Ring topologies
- RESP2 and RESP3 protocols
- Connection pooling and management
- Push notifications (RESP3)
- Hitless upgrades for seamless cluster transitions
- Pub/Sub messaging
- Pipelines and transactions

## Architecture

### Core Components

- **Client Types**: `Client`, `ClusterClient`, `SentinelClient`, `RingClient`
- **Connection Pool**: `internal/pool` package manages connection lifecycle
- **Protocol**: `internal/proto` handles RESP protocol parsing
- **Hitless Upgrades**: `hitless` package provides seamless cluster transitions
- **Push Notifications**: `push` package handles RESP3 push notifications

### Key Packages

- `redis.go` - Main client implementation
- `options.go` - Configuration and client options
- `osscluster.go` - Open source cluster client
- `sentinel.go` - Sentinel failover client
- `ring.go` - Ring (sharding) client
- `internal/pool/` - Connection pool management
- `hitless/` - Hitless upgrade functionality

## Coding Standards

### General Guidelines

1. **Error Handling**: Always handle errors explicitly, prefer descriptive error messages
2. **Context**: Use `context.Context` for cancellation and timeouts
3. **Thread Safety**: All public APIs must be thread-safe
4. **Memory Management**: Minimize allocations, reuse buffers where possible
5. **Testing**: Write comprehensive unit tests, prefer table-driven tests

### Naming Conventions

- Use Go standard naming (camelCase for private, PascalCase for public)
- Interface names should end with `-er` (e.g., `Pooler`, `Cmder`)
- Error variables should start with `Err` (e.g., `ErrClosed`)
- Constants should be grouped and well-documented

### Code Organization

- Keep functions focused and small (prefer < 100 lines)
- Group related functionality in the same file
- Use internal packages for implementation details
- Extract common patterns into helper functions

## Connection Pool Guidelines

### Pool Management

- Connections are managed by `internal/pool/ConnPool`
- Use `pool.Conn` wrapper for Redis connections
- Implement proper connection lifecycle (dial, auth, select DB)
- Handle connection health checks and cleanup

### Pool Hooks

- Use `PoolHook` interface for connection processing
- Hooks are called on `OnGet` and `OnPut` operations
- Support for hitless upgrades through pool hooks
- Maintain backward compatibility when adding hooks

### Connection States

- `IsUsable()` - Connection can be used for commands
- `ShouldHandoff()` - Connection needs handoff during cluster transition
- Proper state management is critical for hitless upgrades

## Hitless Upgrades

### Design Principles

- Seamless connection handoffs during cluster topology changes
- Event-driven architecture with push notifications
- Atomic state management using `sync/atomic`
- Worker pools for concurrent handoff processing

### Key Components

- `HitlessManager` - Orchestrates upgrade operations
- `PoolHook` - Handles connection-level operations
- `NotificationHandler` - Processes push notifications
- Configuration through `hitless.Config`

### Implementation Guidelines

- Use atomic operations for state checks (avoid mutex locks)
- Implement proper timeout handling for handoff operations
- Support retry logic with exponential backoff
- Maintain connection pool integrity during transitions

## Testing Guidelines

### Unit Tests

- Use table-driven tests for multiple scenarios
- Test both success and error paths
- Mock external dependencies (Redis server)
- Verify thread safety with race detection

### Integration Tests

- Separate integration tests from unit tests
- Use real Redis instances when needed
- Test all client types (standalone, cluster, sentinel)
- Verify hitless upgrade scenarios

### Test Structure

```go
func TestFeature(t *testing.T) {
    tests := []struct {
        name     string
        input    InputType
        expected ExpectedType
        wantErr  bool
    }{
        // test cases
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // test implementation
        })
    }
}
```

## Performance Considerations

### Memory Optimization

- Reuse buffers and objects where possible
- Use object pools for frequently allocated types
- Minimize string allocations in hot paths
- Profile memory usage regularly

### Concurrency

- Prefer atomic operations over mutexes for simple state
- Use `sync.Map` for concurrent map access
- Implement proper worker pool patterns
- Avoid blocking operations in hot paths

### Connection Management

- Implement connection pooling efficiently
- Handle connection timeouts properly
- Support connection health checks
- Minimize connection churn

## Common Patterns

### Error Handling

```go
if err != nil {
    return fmt.Errorf("operation failed: %w", err)
}
```

### Context Usage

```go
func (c *Client) operation(ctx context.Context) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        // continue with operation
    }
}
```

### Configuration Validation

```go
func (opt *Options) validate() error {
    if opt.PoolSize <= 0 {
        return errors.New("PoolSize must be positive")
    }
    return nil
}
```

## Documentation Standards

- Use Go doc comments for all public APIs
- Include examples for complex functionality
- Document configuration options thoroughly
- Maintain README.md with usage examples

## Compatibility

- Maintain backward compatibility for public APIs
- Use build tags for version-specific features
- Support multiple Redis versions
- Handle protocol differences gracefully

## Security Considerations

- Validate all user inputs
- Handle authentication securely
- Support TLS connections
- Avoid logging sensitive information

## go-redis Specific Patterns

### Command Interface

All Redis commands implement the `Cmder` interface:

```go
type Cmder interface {
    Name() string
    FullName() string
    Args() []interface{}
    String() string
    stringArg(int) string
    firstKeyPos() int8
    SetFirstKeyPos(int8)
    readTimeout() *time.Duration
    readReply(rd *proto.Reader) error
    SetErr(error)
    Err() error
}
```

### Client Initialization Pattern

```go
func NewClient(opt *Options) *Client {
    if opt == nil {
        panic("redis: NewClient nil options")
    }
    opt.init() // Apply defaults

    c := Client{
        baseClient: &baseClient{opt: opt},
    }
    c.init()

    // Create pools with error handling
    var err error
    c.connPool, err = newConnPool(opt, c.dialHook)
    if err != nil {
        panic(fmt.Errorf("redis: failed to create connection pool: %w", err))
    }

    return &c
}
```

### Pool Hook Pattern

```go
type PoolHook interface {
    OnGet(ctx context.Context, conn *Conn, isNewConn bool) error
    OnPut(ctx context.Context, conn *Conn) (shouldPool bool, shouldRemove bool, err error)
}
```

### Atomic State Management

Prefer atomic operations for simple state:

```go
type Manager struct {
    closed atomic.Bool
    count  atomic.Int64
}

func (m *Manager) isClosed() bool {
    return m.closed.Load()
}

func (m *Manager) close() {
    m.closed.Store(true)
}
```

### Configuration Defaults Pattern

```go
func (opt *Options) init() {
    if opt.PoolSize == 0 {
        opt.PoolSize = 10 * runtime.GOMAXPROCS(0)
    }
    if opt.ReadTimeout == 0 {
        opt.ReadTimeout = 3 * time.Second
    }
    // Apply hitless upgrade defaults
    opt.HitlessUpgradeConfig = opt.HitlessUpgradeConfig.ApplyDefaultsWithPoolSize(opt.PoolSize)
}
```

### Push Notification Handling

```go
type NotificationProcessor interface {
    ProcessPushNotification(ctx context.Context, data []byte) error
    RegisterHandler(notificationType string, handler NotificationHandler) error
    Close() error
}
```

### Error Definitions

Group related errors in separate files:

```go
// errors.go
var (
    ErrClosed = errors.New("redis: client is closed")
    ErrPoolExhausted = errors.New("redis: connection pool exhausted")
    ErrPoolTimeout = errors.New("redis: connection pool timeout")
)
```

### Panics 
Creating the client (NewClient, NewClusterClient, etc.) is the only time when the library can panic.
This includes initialization of the pool, hitless upgrade manager, and other critical components.
Other than that, the library should never panic. 