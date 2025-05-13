# Redis Command Processing

This document describes how commands are processed in the Redis client, including the command pipeline, error handling, and various command execution modes.

## Command Interface

The core of command processing is the `Cmder` interface:

```go
type Cmder interface {
    Name() string          // Command name (e.g., "set", "get")
    FullName() string      // Full command name (e.g., "cluster info")
    Args() []interface{}   // Command arguments
    String() string        // String representation of command and response
    readTimeout() *time.Duration
    readReply(rd *proto.Reader) error
    SetErr(error)
    Err() error
}
```

## Command Processing Pipeline

### 1. Command Creation
- Commands are created using factory functions (e.g., `NewCmd`, `NewStatusCmd`)
- Each command type implements the `Cmder` interface
- Commands can specify read timeouts and key positions

### 2. Command Execution
The execution flow:
1. Command validation
2. Connection acquisition from pool
3. Command writing to Redis
4. Response reading
5. Error handling and retries

### 3. Error Handling
- Network errors trigger retries based on configuration
- Redis errors are returned directly
- Timeout handling with configurable backoff

## Command Execution Modes

### 1. Single Command
```go
err := client.Process(ctx, cmd)
```

### 2. Pipeline
```go
pipe := client.Pipeline()
pipe.Process(ctx, cmd1)
pipe.Process(ctx, cmd2)
cmds, err := pipe.Exec(ctx)
```

### 3. Transaction Pipeline
```go
pipe := client.TxPipeline()
pipe.Process(ctx, cmd1)
pipe.Process(ctx, cmd2)
cmds, err := pipe.Exec(ctx)
```

## Command Types

### 1. Basic Commands
- String commands (SET, GET)
- Hash commands (HGET, HSET)
- List commands (LPUSH, RPOP)
- Set commands (SADD, SMEMBERS)
- Sorted Set commands (ZADD, ZRANGE)

### 2. Advanced Commands
- Scripting (EVAL, EVALSHA)
- Pub/Sub (SUBSCRIBE, PUBLISH)
- Transactions (MULTI, EXEC)
- Cluster commands (CLUSTER INFO)

### 3. Specialized Commands
- Search commands (FT.SEARCH)
- JSON commands (JSON.SET, JSON.GET)
- Time Series commands (TS.ADD, TS.RANGE)
- Probabilistic data structures (BF.ADD, CF.ADD)

## Command Processing in Different Clients

### 1. Standalone Client
- Direct command execution
- Connection pooling
- Automatic retries

### 2. Cluster Client
- Command routing based on key slots
- MOVED/ASK redirection handling
- Cross-slot command batching

### 3. Ring Client
- Command sharding based on key hashing
- Consistent hashing for node selection
- Parallel command execution

## Best Practices

1. **Command Batching**
   - Use pipelines for multiple commands
   - Batch related commands together
   - Consider transaction pipelines for atomic operations

2. **Error Handling**
   - Check command errors after execution
   - Handle network errors appropriately
   - Use retries for transient failures

3. **Performance**
   - Use appropriate command types
   - Leverage pipelining for bulk operations
   - Monitor command execution times

4. **Resource Management**
   - Close connections properly
   - Use context for timeouts
   - Monitor connection pool usage

## Common Issues and Solutions

1. **Timeout Handling**
   - Configure appropriate timeouts
   - Use context for cancellation
   - Implement retry strategies

2. **Connection Issues**
   - Monitor connection pool health
   - Handle connection failures gracefully
   - Implement proper cleanup

3. **Command Errors**
   - Validate commands before execution
   - Handle Redis-specific errors
   - Implement proper error recovery

## Monitoring and Debugging

1. **Command Monitoring**
   - Use SLOWLOG for performance analysis
   - Monitor command execution times
   - Track error rates

2. **Client Information**
   - Monitor client connections
   - Track command usage patterns
   - Analyze performance bottlenecks

## Future Improvements

1. **Command Processing**
   - Enhanced error handling
   - Improved retry mechanisms
   - Better connection management

2. **Performance**
   - Optimized command batching
   - Enhanced pipelining
   - Better resource utilization 