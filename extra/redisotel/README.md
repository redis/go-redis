# OpenTelemetry instrumentation for go-redis

## Installation

```bash
go get github.com/redis/go-redis/extra/redisotel/v9
```

## Usage

Tracing is enabled by adding a hook:

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/redis/go-redis/extra/redisotel/v9"
)

rdb := rdb.NewClient(&rdb.Options{...})

// Enable tracing instrumentation.
if err := redisotel.InstrumentTracing(rdb); err != nil {
	panic(err)
}

// Enable metrics instrumentation.
if err := redisotel.InstrumentMetrics(rdb); err != nil {
	panic(err)
}
```

## Advanced Tracing Options

### High-Performance Command Filtering

For production systems, use O(1) command exclusion for optimal performance:

```go
// Recommended: O(1) command exclusion
err := redisotel.InstrumentTracing(rdb, 
    redisotel.WithExcludedCommands("PING", "INFO", "SELECT"))
if err != nil {
    panic(err)
}
```

### Custom Filtering Logic

For complex filtering requirements, use custom filter functions:

```go
// Filter individual commands
err := redisotel.InstrumentTracing(rdb,
    redisotel.WithProcessFilter(func(cmd redis.Cmder) bool {
        // Return true to exclude the command from tracing
        return strings.HasPrefix(cmd.Name(), "INTERNAL_")
    }))
if err != nil {
    panic(err)
}

// Filter pipeline commands
err := redisotel.InstrumentTracing(rdb,
    redisotel.WithProcessPipelineFilter(func(cmds []redis.Cmder) bool {
        // Return true to exclude pipelines with more than 10 commands
        return len(cmds) > 10
    }))
if err != nil {
    panic(err)
}

// Filter dial operations
err := redisotel.InstrumentTracing(rdb,
    redisotel.WithDialFilterFunc(func(network, addr string) bool {
        // Return true to exclude connections to localhost
        return strings.Contains(addr, "localhost")
    }))
if err != nil {
    panic(err)
}
```

### Combining Filtering Approaches

Exclusion sets are checked first for optimal performance:

```go
err := redisotel.InstrumentTracing(rdb,
    // Fast O(1) exclusion for common commands
    redisotel.WithExcludedCommands("PING", "INFO"),
    // Custom logic for additional cases
    redisotel.WithProcessFilter(func(cmd redis.Cmder) bool {
        return strings.HasPrefix(cmd.Name(), "DEBUG_")
    }))
if err != nil {
    panic(err)
}
```

### Legacy API Compatibility

Original filtering APIs remain supported:

```go
// Legacy command filter
redisotel.WithCommandFilter(func(cmd redis.Cmder) bool {
    return cmd.Name() == "AUTH" // Exclude AUTH commands
})

// Legacy pipeline filter
redisotel.WithCommandsFilter(func(cmds []redis.Cmder) bool {
    for _, cmd := range cmds {
        if cmd.Name() == "AUTH" {
            return true // Exclude pipelines with AUTH commands
        }
    }
    return false
})

// Legacy dial filter
redisotel.WithDialFilter(true) // Enable dial filtering
```

See [example](../../example/otel) and
[Monitoring Go Redis Performance and Errors](https://redis.uptrace.dev/guide/go-redis-monitoring.html)
for details.
