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

See [example](../../example/otel) and
[Monitoring Go Redis Performance and Errors](https://redis.uptrace.dev/guide/go-redis-monitoring.html)
for details.
