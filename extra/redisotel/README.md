# OpenTelemetry instrumentation for go-redis

## Installation

```bash
go get github.com/go-redis/redis/extra/redisotel/v9
```

## Usage

Tracing is enabled by adding a hook:

```go
import (
    "github.com/go-redis/redis/v9"
    "github.com/go-redis/redis/extra/redisotel/v9"
)

rdb := rdb.NewClient(&rdb.Options{...})

rdb.AddHook(redisotel.NewTracingHook())
```

See [example](example) and [documentation](https://redis.uptrace.dev/tracing/) for more details.
