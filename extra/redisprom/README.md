# Prometheuses instrumentation for go-redis UniversalClient

## Installation

```bash
go get github.com/go-redis/redis/extra/redisprom/v8
```

## Usage

Pool metrics are exported by creating a collector:

```go
import (
    "github.com/go-redis/redis/v8"
    "github.com/go-redis/redis/extra/redisprom"
)

rdb := redis.NewUniversalClient(&redis.UniversalOptions{...})
stats := redisprom.NewPoolStatsCollector(rdb)
```

See [example](../../example) for more details.
