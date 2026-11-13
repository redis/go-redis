# OpenTelemetry Metrics Example

This example demonstrates how to enable OpenTelemetry metrics for Redis operations using the `extra/redisotel-native` package.

## Features

- ✅ OTLP exporter configuration
- ✅ Periodic metric export (every 10 seconds)
- ✅ Concurrent Redis operations
- ✅ Automatic metric collection for:
  - Operation duration
  - Connection metrics
  - Error tracking

## Prerequisites

- Go 1.24.0 or later
- Redis server running on `localhost:6379`
- OTLP collector running on `localhost:4317` (optional)

## Running the Example

```bash
# Start Redis (if not already running)
redis-server

# Optional: Start OTLP collector
# See: https://opentelemetry.io/docs/collector/

# Run the example
go run main.go
```

## What It Does

1. Creates an OTLP exporter that sends metrics to a collector
2. Sets up a meter provider with periodic export (every 10 seconds)
3. Initializes Redis client with OTel instrumentation
4. Executes concurrent Redis operations (SET commands)
5. Waits for metrics to be exported

## Metrics Collected

The example automatically collects:

- **db.client.operation.duration** - Operation latency histogram
- **db.client.connection.create_time** - Connection creation time
- **db.client.connection.count** - Active connection count
- **db.client.errors** - Error counter with error type classification

## Configuration

To use with a production OTLP collector:

```go
exporter, err := otlpmetricgrpc.New(ctx,
    otlpmetricgrpc.WithEndpoint("your-collector:4317"),
    otlpmetricgrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(certPool, "")),
)
```

To attach your own attributes to every emitted metric (deployment, team, tier, …),
use `WithCustomAttributes`. A custom attribute with the same key as a built-in
semantic-convention attribute replaces it. Every custom attribute lands on every
data point, so keep the list short and its values low-cardinality:

```go
config := redisotel.NewConfig().
    WithEnabled(true).
    WithCustomAttributes(
        attribute.String("deployment.environment", "staging"),
        attribute.String("team", "platform"),
    )
```

### Per-client recorders

One process can carry clients that belong to different tenants, shards or
roles. Give each its own recorder with `NewRecorder` and pass it in that
client's options; everything the client does (commands, errors, Pub/Sub,
stream lag, pool and connection events) is recorded to it instead of the
global recorder:

```go
inst := redisotel.GetObservabilityInstance()
_ = inst.Init(redisotel.NewConfig().
    WithEnabled(true).
    WithCustomAttributes(attribute.String("deployment.environment", "prod")))
defer inst.Shutdown()

rec, err := inst.NewRecorder(redisotel.NewConfig().
    WithCustomAttributes(attribute.String("tenant", "acme")))
if err != nil { /* Init has not run */ }

acme := redis.NewClient(&redis.Options{Addr: "acme:6379", OTelRecorder: rec})
```

`ClusterOptions`, `RingOptions`, `FailoverOptions`, `UniversalOptions` and
`MultiDBOptions` carry the same `OTelRecorder` field; cluster nodes, ring
shards, sentinel clients and MultiDB members inherit it. Attribute precedence,
lowest to highest:

| Layer | Source |
|-------|--------|
| built-in | semantic-convention attributes set by the recorder |
| global | `Init` config `WithCustomAttributes` |
| per-client | `NewRecorder` config `WithCustomAttributes` |

Two caveats: the per-client config's `Enabled` flag is ignored (`Init` decides
whether the process records), and histogram settings that differ from `Init`'s
on a shared meter provider hit the SDK's duplicate-instrument rule (the first
registration wins). Any `redis.OTelRecorder` implementation can be used the
same way, globally through `redis.SetOTelRecorder` or per client through the
options field.

## See Also

- [OpenTelemetry Go SDK](https://opentelemetry.io/docs/languages/go/)
- [OTLP Exporter Documentation](https://opentelemetry.io/docs/specs/otlp/)
- [Redis OTel Native Package](../../extra/redisotel-native/)

