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

- Go 1.23.0 or later
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

## See Also

- [OpenTelemetry Go SDK](https://opentelemetry.io/docs/languages/go/)
- [OTLP Exporter Documentation](https://opentelemetry.io/docs/specs/otlp/)
- [Redis OTel Native Package](../../extra/redisotel-native/)

