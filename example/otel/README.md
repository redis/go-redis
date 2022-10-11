# Example for go-redis OpenTelemetry instrumentation

See
[Monitoring Go Redis Performance and Errors](https://redis.uptrace.dev/guide/go-redis-monitoring.html)
for details.

This example requires Redis Server on port `:6379`. You can start Redis Server using Docker:

```shell
docker-compose up -d
```

You can run this example with different OpenTelemetry exporters by providing environment variables.

**Stdout** exporter (default):

```shell
go run .
```

[Uptrace](https://github.com/uptrace/uptrace) exporter:

```shell
UPTRACE_DSN="https://<token>@uptrace.dev/<project_id>" go run .
```

**Jaeger** exporter:

```shell
OTEL_EXPORTER_JAEGER_ENDPOINT=http://localhost:14268/api/traces go run .
```

To instrument Redis Cluster client, see
[go-redis-cluster](https://github.com/uptrace/opentelemetry-go-extra/tree/main/example/go-redis-cluster)
example.

## Links

- [OpenTelemetry Go instrumentations](https://uptrace.dev/opentelemetry/instrumentations/?lang=go)
- [OpenTelemetry Go Tracing API](https://uptrace.dev/opentelemetry/go-tracing.html)
