# Example for go-redis OpenTelemetry instrumentation

This example demonstrates how to monitor Redis using OpenTelemetry and
[Uptrace](https://github.com/uptrace/uptrace). It requires Docker to start Redis Server and Uptrace.

See
[Monitoring Go Redis Performance and Errors](https://redis.uptrace.dev/guide/go-redis-monitoring.html)
for details.

**Step 1**. Download the example using Git:

```shell
git clone https://github.com/go-redis/redis.git
cd example/otel
```

**Step 2**. Start the services using Docker:

```shell
docker-compose pull
docker-compose up -d
```

**Step 3**. Make sure Uptrace is running:

```shell
docker-compose logs uptrace
```

**Step 4**. Run the Redis client example:

```shell
UPTRACE_DSN=http://project2_secret_token@localhost:14317/2 go run client.go
```

**Step 5**. Follow the link from the CLI to view the trace:

```shell
UPTRACE_DSN=http://project2_secret_token@localhost:14317/2 go run client.go
trace: http://localhost:14318/traces/ee029d8782242c8ed38b16d961093b35
```

## Links

- [Uptrace open-source APM](https://uptrace.dev/get/open-source-apm.html)
- [OpenTelemetry Go instrumentations](https://uptrace.dev/opentelemetry/instrumentations/?lang=go)
- [OpenTelemetry Go Tracing API](https://uptrace.dev/opentelemetry/go-tracing.html)
