# Example for go-redis Prometheuses instrumentation

This example requires running Redis Server. You can start Redis Server using Docker:

```shell
docker-compose up -d
```

To run this example:

```shell
go run .
```

Metrics are exposed via `http://localhost:9530/metrics`.
