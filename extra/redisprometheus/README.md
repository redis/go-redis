# Prometheus Metric Collector

This package implements a [`prometheus.Collector`](https://pkg.go.dev/github.com/prometheus/client_golang@v1.12.2/prometheus#Collector)
for collecting metrics about the connection pool used by the various redis clients.
Supported clients are `redis.Client`, `redis.ClusterClient`, `redis.Ring` and `redis.UniversalClient`.

### Example

```go
client := redis.NewClient(options)
collector := redisprometheus.NewCollector(namespace, subsystem, client)
prometheus.MustRegister(collector)
```

### Metrics

| Name                      | Type           | Description                                                                 |
|---------------------------|----------------|-----------------------------------------------------------------------------|
| `pool_hit_total`          | Counter metric | number of times a connection was found in the pool                          |
| `pool_miss_total`         | Counter metric | number of times a connection was not found in the pool                      |
| `pool_timeout_total`      | Counter metric | number of times a timeout occurred when getting a connection from the pool  |
| `pool_conn_total_current` | Gauge metric   | current number of connections in the pool                                   |
| `pool_conn_idle_current`  | Gauge metric   | current number of idle connections in the pool                              |
| `pool_conn_stale_total`   | Counter metric | number of times a connection was removed from the pool because it was stale |


