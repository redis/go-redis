# Grafana Queries for go-redis OTel Metrics

This document provides ready-to-use Grafana queries for visualizing go-redis OpenTelemetry metrics.

## Connection Creation Time Metrics

### Metric Name
`db.client.connection.create_time`

### Available Attributes
- `db.system` - Always "redis"
- `server.address` - Redis server address
- `server.port` - Redis server port (if not default 6379)
- `redis.client.library` - Client library name and version (e.g., "go-redis:9.16.0-beta.1")
- `db.client.connection.pool.name` - Unique pool identifier

---

## Query Examples

### 1. Average Connection Creation Time

**Use Case**: Monitor average time to create connections over time

**PromQL**:
```promql
rate(db_client_connection_create_time_sum{db_system="redis"}[5m]) 
/ 
rate(db_client_connection_create_time_count{db_system="redis"}[5m])
```

**Panel Settings**:
- Type: Time series
- Unit: seconds (s)
- Legend: `{{server_address}} - {{db_client_connection_pool_name}}`

---

### 2. Connection Creation Time Percentiles

**Use Case**: Understand latency distribution (P50, P95, P99)

**PromQL (P50)**:
```promql
histogram_quantile(0.50, 
  rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])
)
```

**PromQL (P95)**:
```promql
histogram_quantile(0.95, 
  rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])
)
```

**PromQL (P99)**:
```promql
histogram_quantile(0.99, 
  rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])
)
```

**Panel Settings**:
- Type: Time series
- Unit: seconds (s)
- Multiple queries in one panel
- Color coding: P50 (green), P95 (yellow), P99 (red)

---

### 3. Connection Creation Rate

**Use Case**: Monitor how frequently new connections are being created

**PromQL**:
```promql
rate(db_client_connection_create_time_count{db_system="redis"}[5m])
```

**Panel Settings**:
- Type: Time series
- Unit: connections per second (cps)
- Legend: `{{server_address}}`

---

### 4. Total Connections Created

**Use Case**: See total number of connections created since start

**PromQL**:
```promql
sum(db_client_connection_create_time_count{db_system="redis"})
```

**Panel Settings**:
- Type: Stat
- Unit: short
- Graph mode: Area
- Thresholds: 0 (green), 1000 (yellow), 10000 (red)

---

### 5. Connection Creation Time Heatmap

**Use Case**: Visualize distribution of connection creation times

**PromQL**:
```promql
sum(rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])) by (le)
```

**Panel Settings**:
- Type: Heatmap
- Format: Heatmap
- Color scheme: Spectral

---

### 6. Slow Connection Creations Alert

**Use Case**: Identify pools with slow connection creation (P99 > 100ms)

**PromQL**:
```promql
histogram_quantile(0.99, 
  rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])
) > 0.1
```

**Panel Settings**:
- Type: Table
- Format: Table
- Instant query: true
- Columns: server_address, db_client_connection_pool_name, redis_client_library, Value

---

### 7. Connection Creation Time by Pool

**Use Case**: Compare connection creation time across different pools

**PromQL**:
```promql
avg by (db_client_connection_pool_name) (
  rate(db_client_connection_create_time_sum{db_system="redis"}[5m]) 
  / 
  rate(db_client_connection_create_time_count{db_system="redis"}[5m])
)
```

**Panel Settings**:
- Type: Bar chart or Time series
- Unit: seconds (s)
- Legend: `{{db_client_connection_pool_name}}`

---

## Filtering Examples

### Filter by Server Address
```promql
rate(db_client_connection_create_time_sum{
  db_system="redis",
  server_address="localhost"
}[5m])
```

### Filter by Pool Name
```promql
rate(db_client_connection_create_time_sum{
  db_system="redis",
  db_client_connection_pool_name="localhost:6379"
}[5m])
```

### Filter by Client Library Version
```promql
rate(db_client_connection_create_time_sum{
  db_system="redis",
  redis_client_library=~"go-redis:9.*"
}[5m])
```

---

## Alert Rules

### Alert: High Connection Creation Latency

**Condition**: P99 connection creation time > 500ms for 5 minutes

**PromQL**:
```promql
histogram_quantile(0.99, 
  rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])
) > 0.5
```

**Alert Rule**:
```yaml
- alert: HighRedisConnectionCreationLatency
  expr: |
    histogram_quantile(0.99, 
      rate(db_client_connection_create_time_bucket{db_system="redis"}[5m])
    ) > 0.5
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High Redis connection creation latency"
    description: "P99 connection creation time is {{ $value }}s for pool {{ $labels.db_client_connection_pool_name }}"
```

---

### Alert: High Connection Creation Rate

**Condition**: Creating more than 10 connections/second for 5 minutes

**PromQL**:
```promql
rate(db_client_connection_create_time_count{db_system="redis"}[5m]) > 10
```

**Alert Rule**:
```yaml
- alert: HighRedisConnectionCreationRate
  expr: |
    rate(db_client_connection_create_time_count{db_system="redis"}[5m]) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High Redis connection creation rate"
    description: "Creating {{ $value }} connections/sec for pool {{ $labels.db_client_connection_pool_name }}"
```

---

## Dashboard Import

To import the complete dashboard:

1. Copy the contents of `grafana-dashboard-connection-create-time.json`
2. In Grafana, go to **Dashboards** → **Import**
3. Paste the JSON
4. Select your Prometheus data source
5. Click **Import**

---

## Troubleshooting

### No data showing up?

1. **Check if metrics are enabled**:
   ```go
   redisotel.Init(rdb)
   ```

2. **Verify Prometheus is scraping**:
   ```promql
   db_client_connection_create_time_count
   ```

3. **Check time range**: Metrics only appear after connections are created

4. **Verify labels**: Use Prometheus query browser to see available labels:
   ```promql
   db_client_connection_create_time_count{db_system="redis"}
   ```

### Histogram buckets not showing?

Make sure your Prometheus is configured to scrape histogram buckets:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'my-app'
    static_configs:
      - targets: ['localhost:8080']
```

---

## Next Steps

- See `connection_create_time_test.go` for testing examples
- Check `README.md` for full instrumentation guide
- Explore other connection metrics (coming soon):
  - `db.client.connection.count`
  - `db.client.connection.wait_time`
  - `db.client.connection.use_time`

