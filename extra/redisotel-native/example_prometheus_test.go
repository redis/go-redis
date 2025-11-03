package redisotel_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	redisotel "github.com/redis/go-redis/extra/redisotel-native/v9"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

// ExamplePrometheusExporter demonstrates how to expose Redis metrics to Prometheus
func ExamplePrometheusExporter() {
	// Create Prometheus exporter
	exporter, err := prometheus.New()
	if err != nil {
		panic(err)
	}

	// Create meter provider with Prometheus exporter
	provider := metric.NewMeterProvider(metric.WithReader(exporter))

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Initialize OTel instrumentation with Prometheus exporter
	if err := redisotel.Init(rdb, redisotel.WithMeterProvider(provider)); err != nil {
		panic(err)
	}
	defer redisotel.Shutdown()

	// Start Prometheus HTTP server
	go func() {
		http.Handle("/metrics", exporter)
		fmt.Println("Prometheus metrics available at http://localhost:2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			panic(err)
		}
	}()

	// Perform some Redis operations to generate metrics
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		// This will create connections and record metrics
		if err := rdb.Ping(ctx).Err(); err != nil {
			fmt.Printf("Redis error: %v\n", err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Keep the server running to allow scraping
	fmt.Println("Metrics are being collected. Visit http://localhost:2112/metrics")
	fmt.Println("Press Ctrl+C to stop")
	
	// In a real application, this would run indefinitely
	time.Sleep(1 * time.Minute)

	// Output:
	// Prometheus metrics available at http://localhost:2112/metrics
	// Metrics are being collected. Visit http://localhost:2112/metrics
	// Press Ctrl+C to stop
}

// ExamplePrometheusMetricsOutput shows what the Prometheus metrics look like
func ExamplePrometheusMetricsOutput() {
	fmt.Println(`# Example Prometheus metrics output:

# HELP db_client_connection_create_time_seconds The time it took to create a new connection
# TYPE db_client_connection_create_time_seconds histogram
db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.0001"} 0
db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.0005"} 0
db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.001"} 0
db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.005"} 5
db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.01"} 10
db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="+Inf"} 10
db_client_connection_create_time_seconds_sum{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost"} 0.0456789
db_client_connection_create_time_seconds_count{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost"} 10

# This shows:
# - 10 connections were created
# - Total time: 0.0456789 seconds (45.6ms)
# - Average: 4.56ms per connection
# - 5 connections took between 1-5ms
# - 5 connections took between 5-10ms
`)

	// Output:
	// # Example Prometheus metrics output:
	//
	// # HELP db_client_connection_create_time_seconds The time it took to create a new connection
	// # TYPE db_client_connection_create_time_seconds histogram
	// db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.0001"} 0
	// db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.0005"} 0
	// db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.001"} 0
	// db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.005"} 5
	// db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="0.01"} 10
	// db_client_connection_create_time_seconds_bucket{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost",le="+Inf"} 10
	// db_client_connection_create_time_seconds_sum{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost"} 0.0456789
	// db_client_connection_create_time_seconds_count{db_client_connection_pool_name="localhost:6379",db_system="redis",redis_client_library="go-redis:9.16.0-beta.1",server_address="localhost"} 10
	//
	// # This shows:
	// # - 10 connections were created
	// # - Total time: 0.0456789 seconds (45.6ms)
	// # - Average: 4.56ms per connection
	// # - 5 connections took between 1-5ms
	// # - 5 connections took between 5-10ms
}

