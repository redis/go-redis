package redisotel

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestConnectionCreateTimeMetric(t *testing.T) {
	// Create a manual reader for testing
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Initialize instrumentation with test provider
	Shutdown() // Reset any previous state
	if err := Init(rdb, WithMeterProvider(provider)); err != nil {
		t.Fatalf("Failed to initialize instrumentation: %v", err)
	}
	defer Shutdown()

	// Perform an operation that creates a connection
	ctx := context.Background()
	err := rdb.Ping(ctx).Err()
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Give metrics time to be recorded
	time.Sleep(100 * time.Millisecond)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Find the connection create time metric
	var found bool
	var histogram *metricdata.Histogram[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "db.client.connection.create_time" {
				found = true
				if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
					histogram = &h
				}
			}
		}
	}

	if !found {
		t.Fatal("db.client.connection.create_time metric not found")
	}

	if histogram == nil {
		t.Fatal("db.client.connection.create_time is not a histogram")
	}

	// Verify we have at least one data point
	if len(histogram.DataPoints) == 0 {
		t.Fatal("No data points recorded for connection create time")
	}

	// Verify the data point has reasonable values
	dp := histogram.DataPoints[0]
	if dp.Count == 0 {
		t.Error("Connection create time count is 0")
	}

	// Connection creation should take some time (at least microseconds)
	if dp.Sum <= 0 {
		t.Errorf("Connection create time sum is invalid: %v", dp.Sum)
	}

	t.Logf("Connection create time - count: %d, sum: %v seconds", dp.Count, dp.Sum)
}

func TestConnectionCreateTimeAttributes(t *testing.T) {
	// Create a manual reader for testing
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Initialize instrumentation with test provider
	Shutdown() // Reset any previous state
	if err := Init(rdb, WithMeterProvider(provider)); err != nil {
		t.Fatalf("Failed to initialize instrumentation: %v", err)
	}
	defer Shutdown()

	// Perform an operation that creates a connection
	ctx := context.Background()
	err := rdb.Ping(ctx).Err()
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Give metrics time to be recorded
	time.Sleep(100 * time.Millisecond)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Find the connection create time metric
	var histogram *metricdata.Histogram[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "db.client.connection.create_time" {
				if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
					histogram = &h
				}
			}
		}
	}

	if histogram == nil {
		t.Fatal("db.client.connection.create_time metric not found")
	}

	// Verify attributes
	if len(histogram.DataPoints) == 0 {
		t.Fatal("No data points recorded")
	}

	dp := histogram.DataPoints[0]
	attrs := dp.Attributes.ToSlice()

	// Check required attributes
	requiredAttrs := map[string]bool{
		"db.system":                         false,
		"server.address":                    false,
		"redis.client.library":              false,
		"db.client.connection.pool.name":    false,
	}

	for _, attr := range attrs {
		if _, ok := requiredAttrs[string(attr.Key)]; ok {
			requiredAttrs[string(attr.Key)] = true
			t.Logf("Attribute: %s = %v", attr.Key, attr.Value.AsString())
		}
	}

	// Verify all required attributes are present
	for attr, found := range requiredAttrs {
		if !found {
			t.Errorf("Required attribute %s not found", attr)
		}
	}
}

