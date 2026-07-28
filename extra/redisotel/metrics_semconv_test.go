package redisotel

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// cumulativeMetricNames are the connection-pool statistics that the
// OpenTelemetry database semantic conventions define as monotonic Counters.
var cumulativeMetricNames = map[string]struct{}{
	"db.client.connections.waits":    {},
	"db.client.connections.timeouts": {},
	"db.client.connections.hits":     {},
	"db.client.connections.misses":   {},
}

// collectPoolMetrics wires a manual reader to a fresh client instrumented with
// the given options, then returns the collected instrument for each cumulative
// pool-stat metric name.
func collectPoolMetrics(t *testing.T, opts ...MetricsOption) map[string]metricdata.Aggregation {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = rdb.Close() })

	allOpts := append([]MetricsOption{WithMeterProvider(mp)}, opts...)
	if err := InstrumentMetrics(rdb, allOpts...); err != nil {
		t.Fatalf("InstrumentMetrics: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := make(map[string]metricdata.Aggregation)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if _, ok := cumulativeMetricNames[m.Name]; ok {
				got[m.Name] = m.Data
			}
		}
	}
	return got
}

// TestCumulativePoolStats_DefaultIsUpDownCounter verifies the backwards-compatible
// default: the cumulative pool metrics are exported as non-monotonic sums
// (UpDownCounter), matching pre-#3612 behaviour so existing exporters do not
// see an instrument-type change.
func TestCumulativePoolStats_DefaultIsUpDownCounter(t *testing.T) {
	got := collectPoolMetrics(t)

	for name := range cumulativeMetricNames {
		data, ok := got[name]
		if !ok {
			t.Errorf("metric %q not collected", name)
			continue
		}
		sum, ok := data.(metricdata.Sum[int64])
		if !ok {
			t.Errorf("metric %q: expected Sum[int64], got %T", name, data)
			continue
		}
		if sum.IsMonotonic {
			t.Errorf("metric %q: expected non-monotonic (UpDownCounter) by default, got monotonic", name)
		}
	}
}

// TestCumulativePoolStats_OptInIsCounter verifies that enabling
// WithSemConvCompliantMetrics switches the cumulative pool metrics to monotonic
// sums (Counter), as required by the OpenTelemetry database semantic
// conventions. See #3612.
func TestCumulativePoolStats_OptInIsCounter(t *testing.T) {
	got := collectPoolMetrics(t, WithSemConvCompliantMetrics(true))

	for name := range cumulativeMetricNames {
		data, ok := got[name]
		if !ok {
			t.Errorf("metric %q not collected", name)
			continue
		}
		sum, ok := data.(metricdata.Sum[int64])
		if !ok {
			t.Errorf("metric %q: expected Sum[int64], got %T", name, data)
			continue
		}
		if !sum.IsMonotonic {
			t.Errorf("metric %q: expected monotonic (Counter) when semconv-compliant, got non-monotonic", name)
		}
	}
}
