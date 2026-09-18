package redisotel

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestOperationDurationSkipsNilReplyAttributes verifies a Nil reply records no
// error attributes on db.client.operation.duration.
func TestOperationDurationSkipsNilReplyAttributes(t *testing.T) {
	ctx := context.Background()
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() {
		_ = meterProvider.Shutdown(ctx)
	}()

	histogram, err := meterProvider.Meter("test").Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}
	recorder := &metricsRecorder{operationDuration: histogram}

	recorder.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, redis.Nil, nil, 0)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	points := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != MetricOperationDuration {
				continue
			}
			for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				points++
				for _, attr := range dp.Attributes.ToSlice() {
					switch string(attr.Key) {
					case AttrErrorType, AttrRedisClientErrorsCategory, AttrDBResponseStatusCode:
						t.Errorf("Nil reply tagged %s=%v, expected no error attributes", attr.Key, attr.Value.AsString())
					}
				}
			}
		}
	}
	if points != 1 {
		t.Fatalf("Recorded %d data points, expected 1", points)
	}
}
