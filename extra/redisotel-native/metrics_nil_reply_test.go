package redisotel

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
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

// TestOperationDurationRecordsNilReplyAttributes verifies a Nil reply is tagged
// with the NIL error type on db.client.operation.duration when WithRecordNilErrors is set.
func TestOperationDurationRecordsNilReplyAttributes(t *testing.T) {
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
	recorder := &metricsRecorder{
		operationDuration: histogram,
		cfg:               &config{recordNilErrors: true},
	}

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
				for _, key := range []string{AttrErrorType, AttrRedisClientErrorsCategory, AttrDBResponseStatusCode} {
					value, ok := dp.Attributes.Value(attribute.Key(key))
					if !ok || value.AsString() != redis.ErrorTypeNil {
						t.Errorf("Nil reply tagged %s=%v, expected %s", key, value.AsString(), redis.ErrorTypeNil)
					}
				}
			}
		}
	}
	if points != 1 {
		t.Fatalf("Recorded %d data points, expected 1", points)
	}
}

// TestRecordErrorNilReply verifies RecordError drops the NIL error type by
// default, records it with WithRecordNilErrors, and always records real errors.
func TestRecordErrorNilReply(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recordNil bool
		want      map[string]int64
	}{
		{name: "default", recordNil: false, want: map[string]int64{"WRONGTYPE": 1}},
		{name: "record nil", recordNil: true, want: map[string]int64{"WRONGTYPE": 1, "NIL": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			reader := metric.NewManualReader()
			meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
			defer func() {
				_ = meterProvider.Shutdown(ctx)
			}()

			counter, err := meterProvider.Meter("test").Int64Counter(MetricClientErrors)
			if err != nil {
				t.Fatalf("Failed to create counter: %v", err)
			}
			recorder := &metricsRecorder{
				clientErrors: counter,
				cfg:          &config{recordNilErrors: tc.recordNil},
			}

			recorder.RecordError(ctx, "NIL", nil, "NIL", false, 0)
			recorder.RecordError(ctx, "WRONGTYPE", nil, "WRONGTYPE", false, 0)

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(ctx, &rm); err != nil {
				t.Fatalf("Failed to collect metrics: %v", err)
			}

			got := map[string]int64{}
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name != MetricClientErrors {
						continue
					}
					for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
						errorType, _ := dp.Attributes.Value(AttrErrorType)
						got[errorType.AsString()] += dp.Value
					}
				}
			}
			if len(got) != len(tc.want) {
				t.Fatalf("Recorded %v, expected %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("Recorded %v, expected %v", got, tc.want)
				}
			}
		})
	}
}
