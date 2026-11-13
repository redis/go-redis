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

// collectAttrs returns the attribute sets of every data point recorded under
// metric name on reader.
func collectAttrs(t *testing.T, reader *metric.ManualReader, name string) []attribute.Set {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}
	var sets []attribute.Set
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					sets = append(sets, dp.Attributes)
				}
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					sets = append(sets, dp.Attributes)
				}
			default:
				t.Fatalf("unexpected data type %T for %s", m.Data, name)
			}
		}
	}
	return sets
}

func requireAttr(t *testing.T, set attribute.Set, key, want string) {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %s missing, got %v", key, set.ToSlice())
	}
	if v.AsString() != want {
		t.Fatalf("attribute %s = %q, want %q", key, v.AsString(), want)
	}
}

// TestCustomAttributesOnEveryMetric verifies the configured custom attributes
// land on a histogram, a counter and a MultiDB instrument.
func TestCustomAttributesOnEveryMetric(t *testing.T) {
	ctx := context.Background()
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = meterProvider.Shutdown(ctx) }()
	meter := meterProvider.Meter("test")

	opDuration, err := meter.Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatal(err)
	}
	connCount, err := meter.Int64UpDownCounter(MetricConnectionCount)
	if err != nil {
		t.Fatal(err)
	}
	failovers, err := meter.Int64Counter(MetricMultiDBFailovers)
	if err != nil {
		t.Fatal(err)
	}

	recorder := &metricsRecorder{
		operationDuration: opDuration,
		connectionCount:   connCount,
		multiDBFailovers:  failovers,
		cfg: &config{customAttributes: []attribute.KeyValue{
			attribute.String("team", "platform"),
			attribute.String("deployment", "test"),
		}},
	}

	recorder.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, nil, nil, 0)
	recorder.RecordConnectionCount(ctx, 1, nil, ConnectionStateIdle, false)
	recorder.RecordMultiDBFailover(ctx, "a.example", "b.example", "automatic", time.Millisecond)

	for _, name := range []string{MetricOperationDuration, MetricConnectionCount, MetricMultiDBFailovers} {
		sets := collectAttrs(t, reader, name)
		if len(sets) != 1 {
			t.Fatalf("%s: recorded %d data points, want 1", name, len(sets))
		}
		requireAttr(t, sets[0], "team", "platform")
		requireAttr(t, sets[0], "deployment", "test")
		requireAttr(t, sets[0], AttrDBSystemName, DBSystemRedis)
	}
}

// TestCustomAttributesOverrideBuiltIns verifies a custom attribute with a
// built-in key replaces the built-in value.
func TestCustomAttributesOverrideBuiltIns(t *testing.T) {
	ctx := context.Background()
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = meterProvider.Shutdown(ctx) }()

	opDuration, err := meterProvider.Meter("test").Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &metricsRecorder{
		operationDuration: opDuration,
		cfg:               &config{customAttributes: []attribute.KeyValue{attribute.String(AttrDBSystemName, "other")}},
	}
	recorder.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, nil, nil, 0)

	sets := collectAttrs(t, reader, MetricOperationDuration)
	if len(sets) != 1 {
		t.Fatalf("recorded %d data points, want 1", len(sets))
	}
	requireAttr(t, sets[0], AttrDBSystemName, "other")
}

// TestCustomAttributesNilConfig verifies a recorder without a config still
// records (the helper must tolerate cfg == nil).
func TestCustomAttributesNilConfig(t *testing.T) {
	ctx := context.Background()
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = meterProvider.Shutdown(ctx) }()

	opDuration, err := meterProvider.Meter("test").Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &metricsRecorder{operationDuration: opDuration}
	recorder.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, nil, nil, 0)

	if sets := collectAttrs(t, reader, MetricOperationDuration); len(sets) != 1 {
		t.Fatalf("recorded %d data points, want 1", len(sets))
	}
}

// TestWithCustomAttributesConfig verifies the builder accumulates and the
// internal config holds its own copy.
func TestWithCustomAttributesConfig(t *testing.T) {
	cfg := NewConfig().
		WithCustomAttributes(attribute.String("a", "1")).
		WithCustomAttributes(attribute.String("b", "2"))
	if len(cfg.CustomAttributes) != 2 {
		t.Fatalf("CustomAttributes has %d entries, want 2", len(cfg.CustomAttributes))
	}

	internal := (&ObservabilityInstance{}).configToInternal(cfg)
	cfg.CustomAttributes[0] = attribute.String("a", "changed")
	if got := internal.customAttributes[0].Value.AsString(); got != "1" {
		t.Fatalf("internal config shares the caller's slice: got %q, want %q", got, "1")
	}
}
