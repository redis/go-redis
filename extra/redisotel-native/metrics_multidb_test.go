package redisotel

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// multiDBRecorder mirrors redis.OTelMultiDBRecorder, the capability interface
// the MultiDB client asserts for on the recorder (go-redis #3954). Pinning
// the method set here keeps the signatures in step until that interface is
// on master; then this can become a direct assertion against it.
type multiDBRecorder interface {
	RecordMultiDBFailover(ctx context.Context, fromFQDN, toFQDN, reason string, duration time.Duration)
	RecordMultiDBActiveDatabaseChange(ctx context.Context, fromFQDN, toFQDN string)
	RecordMultiDBCircuitStateChange(ctx context.Context, dbFQDN, fromState, toState string)
	RecordMultiDBHealthCheck(ctx context.Context, dbFQDN string, success bool, duration time.Duration)
}

var _ multiDBRecorder = (*metricsRecorder)(nil)

// newTestRecorder builds a recorder for the given metric groups on a manual
// reader, without touching the global observability instance.
func newTestRecorder(t *testing.T, groups MetricGroupFlags) (*metricsRecorder, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	o := &ObservabilityInstance{}
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(mp).WithMetricGroups(groups)
	rec, err := o.createRecorder(mp.Meter("test"), o.configToInternal(cfg))
	if err != nil {
		t.Fatalf("createRecorder: %v", err)
	}
	return rec, reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func attrString(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %q missing from %v", key, set.ToSlice())
	}
	return v.AsString()
}

func TestMultiDBMetricNames(t *testing.T) {
	for name, want := range map[string]string{
		MetricMultiDBFailovers:             "redis.client.multidb.failovers",
		MetricMultiDBFailoverDuration:      "redis.client.multidb.failover.duration",
		MetricMultiDBActiveDatabaseChanges: "redis.client.multidb.active_database.changes",
		MetricMultiDBCircuitStateChanges:   "redis.client.multidb.circuit_breaker.state_changes",
		MetricMultiDBHealthChecks:          "redis.client.multidb.health_checks",
		MetricMultiDBHealthCheckDuration:   "redis.client.multidb.health_check.duration",
	} {
		if name != want {
			t.Errorf("metric name %q, want %q", name, want)
		}
	}
}

func TestMultiDBMetricsRecorded(t *testing.T) {
	ctx := context.Background()
	rec, reader := newTestRecorder(t, MetricGroupFlagMultiDB)

	rec.RecordMultiDBFailover(ctx, "a.example.com", "b.example.com", "automatic", 250*time.Millisecond)
	rec.RecordMultiDBActiveDatabaseChange(ctx, "a.example.com", "b.example.com")
	rec.RecordMultiDBCircuitStateChange(ctx, "a.example.com", "closed", "open")
	rec.RecordMultiDBHealthCheck(ctx, "b.example.com", true, 10*time.Millisecond)
	rec.RecordMultiDBHealthCheck(ctx, "a.example.com", false, 3*time.Second)

	got := collectMetrics(t, reader)

	failovers, ok := got[MetricMultiDBFailovers].Data.(metricdata.Sum[int64])
	if !ok || len(failovers.DataPoints) != 1 || failovers.DataPoints[0].Value != 1 {
		t.Fatalf("%s: want one data point with value 1, got %+v", MetricMultiDBFailovers, got[MetricMultiDBFailovers].Data)
	}
	fa := failovers.DataPoints[0].Attributes
	if attrString(t, fa, AttrRedisClientMultiDBFromDatabase) != "a.example.com" ||
		attrString(t, fa, AttrRedisClientMultiDBToDatabase) != "b.example.com" ||
		attrString(t, fa, AttrRedisClientMultiDBFailoverReason) != "automatic" {
		t.Fatalf("%s attributes = %v", MetricMultiDBFailovers, fa.ToSlice())
	}
	failoverDur, ok := got[MetricMultiDBFailoverDuration].Data.(metricdata.Histogram[float64])
	if !ok || len(failoverDur.DataPoints) != 1 || failoverDur.DataPoints[0].Count != 1 || failoverDur.DataPoints[0].Sum != 0.25 {
		t.Fatalf("%s: want one point, count 1, sum 0.25s, got %+v", MetricMultiDBFailoverDuration, got[MetricMultiDBFailoverDuration].Data)
	}

	changes, ok := got[MetricMultiDBActiveDatabaseChanges].Data.(metricdata.Sum[int64])
	if !ok || len(changes.DataPoints) != 1 || changes.DataPoints[0].Value != 1 {
		t.Fatalf("%s: want one data point with value 1, got %+v", MetricMultiDBActiveDatabaseChanges, got[MetricMultiDBActiveDatabaseChanges].Data)
	}

	cb, ok := got[MetricMultiDBCircuitStateChanges].Data.(metricdata.Sum[int64])
	if !ok || len(cb.DataPoints) != 1 {
		t.Fatalf("%s: want one data point, got %+v", MetricMultiDBCircuitStateChanges, got[MetricMultiDBCircuitStateChanges].Data)
	}
	ca := cb.DataPoints[0].Attributes
	if attrString(t, ca, AttrRedisClientMultiDBDatabase) != "a.example.com" ||
		attrString(t, ca, AttrRedisClientMultiDBCircuitFromState) != "closed" ||
		attrString(t, ca, AttrRedisClientMultiDBCircuitToState) != "open" {
		t.Fatalf("%s attributes = %v", MetricMultiDBCircuitStateChanges, ca.ToSlice())
	}

	// Health checks: one series per (database, success).
	hc, ok := got[MetricMultiDBHealthChecks].Data.(metricdata.Sum[int64])
	if !ok || len(hc.DataPoints) != 2 {
		t.Fatalf("%s: want two data points (one per database/outcome), got %+v", MetricMultiDBHealthChecks, got[MetricMultiDBHealthChecks].Data)
	}
	for _, dp := range hc.DataPoints {
		success, ok := dp.Attributes.Value(attribute.Key(AttrRedisClientMultiDBHealthCheckSuccess))
		if !ok {
			t.Fatalf("%s: success attribute missing: %v", MetricMultiDBHealthChecks, dp.Attributes.ToSlice())
		}
		db := attrString(t, dp.Attributes, AttrRedisClientMultiDBDatabase)
		if (db == "b.example.com") != success.AsBool() {
			t.Fatalf("%s: database %s recorded with success=%v", MetricMultiDBHealthChecks, db, success.AsBool())
		}
	}
	hcDur, ok := got[MetricMultiDBHealthCheckDuration].Data.(metricdata.Histogram[float64])
	if !ok || len(hcDur.DataPoints) != 2 {
		t.Fatalf("%s: want two data points, got %+v", MetricMultiDBHealthCheckDuration, got[MetricMultiDBHealthCheckDuration].Data)
	}
}

// With the group disabled the instruments are nil and the methods are no-ops.
func TestMultiDBMetricsDisabledGroup(t *testing.T) {
	ctx := context.Background()
	rec, reader := newTestRecorder(t, MetricGroupFlagCommand)

	rec.RecordMultiDBFailover(ctx, "a", "b", "manual", time.Second)
	rec.RecordMultiDBActiveDatabaseChange(ctx, "a", "b")
	rec.RecordMultiDBCircuitStateChange(ctx, "a", "open", "half-open")
	rec.RecordMultiDBHealthCheck(ctx, "a", true, time.Millisecond)

	got := collectMetrics(t, reader)
	for _, name := range []string{
		MetricMultiDBFailovers, MetricMultiDBFailoverDuration, MetricMultiDBActiveDatabaseChanges,
		MetricMultiDBCircuitStateChanges, MetricMultiDBHealthChecks, MetricMultiDBHealthCheckDuration,
	} {
		if _, present := got[name]; present {
			t.Errorf("%s recorded although the multidb metric group is disabled", name)
		}
	}
}

// MetricGroupAll includes the MultiDB group.
func TestMultiDBGroupInAll(t *testing.T) {
	if MetricGroupAll&MetricGroupFlagMultiDB == 0 {
		t.Fatal("MetricGroupAll does not include MetricGroupFlagMultiDB")
	}
	o := &ObservabilityInstance{}
	internal := o.configToInternal(NewConfig())
	if !internal.isMetricGroupEnabled(MetricGroupMultiDB) {
		t.Fatal("default config does not enable the multidb metric group")
	}
}
