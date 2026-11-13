package redisotel

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/redis/go-redis/v9"
)

// initForTest resets the singleton and initializes it on a fresh manual
// reader with the given global custom attributes.
func initForTest(t *testing.T, globalAttrs ...attribute.KeyValue) (*ObservabilityInstance, *metric.ManualReader) {
	t.Helper()
	resetObservabilityForTest()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	inst := GetObservabilityInstance()
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(provider).WithCustomAttributes(globalAttrs...)
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		_ = inst.Shutdown()
		_ = provider.Shutdown(context.Background())
		resetObservabilityForTest()
	})
	return inst, reader
}

func TestNewRecorderRequiresInit(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	if _, err := GetObservabilityInstance().NewRecorder(NewConfig()); err == nil {
		t.Fatal("NewRecorder before Init: want error")
	}
}

// TestNewRecorderWhenMetricsDisabled pins that, with Init called on a disabled config,
// NewRecorder returns a nil recorder and no error, so an app with a metrics
// on/off switch can pass the result to Options.OTelRecorder unconditionally.
func TestNewRecorderWhenMetricsDisabled(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	inst := GetObservabilityInstance()
	if err := inst.Init(NewConfig().WithEnabled(false)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	rec, err := inst.NewRecorder(NewConfig())
	if err != nil {
		t.Fatalf("NewRecorder with metrics disabled: %v, want nil error", err)
	}
	if rec != nil {
		t.Fatalf("NewRecorder with metrics disabled = %v, want nil recorder", rec)
	}
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", OTelRecorder: rec})
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestNewRecorderAttributePrecedence pins built-in < Init < per-client on a
// histogram, a counter and a MultiDB instrument, and that the two recorders
// on one provider produce separate series with separate caches.
func TestNewRecorderAttributePrecedence(t *testing.T) {
	inst, reader := initForTest(t,
		attribute.String("deployment.environment", "prod"),
		attribute.String("tenant", "global"),
		attribute.String(AttrDBSystemName, "overridden-global"), // built-in key: Init wins over built-in
	)
	rec, err := inst.NewRecorder(NewConfig().WithCustomAttributes(
		attribute.String("tenant", "acme"),
		attribute.String(AttrRedisClientErrorsCategory, "per-client"), // built-in key on the error path
	))
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	mr := rec.(*metricsRecorder)
	if mr.opq != inst.recorder.opq || mr.opq == nil {
		t.Fatal("per-client recorder on the Init provider did not share the operation-duration queue")
	}
	if mr.opAttrsLen != 0 || &mr.opAttrs == &inst.recorder.opAttrs {
		t.Fatal("per-client recorder shares the attribute cache")
	}

	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, "ping")
	mr.RecordOperationDuration(ctx, time.Millisecond, cmd, 1, nil, nil, 0)
	inst.recorder.RecordOperationDuration(ctx, time.Millisecond, cmd, 1, nil, nil, 0)
	mr.RecordError(ctx, "ERR", nil, "ERR", false, 0)
	mr.RecordMultiDBFailover(ctx, "a", "b", "manual", time.Second)
	mr.RecordConnectionCount(ctx, 1, nil, "idle", false)

	sets := collectAttrs(t, reader, MetricOperationDuration)
	if len(sets) != 2 {
		t.Fatalf("operation duration series = %d, want 2 (global and per-client)", len(sets))
	}
	var perClient, global int
	for _, set := range sets {
		requireAttr(t, set, "deployment.environment", "prod")
		requireAttr(t, set, AttrDBSystemName, "overridden-global")
		v, _ := set.Value("tenant")
		switch v.AsString() {
		case "acme":
			perClient++
		case "global":
			global++
		}
	}
	if perClient != 1 || global != 1 {
		t.Fatalf("tenant split: per-client=%d global=%d", perClient, global)
	}
	for _, name := range []string{MetricClientErrors, MetricMultiDBFailovers, MetricConnectionCount} {
		sets := collectAttrs(t, reader, name)
		if len(sets) != 1 {
			t.Fatalf("%s series = %d, want 1", name, len(sets))
		}
		requireAttr(t, sets[0], "tenant", "acme")
		requireAttr(t, sets[0], "deployment.environment", "prod")
	}
	requireAttr(t, collectAttrs(t, reader, MetricClientErrors)[0], AttrRedisClientErrorsCategory, "per-client")
}

func TestNewRecorderIgnoresEnabledAndDefaultsProvider(t *testing.T) {
	inst, reader := initForTest(t)
	rec, err := inst.NewRecorder(NewConfig()) // Enabled false, no provider
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if !rec.(*metricsRecorder).Enabled() {
		t.Fatal("recorder from a disabled config is off; cfg.Enabled must be ignored")
	}
	rec.RecordError(context.Background(), "ERR", nil, "ERR", false, 0)
	if got := len(collectAttrs(t, reader, MetricClientErrors)); got != 1 {
		t.Fatalf("error series on the Init provider = %d, want 1", got)
	}
}

func TestNewRecorderOwnProviderOwnQueue(t *testing.T) {
	inst, initReader := initForTest(t)
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	rec, err := inst.NewRecorder(NewConfig().WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	mr := rec.(*metricsRecorder)
	if mr.opq == inst.recorder.opq {
		t.Fatal("recorder on another provider shares the Init queue")
	}
	rec.RecordError(context.Background(), "ERR", nil, "ERR", false, 0)
	if got := len(collectAttrs(t, reader, MetricClientErrors)); got != 1 {
		t.Fatalf("error series on the client's provider = %d, want 1", got)
	}
	if got := len(collectAttrs(t, initReader, MetricClientErrors)); got != 0 {
		t.Fatalf("error series leaked to the Init provider: %d", got)
	}
}

// TestNewRecorderStopsOnShutdown pins that Shutdown turns issued recorders off
// but keeps their UpDown counters flowing so a later client Close balances them.
func TestNewRecorderStopsOnShutdown(t *testing.T) {
	inst, reader := initForTest(t)
	rec, err := inst.NewRecorder(NewConfig())
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	ctx := context.Background()
	mr := rec.(*metricsRecorder)
	mr.RecordConnectionCount(ctx, 1, nil, "idle", false)
	if err := inst.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if mr.Enabled() {
		t.Fatal("recorder still enabled after Shutdown")
	}
	rec.RecordError(ctx, "ERR", nil, "ERR", false, 0)
	if got := len(collectAttrs(t, reader, MetricClientErrors)); got != 0 {
		t.Fatalf("error recorded after Shutdown: %d series", got)
	}
	mr.RecordConnectionCount(ctx, -1, nil, "idle", false)
	sets := collectAttrs(t, reader, MetricConnectionCount)
	if len(sets) != 1 {
		t.Fatalf("connection count series = %d, want 1", len(sets))
	}
}

// TestNewRecorderSnapshotsInitAttributes pins that the base layer is Init's
// attributes as they were at Init, not the caller-owned Config read later.
func TestNewRecorderSnapshotsInitAttributes(t *testing.T) {
	resetObservabilityForTest()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	inst := GetObservabilityInstance()
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(provider).
		WithCustomAttributes(attribute.String("deployment.environment", "prod"))
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		_ = inst.Shutdown()
		_ = provider.Shutdown(context.Background())
		resetObservabilityForTest()
	})
	cfg.CustomAttributes[0] = attribute.String("deployment.environment", "mutated")

	// The same provider passed explicitly is still the Init provider.
	rec, err := inst.NewRecorder(NewConfig().WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if rec.(*metricsRecorder).opq != inst.recorder.opq {
		t.Fatal("explicit Init provider did not share the operation-duration queue")
	}
	rec.RecordError(context.Background(), "ERR", nil, "ERR", false, 0)
	sets := collectAttrs(t, reader, MetricClientErrors)
	if len(sets) != 1 {
		t.Fatalf("error series = %d, want 1", len(sets))
	}
	requireAttr(t, sets[0], "deployment.environment", "prod")
}

// TestShutdownStopsOwnQueues pins that Shutdown ends the worker of a recorder
// built on another provider, as it does for the global recorder.
func TestShutdownStopsOwnQueues(t *testing.T) {
	inst, _ := initForTest(t)
	provider := metric.NewMeterProvider(metric.WithReader(metric.NewManualReader()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	rec, err := inst.NewRecorder(NewConfig().WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	q := rec.(*metricsRecorder).opq
	if q == nil {
		t.Fatal("recorder on its own provider has no queue")
	}
	if err := inst.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-q.exit:
	case <-time.After(time.Second):
		t.Fatal("own queue worker still running after Shutdown")
	}
}
