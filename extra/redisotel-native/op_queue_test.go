package redisotel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestOpQueue(t *testing.T, opts ...metric.Option) (*opQueue, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(append([]metric.Option{metric.WithReader(reader)}, opts...)...)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	meter := mp.Meter("test")
	h, err := meter.Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatal(err)
	}
	q, err := newOpQueue(meter, h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.stop)
	return q, reader
}

// collect returns the operation-duration sample count and every metric name.
func collect(t *testing.T, reader *metric.ManualReader) (uint64, []string) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var n uint64
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == MetricOperationDuration {
				for _, p := range h.DataPoints {
					n += p.Count
				}
			}
		}
	}
	return n, names
}

// TestOpQueueCollectSeesQueuedSamples: a Collect right after recording counts
// every sample, though the worker records them on its own goroutine.
func TestOpQueueCollectSeesQueuedSamples(t *testing.T) {
	q, reader := newTestOpQueue(t)
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	var want uint64
	for round := 0; round < 200; round++ {
		for i := 0; i < 500; i++ {
			q.record(context.Background(), 0.001, set)
		}
		want += 500
		got, names := collect(t, reader)
		if got != want {
			t.Fatalf("round %d: collected %d samples, want %d", round, got, want)
		}
		for _, name := range names {
			if name != MetricOperationDuration {
				t.Fatalf("exported %q: the queue's gauge must not be exported", name)
			}
		}
	}
}

// TestOpQueueConcurrentRecordAndCollect: producers and collects at once lose
// no sample, and the queue never exports its own gauge.
func TestOpQueueConcurrentRecordAndCollect(t *testing.T) {
	q, reader := newTestOpQueue(t)
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	const producers, each = 8, 20000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = collect(t, reader)
			}
		}
	}()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				q.record(context.Background(), 0.001, set)
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-collected
	if got, _ := collect(t, reader); got != producers*each {
		t.Fatalf("collected %d samples, want %d", got, producers*each)
	}
}

// TestOpQueueStopRecordsQueued: stop records what is queued, and a sample
// after stop is recorded on the caller's goroutine.
func TestOpQueueStopRecordsQueued(t *testing.T) {
	q, reader := newTestOpQueue(t)
	collect(t, reader) // the first Collect turns the queue on
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	for i := 0; i < 1000; i++ {
		q.record(context.Background(), 0.001, set)
	}
	q.stop()
	q.record(context.Background(), 0.001, set)
	if got, _ := collect(t, reader); got != 1001 {
		t.Fatalf("collected %d samples, want 1001", got)
	}
}

// TestOpQueueWorkerEndsOnShutdown: each Init starts a worker and each Shutdown
// ends it (the worker closes exit when it returns).
func TestOpQueueWorkerEndsOnShutdown(t *testing.T) {
	mp := metric.NewMeterProvider(metric.WithReader(metric.NewManualReader()))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	inst := GetObservabilityInstance()
	_ = inst.Shutdown()
	for i := 0; i < 3; i++ {
		if err := inst.Init(NewConfig().WithEnabled(true).WithMeterProvider(mp)); err != nil {
			t.Fatal(err)
		}
		q := inst.recorder.opq
		if q == nil {
			t.Fatal("Init created no queue")
		}
		select {
		case <-q.exit:
			t.Fatal("worker ended before Shutdown")
		default:
		}
		if err := inst.Shutdown(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-q.exit:
		default:
			t.Fatal("worker still running after Shutdown")
		}
	}
}

// TestOpQueueStartsAfterFirstCollect: samples are recorded inline until a
// Collect has run the drain callback, then they are queued.
func TestOpQueueStartsAfterFirstCollect(t *testing.T) {
	q, reader := newTestOpQueue(t)
	if q.active.Load() {
		t.Fatal("queue on before any Collect")
	}
	collect(t, reader)
	if !q.active.Load() {
		t.Fatal("queue still off after a Collect")
	}
}

// TestOpQueueDroppedGaugeRecordsInline: a View that drops the queue's gauge
// leaves no drain callback, so the queue stays off and Collect still counts
// every sample.
func TestOpQueueDroppedGaugeRecordsInline(t *testing.T) {
	drop := metric.NewView(
		metric.Instrument{Name: "redis.client.internal.*"},
		metric.Stream{Aggregation: metric.AggregationDrop{}},
	)
	q, reader := newTestOpQueue(t, metric.WithView(drop))
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	var want uint64
	for round := 0; round < 20; round++ {
		for i := 0; i < 500; i++ {
			q.record(context.Background(), 0.001, set)
		}
		want += 500
		if got, _ := collect(t, reader); got != want {
			t.Fatalf("round %d: collected %d samples, want %d", round, got, want)
		}
	}
	if q.active.Load() {
		t.Fatal("queue turned on with its gauge dropped")
	}
}

// panicHistogram panics on every other Record.
type panicHistogram struct {
	embedded.Float64Histogram
	n, ok atomic.Int64
}

func (h *panicHistogram) Record(context.Context, float64, ...otelmetric.RecordOption) {
	if h.n.Add(1)%2 == 0 {
		panic("record")
	}
	h.ok.Add(1)
}

func (h *panicHistogram) Enabled(context.Context) bool { return true }

// TestOpQueueRecoversRecordPanic: a histogram that panics in Record does not end
// the worker or the collecting goroutine; the other samples are still recorded.
func TestOpQueueRecoversRecordPanic(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	h := &panicHistogram{}
	q, err := newOpQueue(mp.Meter("test"), h)
	if err != nil {
		t.Fatal(err)
	}
	defer q.stop()
	collect(t, reader) // turn the queue on
	set := attribute.NewSet(attribute.String(AttrDBOperationName, "get"))
	for i := 0; i < 1000; i++ {
		func() {
			// a sample recorded inline (queue busy) panics to the caller, as
			// before the queue; the caller's own guard recovers it
			defer func() { _ = recover() }()
			q.record(context.Background(), 0.001, set)
		}()
	}
	collect(t, reader) // drains on this goroutine: must not panic
	q.stop()
	select {
	case <-q.exit:
	default:
		t.Fatal("worker still running after stop")
	}
	if got := h.ok.Load(); got != 500 {
		t.Fatalf("recorded %d samples, want 500", got)
	}
}

// gaugeRejectingMeter fails every observable gauge.
type gaugeRejectingMeter struct{ otelmetric.Meter }

func (gaugeRejectingMeter) Int64ObservableGauge(string, ...otelmetric.Int64ObservableGaugeOption) (otelmetric.Int64ObservableGauge, error) {
	return nil, errors.New("observable gauges not supported")
}

// TestCreateRecorderWithoutQueueSupport: a meter that rejects the queue's gauge
// still yields a recorder, recording durations inline.
func TestCreateRecorderWithoutQueueSupport(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	o := &ObservabilityInstance{}
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(mp)
	rec, err := o.createRecorder(gaugeRejectingMeter{mp.Meter("test")}, o.configToInternal(cfg), nil)
	if err != nil {
		t.Fatalf("createRecorder: %v", err)
	}
	if rec.opq != nil {
		t.Fatal("queue created although its gauge was rejected")
	}
	ctx := context.Background()
	rec.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, nil, nil, 0)
	if got, _ := collect(t, reader); got != 1 {
		t.Fatalf("collected %d samples, want 1", got)
	}
}
