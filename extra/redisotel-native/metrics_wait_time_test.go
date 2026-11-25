package redisotel

import (
	"context"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/redis/go-redis/v9"
)

// waitTimeCount returns the db.client.connection.wait_time sample count of
// pools whose name starts with prefix.
func waitTimeCount(t *testing.T, reader *metric.ManualReader, prefix string) uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var n uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != MetricConnectionWaitTime {
				continue
			}
			for _, dp := range h.DataPoints {
				if name, _ := dp.Attributes.Value(AttrDBClientConnectionPoolName); strings.HasPrefix(name.AsString(), prefix) {
					n += dp.Count
				}
			}
		}
	}
	return n
}

// The global recorder has only connection-advanced on, so its queue was built
// for wait time. A per-client recorder on the same provider shares that queue
// and records its operation durations through it, to its own instrument.
func TestSharedQueueRecordsPerClientOperationDuration(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	reader := metric.NewManualReader()
	inst := initOn(t, reader, NewConfig().WithMetricGroups(MetricGroupFlagConnectionAdvanced))
	rec, err := inst.NewRecorder(NewConfig().WithMetricGroups(MetricGroupFlagCommand))
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.(*metricsRecorder).opq; got == nil || got != inst.recorder.opq {
		t.Fatal("per-client recorder does not share the global queue")
	}
	c := redis.NewClient(memClientOptions("shared:6379", rec))
	t.Cleanup(func() { _ = c.Close() })
	waitConns(t, c, 3)
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil { // first use runs the connection handshake
		t.Fatal(err)
	}
	collectOps := func() uint64 {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatal(err)
		}
		var n uint64
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == MetricOperationDuration {
					for _, dp := range h.DataPoints {
						n += dp.Count
					}
				}
			}
		}
		return n
	}
	collectOps() // turn the queue on
	before := collectOps()
	for i := 0; i < 100; i++ {
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if got := collectOps() - before; got != 100 {
		t.Fatalf("%d operation durations, want 100", got)
	}
}

// Every Get records one wait_time sample, also when the queue records most of
// them and concurrent Gets fall back to inline recording. Collect sees them
// all at once, without waiting for the worker.
func TestWaitTimeOneSamplePerGet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups MetricGroupFlags
	}{
		{"all groups", MetricGroupAll},
		{"connection-advanced only", MetricGroupFlagConnectionAdvanced},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetObservabilityForTest()
			t.Cleanup(resetObservabilityForTest)
			reader := metric.NewManualReader()
			inst := initOn(t, reader, NewConfig().WithMetricGroups(tc.groups))
			if inst.recorder.opq == nil {
				t.Fatal("no queue: wait_time is recorded inline")
			}
			c := redis.NewClient(memClientOptions("wait:6379", nil))
			t.Cleanup(func() { _ = c.Close() })
			waitConns(t, c, 3)
			ctx := context.Background()
			waitTimeCount(t, reader, "wait:6379_") // turn the queue on

			for round := 0; round < 20; round++ {
				var wg sync.WaitGroup
				for g := 0; g < 16; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 100; i++ {
							if err := c.Ping(ctx).Err(); err != nil {
								t.Error(err)
								return
							}
						}
					}()
				}
				wg.Wait()
				s := c.PoolStats()
				if got, want := waitTimeCount(t, reader, "wait:6379_"), uint64(s.Hits+s.Misses); got != want {
					t.Fatalf("round %d: %d wait_time samples, want %d (one per Get)", round, got, want)
				}
			}
		})
	}
}
