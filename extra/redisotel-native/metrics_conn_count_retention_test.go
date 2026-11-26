package redisotel

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/redis/go-redis/v9"
)

// Closed pools are observed at 0 for the retention window and then dropped,
// so client churn does not grow the observer or the exported series. Every
// reader that collects within the window still gets the final decrement.
func TestConnectionCountClosedSeriesExpire(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	deltaReader := func() *metric.ManualReader {
		return metric.NewManualReader(metric.WithTemporalitySelector(func(metric.InstrumentKind) metricdata.Temporality {
			return metricdata.DeltaTemporality
		}))
	}
	fast, slow := deltaReader(), deltaReader()
	cumulative := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(fast), metric.WithReader(slow), metric.WithReader(cumulative))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	inst := GetObservabilityInstance()
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(provider).WithClosedPoolSeriesRetention(time.Minute)
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = inst.Shutdown() })

	obs := inst.recorder.poolObs
	now := time.Unix(1000, 0)
	obs.mu.Lock()
	obs.now = func() time.Time { return now }
	obs.mu.Unlock()
	advance := func(d time.Duration) {
		obs.mu.Lock()
		now = now.Add(d)
		obs.mu.Unlock()
	}
	closedLen := func() int {
		obs.mu.Lock()
		defer obs.mu.Unlock()
		return len(obs.closed)
	}

	const prefix = "expire:6379_"
	sums := map[*metric.ManualReader]map[countKey]int64{fast: {}, slow: {}}
	collect := func(r *metric.ManualReader) {
		t.Helper()
		for k, v := range collectCounts(t, r, prefix) {
			sums[r][k] += v
		}
	}
	checkZero := func(step string) {
		t.Helper()
		for r, name := range map[*metric.ManualReader]string{fast: "fast", slow: "slow"} {
			if idle, used, _ := byState(sums[r]); idle != 0 || used != 0 {
				t.Fatalf("%s: %s reader summed idle=%d used=%d, want 0", step, name, idle, used)
			}
		}
	}

	const clients = 20
	for range clients {
		c := redis.NewClient(memClientOptions("expire:6379", nil))
		waitConns(t, c, 3)
		collect(fast)
		collect(slow)
		collectCounts(t, cumulative, prefix)
		_ = c.Close()
	}
	if got := closedLen(); got < clients {
		t.Fatalf("closed series = %d, want at least %d", got, clients)
	}

	// The slow reader collects once inside the window and gets the decrement.
	advance(30 * time.Second)
	collect(slow)
	collect(fast)
	checkZero("inside the window")

	// Past the window the series is observed one last time, then dropped.
	advance(time.Minute)
	collect(fast)
	if got := closedLen(); got != 0 {
		t.Fatalf("closed series after the window = %d, want 0", got)
	}
	collect(fast)
	collect(slow)
	checkZero("after the window")
	if got := collectCounts(t, cumulative, prefix); len(got) != 0 {
		t.Fatalf("cumulative reader still exports closed pools: %v", got)
	}
}

func TestClosedPoolSeriesRetentionDefault(t *testing.T) {
	o := &ObservabilityInstance{}
	if got := o.configToInternal(NewConfig()).closedSeriesRetention; got != defaultClosedSeriesRetention {
		t.Fatalf("default retention = %v, want %v", got, defaultClosedSeriesRetention)
	}
	if got := o.configToInternal(NewConfig().WithClosedPoolSeriesRetention(-time.Second)).closedSeriesRetention; got != defaultClosedSeriesRetention {
		t.Fatalf("negative retention = %v, want the default %v", got, defaultClosedSeriesRetention)
	}
	if got := o.configToInternal(NewConfig().WithClosedPoolSeriesRetention(time.Minute)).closedSeriesRetention; got != time.Minute {
		t.Fatalf("retention = %v, want 1m", got)
	}
}
