package redisotel

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/redis/go-redis/v9"
)

// collectPending returns the pending_requests values on reader for pools whose
// name starts with prefix, and checks the exported shape.
func collectPending(t *testing.T, reader *metric.ManualReader, prefix string) []int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var got []int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != MetricConnectionPendingReqs {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || sum.IsMonotonic || m.Unit != "{request}" {
				t.Fatalf("%s: %T monotonic=%v unit=%q, want a non-monotonic Sum in {request}", m.Name, m.Data, sum.IsMonotonic, m.Unit)
			}
			for _, dp := range sum.DataPoints {
				name, _ := dp.Attributes.Value(AttrDBClientConnectionPoolName)
				if strings.HasPrefix(name.AsString(), prefix) {
					got = append(got, dp.Value)
				}
			}
		}
	}
	return got
}

// A Get blocked on a full pool is observed as pending, and the value is back
// to 0 once it gets the connection. The series appears with the first Get,
// and stays at 0 after the client is closed, as the synchronous counter did.
// Only connection-advanced is on: pending must not depend on the count's group.
func TestPendingRequestsObservedWhileGetBlocks(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	reader := metric.NewManualReader()
	initOn(t, reader, NewConfig().WithMetricGroups(MetricGroupFlagConnectionAdvanced))
	opt := memClientOptions("pending:6379", nil)
	opt.PoolSize, opt.MinIdleConns, opt.PoolTimeout = 1, 1, 10*time.Second
	c := redis.NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	waitConns(t, c, 1)
	ctx := context.Background()

	check := func(step string, want ...int64) {
		t.Helper()
		got := collectPending(t, reader, "pending:6379_")
		if len(got) != len(want) || (len(want) == 1 && got[0] != want[0]) {
			t.Fatalf("%s: pending %v, want %v", step, got, want)
		}
	}
	check("before any Get")

	held := c.Conn()
	if err := held.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Ping(ctx).Err() }()
	deadline := time.Now().Add(2 * time.Second)
	for c.PoolStats().PendingRequests != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	check("while a Get waits", 1)

	_ = held.Close()
	if err := <-done; err != nil {
		t.Fatalf("blocked Ping: %v", err)
	}
	check("after the Get", 0)

	_ = c.Close()
	check("after Close", 0)
}
