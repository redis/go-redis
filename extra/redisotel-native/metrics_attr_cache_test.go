package redisotel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeConnInfo struct{ remote net.Addr }

func (c *fakeConnInfo) RemoteAddr() net.Addr { return c.remote }
func (c *fakeConnInfo) PoolName() string     { return "p" }

func newDurationRecorder(t *testing.T) (*metricsRecorder, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	h, err := mp.Meter("test").Float64Histogram(MetricOperationDuration)
	if err != nil {
		t.Fatal(err)
	}
	return &metricsRecorder{operationDuration: h}, reader
}

func durationPoints(t *testing.T, reader *metric.ManualReader) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var out []metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == MetricOperationDuration {
				out = append(out, m.Data.(metricdata.Histogram[float64]).DataPoints...)
			}
		}
	}
	return out
}

// TestOperationDurationAttributesWithCache records the same command on the same
// connection twice and then with an error: the successes share one series with
// the full attribute set, and the error gets its own series with error attributes.
func TestOperationDurationAttributesWithCache(t *testing.T) {
	ctx := context.Background()
	r, reader := newDurationRecorder(t)
	cn := &fakeConnInfo{remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 7001}}

	for i := 0; i < 2; i++ {
		r.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, nil, cn, 3)
	}
	r.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, errors.New("WRONGTYPE Operation"), cn, 3)

	want := attribute.NewSet(
		attribute.String(AttrDBOperationName, "get"),
		getLibraryVersionAttr(),
		attribute.Int(AttrRedisClientOperationRetryAttempts, 0),
		attribute.String(AttrDBSystemName, DBSystemRedis),
		attribute.String(AttrServerAddress, "10.0.0.7"),
		attribute.String(AttrDBNamespace, "3"),
		attribute.String(AttrServerPort, "7001"),
		attribute.String(AttrNetworkPeerAddress, "10.0.0.7"),
		attribute.String(AttrNetworkPeerPort, "7001"),
	)
	var ok, failed int
	for _, p := range durationPoints(t, reader) {
		if _, isErr := p.Attributes.Value(AttrErrorType); isErr {
			failed++
			if p.Count != 1 {
				t.Errorf("error series count = %d, want 1", p.Count)
			}
			continue
		}
		ok++
		if !p.Attributes.Equals(&want) {
			t.Errorf("success attributes = %v, want %v", p.Attributes.ToSlice(), want.ToSlice())
		}
		if p.Count != 2 {
			t.Errorf("success series count = %d, want 2", p.Count)
		}
	}
	if ok != 1 || failed != 1 {
		t.Fatalf("got %d success and %d error series, want 1 and 1", ok, failed)
	}
}

// TestOperationDurationAttrCacheIsBounded records more distinct commands than the
// cache holds: the cache is cleared instead of growing, and every record still lands.
func TestOperationDurationAttrCacheIsBounded(t *testing.T) {
	ctx := context.Background()
	r, reader := newDurationRecorder(t)
	n := maxOpAttrSets + 100
	for i := 0; i < n; i++ {
		r.RecordOperationDuration(ctx, time.Millisecond, redis.NewCmd(ctx, fmt.Sprintf("cmd%d", i)), 1, nil, nil, 0)
	}
	if got := cachedSets(r); got > maxOpAttrSets {
		t.Fatalf("cache holds %d sets, want at most %d", got, maxOpAttrSets)
	}
	var total uint64 // the SDK folds series past its cardinality limit into one overflow series
	for _, p := range durationPoints(t, reader) {
		total += p.Count
	}
	if total != uint64(n) {
		t.Fatalf("recorded %d samples, want %d", total, n)
	}
}

func cachedSets(r *metricsRecorder) int {
	n := 0
	r.opAttrs.Range(func(any, any) bool { n++; return true })
	return n
}

// TestOperationDurationAttrCacheBoundedUnderConcurrentMisses: many goroutines
// missing at once never grow the cache past its bound.
func TestOperationDurationAttrCacheBoundedUnderConcurrentMisses(t *testing.T) {
	ctx := context.Background()
	r, _ := newDurationRecorder(t)
	const workers, each = 16, maxOpAttrSets / 2
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				r.RecordOperationDuration(ctx, time.Millisecond, redis.NewCmd(ctx, fmt.Sprintf("cmd%d-%d", w, i)), 1, nil, nil, 0)
			}
		}()
	}
	wg.Wait()
	if got := cachedSets(r); got > maxOpAttrSets {
		t.Fatalf("cache holds %d sets, want at most %d", got, maxOpAttrSets)
	}
	r.opAttrsMu.Lock()
	defer r.opAttrsMu.Unlock()
	if got := cachedSets(r); got != r.opAttrsLen {
		t.Fatalf("counter %d, cache holds %d sets", r.opAttrsLen, got)
	}
}
