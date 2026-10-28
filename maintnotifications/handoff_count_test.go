package maintnotifications

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestFailedHandoffUncountsParkedConnAsIdle(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Name: "handoff-count",
		Dialer: func(context.Context) (net.Conn, error) {
			c, _ := net.Pipe()
			return c, nil
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })

	failingDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, errors.New("dial failed")
	}
	processor := NewPoolHook(failingDialer, "tcp", &Config{
		Mode:              ModeAuto,
		EndpointType:      EndpointTypeAuto,
		MaxWorkers:        1,
		HandoffQueueSize:  10,
		MaxHandoffRetries: 1,
		HandoffTimeout:    100 * time.Millisecond,
	}, nil)
	processor.SetPool(connPool)
	connPool.AddPoolHook(processor)
	t.Cleanup(func() { _ = processor.Shutdown(context.Background()) })

	var mu sync.Mutex
	counts := map[string]int{}
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		ConnectionCount: func(_ context.Context, delta int, cn *pool.Conn, state string, _ bool) {
			if cn.PoolName() != "handoff-count" {
				return
			}
			mu.Lock()
			counts[state] += delta
			mu.Unlock()
		},
	})
	t.Cleanup(func() { pool.SetAllMetricCallbacks(nil) })

	ctx := context.Background()
	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		t.Fatal(err)
	}
	// The handoff is queued and the connection is parked in idleConns until it fails.
	connPool.Put(ctx, cn)

	want := map[string]int{pool.MetricStateIdle: 0, pool.MetricStateUsed: 0}
	snapshot := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		return map[string]int{pool.MetricStateIdle: counts[pool.MetricStateIdle], pool.MetricStateUsed: counts[pool.MetricStateUsed]}
	}
	// The -1 is emitted after the conn leaves the pool, so wait for the counts too.
	deadline := time.Now().Add(3 * time.Second)
	for (connPool.Len() != 0 || !reflect.DeepEqual(snapshot(), want)) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := connPool.Len(); got != 0 {
		t.Fatalf("pool length after failed handoff: got %d, want 0", got)
	}
	if got := snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("connection count per state after failed handoff: got %v, want %v", got, want)
	}
}
