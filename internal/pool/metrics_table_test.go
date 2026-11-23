package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func closedTable() *pool.MetricCallbacks {
	return &pool.MetricCallbacks{
		ConnectionClosed: func(context.Context, *pool.Conn, string, error) {},
	}
}

func waitLen(t *testing.T, p *pool.ConnPool, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for p.Len() != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.Len(); got != n {
		t.Fatalf("pool len: got %d, want %d", got, n)
	}
}

func newPool(name string, metrics *pool.MetricCallbacks) *pool.ConnPool {
	return pool.NewConnPool(&pool.Options{
		Name:               name,
		Dialer:             dummyDialer,
		PoolSize:           2,
		MinIdleConns:       2,
		MaxConcurrentDials: 2,
		PoolTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
		Metrics:            metrics,
	})
}

// GlobalPools lists the open pools on the global table, not pools with their
// own table and not closed pools.
func TestGlobalPoolsListsOnlyGlobalTablePools(t *testing.T) {
	ownTable := closedTable()
	ownPool := newPool("own", ownTable)
	t.Cleanup(func() { _ = ownPool.Close() })
	globalPool := newPool("global", nil)
	t.Cleanup(func() { _ = globalPool.Close() })
	waitLen(t, ownPool, 2)
	waitLen(t, globalPool, 2)

	listed := func(p *pool.ConnPool) bool {
		conns, _ := pool.GlobalPools()
		for _, c := range conns {
			if c == p {
				return true
			}
		}
		return false
	}
	if listed(ownPool) {
		t.Fatal("GlobalPools lists a pool with its own table")
	}
	if !listed(globalPool) {
		t.Fatal("GlobalPools does not list a global-table pool")
	}

	// A conn dialed by the own pool reports the own table.
	ctx := context.Background()
	cn, err := ownPool.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if pool.MetricsOf(cn) != ownTable {
		t.Fatalf("MetricsOf(own conn) is not the own table")
	}
	ownPool.Put(ctx, cn)

	_ = globalPool.Close()
	if listed(globalPool) {
		t.Fatal("GlobalPools lists a closed pool")
	}
}

func TestMetricsOfFallsBackToGlobal(t *testing.T) {
	pool.SetAllMetricCallbacks(closedTable())
	defer pool.SetAllMetricCallbacks(nil)

	p := newPool("fallback", nil)
	t.Cleanup(func() { _ = p.Close() })
	waitLen(t, p, 2)
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer p.Put(context.Background(), cn)
	if pool.MetricsOf(cn).ConnectionClosed == nil {
		t.Fatal("MetricsOf(global conn) did not resolve to the installed global table")
	}
	pool.SetAllMetricCallbacks(nil)
	if pool.MetricsOf(cn).ConnectionClosed != nil {
		t.Fatal("MetricsOf(global conn) still holds callbacks after uninstall")
	}
}
