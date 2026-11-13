package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// countingTable records connection.count deltas per pool name.
type countingTable struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCountingTable() (*countingTable, *pool.MetricCallbacks) {
	c := &countingTable{counts: map[string]int{}}
	return c, &pool.MetricCallbacks{
		ConnectionCount: func(_ context.Context, delta int, cn *pool.Conn, _ string, _ bool) {
			c.mu.Lock()
			c.counts[cn.PoolName()] += delta
			c.mu.Unlock()
		},
	}
}

func (c *countingTable) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
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

// A pool with its own table keeps its count across global install/uninstall
// and returns it to zero on Close; a pool on the global table moves old -> new.
func TestOwnMetricsTableIsNotRecounted(t *testing.T) {
	own, ownTable := newCountingTable()
	global, globalTable := newCountingTable()
	defer pool.SetAllMetricCallbacks(nil)

	ownPool := newPool("own", ownTable)
	t.Cleanup(func() { _ = ownPool.Close() })
	globalPool := newPool("global", nil)
	t.Cleanup(func() { _ = globalPool.Close() })
	waitLen(t, ownPool, 2)
	waitLen(t, globalPool, 2)

	if got := own.get("own"); got != 2 {
		t.Fatalf("own table before global install: got %d, want 2", got)
	}
	if got := own.get("global"); got != 0 {
		t.Fatalf("own table saw the global pool: got %d", got)
	}

	pool.SetAllMetricCallbacks(globalTable)
	if got := global.get("global"); got != 2 {
		t.Fatalf("global table after install: got %d, want 2", got)
	}
	if got := global.get("own"); got != 0 {
		t.Fatalf("global table saw the own pool: got %d", got)
	}
	if got := own.get("own"); got != 2 {
		t.Fatalf("own table after global install: got %d, want 2", got)
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

	pool.SetAllMetricCallbacks(nil)
	if got := global.get("global"); got != 0 {
		t.Fatalf("global table after uninstall: got %d, want 0", got)
	}
	if got := own.get("own"); got != 2 {
		t.Fatalf("own table after global uninstall: got %d, want 2", got)
	}

	_ = ownPool.Close()
	if got := own.get("own"); got != 0 {
		t.Fatalf("own table after Close: got %d, want 0", got)
	}
}

func TestMetricsOfFallsBackToGlobal(t *testing.T) {
	_, table := newCountingTable()
	pool.SetAllMetricCallbacks(table)
	defer pool.SetAllMetricCallbacks(nil)

	p := newPool("fallback", nil)
	t.Cleanup(func() { _ = p.Close() })
	waitLen(t, p, 2)
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer p.Put(context.Background(), cn)
	if pool.MetricsOf(cn).ConnectionCount == nil {
		t.Fatal("MetricsOf(global conn) did not resolve to the installed global table")
	}
	pool.SetAllMetricCallbacks(nil)
	if pool.MetricsOf(cn).ConnectionCount != nil {
		t.Fatal("MetricsOf(global conn) still holds callbacks after uninstall")
	}
}
