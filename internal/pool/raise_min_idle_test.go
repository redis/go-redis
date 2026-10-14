package pool_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// waitPool waits until the pool has total connections, idle of them idle.
// IdleLen counts a min-idle connection as soon as its dial STARTS; Len only
// once the dial finished and the connection is in the pool, so both are needed
// to know the idle connections are really there.
func waitPool(t *testing.T, p *pool.ConnPool, total, idle int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for p.Len() != total || p.IdleLen() != idle {
		if time.Now().After(deadline) {
			t.Fatalf("Len=%d IdleLen=%d, want %d/%d", p.Len(), p.IdleLen(), total, idle)
		}
		time.Sleep(time.Millisecond)
	}
}

func newRaiseTestPool(t *testing.T, minIdle int32, dials *atomic.Int32) *pool.ConnPool {
	t.Helper()
	p := pool.NewConnPool(&pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			return dummyDialer(ctx)
		},
		PoolSize:           4,
		MinIdleConns:       minIdle,
		MaxConcurrentDials: 4,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// TestRaiseMinIdleConnsFillsAndRefills checks RaiseMinIdleConns pre-dials up to
// the new floor and that the pool refills it after the idle connection is taken.
func TestRaiseMinIdleConnsFillsAndRefills(t *testing.T) {
	var dials atomic.Int32
	p := newRaiseTestPool(t, 0, &dials)
	if p.IdleLen() != 0 || dials.Load() != 0 {
		t.Fatalf("fresh pool: IdleLen=%d dials=%d, want 0/0", p.IdleLen(), dials.Load())
	}

	p.RaiseMinIdleConns(1)
	waitPool(t, p, 1, 1)

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The leased spare is replaced in the background.
	waitPool(t, p, 2, 1)
	if got := dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want 2 (spare + refill)", got)
	}
	p.Put(context.Background(), cn)
}

// spareRefreshPool returns a LIFO pool with a short ConnMaxIdleTime, set up as
// the full-duplex engine leaves it: one leased-and-returned connection on top
// (newest) and, below it, the min-idle spare (oldest). It returns the pool and
// the returned connection, which the next Get should hand out.
func spareRefreshPool(t *testing.T, raise bool, dials *atomic.Int32) (*pool.ConnPool, *pool.Conn) {
	t.Helper()
	return spareRefreshPoolWith(t, raise, dials, 400*time.Millisecond, 0)
}

func spareRefreshPoolWith(t *testing.T, raise bool, dials *atomic.Int32, maxIdle, maxLife time.Duration) (*pool.ConnPool, *pool.Conn) {
	t.Helper()
	p := pool.NewConnPool(&pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			return dummyDialer(ctx)
		},
		PoolSize:           4,
		MaxConcurrentDials: 4,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    maxIdle,
		ConnMaxLifetime:    maxLife,
	})
	t.Cleanup(func() { _ = p.Close() })
	ctx := context.Background()
	if raise {
		p.RaiseMinIdleConns(1)
		waitPool(t, p, 1, 1)
	}
	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if raise {
		waitPool(t, p, 2, 1) // the leased spare was replaced
	}
	p.Put(ctx, cn)
	return p, cn
}

// TestRefreshStaleSpareReplacesAgedSpare checks a spare past half of
// ConnMaxIdleTime is replaced on the next Get, which still gets the newest
// idle connection, so the spare never ages out while it waits.
func TestRefreshStaleSpareReplacesAgedSpare(t *testing.T) {
	var dials atomic.Int32
	p, returned := spareRefreshPool(t, true, &dials)
	before := dials.Load()

	time.Sleep(300 * time.Millisecond) // > ConnMaxIdleTime/2, < ConnMaxIdleTime
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cn != returned {
		t.Fatal("Get did not return the newest idle connection")
	}
	// The aged spare was closed and a fresh one dialed in the background.
	waitPool(t, p, 2, 1)
	if got := dials.Load() - before; got != 1 {
		t.Fatalf("dials after refresh = %d, want 1", got)
	}
	p.Put(context.Background(), cn)
}

// TestRefreshStaleSpareBeforeLifetimeEnds checks the spare is also replaced
// once less than half of its ConnMaxLifetime is left, with ConnMaxIdleTime off
// (Codex on #4101).
func TestRefreshStaleSpareBeforeLifetimeEnds(t *testing.T) {
	var dials atomic.Int32
	p, returned := spareRefreshPoolWith(t, true, &dials, -1, 800*time.Millisecond)
	before := dials.Load()

	time.Sleep(500 * time.Millisecond) // < half of the lifetime left, not expired
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cn != returned {
		t.Fatal("Get did not return the newest idle connection")
	}
	waitPool(t, p, 2, 1)
	if got := dials.Load() - before; got != 1 {
		t.Fatalf("dials after lifetime refresh = %d, want 1", got)
	}
	p.Put(context.Background(), cn)
}

// TestRefreshStaleSpareKeepsYoungSpare checks a spare younger than half of
// ConnMaxIdleTime is kept.
func TestRefreshStaleSpareKeepsYoungSpare(t *testing.T) {
	var dials atomic.Int32
	p, _ := spareRefreshPool(t, true, &dials)
	before := dials.Load()

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := dials.Load() - before; got != 0 {
		t.Fatalf("dials = %d for a young spare, want 0", got)
	}
	p.Put(context.Background(), cn)
}

// TestRefreshStaleSpareOnlyRaisedPools checks a pool whose floor was never
// raised keeps its old idle connections (no proactive close).
func TestRefreshStaleSpareOnlyRaisedPools(t *testing.T) {
	var dials atomic.Int32
	p, _ := spareRefreshPool(t, false, &dials)
	ctx := context.Background()
	// Two idle connections, the older one aged past half of ConnMaxIdleTime.
	extra, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Put(ctx, extra)
	cn, err := p.Get(ctx) // newest
	if err != nil {
		t.Fatal(err)
	}
	other, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Put(ctx, other)
	p.Put(ctx, cn)
	before := dials.Load()
	lenBefore := p.Len()

	time.Sleep(300 * time.Millisecond)
	got, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if d := dials.Load() - before; d != 0 || p.Len() != lenBefore {
		t.Fatalf("unraised pool: dials=%d Len=%d (was %d), want no refresh", d, p.Len(), lenBefore)
	}
	p.Put(ctx, got)
}

// TestRaiseMinIdleConnsNeverLowers checks a raise below the configured
// MinIdleConns (or an earlier raise) changes nothing.
func TestRaiseMinIdleConnsNeverLowers(t *testing.T) {
	var dials atomic.Int32
	p := newRaiseTestPool(t, 3, &dials)
	waitPool(t, p, 3, 3)

	p.RaiseMinIdleConns(1)
	p.RaiseMinIdleConns(0)
	p.RaiseMinIdleConns(-1)
	time.Sleep(20 * time.Millisecond)
	if got := p.IdleLen(); got != 3 {
		t.Fatalf("IdleLen = %d after lower raises, want 3", got)
	}
	if got := dials.Load(); got != 3 {
		t.Fatalf("dials = %d, want 3", got)
	}
}

// TestRaiseMinIdleConnsClosedPool checks a raise on a closed pool dials
// nothing.
func TestRaiseMinIdleConnsClosedPool(t *testing.T) {
	var dials atomic.Int32
	p := newRaiseTestPool(t, 0, &dials)
	_ = p.Close()

	p.RaiseMinIdleConns(1)
	time.Sleep(20 * time.Millisecond)
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials = %d on a closed pool, want 0", got)
	}
}
