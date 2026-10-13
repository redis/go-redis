package pool_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func waitIdleLen(t *testing.T, p *pool.ConnPool, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for p.IdleLen() != want {
		if time.Now().After(deadline) {
			t.Fatalf("IdleLen = %d, want %d", p.IdleLen(), want)
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
	waitIdleLen(t, p, 1)

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The leased spare is replaced in the background.
	waitIdleLen(t, p, 1)
	if got := dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want 2 (spare + refill)", got)
	}
	p.Put(context.Background(), cn)
}

// TestRaiseMinIdleConnsNeverLowers checks a raise below the configured
// MinIdleConns (or an earlier raise) changes nothing.
func TestRaiseMinIdleConnsNeverLowers(t *testing.T) {
	var dials atomic.Int32
	p := newRaiseTestPool(t, 3, &dials)
	waitIdleLen(t, p, 3)

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
