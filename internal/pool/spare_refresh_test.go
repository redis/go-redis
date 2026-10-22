package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

func newUnpooledIdleConn(t *testing.T) (*ConnPool, *Conn) {
	t.Helper()
	p := NewConnPool(&Options{
		Dialer: func(context.Context) (net.Conn, error) {
			c, _ := net.Pipe()
			return c, nil
		},
		PoolSize:        2,
		PoolTimeout:     time.Second,
		DialTimeout:     time.Second,
		ConnMaxIdleTime: time.Minute,
	})
	t.Cleanup(func() { _ = p.Close() })

	// An undelivered dial made while the pool was full is parked in
	// idleConns by putIdleConn without being pooled.
	cn, err := p.newConn(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	p.putIdleConn(context.Background(), cn)
	return p, cn
}

func assertNotIdle(t *testing.T, p *ConnPool, cn *Conn) {
	t.Helper()
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	for _, ic := range p.idleConns {
		if ic == cn {
			t.Fatal("removed unpooled conn is still in idleConns")
		}
	}
}

// TestRefreshStaleSpareDropsUnpooledConn checks an unpooled conn parked in
// idleConns is dropped from idleConns when it is refreshed as the stale spare.
func TestRefreshStaleSpareDropsUnpooledConn(t *testing.T) {
	p, cn := newUnpooledIdleConn(t)
	p.minIdleFloor.Store(1)

	now := time.Now()
	cn.SetUsedAt(now.Add(-time.Hour))
	p.refreshStaleSpare(context.Background(), now.UnixNano())
	assertNotIdle(t, p, cn)

	// The floor is refilled with a fresh pooled spare.
	deadline := time.Now().Add(5 * time.Second)
	for p.Len() != 1 || p.IdleLen() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("Len=%d IdleLen=%d, want 1/1", p.Len(), p.IdleLen())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRemoveConnDropsUnpooledIdleConn checks the other removeConn callers
// that act on idle conns drop an unpooled one from idleConns too.
func TestRemoveConnDropsUnpooledIdleConn(t *testing.T) {
	t.Run("RetireConns", func(t *testing.T) {
		p, cn := newUnpooledIdleConn(t)
		p.RetireConns(context.Background(), []*Conn{cn}, CloseReasonStale)
		assertNotIdle(t, p, cn)
		if n := p.IdleLen(); n != 0 {
			t.Fatalf("IdleLen=%d, want 0", n)
		}
	})
	t.Run("Filter", func(t *testing.T) {
		p, cn := newUnpooledIdleConn(t)
		_ = p.Filter(func(c *Conn) bool { return c == cn })
		assertNotIdle(t, p, cn)
		if n := p.IdleLen(); n != 0 {
			t.Fatalf("IdleLen=%d, want 0", n)
		}
	})
}
