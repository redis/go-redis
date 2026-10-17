package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestRefreshStaleSpareDropsUnpooledConn checks an unpooled conn parked in
// idleConns (an undelivered dial made while the pool was full) is dropped
// from idleConns when it is refreshed as the stale spare.
func TestRefreshStaleSpareDropsUnpooledConn(t *testing.T) {
	ctx := context.Background()
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

	cn, err := p.newConn(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	p.putIdleConn(ctx, cn)
	p.minIdleFloor.Store(1)

	now := time.Now()
	cn.SetUsedAt(now.Add(-time.Hour))
	p.refreshStaleSpare(ctx, now.UnixNano())

	p.connsMu.Lock()
	for _, ic := range p.idleConns {
		if ic == cn {
			p.connsMu.Unlock()
			t.Fatal("refreshed unpooled conn is still in idleConns")
		}
	}
	p.connsMu.Unlock()

	// The floor is refilled with a fresh pooled spare.
	deadline := time.Now().Add(5 * time.Second)
	for p.Len() != 1 || p.IdleLen() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("Len=%d IdleLen=%d, want 1/1", p.Len(), p.IdleLen())
		}
		time.Sleep(time.Millisecond)
	}
}
