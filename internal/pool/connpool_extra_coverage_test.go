package pool

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func newTestConnPool(t *testing.T) *ConnPool {
	t.Helper()
	return NewConnPool(&Options{
		Dialer: func(context.Context) (net.Conn, error) {
			return psFakeConn{}, nil
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        time.Second,
		PoolTimeout:        time.Second,
	})
}

func TestConnPool_LastDialError(t *testing.T) {
	p := newTestConnPool(t)
	defer p.Close()

	if p.getLastDialError() != nil {
		t.Errorf("fresh pool getLastDialError = %v, want nil", p.getLastDialError())
	}
	sentinel := errors.New("dial boom")
	p.setLastDialError(sentinel)
	if p.getLastDialError() != sentinel {
		t.Errorf("getLastDialError = %v, want %v", p.getLastDialError(), sentinel)
	}
}

func TestConnPool_PutConnWithoutTurnAndRemoveWithoutTurn(t *testing.T) {
	ctx := context.Background()
	p := newTestConnPool(t)
	defer p.Close()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// putConnWithoutTurn returns the connection to the pool without freeing a turn.
	p.putConnWithoutTurn(ctx, cn)
	if p.IdleLen() == 0 {
		t.Error("connection should be idle after putConnWithoutTurn")
	}
	// The turn is intentionally still held; free it so we can Get again.
	p.freeTurn()

	// Re-acquire and remove it without freeing a turn.
	cn2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	p.RemoveWithoutTurn(ctx, cn2, errors.New("removed"))
	// Free the turn that RemoveWithoutTurn intentionally does not free.
	p.freeTurn()
}

func TestConnPool_Filter(t *testing.T) {
	ctx := context.Background()
	p := newTestConnPool(t)
	defer p.Close()

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Put(ctx, cn) // becomes idle

	// Filter matching every connection removes the idle connection.
	if err := p.Filter(func(*Conn) bool { return true }); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if p.IdleLen() != 0 {
		t.Errorf("IdleLen after Filter = %d, want 0", p.IdleLen())
	}
}
