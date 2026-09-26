package pool

import (
	"context"
	"errors"
	"net"
	"testing"
)

// stubPooler records puts and removals; Get hands out a fixed conn.
type stubPooler struct {
	cn      *Conn
	put     bool
	removed bool
}

func (s *stubPooler) NewConn(context.Context) (*Conn, error) { return s.cn, nil }
func (s *stubPooler) CloseConn(context.Context, *Conn, string, string) error {
	return nil
}
func (s *stubPooler) Get(context.Context) (*Conn, error) { return s.cn, nil }
func (s *stubPooler) Put(context.Context, *Conn)         { s.put = true }
func (s *stubPooler) Remove(_ context.Context, _ *Conn, _ error) {
	s.removed = true
}
func (s *stubPooler) RemoveWithoutTurn(ctx context.Context, cn *Conn, reason error) {
	s.Remove(ctx, cn, reason)
}
func (s *stubPooler) Len() int                { return 1 }
func (s *stubPooler) IdleLen() int            { return 0 }
func (s *stubPooler) Size() int               { return 1 }
func (s *stubPooler) Stats() *Stats           { return &Stats{} }
func (s *stubPooler) Close() error            { return nil }
func (s *stubPooler) AddPoolHook(PoolHook)    {}
func (s *stubPooler) RemovePoolHook(PoolHook) {}

// A connection marked for removal on release must not be served to the next
// sticky Get: it may hold unread replies, and reusing it desyncs the reply
// stream for the Conn/Tx that owns the sticky pool.
func TestStickyConnPoolPutHonorsCloseOnPut(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	stub := &stubPooler{cn: NewConn(client)}
	p := NewStickyConnPool(stub)

	ctx := context.Background()
	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}

	cn.MarkCloseOnPut("unread replies")
	p.Put(ctx, cn)

	if _, err := p.Get(ctx); err == nil {
		t.Fatal("second get must refuse a connection marked close-on-put")
	} else {
		var badConn BadConnError
		if !errors.As(err, &badConn) {
			t.Fatalf("second get error = %v, want BadConnError", err)
		}
	}

	// Unwinding the sticky pool removes the connection from the parent.
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !stub.removed {
		t.Error("underlying connection must be removed from the parent pool")
	}
}

// A sticky pool configured with DiscardOnClose must remove its connection from
// the parent on Close instead of returning it: a dedicated Conn carries session
// state (AUTH, SELECT, ...) that the next pooled caller must not inherit.
func TestStickyConnPoolDiscardOnClose(t *testing.T) {
	ctx := context.Background()
	for _, discard := range []bool{false, true} {
		client, server := net.Pipe()

		stub := &stubPooler{cn: NewConn(client)}
		p := NewStickyConnPool(stub)
		if discard {
			p.DiscardOnClose()
		}

		cn, err := p.Get(ctx)
		if err != nil {
			t.Fatalf("discard=%v: get: %v", discard, err)
		}
		p.Put(ctx, cn)
		if err := p.Close(); err != nil {
			t.Fatalf("discard=%v: close: %v", discard, err)
		}

		if stub.removed != discard || stub.put == discard {
			t.Errorf("discard=%v: removed=%v put=%v", discard, stub.removed, stub.put)
		}

		client.Close()
		server.Close()
	}
}
