package pool

import (
	"context"
	"errors"
	"testing"
)

// mockPooler is a minimal Pooler used to drive SingleConnPool/StickyConnPool.
type mockPooler struct {
	newConnErr error
	getErr     error
	getConn    *Conn
	putCount   int
	removeErr  []error
	closeErr   error
}

func (m *mockPooler) NewConn(context.Context) (*Conn, error) {
	if m.newConnErr != nil {
		return nil, m.newConnErr
	}
	return NewConn(nil), nil
}
func (m *mockPooler) CloseConn(context.Context, *Conn, string, string) error { return m.closeErr }
func (m *mockPooler) Get(context.Context) (*Conn, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if m.getConn != nil {
		return m.getConn, nil
	}
	return NewConn(nil), nil
}
func (m *mockPooler) Put(context.Context, *Conn)            { m.putCount++ }
func (m *mockPooler) Remove(_ context.Context, _ *Conn, e error) { m.removeErr = append(m.removeErr, e) }
func (m *mockPooler) RemoveWithoutTurn(c context.Context, cn *Conn, e error) {
	m.Remove(c, cn, e)
}
func (m *mockPooler) Len() int             { return 0 }
func (m *mockPooler) IdleLen() int         { return 0 }
func (m *mockPooler) Stats() *Stats        { return &Stats{} }
func (m *mockPooler) Size() int            { return 1 }
func (m *mockPooler) AddPoolHook(PoolHook) {}
func (m *mockPooler) RemovePoolHook(PoolHook) {}
func (m *mockPooler) Close() error         { return m.closeErr }

func TestSingleConnPool_Lifecycle(t *testing.T) {
	ctx := context.Background()
	cn := NewConn(nil)
	mp := &mockPooler{}
	p := NewSingleConnPool(mp, cn)

	if _, err := p.NewConn(ctx); err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	if err := p.CloseConn(ctx, cn, "reason", "IDLE"); err != nil {
		t.Fatalf("CloseConn: %v", err)
	}

	got, err := p.Get(ctx)
	if err != nil || got != cn {
		t.Fatalf("Get = %v, %v", got, err)
	}

	// Put with a different conn is a no-op; Put with the tracked conn releases it.
	p.Put(ctx, NewConn(nil))
	p.Put(ctx, cn)

	if p.Len() != 0 || p.IdleLen() != 0 || p.Size() != 1 {
		t.Errorf("unexpected sizes: len=%d idle=%d size=%d", p.Len(), p.IdleLen(), p.Size())
	}
	if p.Stats() == nil {
		t.Error("Stats should not be nil")
	}
	p.AddPoolHook(nil)
	p.RemovePoolHook(nil)

	// Remove sets stickyErr; subsequent Get returns it.
	sentinel := errors.New("boom")
	p.Remove(ctx, cn, sentinel)
	if _, err := p.Get(ctx); err != sentinel {
		t.Errorf("Get after Remove = %v, want %v", err, sentinel)
	}
}

func TestSingleConnPool_GetErrorsAndClose(t *testing.T) {
	ctx := context.Background()

	// Nil conn -> ErrClosed.
	p := NewSingleConnPool(&mockPooler{}, nil)
	if _, err := p.Get(ctx); err != ErrClosed {
		t.Errorf("Get(nil conn) = %v, want ErrClosed", err)
	}
	// Put/RemoveWithoutTurn on nil conn are safe no-ops.
	p.Put(ctx, NewConn(nil))

	p2 := NewSingleConnPool(&mockPooler{}, NewConn(nil))
	if err := p2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := p2.Get(ctx); err != ErrClosed {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}

	cn := NewConn(nil)
	p3 := NewSingleConnPool(&mockPooler{}, cn)
	p3.RemoveWithoutTurn(ctx, cn, ErrClosed)
	if _, err := p3.Get(ctx); err != ErrClosed {
		t.Errorf("Get after RemoveWithoutTurn = %v, want ErrClosed", err)
	}
}

func TestBadConnError(t *testing.T) {
	bare := BadConnError{}
	if bare.Error() == "" {
		t.Error("Error() should be non-empty")
	}
	if bare.Unwrap() != nil {
		t.Error("Unwrap() should be nil when no wrapped error")
	}
	wrapped := BadConnError{wrapped: errors.New("inner")}
	if wrapped.Unwrap() == nil {
		t.Error("Unwrap() should return wrapped error")
	}
	if wrapped.Error() == bare.Error() {
		t.Error("wrapped error should include inner message")
	}
}
