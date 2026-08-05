package pool

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// fakeNetConn is a no-op net.Conn for pool tests.
type fakeNetConn struct{}

func (fakeNetConn) Read([]byte) (int, error)         { return 0, nil }
func (fakeNetConn) Write(b []byte) (int, error)      { return len(b), nil }
func (fakeNetConn) Close() error                     { return nil }
func (fakeNetConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (fakeNetConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (fakeNetConn) SetDeadline(time.Time) error      { return nil }
func (fakeNetConn) SetReadDeadline(time.Time) error  { return nil }
func (fakeNetConn) SetWriteDeadline(time.Time) error { return nil }

func TestPubSubPool_NewConnAndTracking(t *testing.T) {
	ctx := context.Background()
	dialer := func(context.Context, string, string) (net.Conn, error) {
		return fakeNetConn{}, nil
	}
	p := NewPubSubPool(&Options{Name: "ps"}, dialer)

	cn, err := p.NewConn(ctx, "tcp", "127.0.0.1:6379", nil)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	if !cn.IsPubSub() {
		t.Error("connection should be marked as pubsub")
	}
	if cn.PoolName() != "ps" {
		t.Errorf("PoolName = %q, want ps", cn.PoolName())
	}
	if s := p.Stats(); s.Created != 1 {
		t.Errorf("Created = %d, want 1", s.Created)
	}

	p.TrackConn(cn)
	if s := p.Stats(); s.Active != 1 {
		t.Errorf("Active = %d, want 1", s.Active)
	}

	p.UntrackConn(cn)
	if s := p.Stats(); s.Active != 0 || s.Untracked != 1 {
		t.Errorf("after untrack: Active=%d Untracked=%d", s.Active, s.Untracked)
	}

	// Untracking again is a no-op (already removed).
	p.UntrackConn(cn)
	if s := p.Stats(); s.Untracked != 1 {
		t.Errorf("double untrack changed Untracked to %d", s.Untracked)
	}
}

func TestPubSubPool_DialErrorAndClosed(t *testing.T) {
	ctx := context.Background()
	dialErr := errors.New("dial failed")
	failing := NewPubSubPool(&Options{}, func(context.Context, string, string) (net.Conn, error) {
		return nil, dialErr
	})
	if _, err := failing.NewConn(ctx, "tcp", "addr", nil); err != dialErr {
		t.Errorf("NewConn dial error = %v, want %v", err, dialErr)
	}

	p := NewPubSubPool(&Options{}, func(context.Context, string, string) (net.Conn, error) {
		return fakeNetConn{}, nil
	})
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := p.NewConn(ctx, "tcp", "addr", nil); err != ErrClosed {
		t.Errorf("NewConn after Close = %v, want ErrClosed", err)
	}
}

func TestPubSubPool_CloseUntracksActive(t *testing.T) {
	ctx := context.Background()
	p := NewPubSubPool(&Options{}, func(context.Context, string, string) (net.Conn, error) {
		return fakeNetConn{}, nil
	})
	cn, err := p.NewConn(ctx, "tcp", "addr", nil)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	p.TrackConn(cn)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if s := p.Stats(); s.Active != 0 || s.Untracked != 1 {
		t.Errorf("after Close: Active=%d Untracked=%d", s.Active, s.Untracked)
	}
}
