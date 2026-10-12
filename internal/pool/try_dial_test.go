package pool

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestTryDial_AppliesDialTimeoutWhenSet(t *testing.T) {
	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			if _, ok := ctx.Deadline(); !ok {
				return nil, errors.New("expected deadline in tryDial")
			}
			c1, c2 := net.Pipe()
			_ = c2.Close()
			return c1, nil
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        200 * time.Millisecond,
	})
	defer p.Close()

	p.tryDial()
}

func TestTryDial_DoesNotApplyDialTimeoutWhenDisabled(t *testing.T) {
	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			if _, ok := ctx.Deadline(); ok {
				return nil, errors.New("unexpected deadline in tryDial when DialTimeout disabled")
			}
			// Ensure context is still a real context.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			c1, c2 := net.Pipe()
			_ = c2.Close()
			return c1, nil
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        0,
	})
	defer p.Close()

	p.tryDial()
}

func TestTryDial_RespectsPoolClose(t *testing.T) {
	// If Dialer keeps failing, tryDial should exit once the pool is closed.
	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			return nil, errors.New("dial failed")
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        10 * time.Millisecond,
	})

	done := make(chan struct{})
	go func() {
		p.tryDial()
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	_ = p.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("tryDial did not exit after pool close")
	}
}
