package pool

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestDialConn_HangingDial_RetriesWithPerAttemptTimeout(t *testing.T) {
	var calls atomic.Int32
	var sawDeadline atomic.Int32

	const (
		dialTimeout = 50 * time.Millisecond
		backoff     = 10 * time.Millisecond
		retries     = 3
	)

	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			calls.Add(1)

			// Ensure each attempt has a deadline (pool applies DialTimeout per attempt).
			if dl, ok := ctx.Deadline(); ok {
				rem := time.Until(dl)
				// Very generous bounds to avoid flakes.
				if rem > 5*time.Millisecond && rem <= 2*dialTimeout {
					sawDeadline.Add(1)
				}
			}

			// Simulate a TCP connect hang: block until the context cancels.
			<-ctx.Done()
			return nil, ctx.Err()
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        dialTimeout,
		DialerRetries:      retries,
		DialerRetryTimeout: backoff,
	})
	defer p.Close()

	start := time.Now()
	_, err := p.dialConn(context.Background(), true)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected error")
	}
	if got := calls.Load(); got != retries {
		t.Fatalf("expected %d dial attempts, got %d", retries, got)
	}
	if got := sawDeadline.Load(); got != retries {
		t.Fatalf("expected deadline on all attempts, got %d/%d", got, retries)
	}

	// Each attempt should wait ~dialTimeout, plus backoff between attempts.
	// Allow wide bounds for CI noise.
	min := dialTimeout*time.Duration(retries) + backoff*time.Duration(retries-1)
	if elapsed < min/2 {
		t.Fatalf("dialConn returned too quickly (%v < %v), retries/backoff may not have occurred", elapsed, min/2)
	}
	if elapsed > 5*min {
		t.Fatalf("dialConn took too long (%v > %v), likely hung beyond expected timeouts", elapsed, 5*min)
	}
}

func TestDialConn_DoesNotExtendEarlierParentDeadline(t *testing.T) {
	var calls atomic.Int32

	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			calls.Add(1)
			dl, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("expected deadline")
			}
			// Parent deadline should win (be soon).
			if time.Until(dl) > 100*time.Millisecond {
				return nil, errors.New("deadline was unexpectedly extended")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        500 * time.Millisecond,
		DialerRetries:      1,
	})
	defer p.Close()

	parent, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := p.dialConn(parent, true)
	if err == nil {
		t.Fatalf("expected error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 dial attempt, got %d", got)
	}
}

func TestDialConn_ContextCancelStopsFurtherRetries(t *testing.T) {
	var calls atomic.Int32

	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			n := calls.Add(1)
			if n == 1 {
				// First attempt fails immediately; test cancels context to stop retries.
				return nil, errors.New("dial failed")
			}
			return nil, errors.New("unexpected extra attempt")
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        5 * time.Second,
		DialerRetries:      5,
		DialerRetryTimeout: 5 * time.Second,
	})
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately after the first attempt fails by wrapping dialConn call.
	// We do it via a goroutine so dialConn has a chance to enter the backoff select.
	go func() {
		// Give dialConn a moment to start. This avoids a race where ctx is already canceled
		// before the first attempt and we wouldn't be testing the retry stop path.
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_, _ = p.dialConn(ctx, true)

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected dialer to be called once after cancel, got %d", got)
	}
}

func TestDialConn_DialTimeoutDisabled_DoesNotSetDeadline(t *testing.T) {
	var calls atomic.Int32

	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			calls.Add(1)
			if _, ok := ctx.Deadline(); ok {
				return nil, errors.New("unexpected deadline when DialTimeout disabled")
			}
			return nil, errors.New("dial failed")
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        0,
		DialerRetries:      1,
	})
	defer p.Close()

	_, err := p.dialConn(context.Background(), true)
	if err == nil {
		t.Fatalf("expected error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 dial attempt, got %d", got)
	}
}

func TestDialConn_NoBackoffAfterLastAttempt(t *testing.T) {
	var calls atomic.Int32
	backoff := 300 * time.Millisecond

	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			calls.Add(1)
			return nil, errors.New("dial failed")
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        5 * time.Second,
		DialerRetries:      1,
		DialerRetryTimeout: backoff,
	})
	defer p.Close()

	start := time.Now()
	_, err := p.dialConn(context.Background(), true)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 dial attempt, got %d", got)
	}
	// If we slept after the last attempt, this will be ~backoff.
	if elapsed >= backoff/2 {
		t.Fatalf("dialConn took too long (%v); likely slept after last attempt (backoff=%v)", elapsed, backoff)
	}
}
