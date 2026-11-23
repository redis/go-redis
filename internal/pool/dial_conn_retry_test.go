package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
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
		// PoolSize is set high so the pool's tryDial probe (triggered when
		// dialErrorsNum == PoolSize) does not fire during this test and race
		// the assertion on `calls`.
		PoolSize:           100,
		MaxConcurrentDials: 1,
		DialTimeout:        dialTimeout,
		DialerRetries:      retries,
		DialerRetryTimeout: backoff,
	})
	defer p.Close()

	// Use a parent context with a bounded timeout so this test fails fast (instead of hanging)
	// when dialConn does not apply per-attempt DialTimeout via context.
	parentBudget := dialTimeout*time.Duration(retries) + backoff*time.Duration(retries-1) + 250*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), parentBudget)
	defer cancel()

	start := time.Now()
	_, err := p.dialConn(ctx, true)
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

func TestQueuedDialExhaustionDoesNotTaintDeliveredConnection(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var calls atomic.Int32
	dialErr := errors.New("second dial failed")
	p := NewConnPool(&Options{
		PoolSize: 2, MaxConcurrentDials: 2, DialerRetries: 1,
		Dialer: func(context.Context) (net.Conn, error) {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseFirst
				client, server := net.Pipe()
				t.Cleanup(func() { _ = server.Close() })
				return client, nil
			case 2:
				close(secondStarted)
				<-releaseSecond
				return nil, dialErr
			default:
				return nil, errors.New("unexpected background dial")
			}
		},
	})
	defer p.Close()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone := make(chan struct{})
	go func() {
		_, _ = p.Get(firstCtx)
		close(firstDone)
	}()
	<-firstStarted

	var exhausted atomic.Bool
	secondCtx := p.WithDialExhaustion(context.Background(), &exhausted)
	type outcome struct {
		cn  *Conn
		err error
	}
	secondDone := make(chan outcome, 1)
	go func() {
		cn, err := p.Get(secondCtx)
		secondDone <- outcome{cn, err}
	}()
	<-secondStarted

	cancelFirst()
	<-firstDone
	close(releaseFirst)
	var got outcome
	select {
	case got = <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("idle connection was not transferred to the second waiter")
	}
	if got.err != nil || got.cn == nil {
		t.Fatalf("second waiter received (%v, %v), want a connection", got.cn, got.err)
	}
	close(releaseSecond)
	deadline := time.Now().Add(2 * time.Second)
	for len(p.dialsInProgress) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(p.dialsInProgress) != 0 {
		t.Fatal("losing dial did not finish")
	}
	if exhausted.Load() {
		t.Fatal("failed losing dial marked the successful acquisition exhausted")
	}
	p.Put(secondCtx, got.cn)
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
		PoolSize:           100,
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
		PoolSize:           100,
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
		PoolSize:           100,
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
		PoolSize:           100,
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

func TestDialGenerationAdmission(t *testing.T) {
	for _, dropContext := range []bool{false, true} {
		t.Run(fmt.Sprint("drop-context=", dropContext), func(t *testing.T) {
			var epoch atomic.Uint64
			epoch.Store(1)
			var calls atomic.Int32
			p := NewConnPool(&Options{
				PoolSize: 1, MaxActiveConns: 1, DialerRetries: 3, DialGeneration: &epoch,
				Dialer: func(ctx context.Context) (net.Conn, error) {
					generation := epoch.Load()
					if dropContext {
						ctx = context.Background()
					}
					SetDialGeneration(ctx, generation)
					a, b := net.Pipe()
					t.Cleanup(func() { b.Close() })
					if calls.Add(1) == 1 {
						epoch.Add(1)
					}
					return a, nil
				},
			})
			defer p.Close()
			cn, err := p.NewConn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer p.RemoveWithoutTurn(context.Background(), cn, nil)
			if cn.DialGeneration() != 2 || calls.Load() != 2 {
				t.Fatalf("generation=%d calls=%d", cn.DialGeneration(), calls.Load())
			}
			if p.dialErrorsNum.Load() != 0 || p.pendingConns != 0 {
				t.Fatal("obsolete dial polluted accounting")
			}
		})
	}
}

func TestDialGenerationMinIdleAndBudget(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(1)
	p := NewConnPool(&Options{
		PoolSize: 1, DialerRetries: 2, DialGeneration: &epoch,
		Dialer: func(ctx context.Context) (net.Conn, error) {
			SetDialGeneration(ctx, epoch.Load())
			epoch.Add(1)
			a, b := net.Pipe()
			t.Cleanup(func() { b.Close() })
			return a, nil
		},
	})
	defer p.Close()
	if err := p.addIdleConn(); !errors.Is(err, ErrDialGenerationChanged) {
		t.Fatalf("error=%v", err)
	}
	if p.pendingConns != 0 || p.Stats().TotalConns != 0 || p.dialErrorsNum.Load() != 0 {
		t.Fatal("obsolete prewarm leaked capacity or dial-error state")
	}
}

func TestFilterCloseOutsidePoolLock(t *testing.T) {
	p := NewConnPool(&Options{PoolSize: 1, Dialer: func(context.Context) (net.Conn, error) {
		a, b := net.Pipe()
		t.Cleanup(func() { b.Close() })
		return a, nil
	}})
	defer p.Close()
	cn, err := p.NewConn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cn.SetOnCscClose(func() error { p.Stats(); return nil })
	done := make(chan error, 1)
	go func() { done <- p.Filter(func(*Conn) bool { return true }) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close callback ran under pool lock")
	}
	p.RemoveWithoutTurn(context.Background(), cn, nil)
}

func TestDialGenerationBorrowDeadline(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(fmt.Sprint("wait=", wait), func(t *testing.T) {
			var epoch atomic.Uint64
			epoch.Store(1)
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			var armed atomic.Bool
			metrics := &MetricCallbacks{ConnectionCount: func(_ context.Context, delta int, _ *Conn, state string, pubsub bool) {
				if armed.Load() && delta == 1 && state == "used" && !pubsub {
					once.Do(func() { close(entered); <-release })
				}
			}}
			p := NewConnPool(&Options{
				PoolSize: 1, MaxActiveConns: 1, MaxConcurrentDials: 1, PoolTimeout: 20 * time.Millisecond, DialGeneration: &epoch,
				Metrics: metrics,
				Dialer: func(ctx context.Context) (net.Conn, error) {
					SetDialGeneration(ctx, epoch.Load())
					a, b := net.Pipe()
					t.Cleanup(func() { b.Close() })
					return a, nil
				},
			})
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				p.Close()
			})
			done := make(chan error, 1)
			armed.Store(true)
			go func() { _, err := p.getConn(context.Background(), wait); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("connection was not admitted")
			}
			// The socket was admitted but not delivered. Advance its source and
			// let the existing acquisition deadline expire before delivery.
			epoch.Add(1)
			<-time.After(40 * time.Millisecond)
			releaseOnce.Do(func() { close(release) })
			want := ErrPoolTryFull
			if wait {
				want = ErrPoolTimeout
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("error=%v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("acquisition did not finish")
			}
			if p.semaphore.Available() != 1 || p.Stats().TotalConns != 0 || p.pendingConns != 0 {
				t.Fatal("generation rejection leaked a turn or connection")
			}
			cn, err := p.Get(context.Background())
			if err != nil || cn.DialGeneration() != 2 {
				t.Fatalf("subsequent borrow failed: %v", err)
			}
			p.Put(context.Background(), cn)
		})
	}
}

func TestDialGenerationIdleCloseReason(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(1)
	closed := make(chan string, 4)
	metrics := &MetricCallbacks{ConnectionClosed: func(_ context.Context, _ *Conn, reason string, _ error) { closed <- reason }}
	p := NewConnPool(&Options{PoolSize: 1, MaxConcurrentDials: 1, PoolTimeout: time.Second, DialGeneration: &epoch, Metrics: metrics, Dialer: func(ctx context.Context) (net.Conn, error) {
		SetDialGeneration(ctx, epoch.Load())
		a, b := net.Pipe()
		t.Cleanup(func() { b.Close() })
		return a, nil
	}})
	t.Cleanup(func() { p.Close() })
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Put(context.Background(), cn)
	epoch.Add(1)
	next, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Put(context.Background(), next)
	select {
	case reason := <-closed:
		if reason != CloseReasonFailover {
			t.Fatalf("closed reason=%q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("obsolete connection was not closed")
	}
}

func TestDialGenerationDoesNotMarkDialExhaustion(t *testing.T) {
	for _, networkFailure := range []bool{false, true} {
		t.Run(fmt.Sprint("network-failure=", networkFailure), func(t *testing.T) {
			var epoch atomic.Uint64
			epoch.Store(1)
			var calls atomic.Int32
			dialErr := errors.New("dial failed")
			p := NewConnPool(&Options{
				PoolSize: 2, MaxActiveConns: 1, DialerRetries: 2, DialerRetryTimeout: time.Millisecond, DialGeneration: &epoch,
				Dialer: func(ctx context.Context) (net.Conn, error) {
					calls.Add(1)
					if networkFailure {
						return nil, dialErr
					}
					SetDialGeneration(ctx, epoch.Load())
					a, b := net.Pipe()
					t.Cleanup(func() { b.Close() })
					epoch.Add(1)
					return a, nil
				},
			})
			defer p.Close()
			var exhausted atomic.Bool
			_, err := p.NewConn(p.WithDialExhaustion(context.Background(), &exhausted))
			want := ErrDialGenerationChanged
			if networkFailure {
				want = dialErr
			}
			if !errors.Is(err, want) || exhausted.Load() != networkFailure || calls.Load() != 2 {
				t.Fatalf("error=%v exhausted=%t calls=%d", err, exhausted.Load(), calls.Load())
			}
			if !networkFailure && p.dialErrorsNum.Load() != 0 {
				t.Fatal("source retirement opened the network dial breaker")
			}
			if p.Stats().TotalConns != 0 || p.pendingConns != 0 {
				t.Fatal("failed admission retained a connection reservation")
			}
		})
	}
}

func TestOpenDialBreakerDoesNotMarkThisAttemptExhausted(t *testing.T) {
	dialErr := errors.New("previous dial failed")
	var dials atomic.Int32
	p := NewConnPool(&Options{
		PoolSize: 1,
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			return nil, dialErr
		},
	})
	defer p.Close()
	p.setLastDialError(dialErr)
	p.dialErrorsNum.Store(1)

	var exhausted atomic.Bool
	ctx := p.WithDialExhaustion(context.Background(), &exhausted)
	_, err := p.dialConn(ctx, true)
	if err != dialErr {
		t.Fatalf("open breaker returned %v, want original dial error", err)
	}
	if exhausted.Load() {
		t.Fatal("open breaker did not run a dial sequence; retries must remain available")
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("open breaker made %d dials, want 0", got)
	}
}
