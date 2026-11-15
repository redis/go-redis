package pool_test

import (
	"context"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/auth/streaming"
	"github.com/redis/go-redis/v9/internal/pool"
)

// A re-authentication hook can reject a connection before its worker has
// transitioned the connection out of StateIdle.
type rejectIdleOnGetHook struct {
	rejected atomic.Int32
	only     *pool.Conn
}

func (h *rejectIdleOnGetHook) OnGet(_ context.Context, cn *pool.Conn, isNew bool) (bool, error) {
	if isNew || (h.only != nil && cn != h.only) {
		return true, nil
	}
	h.rejected.Add(1)
	return false, nil
}

func (*rejectIdleOnGetHook) OnPut(context.Context, *pool.Conn) (bool, bool, error) {
	return true, false, nil
}

func (*rejectIdleOnGetHook) OnRemove(context.Context, *pool.Conn, error) {}

func TestGetBoundsHookRejectedIdleRetries(t *testing.T) {
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 1, MaxActiveConns: 1, MaxConcurrentDials: 1,
		PoolTimeout: 20 * time.Millisecond, Dialer: dummyDialer,
	})
	t.Cleanup(func() { _ = p.Close() })
	ctx := context.Background()
	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cn.GetStateMachine().Transition(pool.StateInUse)
	p.Put(ctx, cn)

	hook := &rejectIdleOnGetHook{}
	p.AddPoolHook(hook)
	boundedCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = p.Get(boundedCtx)
	if err != pool.ErrPoolTimeout {
		t.Fatalf("hook-rejected idle Get returned %v, want pool timeout", err)
	}
	if got := hook.rejected.Load(); got == 0 || got > 100 {
		t.Fatalf("hook rejected %d times, want bounded retries", got)
	}
	if p.QueueLen() != 0 {
		t.Fatalf("pool turn leaked after timeout: %d", p.QueueLen())
	}
}

func TestGetSkipsHookRejectedIdleForHealthyPeer(t *testing.T) {
	for _, try := range []bool{false, true} {
		name := "Get"
		if try {
			name = "TryGet"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := pool.NewConnPool(&pool.Options{
				PoolSize: 2, MaxActiveConns: 2, MaxConcurrentDials: 2,
				PoolTimeout: 30 * time.Millisecond, Dialer: dummyDialer,
			})
			t.Cleanup(func() { _ = p.Close() })
			healthy, err := p.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			blocked, err := p.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			healthy.GetStateMachine().Transition(pool.StateInUse)
			blocked.GetStateMachine().Transition(pool.StateInUse)
			p.Put(ctx, healthy)
			p.Put(ctx, blocked)

			hook := &rejectIdleOnGetHook{only: blocked}
			p.AddPoolHook(hook)
			var got *pool.Conn
			if try {
				got, err = p.TryGet(ctx)
			} else {
				got, err = p.Get(ctx)
			}
			if err != nil {
				t.Fatalf("%s overlooked healthy idle peer: %v", name, err)
			}
			if got != healthy {
				t.Fatalf("%s returned %p, want healthy conn %p", name, got, healthy)
			}
			p.Put(ctx, got)
			if hook.rejected.Load() != 1 || p.QueueLen() != 0 {
				t.Fatalf("rejections=%d turns=%d, want 1 and 0", hook.rejected.Load(), p.QueueLen())
			}
		})
	}
}

// A rejected idle connection may be temporarily absent from idleConns while its
// OnGet hook is running. Another Get must wait for its disposition rather than
// dial a replacement before the hook has a chance to requeue it.
type delayedIdleRejectHook struct {
	entered  chan struct{}
	release  chan struct{}
	rejected atomic.Bool
}

func (h *delayedIdleRejectHook) OnGet(_ context.Context, _ *pool.Conn, isNew bool) (bool, error) {
	if !isNew && h.rejected.CompareAndSwap(false, true) {
		close(h.entered)
		<-h.release
		return false, nil
	}
	return true, nil
}

func (*delayedIdleRejectHook) OnPut(context.Context, *pool.Conn) (bool, bool, error) {
	return true, false, nil
}

func (*delayedIdleRejectHook) OnRemove(context.Context, *pool.Conn, error) {}

func TestGetWaitsForInFlightIdleHook(t *testing.T) {
	ctx := context.Background()
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 2, MaxActiveConns: 2, MaxConcurrentDials: 2,
		PoolTimeout: 200 * time.Millisecond,
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			return newDummyConn(), nil
		},
	})
	t.Cleanup(func() { _ = p.Close() })
	original, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	original.GetStateMachine().Transition(pool.StateInUse)
	p.Put(ctx, original)

	h := &delayedIdleRejectHook{entered: make(chan struct{}), release: make(chan struct{})}
	p.AddPoolHook(h)
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	type result struct {
		cn *pool.Conn
		err error
	}
	firstDone := make(chan result, 1)
	go func() {
		cn, err := p.Get(firstCtx)
		firstDone <- result{cn, err}
	}()
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("first OnGet did not enter")
	}
	// Ensure the blocked OnGet is always released on test failure.
	defer func() {
		select {
		case <-h.release:
		default:
			close(h.release)
		}
	}()
	secondCtx, cancelSecond := context.WithTimeout(ctx, time.Second)
	defer cancelSecond()
	secondDone := make(chan result, 1)
	go func() {
		cn, err := p.Get(secondCtx)
		secondDone <- result{cn, err}
	}()
	select {
	case r := <-secondDone:
		if r.cn != nil {
			p.Put(ctx, r.cn)
		}
		t.Fatalf("second Get bypassed pending OnGet: %v", r.err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dialed a replacement while OnGet was pending: %d dials", got)
	}
	cancelFirst()
	close(h.release)
	first := <-firstDone
	if first.cn != nil {
		p.Put(ctx, first.cn)
	}
	if first.err != context.Canceled {
		t.Fatalf("first Get returned %v, want cancellation", first.err)
	}
	select {
	case second := <-secondDone:
		if second.err != nil || second.cn != original {
			t.Fatalf("second Get returned conn=%p err=%v; want original=%p", second.cn, second.err, original)
		}
		p.Put(ctx, second.cn)
	case <-time.After(time.Second):
		t.Fatal("second Get did not reuse the requeued idle connection")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("unexpected extra dial after requeue: %d", got)
	}
}

// Closing a stale idle connection can outlast both the request and pool
// deadlines. In that race the caller context error must take precedence.
type slowStaleCloseConn struct{ net.Conn }

func (c *slowStaleCloseConn) Close() error {
	time.Sleep(12 * time.Millisecond)
	return c.Conn.Close()
}

func TestGetStaleScanPrefersExpiredContext(t *testing.T) {
	ctx := context.Background()
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 4, MaxActiveConns: 4, MaxConcurrentDials: 4,
		PoolTimeout: 10 * time.Millisecond, ConnMaxIdleTime: time.Minute,
		Dialer: func(context.Context) (net.Conn, error) {
			return &slowStaleCloseConn{newDummyConn()}, nil
		},
	})
	t.Cleanup(func() { _ = p.Close() })
	conns := make([]*pool.Conn, 4)
	for i := range conns {
		var err error
		conns[i], err = p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns[i].GetStateMachine().Transition(pool.StateInUse)
	}
	for i, cn := range conns {
		p.Put(ctx, cn)
		if i > 0 {
			cn.SetUsedAt(time.Now().Add(-time.Hour))
		}
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	cn, err := p.Get(deadlineCtx)
	if cn != nil {
		p.Put(ctx, cn)
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("stale scan returned %v, want context deadline exceeded", err)
	}
	if p.Stats().Timeouts != 0 {
		t.Fatal("caller deadline incorrectly counted as a pool timeout")
	}
}

func TestTryGetSkipsMoreThanThreeStaleIdleConns(t *testing.T) {
	ctx := context.Background()
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 4, MaxActiveConns: 4, MaxConcurrentDials: 4,
		PoolTimeout: time.Second, ConnMaxIdleTime: time.Minute,
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			return newDummyConn(), nil
		},
	})
	t.Cleanup(func() { _ = p.Close() })

	conns := make([]*pool.Conn, 4)
	for i := range conns {
		var err error
		conns[i], err = p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns[i].GetStateMachine().Transition(pool.StateInUse)
	}
	// The healthy entry is deepest in the LIFO stack. All three entries
	// above it are stale and must be closed before TryGet can reach it.
	for i, cn := range conns {
		p.Put(ctx, cn)
		if i > 0 {
			cn.SetUsedAt(time.Now().Add(-time.Hour))
		}
	}

	got, err := p.TryGet(ctx)
	if err != nil {
		t.Fatalf("TryGet skipped healthy idle connection: %v", err)
	}
	if got != conns[0] {
		t.Errorf("TryGet returned %p, want healthy idle conn %p", got, conns[0])
	}
	p.Put(ctx, got)
	if dials.Load() != 4 || p.QueueLen() != 0 {
		t.Fatalf("dials=%d turns=%d, want 4 and 0", dials.Load(), p.QueueLen())
	}
}

// MinIdleConns may replenish a failed connection while a caller is still
// discarding it. New idle entries must not renew that caller's scan budget.
type failingIdleHealthConn struct{ net.Conn }

func (*failingIdleHealthConn) SyscallConn() (syscall.RawConn, error) {
	return nil, net.ErrClosed
}

func (cn *failingIdleHealthConn) Close() error {
	// Give the background MinIdleConns dialer time to refill each removal.
	time.Sleep(time.Millisecond)
	return cn.Conn.Close()
}

func TestGetBoundsMinIdleHealthFailureRetries(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("socket health checks are disabled on Windows")
	}
	for _, try := range []bool{false, true} {
		name := "Get"
		if try {
			name = "TryGet"
		}
		t.Run(name, func(t *testing.T) {
			const minIdle = 16
			p := pool.NewConnPool(&pool.Options{
				PoolSize: minIdle, MinIdleConns: minIdle, MaxConcurrentDials: minIdle,
				PoolTimeout: 5 * time.Millisecond,
				Dialer: func(context.Context) (net.Conn, error) {
					return &failingIdleHealthConn{newDummyConn()}, nil
				},
			})
			t.Cleanup(func() { _ = p.Close() })

			warmDeadline := time.Now().Add(2 * time.Second)
			for p.Stats().IdleConns < minIdle && time.Now().Before(warmDeadline) {
				time.Sleep(time.Millisecond)
			}
			if p.Stats().IdleConns < minIdle {
				t.Fatal("min-idle connections were not ready")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			var cn *pool.Conn
			var err error
			if try {
				cn, err = p.TryGet(ctx)
			} else {
				cn, err = p.Get(ctx)
			}
			if cn != nil {
				p.Put(ctx, cn)
			}
			if err == context.DeadlineExceeded {
				t.Fatalf("%s kept retrying unhealthy min-idle replacements until caller deadline", name)
			}
			if pending := p.Stats().PendingRequests; pending != 0 {
				t.Fatalf("%s leaked %d pending requests", name, pending)
			}
			// MinIdleConns replenishment borrows its own pool turns in background
			// goroutines. Wait for those workers instead of mistaking their turns
			// for a leak by the completed Get/TryGet call.
			turnDeadline := time.Now().Add(2 * time.Second)
			for p.QueueLen() != 0 && time.Now().Before(turnDeadline) {
				time.Sleep(time.Millisecond)
			}
			if turns := p.QueueLen(); turns != 0 {
				t.Fatalf("%s left %d pool turns after min-idle refill", name, turns)
			}
		})
	}
}

func TestGetWaitsForReAuth(t *testing.T) {
	for _, capped := range []bool{true, false} {
		name := "unlimited"
		if capped {
			name = "capped"
		}
		t.Run(name, func(t *testing.T) {
			for _, running := range []bool{false, true} {
				name := "pending"
				if running {
					name = "running"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					var dials atomic.Int32
					opt := &pool.Options{
						PoolSize: 1, MaxConcurrentDials: 1,
						PoolTimeout: time.Second, DialTimeout: time.Second,
						Dialer: func(context.Context) (net.Conn, error) {
							dials.Add(1)
							return newDummyConn(), nil
						},
					}
					if capped {
						opt.MaxActiveConns = 1
					}
					p := pool.NewConnPool(opt)
					t.Cleanup(func() { _ = p.Close() })
					manager := streaming.NewManager(p, time.Second)
					p.AddPoolHook(manager.PoolHook())
					cn, err := p.Get(ctx)
					if err != nil {
						t.Fatal(err)
					}
					cn.GetStateMachine().Transition(pool.StateInUse)
					p.Put(ctx, cn)

					started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					t.Cleanup(unblock)
					listener, err := manager.Listener(cn, func(*pool.Conn, auth.Credentials) error {
						close(started)
						<-release
						close(finished)
						return nil
					}, func(_ *pool.Conn, err error) { t.Errorf("reauth: %v", err) })
					if err != nil {
						t.Fatal(err)
					}
					listener.OnNext(auth.NewBasicCredentials("app", "rotated"))
					if running {
						_, _, _ = manager.PoolHook().OnPut(ctx, cn)
						select {
						case <-started:
						case <-time.After(time.Second):
							t.Fatal("reauth worker did not start")
						}
					}
					type result struct {
						cn  *pool.Conn
						err error
					}
					done := make(chan result, 1)
					go func() {
						got, err := p.Get(ctx)
						done <- result{got, err}
					}()
					select {
					case got := <-done:
						if got.cn != nil {
							p.Put(ctx, got.cn)
						}
						t.Fatalf("Get returned before reauthentication completed: %v", got.err)
					case <-time.After(20 * time.Millisecond):
					}
					unblock()
					select {
					case got := <-done:
						if got.err != nil {
							t.Fatalf("Get after reauthentication: %v", got.err)
						}
						if got.cn != cn {
							t.Error("Get did not reuse the reauthenticated connection")
						}
						p.Put(ctx, got.cn)
					case <-time.After(2 * time.Second):
						t.Fatal("Get did not resume after reauthentication")
					}
					select {
					case <-finished:
					default:
						t.Error("Get returned without completing reauthentication")
					}
					if dials.Load() != 1 || p.QueueLen() != 0 {
						t.Fatalf("dials=%d turns=%d, want 1 and 0", dials.Load(), p.QueueLen())
					}
				})
			}
		})
	}
}

func TestGetUnavailableIdleStops(t *testing.T) {
	for _, mode := range []string{"timeout", "context", "close", "try"} {
		t.Run(mode, func(t *testing.T) {
			p := pool.NewConnPool(&pool.Options{
				PoolSize: 1, MaxActiveConns: 1, MaxConcurrentDials: 1,
				PoolTimeout: 30 * time.Millisecond, Dialer: dummyDialer,
			})
			t.Cleanup(func() { _ = p.Close() })
			ctx := context.Background()
			cn, err := p.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			cn.GetStateMachine().Transition(pool.StateInUse)
			p.Put(ctx, cn)
			cn.GetStateMachine().Transition(pool.StateUnusable)

			want := pool.ErrPoolTimeout
			switch mode {
			case "context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
				want = context.DeadlineExceeded
			case "close":
				timer := time.AfterFunc(10*time.Millisecond, func() { _ = p.Close() })
				defer timer.Stop()
				want = pool.ErrClosed
			case "try":
				want = pool.ErrPoolTryFull
			}
			var got *pool.Conn
			if mode == "try" {
				got, err = p.TryGet(ctx)
			} else {
				got, err = p.Get(ctx)
			}
			if got != nil || err != want {
				t.Fatalf("Get returned conn=%v err=%v, want nil and %v", got, err, want)
			}
			if p.QueueLen() != 0 || p.Stats().PendingRequests != 0 {
				t.Fatal("waiting Get leaked a pool turn or pending request")
			}
			if mode != "close" {
				cn.GetStateMachine().Transition(pool.StateIdle)
				got, err = p.Get(context.Background())
				if err != nil {
					t.Fatalf("Get after maintenance: %v", err)
				}
				p.Put(context.Background(), got)
			}
		})
	}
}

func TestGetRechecksLifetimeAfterReauthWait(t *testing.T) {
	ctx := context.Background()
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 1, MaxActiveConns: 1, MaxConcurrentDials: 1,
		PoolTimeout: time.Second, ConnMaxLifetime: 35 * time.Millisecond,
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			return newDummyConn(), nil
		},
	})
	defer p.Close()

	original, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	original.GetStateMachine().Transition(pool.StateInUse)
	p.Put(ctx, original)
	original.GetStateMachine().Transition(pool.StateUnusable)

	type result struct {
		cn *pool.Conn
		err error
	}
	done := make(chan result, 1)
	go func() {
		cn, err := p.Get(ctx)
		done <- result{cn, err}
	}()
	// Let the connection expire while it is in maintenance.
	time.Sleep(85 * time.Millisecond)
	original.GetStateMachine().Transition(pool.StateIdle)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Get after maintenance: %v", got.err)
		}
		if got.cn == original || dials.Load() != 2 {
			t.Fatalf("expired connection reused; conn=%p original=%p dials=%d", got.cn, original, dials.Load())
		}
		p.Put(ctx, got.cn)
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not replace expired idle connection")
	}
}

func TestCancelledGetDoesNotTakeReauthenticatedConnection(t *testing.T) {
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 1, MaxActiveConns: 1, MaxConcurrentDials: 1,
		PoolTimeout: time.Second, Dialer: dummyDialer,
	})
	defer p.Close()
	ctx := context.Background()
	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cn.GetStateMachine().Transition(pool.StateInUse)
	p.Put(ctx, cn)
	cn.GetStateMachine().Transition(pool.StateUnusable)

	waitCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		got, err := p.Get(waitCtx)
		if got != nil {
			p.Put(ctx, got)
		}
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	cancel()
	cn.GetStateMachine().Transition(pool.StateIdle)
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("cancelled Get returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled Get did not finish")
	}
}
