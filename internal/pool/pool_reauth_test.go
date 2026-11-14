package pool_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/auth/streaming"
	"github.com/redis/go-redis/v9/internal/pool"
)

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
