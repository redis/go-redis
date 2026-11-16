package pool_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/auth/streaming"
	"github.com/redis/go-redis/v9/internal/pool"
)

// A re-authentication worker keeps its connection in the pool's idle list
// while the state machine makes it temporarily unusable. Only a hard-cap
// exhaustion should wait for that connection; otherwise Get must dial.
func TestGetWaitsForReAuth(t *testing.T) {
	for _, tc := range []struct {
		name      string
		maxActive int32
		wait      bool
	}{
		{name: "capped", maxActive: 1, wait: true},
		{name: "unlimited", maxActive: 0},
		{name: "spare-capacity", maxActive: 2},
		{name: "soft-pool-size", maxActive: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, phase := range []string{"pending", "running", "auth-command"} {
				t.Run(phase, func(t *testing.T) {
					ctx := context.Background()
					var dials atomic.Int32
					p := pool.NewConnPool(&pool.Options{
						PoolSize: 1, MaxActiveConns: tc.maxActive, MaxConcurrentDials: 2,
						PoolTimeout: 400 * time.Millisecond, DialTimeout: time.Second,
						Dialer: func(context.Context) (net.Conn, error) {
							dials.Add(1)
							return newDummyConn(), nil
						},
					})
					t.Cleanup(func() { _ = p.Close() })
					manager := streaming.NewManager(p, time.Second)
					p.AddPoolHook(manager.PoolHook())
					original, err := p.Get(ctx)
					if err != nil {
						t.Fatal(err)
					}
					original.GetStateMachine().Transition(pool.StateInUse)
					p.Put(ctx, original)

					started := make(chan struct{})
					release := make(chan struct{})
					finished := make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					t.Cleanup(unblock)
					listener, err := manager.Listener(original, func(*pool.Conn, auth.Credentials) error {
						if phase == "auth-command" {
							// AUTH temporarily uses StateInUse via SingleConnPool.
							authPool := pool.NewSingleConnPool(p, original)
							active, err := authPool.Get(ctx)
							if err != nil {
								return err
							}
							defer authPool.Put(ctx, active)
						}
						close(started)
						<-release
						close(finished)
						return nil
					}, func(_ *pool.Conn, err error) { t.Errorf("reauth: %v", err) })
					if err != nil {
						t.Fatal(err)
					}
					listener.OnNext(auth.NewBasicCredentials("app", "rotated"))
					if phase != "pending" {
						_, _, _ = manager.PoolHook().OnPut(ctx, original)
						select {
						case <-started:
						case <-time.After(time.Second):
							t.Fatal("re-auth worker did not start")
						}
					}

					type result struct {
						cn  *pool.Conn
						err error
					}
					done := make(chan result, 1)
					go func() {
						cn, err := p.Get(ctx)
						done <- result{cn, err}
					}()

					if tc.wait {
						select {
						case got := <-done:
							if got.cn != nil {
								p.Put(ctx, got.cn)
							}
							t.Fatalf("capped Get returned before re-auth finished: %v", got.err)
						case <-time.After(20 * time.Millisecond):
						}
					} else {
						select {
						case got := <-done:
							if got.err != nil || got.cn == original {
								t.Fatalf("Get with spare capacity: conn=%p err=%v", got.cn, got.err)
							}
							got.cn.GetStateMachine().Transition(pool.StateInUse)
							p.Put(ctx, got.cn)
						case <-time.After(200 * time.Millisecond):
							unblock()
							t.Fatal("Get waited instead of dialing below MaxActiveConns")
						}
					}

					unblock()
					if tc.wait {
						select {
						case got := <-done:
							if got.err != nil || got.cn != original {
								t.Fatalf("capped Get did not reuse re-auth connection: conn=%p err=%v", got.cn, got.err)
							}
							p.Put(ctx, got.cn)
						case <-time.After(time.Second):
							t.Fatal("Get did not resume after re-auth")
						}
					}
					if phase != "pending" {
						select {
						case <-finished:
						case <-time.After(time.Second):
							t.Fatal("re-auth did not finish")
						}
					}
					wantDials := int32(2)
					if tc.wait {
						wantDials = 1
					}
					if dials.Load() != wantDials || p.QueueLen() != 0 {
						t.Fatalf("dials=%d turns=%d, want %d and 0", dials.Load(), p.QueueLen(), wantDials)
					}
				})
			}
		})
	}
}

func TestTryGetReAuthOnlySpillsAtHardCap(t *testing.T) {
	for _, maxActive := range []int32{0, 1, 2} {
		t.Run(string(rune('0'+maxActive)), func(t *testing.T) {
			ctx := context.Background()
			var dials atomic.Int32
			p := pool.NewConnPool(&pool.Options{
				PoolSize: 1, MaxActiveConns: maxActive, MaxConcurrentDials: 2,
				PoolTimeout: time.Second,
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
			original.GetStateMachine().Transition(pool.StateUnusable)
			got, err := p.TryGet(ctx)
			if maxActive == 1 {
				if got != nil || !errors.Is(err, pool.ErrPoolTryFull) || dials.Load() != 1 {
					t.Fatalf("capped TryGet: conn=%p err=%v dials=%d", got, err, dials.Load())
				}
			} else {
				if err != nil || got == original || dials.Load() != 2 {
					t.Fatalf("spare-capacity TryGet: conn=%p err=%v dials=%d", got, err, dials.Load())
				}
				got.GetStateMachine().Transition(pool.StateInUse)
				p.Put(ctx, got)
			}
			original.GetStateMachine().Transition(pool.StateIdle)
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
			if got != nil || !errors.Is(err, want) {
				t.Fatalf("Get returned conn=%p err=%v, want nil and %v", got, err, want)
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
	time.Sleep(85 * time.Millisecond)
	original.GetStateMachine().Transition(pool.StateIdle)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Get after maintenance: %v", got.err)
		}
		if got.cn == original || dials.Load() != 2 {
			t.Fatalf("expired connection reused: conn=%p original=%p dials=%d", got.cn, original, dials.Load())
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
