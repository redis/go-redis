package pool_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestFreshSessionMaxActiveConns(t *testing.T) {
	ctx := context.Background()
	p := pool.NewConnPool(&pool.Options{
		Dialer: dummyDialer, PoolSize: 4, MaxActiveConns: 1, MaxConcurrentDials: 4,
		PoolTimeout: time.Second, DialTimeout: time.Second,
	})
	t.Cleanup(func() { _ = p.Close() })
	first := pool.NewStickyConnPool(p)
	first.NewConnOnGet()
	t.Cleanup(func() { _ = first.Close() })
	cn, err := first.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first.Put(ctx, cn)
	second := pool.NewStickyConnPool(p)
	second.NewConnOnGet()
	t.Cleanup(func() { _ = second.Close() })
	if extra, err := second.Get(ctx); !errors.Is(err, pool.ErrPoolExhausted) {
		if extra != nil {
			second.Put(ctx, extra)
		}
		t.Fatalf("second fresh session: %v, want ErrPoolExhausted", err)
	}
	if extra, err := p.Get(ctx); !errors.Is(err, pool.ErrPoolExhausted) {
		if extra != nil {
			p.Remove(ctx, extra, pool.ErrClosed)
		}
		t.Fatalf("pooled Get alongside fresh session: %v, want ErrPoolExhausted", err)
	}
	if p.HasFreeCapacity() {
		t.Fatal("fresh session at MaxActiveConns must consume dial capacity")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	cn, err = second.Get(ctx)
	if err != nil {
		t.Fatalf("fresh capacity was not released on Close: %v", err)
	}
	second.Put(ctx, cn)
	if p.Len() != 1 || p.QueueLen() != 0 {
		t.Fatalf("connections=%d turns=%d, want 1 and 0", p.Len(), p.QueueLen())
	}
}

func TestFreshSessionConcurrentLimits(t *testing.T) {
	for _, limit := range []string{"active", "dials"} {
		for _, firstFresh := range []bool{false, true} {
			name := limit + "/pooled-first"
			if firstFresh {
				name = limit + "/fresh-first"
			}
			t.Run(name, func(t *testing.T) {
				var calls atomic.Int32
				started, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				opt := &pool.Options{
					PoolSize: 4, MaxConcurrentDials: 1, DialerRetries: 1,
					PoolTimeout: time.Second, DialTimeout: time.Second,
					Dialer: func(ctx context.Context) (net.Conn, error) {
						if calls.Add(1) == 1 {
							close(started)
						}
						select {
						case <-release:
							return newDummyConn(), nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
				}
				if limit == "active" {
					opt.MaxActiveConns = 1
					opt.MaxConcurrentDials = 4
				}
				p := pool.NewConnPool(opt)
				t.Cleanup(func() { _ = p.Close() })
				done := make(chan error, 1)
				go func() {
					var cn *pool.Conn
					var err error
					if firstFresh {
						cn, err = p.NewConn(context.Background())
						if cn != nil {
							p.RemoveWithoutTurn(context.Background(), cn, pool.ErrClosed)
						}
					} else {
						cn, err = p.Get(context.Background())
						if cn != nil {
							p.Remove(context.Background(), cn, pool.ErrClosed)
						}
					}
					done <- err
				}()
				<-started
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
				cn, err := p.NewConn(ctx)
				if cn != nil {
					p.RemoveWithoutTurn(context.Background(), cn, pool.ErrClosed)
				}
				want := error(context.DeadlineExceeded)
				if limit == "active" {
					want = pool.ErrPoolExhausted
				}
				if !errors.Is(err, want) {
					t.Errorf("second dial error=%v, want %v", err, want)
				}
				if n := calls.Load(); n != 1 {
					t.Errorf("dial calls=%d, want only the admitted dial", n)
				}
				unblock()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				// Cancellation/exhaustion must not leak permits or reservations.
				cn, err = p.NewConn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				p.RemoveWithoutTurn(context.Background(), cn, pool.ErrClosed)
				if p.Len() != 0 || p.QueueLen() != 0 {
					t.Fatal("admission left connections or pool turns behind")
				}
			})
		}
	}
}

func TestFreshSessionReleasesAdmissionOnDialFailure(t *testing.T) {
	for _, failure := range []string{"error", "cancel", "panic"} {
		t.Run(failure, func(t *testing.T) {
			var calls atomic.Int32
			dialErr := errors.New("dial failed")
			p := pool.NewConnPool(&pool.Options{
				PoolSize: 4, MaxActiveConns: 1, MaxConcurrentDials: 1,
				DialerRetries: 1, DialTimeout: time.Second,
				Dialer: func(ctx context.Context) (net.Conn, error) {
					if calls.Add(1) == 1 {
						switch failure {
						case "cancel":
							<-ctx.Done()
							return nil, ctx.Err()
						case "panic":
							panic(dialErr)
						default:
							return nil, dialErr
						}
					}
					return newDummyConn(), nil
				},
			})
			t.Cleanup(func() { _ = p.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			func() {
				if failure == "panic" {
					defer func() {
						if got := recover(); got != dialErr {
							t.Errorf("panic=%v, want %v", got, dialErr)
						}
					}()
				}
				if _, err := p.NewConn(ctx); err == nil {
					t.Fatal("expected dial failure")
				}
			}()
			ctx, cancelNext := context.WithTimeout(context.Background(), time.Second)
			defer cancelNext()
			cn, err := p.NewConn(ctx)
			if err != nil {
				t.Fatalf("failed dial leaked admission capacity: %v", err)
			}
			p.RemoveWithoutTurn(ctx, cn, pool.ErrClosed)
			if p.Len() != 0 || p.QueueLen() != 0 {
				t.Fatal("failed dial leaked connections or pool turns")
			}
		})
	}
}

func TestFreshSessionWaiterStopsOnClose(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 4, MaxActiveConns: 1, MaxConcurrentDials: 1,
		Dialer: func(context.Context) (net.Conn, error) {
			close(started)
			<-release
			return newDummyConn(), nil
		},
	})
	t.Cleanup(func() { _ = p.Close() })
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := p.NewConn(context.Background()); first <- err }()
	<-started
	go func() { _, err := p.NewConn(context.Background()); second <- err }()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("waiting dial: %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting dial did not exit on pool Close")
	}
	unblock()
	if err := <-first; !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("in-flight dial: %v, want ErrClosed", err)
	}
	if p.Len() != 0 || p.QueueLen() != 0 {
		t.Fatal("Close left connections or turns behind")
	}
}

func TestFreshSessionSharesPrewarmLimits(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 4, MinIdleConns: 3, MaxActiveConns: 1, MaxConcurrentDials: 1,
		Dialer: func(context.Context) (net.Conn, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return newDummyConn(), nil
		},
	})
	t.Cleanup(func() { _ = p.Close() })
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if cn, err := p.NewConn(ctx); !errors.Is(err, context.DeadlineExceeded) {
		if cn != nil {
			p.RemoveWithoutTurn(ctx, cn, pool.ErrClosed)
		}
		t.Errorf("fresh dial alongside prewarm: %v, want deadline exceeded", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("concurrent prewarm/session dials=%d, want 1", n)
	}
	unblock()
	deadline := time.Now().Add(time.Second)
	for p.QueueLen() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.QueueLen() != 0 || p.Len() != 1 || p.IdleLen() != 1 || calls.Load() != 1 {
		t.Fatalf("prewarm exceeded active limit or leaked turns: stats=%+v turns=%d dials=%d", p.Stats(), p.QueueLen(), calls.Load())
	}
}

func TestPrewarmRefillsAfterDial(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		name := "dedicated"
		if pooled {
			name = "pooled"
		}
		t.Run(name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var calls atomic.Int32
			p := pool.NewConnPool(&pool.Options{
				PoolSize: 4, MaxConcurrentDials: 1,
				Dialer: func(context.Context) (net.Conn, error) {
					if calls.Add(1) == 1 {
						close(started)
						<-release
					}
					return newDummyConn(), nil
				},
			})
			t.Cleanup(func() { unblock(); _ = p.Close() })
			done := make(chan error, 1)
			go func() {
				// Keep the first connection checked out throughout the refill.
				var err error
				if pooled {
					_, err = p.Get(context.Background())
				} else {
					_, err = p.NewConn(context.Background())
				}
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("first dial did not start")
			}
			wantTurns := 0
			if pooled {
				wantTurns = 1
			}
			p.RaiseMinIdleConns(3)
			if p.QueueLen() != wantTurns {
				t.Fatal("prewarm held a turn while waiting for dial capacity")
			}
			unblock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for (p.IdleLen() != 3 || p.QueueLen() != wantTurns) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if p.IdleLen() != 3 || p.QueueLen() != wantTurns || p.Len() != 4 {
				t.Fatalf("refill needs unrelated activity: stats=%+v turns=%d", p.Stats(), p.QueueLen())
			}
		})
	}
}

func TestPrewarmDoesNotHoldTurnsBehindFreshDial(t *testing.T) {
	ctx := context.Background()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var calls atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		PoolSize: 1, MaxConcurrentDials: 1, DialerRetries: 1,
		PoolTimeout: 30 * time.Millisecond,
		Dialer: func(context.Context) (net.Conn, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			return newDummyConn(), nil
		},
	})
	t.Cleanup(func() { unblock(); _ = p.Close() })
	done := make(chan error, 1)
	go func() {
		cn, err := p.NewConn(ctx)
		if cn != nil {
			p.RemoveWithoutTurn(ctx, cn, pool.ErrClosed)
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fresh dial did not start")
	}

	// The dedicated dial owns the only dial permit, but no pool turn.
	p.RaiseMinIdleConns(1)
	if turns := p.QueueLen(); turns != 0 {
		t.Errorf("prewarm reserved %d turns behind a fresh dial, want 0", turns)
	}
	getCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	cn, err := p.Get(getCtx)
	if cn != nil {
		p.Remove(ctx, cn, pool.ErrClosed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Get waiting for dial capacity: %v, want context deadline, not PoolTimeout", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("dial calls=%d, want only the dedicated dial", n)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// A later check must still be able to replenish the idle floor.
	p.CheckMinIdleConns()
	deadline := time.Now().Add(time.Second)
	for (p.QueueLen() != 0 || p.Len() != 1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.QueueLen() != 0 || p.Len() != 1 || p.IdleLen() != 1 {
		t.Fatalf("prewarm did not recover: stats=%+v turns=%d", p.Stats(), p.QueueLen())
	}
}
