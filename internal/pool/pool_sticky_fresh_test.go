package pool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestStickyConnPoolNewConnAccounting(t *testing.T) {
	for _, action := range []string{"close", "remove", "close-on-put", "reset"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			parent := pool.NewConnPool(&pool.Options{
				Dialer: dummyDialer, PoolSize: 1, MaxConcurrentDials: 1,
				PoolTimeout: time.Second, DialTimeout: time.Second,
			})
			t.Cleanup(func() { _ = parent.Close() })
			// Hold the only pool turn throughout: an erroneous Remove would
			// steal this turn instead of exposing the bug on an empty semaphore.
			held, err := parent.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { parent.Remove(ctx, held, pool.ErrClosed) })
			baseline := parent.Stats()
			for i := 0; i < 20; i++ {
				sticky := pool.NewStickyConnPool(parent)
				sticky.NewConnOnGet()
				cn, err := sticky.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if cn == held || parent.Len() != 2 || parent.QueueLen() != 1 {
					t.Fatal("fresh acquisition must bypass PoolSize without taking a turn")
				}
				switch action {
				case "remove", "reset":
					sticky.Remove(ctx, cn, errors.New("initialization failed"))
					if action == "reset" {
						if err := sticky.Reset(ctx); err != nil {
							t.Fatal(err)
						}
						replacement, err := sticky.Get(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if replacement == cn {
							t.Fatal("Reset reused the failed connection")
						}
						sticky.Put(ctx, replacement)
					}
				case "close-on-put":
					cn.MarkCloseOnPut("unread replies")
					sticky.Put(ctx, cn)
				default:
					sticky.Put(ctx, cn)
				}
				if err := sticky.Close(); err != nil {
					t.Fatal(err)
				}
				stats := parent.Stats()
				if parent.QueueLen() != 1 || stats.TotalConns != baseline.TotalConns ||
					stats.IdleConns != baseline.IdleConns || stats.PendingRequests != baseline.PendingRequests {
					t.Fatalf("cycle %d leaked a conn or freed the held turn: turns=%d stats=%+v", i, parent.QueueLen(), stats)
				}
			}
		})
	}
}

func TestStickyConnPoolNewConnCloseRaces(t *testing.T) {
	for _, action := range []string{"get", "put", "remove"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			started, resume := make(chan struct{}), make(chan struct{})
			parent := pool.NewConnPool(&pool.Options{
				Dialer: dummyDialer, PoolSize: 1, MaxConcurrentDials: 1,
				PoolTimeout: time.Second, DialTimeout: time.Second,
			})
			t.Cleanup(func() { _ = parent.Close() })
			held, err := parent.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { parent.Remove(ctx, held, pool.ErrClosed) })
			sticky := pool.NewStickyConnPool(parent)
			if action == "get" {
				// Pause the fresh dial before it returns to StickyConnPool.Get.
				sticky = pool.NewStickyConnPool(&blockedNewConnPool{
					Pooler: parent, started: started, resume: resume,
				})
			}
			sticky.NewConnOnGet()
			if action == "get" {
				done := make(chan error, 1)
				go func() { _, err := sticky.Get(ctx); done <- err }()
				<-started
				if err := sticky.Close(); err != nil {
					t.Error(err)
				}
				close(resume)
				if err := <-done; !errors.Is(err, pool.ErrClosed) {
					t.Fatalf("Get racing Close: %v, want ErrClosed", err)
				}
			} else {
				cn, err := sticky.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := sticky.Close(); err != nil {
					t.Fatal(err)
				}
				if action == "put" {
					sticky.Put(ctx, cn)
				} else {
					sticky.Remove(ctx, cn, errors.New("command failed after Close"))
				}
			}
			if parent.QueueLen() != 1 || parent.Len() != 1 {
				t.Fatal("close race leaked a connection or freed an unrelated turn")
			}
		})
	}
}

type blockedNewConnPool struct {
	pool.Pooler
	started chan struct{}
	resume  chan struct{}
}

func (p *blockedNewConnPool) NewConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := p.Pooler.NewConn(ctx)
	close(p.started)
	<-p.resume
	return cn, err
}

func TestStickyConnPoolNewConnHonorsDialGate(t *testing.T) {
	ctx := context.Background()
	parent := pool.NewConnPool(&pool.Options{
		Dialer: dummyDialer, PoolSize: 1, MaxActiveConns: 1, MaxConcurrentDials: 1,
		PoolTimeout: time.Second, DialTimeout: time.Second,
	})
	t.Cleanup(func() { _ = parent.Close() })
	held, err := parent.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { parent.Remove(ctx, held, pool.ErrClosed) })
	sticky := pool.NewStickyConnPool(parent)
	sticky.NewConnOnGet()
	if _, err := sticky.Get(ctx); !errors.Is(err, pool.ErrPoolExhausted) {
		t.Fatalf("fresh Get: %v, want ErrPoolExhausted", err)
	}
	if err := sticky.Close(); err != nil {
		t.Fatal(err)
	}
	if parent.QueueLen() != 1 || parent.Len() != 1 {
		t.Fatal("failed fresh dial changed parent accounting")
	}
}
