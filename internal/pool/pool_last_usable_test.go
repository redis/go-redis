package pool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestPoolReturnsLastUsableIdleConnection(t *testing.T) {
	for _, fifo := range []bool{false, true} {
		for _, try := range []bool{false, true} {
			t.Run(fmt.Sprintf("fifo=%t/try=%t", fifo, try), func(t *testing.T) {
				ctx := context.Background()
				p := pool.NewConnPool(&pool.Options{
					PoolSize:           2,
					MaxActiveConns:     2,
					MaxConcurrentDials: 2,
					PoolFIFO:           fifo,
					PoolTimeout:        100 * time.Millisecond,
					Dialer:             dummyDialer,
				})
				t.Cleanup(func() { _ = p.Close() })

				usable, err := p.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				unusable, err := p.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				usable.GetStateMachine().Transition(pool.StateInUse)
				unusable.GetStateMachine().Transition(pool.StateInUse)

				// Arrange for the unusable connection to be checked first.
				if fifo {
					p.Put(ctx, unusable)
					p.Put(ctx, usable)
				} else {
					p.Put(ctx, usable)
					p.Put(ctx, unusable)
				}
				// Background reauthentication reserves idle connections this way.
				unusable.GetStateMachine().Transition(pool.StateUnusable)

				get := p.Get
				if try {
					get = p.TryGet
				}
				got, err := get(ctx)
				if got != nil {
					defer p.Put(ctx, got)
				}
				if err != nil || got != usable {
					t.Fatalf("did not return the last usable idle connection: err=%v returned=%t state=%v",
						err, got == usable, usable.GetStateMachine().GetState())
				}
				if p.IdleLen() != 1 || p.Len() != 2 {
					t.Fatalf("idle=%d total=%d, want idle=1 total=2", p.IdleLen(), p.Len())
				}
				if state := unusable.GetStateMachine().GetState(); state != pool.StateUnusable {
					t.Fatalf("skipped connection state=%v, want UNUSABLE", state)
				}

				// Once reauthentication finishes, the skipped connection remains available.
				unusable.GetStateMachine().Transition(pool.StateIdle)
				recovered, err := get(ctx)
				if recovered != nil {
					defer p.Put(ctx, recovered)
				}
				if err != nil || recovered != unusable {
					t.Fatalf("did not return the recovered idle connection: err=%v returned=%t", err, recovered == unusable)
				}
			})
		}
	}
}
