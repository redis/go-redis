package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestDialPanicFailsAttempt pins that a panic in a dial (e.g. a user
// OnConnect hook) fails that attempt with errDialPanic instead of
// crashing the process, and that the retries still run.
func TestDialPanicFailsAttempt(t *testing.T) {
	var mu sync.Mutex
	var calls atomic.Int32
	d := newDialer(&mu, func(context.Context, string) (*pool.Conn, error) {
		calls.Add(1)
		panic("test: OnConnect panic")
	}, 2, func(int) time.Duration { return time.Millisecond }, time.Millisecond, time.Millisecond)
	t.Cleanup(d.close)

	var got error
	mu.Lock()
	call := d.dialLocked(func() dialPlan {
		return dialPlan{
			addr: "node:6379",
			installLocked: func(cn *pool.Conn, err error) error {
				if cn != nil {
					t.Errorf("installLocked got a connection from a panicking dial")
				}
				got = err
				return err
			},
		}
	})
	mu.Unlock()

	select {
	case <-call.done:
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not settle")
	}
	if !errors.Is(got, errDialPanic) {
		t.Fatalf("dial error = %v, want errDialPanic", got)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3 (1 + 2 retries)", n)
	}
}
