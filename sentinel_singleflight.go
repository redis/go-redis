package redis

import (
	"context"
	"sync"
	"time"
)

// singleflightGroup coalesces concurrent calls with the same key.
// Unlike golang.org/x/sync/singleflight, each waiter keeps its own context:
// a cancelled waiter returns early without cancelling the shared call.
// The leader also selects on its own context; the shared work runs in a
// goroutine under a timeout-bounded context so a short dial deadline cannot
// stretch into a full multi-sentinel retry budget (#4146 / #4222 review).
type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*singleflightCall

	// onWaiter is invoked (without holding mu) when a caller joins an
	// in-flight call. Tests use it to wait until every waiter has attached.
	onWaiter func()
}

type singleflightCall struct {
	done chan struct{}

	val interface{}
	err error
}

func (c *singleflightCall) wait(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return c.val, c.err
	}
}

// Do runs fn once for key and shares the result with concurrent callers.
// timeout bounds the shared context (typically DialTimeout+ReadTimeout from
// options). A zero timeout leaves the shared context without a deadline.
// Every caller, including the leader, returns when its own ctx ends.
func (g *singleflightGroup) Do(
	ctx context.Context,
	key string,
	timeout time.Duration,
	fn func(shared context.Context) (interface{}, error),
) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*singleflightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		if g.onWaiter != nil {
			g.onWaiter()
		}
		return c.wait(ctx)
	}

	c := &singleflightCall{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		defer func() {
			g.mu.Lock()
			delete(g.m, key)
			g.mu.Unlock()
			close(c.done)
		}()

		shared := context.WithoutCancel(ctx)
		if timeout > 0 {
			var cancel context.CancelFunc
			shared, cancel = context.WithTimeout(shared, timeout)
			defer cancel()
		}
		c.val, c.err = fn(shared)
	}()

	return c.wait(ctx)
}
