package redis

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// singleflightGroup coalesces concurrent calls with the same key.
// Unlike golang.org/x/sync/singleflight, each waiter keeps its own context:
// a cancelled waiter returns early without cancelling the shared call while
// other callers still wait for it. The leader also selects on its own
// context; the shared work runs in a goroutine under a timeout-bounded
// context so a short dial deadline cannot stretch into a full multi-sentinel
// retry budget (#4146 / #4222 review). When every caller has given up, the
// shared context is cancelled so the lookup does not keep running (and keep
// holding sentinelFailover.mu) with nobody waiting for it.
type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*singleflightCall

	// onWaiter is invoked (without holding mu) when a caller joins an
	// in-flight call. Tests use it to wait until every waiter has attached.
	onWaiter func()
}

type singleflightCall struct {
	done   chan struct{}
	cancel context.CancelFunc

	// refs counts callers still waiting for the result. Guarded by
	// singleflightGroup.mu.
	refs int

	val interface{}
	err error
}

func (g *singleflightGroup) wait(ctx context.Context, key string, c *singleflightCall) (interface{}, error) {
	select {
	case <-c.done:
		return c.val, c.err
	case <-ctx.Done():
	}

	g.mu.Lock()
	c.refs--
	if c.refs == 0 {
		// Nobody is waiting any more: stop the shared lookup and let the
		// next caller start a fresh one instead of joining a cancelled call.
		if g.m[key] == c {
			delete(g.m, key)
		}
		c.cancel()
	}
	g.mu.Unlock()
	return nil, ctx.Err()
}

// Do runs fn once for key and shares the result with concurrent callers.
// timeout bounds the shared context (typically DialTimeout+ReadTimeout from
// options). A zero timeout leaves the shared context without a deadline.
// Every caller, including the leader, returns when its own ctx ends.
// A panic in fn is recovered and returned to every caller as an error.
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
		c.refs++
		g.mu.Unlock()
		if g.onWaiter != nil {
			g.onWaiter()
		}
		return g.wait(ctx, key, c)
	}

	shared, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if timeout > 0 {
		var cancelTimeout context.CancelFunc
		shared, cancelTimeout = context.WithTimeout(shared, timeout)
		parentCancel := cancel
		cancel = func() { cancelTimeout(); parentCancel() }
	}
	c := &singleflightCall{done: make(chan struct{}), cancel: cancel, refs: 1}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.val, c.err = nil, fmt.Errorf("redis: sentinel lookup panicked: %v", r)
			}
			g.mu.Lock()
			if g.m[key] == c {
				delete(g.m, key)
			}
			g.mu.Unlock()
			cancel()
			close(c.done)
		}()
		c.val, c.err = fn(shared)
	}()

	return g.wait(ctx, key, c)
}
