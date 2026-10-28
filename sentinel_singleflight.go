package redis

import (
	"context"
	"sync"
)

// singleflightGroup coalesces concurrent calls with the same key.
// Unlike golang.org/x/sync/singleflight, each waiter keeps its own context:
// a cancelled waiter returns early without cancelling the shared call.
type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*singleflightCall
}

type singleflightCall struct {
	wg sync.WaitGroup

	val interface{}
	err error
}

func (g *singleflightGroup) Do(
	ctx context.Context,
	key string,
	fn func(shared context.Context) (interface{}, error),
) (interface{}, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*singleflightCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			return nil, ctx.Err(), false
		case <-done:
			return c.val, c.err, false
		}
	}

	c := &singleflightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	// Shared work must outlive any waiter's context cancellation.
	shared := context.WithoutCancel(ctx)
	c.val, c.err = fn(shared)
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	// If the leader's context ended while the shared call ran, surface that
	// cancellation to the leader. Other waiters already select on their own ctx.
	if err := ctx.Err(); err != nil {
		return nil, err, true
	}
	return c.val, c.err, true
}
