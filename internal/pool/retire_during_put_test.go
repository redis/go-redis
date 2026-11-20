package pool_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// retireOnPutHook retires the connection from inside OnPut, which runs after
// putConn's top-of-function close-on-put check and before the idle append:
// the exact interleaving a concurrent RetireConns can hit.
type retireOnPutHook struct {
	p      *pool.ConnPool
	reason string
	fired  bool
}

func (h *retireOnPutHook) OnGet(context.Context, *pool.Conn, bool) (bool, error) {
	return true, nil
}

func (h *retireOnPutHook) OnPut(ctx context.Context, cn *pool.Conn) (bool, bool, error) {
	if !h.fired {
		h.fired = true
		h.p.RetireConns(ctx, []*pool.Conn{cn}, h.reason)
	}
	return true, false, nil
}

func (h *retireOnPutHook) OnRemove(context.Context, *pool.Conn, error) {}

// A connection retired while its Put is in flight must not be idled.
// RetireConns sees it as in use (not yet in the idle list) and marks it
// close-on-put; Put must honor that mark before appending to the idle list,
// or the marked connection stays in the pool until its next put. This is the
// window behind the flaky ModeAuto downgrade test in the root package.
func TestRetireConnsDuringPutRemovesConnection(t *testing.T) {
	ctx := context.Background()
	p := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    time.Hour,
	})
	defer p.Close()
	hook := &retireOnPutHook{p: p, reason: "retired-during-put"}
	p.AddPoolHook(hook)

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Put(ctx, cn)

	if !hook.fired {
		t.Fatal("the hook did not run: the interleaving was not exercised")
	}
	if got := p.Len(); got != 0 {
		t.Fatalf("pool len = %d after retiring a connection during its Put, want 0 (it was idled with the close-on-put mark set)", got)
	}
}

// retireAndCloseOnPutHook retires the connection from inside OnPut and moves
// it to StateClosed, so putConn takes its StateClosed removal path with the
// close-on-put mark also set.
type retireAndCloseOnPutHook struct {
	p       *pool.ConnPool
	removes atomic.Int32
}

func (h *retireAndCloseOnPutHook) OnGet(context.Context, *pool.Conn, bool) (bool, error) {
	return true, nil
}

func (h *retireAndCloseOnPutHook) OnPut(ctx context.Context, cn *pool.Conn) (bool, bool, error) {
	h.p.RetireConns(ctx, []*pool.Conn{cn}, "retired-during-put")
	cn.GetStateMachine().Transition(pool.StateClosed)
	return true, false, nil
}

func (h *retireAndCloseOnPutHook) OnRemove(context.Context, *pool.Conn, error) {
	h.removes.Add(1)
}

// A connection a hook moves to StateClosed is removed once: the late
// close-on-put recheck must not remove it a second time (OnRemove twice,
// removal metrics skipped, used gauge left at +1).
func TestPutClosedByHookWithRetireMarkRemovesOnce(t *testing.T) {
	ctx := context.Background()
	p := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    time.Hour,
	})
	defer p.Close()
	hook := &retireAndCloseOnPutHook{p: p}
	p.AddPoolHook(hook)

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	var mu sync.Mutex
	used, closed := 0, 0
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		ConnectionCount: func(_ context.Context, delta int, c *pool.Conn, state string, _ bool) {
			if c != cn || state != pool.MetricStateUsed {
				return
			}
			mu.Lock()
			used += delta
			mu.Unlock()
		},
		ConnectionClosed: func(_ context.Context, c *pool.Conn, _ string, _ error) {
			if c != cn {
				return
			}
			mu.Lock()
			closed++
			mu.Unlock()
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	p.Put(ctx, cn)

	if got := hook.removes.Load(); got != 1 {
		t.Errorf("OnRemove ran %d times, want 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	// SetAllMetricCallbacks reports the live in-use conn as +1; the removal
	// must bring it back to 0.
	if used != 0 {
		t.Errorf("used gauge = %d, want 0 (the removal decrements it once)", used)
	}
	if closed != 1 {
		t.Errorf("ConnectionClosed emitted %d times, want 1", closed)
	}
	if got := p.Len(); got != 0 {
		t.Errorf("pool len = %d, want 0", got)
	}
}
