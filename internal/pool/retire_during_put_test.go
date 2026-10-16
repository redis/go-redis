package pool_test

import (
	"context"
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
