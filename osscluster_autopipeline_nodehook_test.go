package redis_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type nodePeekHook struct{ armed *atomic.Bool }

func (h nodePeekHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (h nodePeekHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h nodePeekHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.armed.Load() {
			for _, c := range cmds {
				_ = c.Err() // redisotel via rediscmd.CmdsString does exactly this pre-next
				_ = c.String()
			}
		}
		err := next(ctx, cmds)
		if h.armed.Load() {
			for _, c := range cmds {
				_ = c.Err()
				_ = c.String()
			}
		}
		return err
	}
}

// TestClusterNodeHookResultPeekNoDeadlock pins the batch-executor guard for
// cluster per-node goroutines: redisotel installs tracing hooks on NODE
// clients via OnNewNode, and rediscmd.CmdsString reads every command's Err()
// BEFORE next() — on the deferred face those reads used to block on a batch
// that completes only after the node call returns, wedging the dispatch
// permanently (found while assessing the String review round on #3942).
func TestClusterNodeHookResultPeekNoDeadlock(t *testing.T) {
	ctx := context.Background()
	probe := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}})
	if err := probe.Ping(ctx).Err(); err != nil {
		probe.Close()
		t.Skipf("no cluster: %v", err)
	}
	probe.Close()

	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}})
	defer cc.Close()
	var armed atomic.Bool
	var installs atomic.Int32
	cc.OnNewNode(func(nc *redis.Client) {
		installs.Add(1)
		nc.AddHook(nodePeekHook{armed: &armed})
	})
	cc.Ping(ctx)
	t.Logf("node hooks installed: %d", installs.Load())

	ap, err := cc.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		armed.Store(true)
		defer armed.Store(false)
		cmds := make([]*redis.StatusCmd, 50)
		for i := range cmds {
			cmds[i] = ap.Set(ctx, "nodehook:"+string(rune('a'+i%26))+":"+string(rune('0'+i%10)), "v", 0)
		}
		var firstErr error
		for _, c := range cmds {
			if err := c.Err(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		done <- firstErr
	}()
	select {
	case err := <-done:
		t.Logf("no hang, err=%v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("node-level hook result peek wedged the async cluster dispatch (batch executor registration regressed)")
	}
}
