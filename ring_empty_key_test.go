package redis_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// shardRecorderHook records which shard a command reaches and answers it
// without touching the network.
type shardRecorderHook struct {
	addr string
	mu   *sync.Mutex
	seen map[string]int
}

func (h shardRecorderHook) record() {
	h.mu.Lock()
	h.seen[h.addr]++
	h.mu.Unlock()
}

func (h shardRecorderHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h shardRecorderHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(context.Context, redis.Cmder) error {
		h.record()
		return nil
	}
}

func (h shardRecorderHook) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(context.Context, []redis.Cmder) error {
		h.record()
		return nil
	}
}

func TestRingPipelineRoutesEmptyKeyLikeSingleCommand(t *testing.T) {
	ctx := context.Background()
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"a": "127.0.0.1:1", "b": "127.0.0.1:2", "c": "127.0.0.1:3", "d": "127.0.0.1:4",
		},
		HeartbeatFrequency: time.Hour,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			t.Error("unexpected dial")
			return nil, net.ErrClosed
		},
	})
	defer ring.Close()

	var mu sync.Mutex
	seen := map[string]int{}
	if err := ring.ForEachShard(ctx, func(_ context.Context, c *redis.Client) error {
		c.AddHook(shardRecorderHook{addr: c.Options().Addr, mu: &mu, seen: seen})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const n = 40
	for i := 0; i < n; i++ {
		ring.Get(ctx, "")
		for _, pipe := range []redis.Pipeliner{ring.Pipeline(), ring.TxPipeline()} {
			pipe.Get(ctx, "")
			_, _ = pipe.Exec(ctx)
		}
	}

	if len(seen) != 1 {
		t.Fatalf("empty key reached %d shards, want 1: %v", len(seen), seen)
	}
	for addr, got := range seen {
		if got != 3*n {
			t.Fatalf("shard %s got %d commands, want %d", addr, got, 3*n)
		}
	}
}
