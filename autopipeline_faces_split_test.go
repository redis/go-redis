package redis_test

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestBlockingFaceExecutesOnCall: AutoPipeline() command blocks until executed —
// the returned cmd already holds its result without an explicit further wait.
func TestBlockingFaceExecutesOnCall(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline(nil) // blocking face
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const G, N = 50, 100
	var wg sync.WaitGroup
	var bad int
	var mu sync.Mutex
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			key := func() string { return "blk:" + string(rune('a'+id%26)) }()
			_ = key
			for i := 1; i <= N; i++ {
				// Incr blocks until executed; per-goroutine order must hold.
				cmd := ap.Incr(ctx, "blkkey:"+string(rune(id)))
				if v, err := cmd.Result(); err != nil || v != int64(i) {
					mu.Lock()
					bad++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("blocking face: %d ordering/exec failures", bad)
	}
}

// TestAsyncFaceDeferred: AsyncAutoPipeline() command returns immediately; result
// is read later. Windowed submit then read works.
func TestAsyncFaceDeferred(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AsyncAutoPipeline(nil) // deferred face, ordered default
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const N = 500
	cmds := make([]*redis.IntCmd, 0, N)
	for i := 0; i < N; i++ {
		cmds = append(cmds, ap.Incr(ctx, "asynckey")) // does not block
	}
	for i, cmd := range cmds {
		if v, err := cmd.Result(); err != nil || v != int64(i+1) {
			t.Fatalf("async windowed: cmd %d got v=%d err=%v", i, v, err)
		}
	}
}
