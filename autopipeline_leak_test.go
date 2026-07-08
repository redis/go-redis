package redis_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineNoGoroutineLeak opens and closes many autopipelines (both
// faces, ordered + unordered) and asserts goroutines return to baseline — i.e.
// flushers AND dispatcher workers all exit on Close, nothing leaks.
func TestAutoPipelineNoGoroutineLeak(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 50})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	cfgs := []*redis.AutoPipelineConfig{
		{MaxBatchSize: 300},                                                // ordered, 1 permit, 1 stripe
		{MaxBatchSize: 300, MaxConcurrentBatches: 8, Unordered: true},      // unordered, 8 permits, 8 stripes
		{MaxBatchSize: 300, MaxConcurrentBatches: 50, Unordered: true},     // many permits -> many workers
	}

	cycle := func() {
		for _, cfg := range cfgs {
			for _, async := range []bool{false, true} {
				var ap *redis.AutoPipeliner
				var err error
				if async {
					ap, err = client.AsyncAutoPipeline(cfg)
				} else {
					ap, err = client.AutoPipeline(cfg)
				}
				if err != nil {
					t.Fatal(err)
				}
				var wg = make([]redis.AutoFuture, 0, 200)
				for i := 0; i < 200; i++ {
					if async {
						wg = append(wg, ap.Submit(ctx, redis.NewCmd(ctx, "set", "leak:k", i)))
					} else {
						_ = ap.Set(ctx, "leak:k", i, 0).Err()
					}
				}
				for i := range wg {
					_ = wg[i].Wait()
				}
				if err := ap.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
		}
	}

	cycle() // warmup so lazy client/pool goroutines exist before we snapshot
	runtime.GC()
	time.Sleep(150 * time.Millisecond)
	base := runtime.NumGoroutine()

	const rounds = 40
	for r := 0; r < rounds; r++ {
		cycle()
	}
	runtime.GC()
	time.Sleep(300 * time.Millisecond) // let any exiting goroutines wind down
	got := runtime.NumGoroutine()

	// Allow small slack for pool/runtime background goroutines; a leak of
	// flushers/workers would be rounds*(shards+workers) ~ hundreds.
	if got > base+8 {
		t.Fatalf("goroutine leak: baseline=%d after %d open/close rounds=%d (delta %d)",
			base, rounds, got, got-base)
	}
	t.Logf("no leak: baseline=%d final=%d (delta %d) over %d rounds", base, got, got-base, rounds)
}
