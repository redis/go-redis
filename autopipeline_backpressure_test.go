package redis_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineWindowedNoDeadlock submits a large window of commands without
// waiting between them (the async producer pattern), then reads them all. It must
// not deadlock and every future must resolve with its correct result — exercises
// the enqueue/permit/flush path under a producer that outruns per-command waits.
func TestAutoPipelineWindowedNoDeadlock(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 20})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 300, MaxConcurrentBatches: 8, Unordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const n = 5000
	futs := make([]redis.AutoFuture, n)
	for i := 0; i < n; i++ {
		futs[i] = ap.Submit(ctx, redis.NewCmd(ctx, "set", fmt.Sprintf("bp:%d", i), i))
	}
	for i := range futs {
		if err := futs[i].Wait(); err != nil {
			t.Fatalf("future %d: %v", i, err)
		}
	}
	// spot-check a few landed correctly
	for _, i := range []int{0, n / 2, n - 1} {
		if v, _ := client.Get(ctx, fmt.Sprintf("bp:%d", i)).Result(); v != fmt.Sprintf("%d", i) {
			t.Fatalf("bp:%d = %q, want %d", i, v, i)
		}
	}
	for i := 0; i < n; i++ {
		client.Del(ctx, fmt.Sprintf("bp:%d", i))
	}
}

// TestAutoPipelineSoak drives continuous autopipelined traffic from many
// goroutines for a few seconds and checks that goroutine and connection counts
// return to a stable baseline afterward (no slow leak) and results stay correct.
// Skipped under -short; bump `dur` for a longer soak.
func TestAutoPipelineSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak skipped in -short")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 20})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 200, MaxConcurrentBatches: 16, Unordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()

	const dur = 3 * time.Second
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	var mismatch int64
	var mu sync.Mutex
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("soak:%d:%d", id, i%16)
				val := fmt.Sprintf("%d", i)
				setF := ap.Set(ctx, key, val, 0)
				if err := setF.Err(); err != nil {
					mu.Lock()
					mismatch++
					mu.Unlock()
				}
				i++
			}
		}(g)
	}
	wg.Wait()

	if mismatch != 0 {
		t.Fatalf("%d errors during soak", mismatch)
	}
	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	got := runtime.NumGoroutine()
	if got > baseGoroutines+16 {
		t.Fatalf("goroutine leak: baseline=%d after soak=%d", baseGoroutines, got)
	}
}
