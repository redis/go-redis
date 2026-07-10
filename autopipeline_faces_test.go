package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestFutureFaceTyped verifies the future face exposes the typed command
// surface returning the usual *XxxCmd (same shape as the blocking client),
// with the result deferred until read. No cross-talk across goroutines.
func TestFutureFaceTyped(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	fap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fap.Close()

	const G, N = 100, 50
	var wg sync.WaitGroup
	var bad int64
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			sets := make([]*redis.StatusCmd, N)
			for i := 0; i < N; i++ {
				sets[i] = fap.Set(ctx, fmt.Sprintf("ff:%d:%d", id, i), fmt.Sprintf("v%d-%d", id, i), 0)
			}
			for _, s := range sets {
				if s.Err() != nil {
					atomic.AddInt64(&bad, 1)
				}
			}
			for i := 0; i < N; i++ {
				want := fmt.Sprintf("v%d-%d", id, i)
				if v, _ := fap.Get(ctx, fmt.Sprintf("ff:%d:%d", id, i)).Result(); v != want {
					atomic.AddInt64(&bad, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("future face: %d failures", bad)
	}
}

// TestOrderedModeWindowed verifies that MaxConcurrentBatches=1 gives a single
// ordered command stream: a windowed caller (submit many, read later) sees
// strict per-key ordering even though it never blocks between submits.
func TestOrderedModeWindowed(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	// Deferred face: submits genuinely don't block, so the windowed claim is
	// real (on the blocking face every Incr would wait, proving nothing).
	fap, err := c.AsyncAutoPipeline(&redis.AutoPipelineConfig{MaxBatchSize: 500, MaxConcurrentBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer fap.Close()

	const G, N = 80, 150
	var wg sync.WaitGroup
	var bad int64
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("om:%d", id)
			cs := make([]*redis.IntCmd, 0, N)
			for i := 0; i < N; i++ {
				cs = append(cs, fap.Incr(ctx, key)) // windowed: no read between submits
			}
			for i, cc := range cs {
				if v, err := cc.Result(); err != nil || v != int64(i+1) {
					atomic.AddInt64(&bad, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("ordered windowed: %d ordering violations", bad)
	}
}

// TestAutoPipelineConfigValidate verifies the ordering-safety rule: parallel
// batches (MaxConcurrentBatches>1) require an explicit Unordered:true opt-out.
func TestAutoPipelineConfigValidate(t *testing.T) {
	// conc>1 without Unordered -> error.
	if err := (&redis.AutoPipelineConfig{MaxConcurrentBatches: 8}).Validate(); err == nil {
		t.Fatal("expected error for MaxConcurrentBatches>1 without Unordered")
	}
	// conc>1 with Unordered -> ok.
	if err := (&redis.AutoPipelineConfig{MaxConcurrentBatches: 8, Unordered: true}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ordered single stream -> ok.
	if err := (&redis.AutoPipelineConfig{MaxConcurrentBatches: 1}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// default is ordered and valid.
	if err := redis.DefaultAutoPipelineConfig().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	// negative values are rejected so a typo surfaces at construction.
	if err := (&redis.AutoPipelineConfig{MaxBatchSize: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative MaxBatchSize")
	}
	if err := (&redis.AutoPipelineConfig{MaxConcurrentBatches: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative MaxConcurrentBatches")
	}
	if err := (&redis.AutoPipelineConfig{MaxFlushDelay: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative MaxFlushDelay")
	}
	// zero values are allowed (mean "use default" / "no delay").
	if err := (&redis.AutoPipelineConfig{}).Validate(); err != nil {
		t.Fatalf("zero-value config should be valid: %v", err)
	}
}

// TestAutoPipelineErrorsOnUnsafeConfig verifies the unsafe config is rejected
// with an error (not a panic): AsyncAutoPipeline runs from a post-init call, so a
// bad config surfaces as a returned error the caller can handle.
func TestAutoPipelineErrorsOnUnsafeConfig(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	ap, err := c.AsyncAutoPipeline(&redis.AutoPipelineConfig{MaxConcurrentBatches: 8})
	if err == nil {
		t.Fatal("expected error for MaxConcurrentBatches>1 without Unordered")
	}
	if ap != nil {
		t.Fatal("expected nil AutoPipeliner on config error")
	}
}
