package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Validates: each command gets its OWN result (no cross-talk), under heavy
// concurrency. Each goroutine writes a unique value, reads it back, asserts it
// got exactly what it wrote.
func TestAPNoCrossTalk(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:               ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{MaxBatchSize: 200, MaxConcurrentBatches: 50, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 200
	const perG = 50
	var wg sync.WaitGroup
	var mismatches int64
	var mu sync.Mutex
	errs := []string{}

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("ct:%d:%d", id, i)
				want := fmt.Sprintf("v-%d-%d", id, i)
				// SET then GET via autopipeline; assert GET returns our value.
				if err := ap.Set(ctx, key, want, 0).Err(); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("set %s: %v", key, err))
					mu.Unlock()
					continue
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("get %s: %v", key, err))
					mu.Unlock()
					continue
				}
				if got != want {
					mu.Lock()
					mismatches++
					if len(errs) < 20 {
						errs = append(errs, fmt.Sprintf("key %s: got %q want %q", key, got, want))
					}
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if mismatches > 0 || len(errs) > 0 {
		t.Fatalf("cross-talk detected: %d mismatches, errs=%v", mismatches, errs)
	}
}

// Validates: per-goroutine ordering. Each goroutine does INCR on its own key N
// times; final value must equal N and intermediate results must be strictly
// 1,2,3,... in call order (blocking Do guarantees this only if results map to
// the right command).
func TestAPPerGoroutineOrder(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:               ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{MaxBatchSize: 100, MaxConcurrentBatches: 30, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 100
	const incrs = 100
	var wg sync.WaitGroup
	var bad int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("ord:%d", id)
			for i := 1; i <= incrs; i++ {
				v, err := ap.Incr(ctx, key).Result()
				if err != nil || v != int64(i) {
					atomicAdd(&bad)
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("ordering violated on %d INCR results (expected strict 1..N per goroutine)", bad)
	}
}

// Validates: no lost commands. Issue exactly N writes across goroutines, then
// count keys; must be exactly N.
func TestAPNoLostCommands(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:               ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{MaxBatchSize: 256, MaxConcurrentBatches: 64, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 300
	const perG = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				ap.Set(ctx, fmt.Sprintf("lost:%d:%d", id, i), "1", 0)
			}
		}(g)
	}
	wg.Wait()

	n, err := c.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	want := int64(goroutines * perG)
	if n != want {
		t.Fatalf("lost commands: DBSize=%d want=%d (%d missing)", n, want, want-n)
	}
}

// Validates: error isolation. A WRONGTYPE error on one command must not
// corrupt sibling commands sharing the same batch.
func TestAPErrorIsolation(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:               ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{MaxBatchSize: 64, MaxConcurrentBatches: 16, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	// Seed a list key so INCR on it errors with WRONGTYPE.
	c.RPush(ctx, "alist", "x")
	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 50
	var wg sync.WaitGroup
	var goodFail, badNeighbor int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			// Good neighbor command (own key)
			ok := fmt.Sprintf("iso:%d", id)
			// Interleave a guaranteed-error command (INCR on a list) with a good one.
			errCmd := ap.Incr(ctx, "alist")
			goodCmd := ap.Set(ctx, ok, strconv.Itoa(id), 0)
			if errCmd.Err() == nil {
				atomicAdd(&badNeighbor) // the bad command should have errored
			} else {
				atomicAdd(&goodFail)
			}
			if goodCmd.Err() != nil {
				atomicAdd(&badNeighbor) // the good command must NOT inherit the error
			}
		}(g)
	}
	wg.Wait()
	if badNeighbor > 0 {
		t.Fatalf("error isolation broken: %d cases (good cmd errored or bad cmd succeeded)", badNeighbor)
	}
	if goodFail != goroutines {
		t.Fatalf("expected all %d INCR-on-list to error, got %d", goroutines, goodFail)
	}
}

func atomicAdd(p *int64) {
	atomic.AddInt64(p, 1)
}
