package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Autopipeline + zero-copy GetToBuffer/SetFromBuffer, standalone, concurrent.
func TestAPZeroCopyBuffer(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 100, MaxConcurrentBatches: 30, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const g = 100
	const perG = 30
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	var errs []string
	wg.Add(g)
	for x := 0; x < g; x++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("buf:%d:%d", id, i)
				val := fmt.Sprintf("payload-%d-%d", id, i)
				// write from buffer via autopipeline
				if err := ap.SetFromBuffer(ctx, key, []byte(val)).Err(); err != nil {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, "set:"+err.Error())
					}
					mu.Unlock()
					continue
				}
				// read into caller buffer via autopipeline
				rbuf := make([]byte, len(val))
				cmd := ap.GetToBuffer(ctx, key, rbuf)
				n, err := cmd.Result()
				if err != nil {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, "get:"+err.Error())
					}
					mu.Unlock()
					continue
				}
				got := string(cmd.Bytes())
				if n != len(val) || got != val {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, fmt.Sprintf("key %s n=%d got=%q want=%q", key, n, got, val))
					}
					mu.Unlock()
				}
			}
		}(x)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("zero-copy buffer via autopipeline failed: %d cases; sample=%v", bad, errs)
	}
}
