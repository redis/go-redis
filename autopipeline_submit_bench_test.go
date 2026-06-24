package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkAutoPipelineSubmit measures the non-blocking Submit path. Each
// goroutine submits a window of commands without blocking, then waits on them,
// so it pays one park/unpark round-trip per window instead of per command.
// This is the high-throughput entry point and reaches several times the
// throughput of the blocking Do/Set path under the same concurrency.
func BenchmarkAutoPipelineSubmit(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 250})
	defer client.Close()
	ap, err := client.AutoPipeline()
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()

	const (
		duration   = 3 * time.Second
		goroutines = 500
		window     = 200
	)
	var count int64
	deadline := time.Now().Add(duration)

	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			futs := make([]redis.AutoFuture, 0, window)
			key := fmt.Sprintf("submit:%d", id)
			for time.Now().Before(deadline) {
				futs = futs[:0]
				for j := 0; j < window; j++ {
					futs = append(futs, ap.Submit(ctx, redis.NewStatusCmd(ctx, "set", key, j)))
				}
				for i := range futs {
					_ = futs[i].Wait()
				}
				atomic.AddInt64(&count, window)
			}
		}(g)
	}
	wg.Wait()
	b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
}
