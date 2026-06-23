package redis_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkFutureFace measures the uniform typed future face two ways:
//   - InOrder: fap.Set(...).Err() per command (blocking-like ergonomics).
//   - Window: submit a window of typed commands, then read them (max throughput).
//
// Both use the same fap.Set typed method; only when the result is read differs.
func BenchmarkFutureFace(b *testing.B) {
	run := func(b *testing.B, window int) {
		ctx := context.Background()
		c := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 250})
		defer c.Close()
		fap := c.AutoPipeline()
		defer fap.Close()

		const duration = 3 * time.Second
		goroutines := 2000
		if window > 1 {
			goroutines = 500
		}
		var count int64
		deadline := time.Now().Add(duration)

		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(id int) {
				defer wg.Done()
				key := "k" + strconv.Itoa(id)
				cs := make([]*redis.StatusCmd, 0, window)
				for time.Now().Before(deadline) {
					cs = cs[:0]
					for j := 0; j < window; j++ {
						cs = append(cs, fap.Set(ctx, key, j, 0))
					}
					for _, cc := range cs {
						_ = cc.Err()
					}
					atomic.AddInt64(&count, int64(window))
				}
			}(g)
		}
		wg.Wait()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	}

	b.Run("InOrder", func(b *testing.B) { run(b, 1) })
	b.Run("Window200", func(b *testing.B) { run(b, 200) })
}
