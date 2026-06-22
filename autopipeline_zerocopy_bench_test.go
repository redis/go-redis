package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// BenchmarkAutoPipelineZeroCopy compares regular Get/Set against the zero-copy
// GetToBuffer/SetFromBuffer commands, both issued through the autopipeliner.
//
// Zero-copy wins on both axes when the read buffer has room for the trailing
// CRLF (cap >= n+2, as allocated here): B/op stays flat as the value grows
// (the payload is never allocated — it is decoded straight into the caller's
// buffer), and for large values ops/sec is HIGHER than a regular Get because
// ReadStringInto reads payload+CRLF in a single socket read while Get
// allocates a fresh string each call. Locally at 64 KiB the fast path is
// ~2x the throughput of Get at ~1% of the allocations. For small values the
// two are close on throughput; zero-copy still allocates far less.
func BenchmarkAutoPipelineZeroCopy(b *testing.B) {
	sizes := []int{64, 4096, 65536}
	for _, sz := range sizes {
		for _, zc := range []bool{false, true} {
			name := fmt.Sprintf("size=%d/regular", sz)
			if zc {
				name = fmt.Sprintf("size=%d/zerocopy", sz)
			}
			b.Run(name, func(b *testing.B) {
				ctx := context.Background()
				c := redis.NewClient(&redis.Options{
					Addr:     ":6379",
					PoolSize: 10,
					AutoPipelineConfig: &redis.AutoPipelineConfig{
						MaxBatchSize:         300,
						MaxConcurrentBatches: 80,
					},
				})
				defer c.Close()
				c.FlushDB(ctx)
				ap := c.AutoPipeline()
				defer ap.Close()

				payload := make([]byte, sz)
				for i := range payload {
					payload[i] = 'x'
				}
				pstr := string(payload)

				const goroutines = 500
				per := b.N / goroutines
				if per == 0 {
					per = 1
				}
				var count int64

				b.ResetTimer()
				b.ReportAllocs()
				var wg sync.WaitGroup
				wg.Add(goroutines)
				for g := 0; g < goroutines; g++ {
					go func(id int) {
						defer wg.Done()
						// Allocate cap >= sz+2 so GetToBuffer takes the fast
						// path that reads payload+CRLF in a single socket read.
						// The slice handed to GetToBuffer is still len==sz.
						rbuf := make([]byte, sz, sz+2)
						for i := 0; i < per; i++ {
							key := fmt.Sprintf("zc:%d:%d", id, i)
							if zc {
								ap.SetFromBuffer(ctx, key, payload)
								_ = ap.GetToBuffer(ctx, key, rbuf)
							} else {
								ap.Set(ctx, key, pstr, 0)
								ap.Get(ctx, key)
							}
							atomic.AddInt64(&count, 2)
						}
					}(g)
				}
				wg.Wait()
				b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
			})
		}
	}
}
