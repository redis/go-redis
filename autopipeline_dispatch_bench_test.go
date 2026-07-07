package redis_test

// Focused A/B bench for the flush/dispatch path work: per-command ns/op and
// allocs/op through the real engine against a local Redis. Unlike the
// throughput bench (wall-clock ops/sec, 2000 goroutines) this counts b.N
// commands exactly, so benchstat can compare steps precisely.
//
//   - ordered:    default async face (MaxConcurrentBatches=1, one stripe).
//   - unordered:  parallel batches (8 permits, striped enqueue).
//   - solo:       1 goroutine, blocking face — the lone-caller path
//     (single-command fast path + dispatch spawn) dominated by RTT; allocs/op
//     still shows the dispatch overhead.
//
// Run: go test -run '^$' -bench BenchmarkDispatchPath -benchmem .

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func benchWindowed(b *testing.B, cfg *redis.AutoPipelineConfig, goroutines, window int) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 100})
	defer client.Close()
	ap, err := client.AsyncAutoPipeline(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()

	// one warmup command so pools/conns exist
	if err := ap.Set(ctx, "dispatch:warm", 1, 0).Err(); err != nil {
		b.Fatal(err)
	}

	per := b.N / goroutines
	if per == 0 {
		per = 1
	}
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			futs := make([]redis.AutoFuture, 0, window)
			cmds := 0
			for cmds < per {
				futs = futs[:0]
				n := window
				if rem := per - cmds; rem < n {
					n = rem
				}
				for k := 0; k < n; k++ {
					futs = append(futs, ap.Submit(ctx, redis.NewCmd(ctx, "set", "dispatch:k", k)))
				}
				for i := range futs {
					_ = futs[i].Wait()
				}
				cmds += n
			}
		}(g)
	}
	wg.Wait()
	b.StopTimer()
}

func BenchmarkDispatchPath(b *testing.B) {
	b.Run("ordered/g64_w100", func(b *testing.B) {
		benchWindowed(b, &redis.AutoPipelineConfig{MaxBatchSize: 300}, 64, 100)
	})
	b.Run("unordered/g64_w100", func(b *testing.B) {
		benchWindowed(b, &redis.AutoPipelineConfig{
			MaxBatchSize: 300, MaxConcurrentBatches: 8, Unordered: true,
		}, 64, 100)
	})
	b.Run("solo/blocking", func(b *testing.B) {
		ctx := context.Background()
		client := redis.NewClient(&redis.Options{Addr: ":6379"})
		defer client.Close()
		ap, err := client.AutoPipeline(nil)
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		if err := ap.Set(ctx, "dispatch:warm", 1, 0).Err(); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ap.Set(ctx, "dispatch:k", i, 0).Err()
		}
		b.StopTimer()
	})
}
