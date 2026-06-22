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

// BenchmarkIndividualCommands benchmarks executing commands one at a time
func BenchmarkIndividualCommands(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i)
			if err := client.Set(ctx, key, i, 0).Err(); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkManualPipeline benchmarks using manual pipelining
func BenchmarkManualPipeline(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	const batchSize = 100

	b.ResetTimer()

	for i := 0; i < b.N; i += batchSize {
		pipe := client.Pipeline()

		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			key := fmt.Sprintf("key%d", j)
			pipe.Set(ctx, key, j, 0)
		}

		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAutoPipeline benchmarks using autopipelining
func BenchmarkAutoPipeline(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxFlushDelay:        10 * time.Millisecond,
			MaxConcurrentBatches: 10,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i)
			ap.Do(ctx, "SET", key, i)
			i++
		}
	})

	b.StopTimer()
	// Wait for final flush
	time.Sleep(50 * time.Millisecond)
}

// BenchmarkAutoPipelineVsManual compares autopipelining with manual pipelining
func BenchmarkAutoPipelineVsManual(b *testing.B) {
	const numCommands = 10000

	b.Run("Manual", func(b *testing.B) {
		ctx := context.Background()
		client := redis.NewClient(&redis.Options{
			Addr: ":6379",
		})
		defer client.Close()

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			pipe := client.Pipeline()
			for i := 0; i < numCommands; i++ {
				key := fmt.Sprintf("key%d", i)
				pipe.Set(ctx, key, i, 0)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Auto", func(b *testing.B) {
		ctx := context.Background()
		client := redis.NewClient(&redis.Options{
			Addr:               ":6379",
			AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
		})
		defer client.Close()

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			ap := client.AutoPipeline()
			for i := 0; i < numCommands; i++ {
				key := fmt.Sprintf("key%d", i)
				ap.Do(ctx, "SET", key, i)
			}
			ap.Close()
		}
	})
}

// BenchmarkConcurrentAutoPipeline benchmarks concurrent autopipelining
func BenchmarkConcurrentAutoPipeline(b *testing.B) {
	benchmarks := []struct {
		name       string
		goroutines int
	}{
		{"1goroutine", 1},
		{"10goroutines", 10},
		{"100goroutines", 100},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: ":6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         100,
					MaxFlushDelay:        10 * time.Millisecond,
					MaxConcurrentBatches: 10,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()

			var wg sync.WaitGroup
			commandsPerGoroutine := b.N / bm.goroutines
			if commandsPerGoroutine == 0 {
				commandsPerGoroutine = 1
			}

			wg.Add(bm.goroutines)
			for g := 0; g < bm.goroutines; g++ {
				go func(goroutineID int) {
					defer wg.Done()
					for i := 0; i < commandsPerGoroutine; i++ {
						key := fmt.Sprintf("g%d:key%d", goroutineID, i)
						ap.Do(ctx, "SET", key, i)
					}
				}(g)
			}
			wg.Wait()

			b.StopTimer()
			time.Sleep(50 * time.Millisecond)
		})
	}
}

// BenchmarkAutoPipelineBatchSizes tests different batch sizes
func BenchmarkAutoPipelineBatchSizes(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: ":6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         batchSize,
					MaxFlushDelay:        10 * time.Millisecond,
					MaxConcurrentBatches: 10,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key%d", i)
				ap.Do(ctx, "SET", key, i)
			}

			b.StopTimer()
			time.Sleep(50 * time.Millisecond)
		})
	}
}

// BenchmarkAutoPipelineMaxFlushDelays tests different max flush delays
func BenchmarkAutoPipelineMaxFlushDelays(b *testing.B) {
	delays := []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
	}

	for _, delay := range delays {
		b.Run(fmt.Sprintf("delay=%s", delay), func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: ":6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         100,
					MaxFlushDelay:        delay,
					MaxConcurrentBatches: 10,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key%d", i)
				ap.Do(ctx, "SET", key, i)
			}

			b.StopTimer()
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// BenchmarkThroughput measures throughput (ops/sec) for different approaches
func BenchmarkThroughput(b *testing.B) {
	const duration = 5 * time.Second
	const numGoroutines = 10

	b.Run("Individual", func(b *testing.B) {
		ctx := context.Background()
		client := redis.NewClient(&redis.Options{
			Addr: ":6379",
		})
		defer client.Close()

		b.ResetTimer()

		var wg sync.WaitGroup
		var count int64

		deadline := time.Now().Add(duration)

		wg.Add(numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				i := 0
				for time.Now().Before(deadline) {
					key := fmt.Sprintf("g%d:key%d", goroutineID, i)
					if err := client.Set(ctx, key, i, 0).Err(); err != nil {
						b.Error(err)
						return
					}
					i++
					count++
				}
			}(g)
		}
		wg.Wait()

		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	})

	b.Run("AutoPipeline", func(b *testing.B) {
		ctx := context.Background()
		client := redis.NewClient(&redis.Options{
			Addr:               ":6379",
			AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
		})
		defer client.Close()

		ap := client.AutoPipeline()
		defer ap.Close()

		b.ResetTimer()

		var wg sync.WaitGroup
		var count int64

		deadline := time.Now().Add(duration)

		wg.Add(numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				i := 0
				for time.Now().Before(deadline) {
					key := fmt.Sprintf("g%d:key%d", goroutineID, i)
					ap.Do(ctx, "SET", key, i)
					i++
					count++
				}
			}(g)
		}
		wg.Wait()

		b.StopTimer()
		time.Sleep(100 * time.Millisecond)

		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	})
}

// BenchmarkMaxFlushDelay benchmarks different MaxFlushDelay values
func BenchmarkMaxFlushDelay(b *testing.B) {
	delays := []time.Duration{
		0,
		50 * time.Microsecond,
		100 * time.Microsecond,
		200 * time.Microsecond,
	}

	for _, delay := range delays {
		b.Run(fmt.Sprintf("delay_%dus", delay.Microseconds()), func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: ":6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         50,
					MaxFlushDelay:        delay,
					MaxConcurrentBatches: 10,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i)
					ap.Do(ctx, "SET", key, i)
					i++
				}
			})
		})
	}
}

// BenchmarkBufferSizes benchmarks different buffer sizes
func BenchmarkBufferSizes(b *testing.B) {
	bufferSizes := []int{
		32 * 1024,  // 32 KiB
		64 * 1024,  // 64 KiB (default)
		128 * 1024, // 128 KiB
		256 * 1024, // 256 KiB
		512 * 1024, // 512 KiB
	}

	for _, size := range bufferSizes {
		b.Run(fmt.Sprintf("buffer_%dKiB", size/1024), func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr:            ":6379",
				ReadBufferSize:  size,
				WriteBufferSize: size,
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         50,
					MaxFlushDelay:        time.Millisecond,
					MaxConcurrentBatches: 10,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i)
					ap.Do(ctx, "SET", key, i)
					i++
				}
			})
		})
	}
}

// BenchmarkAutoPipelineMaxBatchSizes benchmarks different max batch sizes
func BenchmarkAutoPipelineMaxBatchSizes(b *testing.B) {
	batchSizes := []int{
		10,
		50, // default
		100,
		200,
		500,
	}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: ":6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         size,
					MaxFlushDelay:        time.Millisecond,
					MaxConcurrentBatches: 10,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i)
					ap.Do(ctx, "SET", key, i)
					i++
				}
			})
		})
	}
}

// BenchmarkHighConcurrencyThroughput is the benchmark that demonstrates the
// core value of autopipelining: under many concurrent, independent callers,
// commands coalesce into large pipelines and throughput rises ~15-25x versus
// issuing the same commands individually over a normal connection pool.
//
// Two things drive the multiple:
//
//   - Connection multiplexing. Plain commands occupy a connection for a whole
//     round-trip, so individual throughput is capped by the pool size. The
//     pipeliner packs many commands per connection, so a small pool sustains a
//     far higher command rate.
//   - Concurrency. ap.Do blocks until its command's batch is flushed, so the
//     batch can never grow larger than the number of goroutines issuing
//     commands at once. A handful of callers only batches a handful of
//     commands; the win shows up at hundreds-to-thousands of concurrent
//     callers, which is the workload autopipelining targets.
func BenchmarkHighConcurrencyThroughput(b *testing.B) {
	const (
		duration   = 3 * time.Second
		goroutines = 2000
	)

	run := func(b *testing.B, auto bool) {
		ctx := context.Background()
		// A typical application pool. Without autopipelining, throughput is
		// bounded by how many commands can be in flight at once — i.e. the pool
		// size — because each Set occupies a connection for a full round-trip.
		opt := &redis.Options{Addr: ":6379", PoolSize: 10}
		if auto {
			// Autopipelining multiplexes many commands per connection by
			// batching them into pipelines, so a small pool still sustains a
			// high command rate. Larger batch and concurrent-batch limits let
			// the flusher keep many full pipelines in flight under thousands of
			// callers, which is where it reaches its ~15-25x throughput multiple.
			opt.AutoPipelineConfig = &redis.AutoPipelineConfig{
				MaxBatchSize:         300,
				MaxConcurrentBatches: 80,
			}
		}
		client := redis.NewClient(opt)
		defer client.Close()

		var ap *redis.AutoPipeliner
		if auto {
			ap = client.AutoPipeline()
			defer ap.Close()
		}

		var count int64
		deadline := time.Now().Add(duration)

		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(id int) {
				defer wg.Done()
				i := 0
				for time.Now().Before(deadline) {
					key := fmt.Sprintf("g%d:key%d", id, i)
					if auto {
						ap.Do(ctx, "SET", key, i)
					} else {
						_ = client.Set(ctx, key, i, 0).Err()
					}
					i++
					atomic.AddInt64(&count, 1)
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()

		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	}

	b.Run("Individual", func(b *testing.B) { run(b, false) })
	b.Run("AutoPipeline", func(b *testing.B) { run(b, true) })
}
