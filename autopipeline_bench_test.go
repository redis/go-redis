package redis_test

import (
	"context"
	"fmt"
	"sync"
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
			FlushInterval:        10 * time.Millisecond,
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
			Addr: ":6379",
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
					FlushInterval:        10 * time.Millisecond,
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
					FlushInterval:        10 * time.Millisecond,
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

// BenchmarkAutoPipelineFlushIntervals tests different flush intervals
func BenchmarkAutoPipelineFlushIntervals(b *testing.B) {
	intervals := []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
	}

	for _, interval := range intervals {
		b.Run(fmt.Sprintf("interval=%s", interval), func(b *testing.B) {
			ctx := context.Background()
			client := redis.NewClient(&redis.Options{
				Addr: ":6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         100,
					FlushInterval:        interval,
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
			Addr: ":6379",
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

