package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// BenchmarkIndividualCommands benchmarks individual Redis commands
func BenchmarkIndividualCommands(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:individual:%d", i)
			client.Set(ctx, key, i, 0)
			i++
		}
	})
}

// BenchmarkManualPipeline benchmarks manual pipeline usage
func BenchmarkManualPipeline(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	const batchSize = 100

	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		pipe := client.Pipeline()
		for j := 0; j < batchSize && i+j < b.N; j++ {
			key := fmt.Sprintf("bench:manual:%d", i+j)
			pipe.Set(ctx, key, i+j, 0)
		}
		pipe.Exec(ctx)
	}
}

// BenchmarkAutoPipelineDefault benchmarks AutoPipeline with default config
func BenchmarkAutoPipelineDefault(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:               "localhost:6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:auto-default:%d", i)
			ap.Do(ctx, "SET", key, i)
			i++
		}
	})
}

// BenchmarkAutoPipelineTuned benchmarks AutoPipeline with tuned config
func BenchmarkAutoPipelineTuned(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:             "localhost:6379",
		PipelinePoolSize: 50,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         500,
			MaxConcurrentBatches: 100,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:auto-tuned:%d", i)
			ap.Do(ctx, "SET", key, i)
			i++
		}
	})
}

// BenchmarkAutoPipelineHighThroughput benchmarks AutoPipeline optimized for throughput
func BenchmarkAutoPipelineHighThroughput(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:                    "localhost:6379",
		PipelinePoolSize:        100,
		PipelineReadBufferSize:  1024 * 1024,
		PipelineWriteBufferSize: 1024 * 1024,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         2000,
			MaxConcurrentBatches: 200,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench:auto-throughput:%d", i)
			ap.Do(ctx, "SET", key, i)
			i++
		}
	})
}

// BenchmarkConcurrentWorkload benchmarks realistic concurrent workload
func BenchmarkConcurrentWorkload(b *testing.B) {
	ctx := context.Background()

	configs := []struct {
		name   string
		client *redis.Client
		usePipeline bool
	}{
		{
			name: "Individual",
			client: redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			}),
			usePipeline: false,
		},
		{
			name: "AutoPipeline-Default",
			client: redis.NewClient(&redis.Options{
				Addr:               "localhost:6379",
				AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
			}),
			usePipeline: true,
		},
		{
			name: "AutoPipeline-Tuned",
			client: redis.NewClient(&redis.Options{
				Addr:             "localhost:6379",
				PipelinePoolSize: 50,
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         500,
					MaxConcurrentBatches: 100,
				},
			}),
			usePipeline: true,
		},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			defer cfg.client.Close()

			var ap *redis.AutoPipeliner
			if cfg.usePipeline {
				ap = cfg.client.AutoPipeline()
				defer ap.Close()
			}

			const numWorkers = 100
			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerWorker := b.N / numWorkers

			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for i := 0; i < opsPerWorker; i++ {
						key := fmt.Sprintf("bench:concurrent:%s:%d:%d", cfg.name, workerID, i)
						if cfg.usePipeline {
							ap.Do(ctx, "SET", key, i)
						} else {
							cfg.client.Set(ctx, key, i, 0)
						}
					}
				}(w)
			}
			wg.Wait()
		})
	}
}

// BenchmarkWebServerSimulation benchmarks web server-like workload
func BenchmarkWebServerSimulation(b *testing.B) {
	ctx := context.Background()

	configs := []struct {
		name   string
		client *redis.Client
		usePipeline bool
	}{
		{
			name: "Individual",
			client: redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			}),
			usePipeline: false,
		},
		{
			name: "AutoPipeline",
			client: redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         200,
					MaxConcurrentBatches: 100,
				},
			}),
			usePipeline: true,
		},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			defer cfg.client.Close()

			var ap *redis.AutoPipeliner
			if cfg.usePipeline {
				ap = cfg.client.AutoPipeline()
				defer ap.Close()
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					// Simulate web request: increment counter + set session
					if cfg.usePipeline {
						ap.Incr(ctx, "requests:total")
						ap.Set(ctx, fmt.Sprintf("session:%d", i), fmt.Sprintf("data-%d", i), 0)
					} else {
						cfg.client.Incr(ctx, "requests:total")
						cfg.client.Set(ctx, fmt.Sprintf("session:%d", i), fmt.Sprintf("data-%d", i), 0)
					}
					i++
				}
			})
		})
	}
}

// BenchmarkBatchSizes benchmarks different batch sizes
func BenchmarkBatchSizes(b *testing.B) {
	ctx := context.Background()
	batchSizes := []int{10, 50, 100, 200, 500, 1000, 2000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", batchSize), func(b *testing.B) {
			client := redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         batchSize,
					MaxConcurrentBatches: 50,
				},
			})
			defer client.Close()

			ap := client.AutoPipeline()
			defer ap.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("bench:batch-%d:%d", batchSize, i)
					ap.Do(ctx, "SET", key, i)
					i++
				}
			})
		})
	}
}

