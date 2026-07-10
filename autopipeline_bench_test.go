// Code consolidated from per-topic autopipeline test files.

package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ===== from autopipeline_bench_test.go =====
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
			Unordered:            true,
		},
	})
	defer client.Close()

	ap, err := client.AutoPipeline()
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i)
			_ = ap.Set(ctx, key, i, 0).Err()
			i++
		}
	})

	b.StopTimer()
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
			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < numCommands; i++ {
				key := fmt.Sprintf("key%d", i)
				_ = ap.Set(ctx, key, i, 0).Err()
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
					Unordered:            true,
				},
			})
			defer client.Close()

			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
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
						_ = ap.Set(ctx, key, i, 0).Err()
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
					Unordered:            true,
				},
			})
			defer client.Close()

			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
			defer ap.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key%d", i)
				_ = ap.Set(ctx, key, i, 0).Err()
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
					Unordered:            true,
				},
			})
			defer client.Close()

			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
			defer ap.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key%d", i)
				_ = ap.Set(ctx, key, i, 0).Err()
			}

			b.StopTimer()
			time.Sleep(100 * time.Millisecond)
		})
	}
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
					Unordered:            true,
				},
			})
			defer client.Close()

			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
			defer ap.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i)
					_ = ap.Set(ctx, key, i, 0).Err()
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
				Addr: ":6379",
				// The dedicated pipeline pool's buffers — what autopipeline
				// batches actually write/read through.
				PipelineReadBufferSize:  size,
				PipelineWriteBufferSize: size,
				AutoPipelineConfig: &redis.AutoPipelineConfig{
					MaxBatchSize:         50,
					MaxFlushDelay:        time.Millisecond,
					MaxConcurrentBatches: 10,
					Unordered:            true,
				},
			})
			defer client.Close()

			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
			defer ap.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i)
					_ = ap.Set(ctx, key, i, 0).Err()
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
					Unordered:            true,
				},
			})
			defer client.Close()

			ap, err := client.AutoPipeline()
			if err != nil {
				b.Fatal(err)
			}
			defer ap.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("key%d", i)
					_ = ap.Set(ctx, key, i, 0).Err()
					i++
				}
			})
		})
	}
}

// ===== from autopipeline_faces_bench_test.go =====
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
		fap, err := c.AsyncAutoPipeline() // windowed submit-then-read is the deferred pattern
		if err != nil {
			b.Fatal(err)
		}
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

// ===== from autopipeline_submit_bench_test.go =====
// BenchmarkAutoPipelineSubmit measures the non-blocking Submit path. Each
// goroutine submits a window of commands without blocking, then waits on them,
// so it pays one park/unpark round-trip per window instead of per command.
// This is the high-throughput entry point and reaches several times the
// throughput of the blocking Do/Set path under the same concurrency.
func BenchmarkAutoPipelineSubmit(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 250})
	defer client.Close()
	ap, err := client.AsyncAutoPipeline() // Submit is only allowed on the deferred face
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

// ===== from autopipeline_cluster_bench_test.go =====
// clusterBenchAddrs is the local osscluster (docker-compose `cluster` profile,
// formed with `redis-cli --cluster create ... --cluster-replicas 1`).
var clusterBenchAddrs = []string{"127.0.0.1:16600", "127.0.0.1:16601", "127.0.0.1:16602"}

func newClusterBenchClient(b *testing.B) *redis.ClusterClient {
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: clusterBenchAddrs, PoolSize: 500})
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		b.Skipf("cluster not reachable on %v: %v", clusterBenchAddrs, err)
	}
	return c
}

// BenchmarkClusterAutoPipelineThroughput measures executed throughput against a
// 3-master cluster. ClusterClient.AutoPipeline routes commands to shards by slot
// so each shard's batch stays on one node (keeping per-node pipelines deep), which
// is what lets a cluster scale past a single instance. As with the standalone
// benchmark, a command is counted only after its result is read.
func BenchmarkClusterAutoPipelineThroughput(b *testing.B) {
	const duration = 3 * time.Second

	b.Run("Blocking", func(b *testing.B) {
		c := newClusterBenchClient(b)
		defer c.Close()
		ap, err := c.AutoPipeline(&redis.AutoPipelineConfig{MaxBatchSize: 512, MaxConcurrentBatches: 200, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		const G = 16000
		var count int64
		dl := time.Now().Add(duration)
		ctx := context.Background()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func(id int) {
				defer wg.Done()
				i, key := 0, fmt.Sprintf("b:%d", id)
				for time.Now().Before(dl) {
					const run = 50
					for r := 0; r < run; r++ {
						i++
						ap.Set(ctx, key, i, 0).Err()
					}
					atomic.AddInt64(&count, run)
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	})

	b.Run("Windowed", func(b *testing.B) {
		c := newClusterBenchClient(b)
		defer c.Close()
		ap, err := c.AsyncAutoPipeline(&redis.AutoPipelineConfig{MaxBatchSize: 300, MaxConcurrentBatches: 96, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		const G, W = 1000, 300
		var count int64
		dl := time.Now().Add(duration)
		ctx := context.Background()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func(id int) {
				defer wg.Done()
				cmds := make([]*redis.StatusCmd, 0, W)
				for time.Now().Before(dl) {
					cmds = cmds[:0]
					for j := 0; j < W; j++ {
						cmds = append(cmds, ap.Set(ctx, fmt.Sprintf("w:%d:%d", id, j), j, 0))
					}
					for _, cm := range cmds {
						cm.Err()
					}
					atomic.AddInt64(&count, int64(W))
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	})
}

// ===== from autopipeline_dispatch_bench_test.go =====
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

// ===== from autopipeline_zerocopy_bench_test.go =====
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
						Unordered:            true,
					},
				})
				defer c.Close()
				c.FlushDB(ctx)
				ap, err := c.AutoPipeline()
				if err != nil {
					b.Fatal(err)
				}
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

// ===== from autopipeline_throughput_bench_test.go =====
// BenchmarkAutoPipelineThroughput compares executed-command throughput three
// ways. In every variant a command is counted ONLY after its result has been
// read (the command actually executed on the server) — there is no counting of
// merely-queued commands, so the reported ops/sec is real throughput.
//
//  1. Normal — a plain client. Each Set is a blocking round-trip; throughput is
//     bounded by the connection pool and Redis's non-pipelined ceiling (~100k
//     SET/sec, matching redis-benchmark without -P).
//  2. AutoPipelineBlocking — ap.Set(...).Result() read immediately, the way the
//     normal client is used (drop-in, one command in flight per caller). Even
//     so, the flusher batches across the many concurrent callers into deep
//     pipelines, so with parallel batches it clears ~1M executed SET/sec.
//  3. AutoPipelineWindowed — submit a window of commands per caller, then read
//     their results. Keeps each pipeline deepest; the high-throughput usage,
//     reaching a few million ops/sec.
//
// The autopipeline variants use a parallel-batch config (MaxConcurrentBatches>1,
// Unordered) — that is what lets blocking usage exceed 1M. The default ordered
// config (MaxConcurrentBatches=1) serializes batch execution and caps blocking
// usage near ~500k; windowed still reaches a few million ordered.
//
// Run: go test -run '^$' -bench BenchmarkAutoPipelineThroughput -benchtime=1x
func BenchmarkAutoPipelineThroughput(b *testing.B) {
	const (
		duration   = 3 * time.Second
		goroutines = 2000
		window     = 200 // commands submitted before reading results (windowed variant)
	)

	// apConfig is a parallel-batch config: many batches execute concurrently so
	// blocking callers don't serialize behind a single flusher. Unordered is
	// required for MaxConcurrentBatches>1.
	apConfig := func() *redis.AutoPipelineConfig {
		return &redis.AutoPipelineConfig{MaxBatchSize: 300, MaxConcurrentBatches: 80, Unordered: true}
	}

	// drive runs `fn` on `goroutines` goroutines until the deadline and reports
	// executed ops/sec. fn returns how many executed commands it performed in one
	// iteration (after reading their results); it must not count un-read commands.
	drive := func(b *testing.B, fn func(id int) int) {
		var count int64
		deadline := time.Now().Add(duration)
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(id int) {
				defer wg.Done()
				for time.Now().Before(deadline) {
					atomic.AddInt64(&count, int64(fn(id)))
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	}

	ctx := context.Background()

	b.Run("Normal", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 100})
		defer c.Close()
		i := 0
		drive(b, func(id int) int {
			const run = 50 // amortize the harness per-step cost; each Set still blocks
			for k := 0; k < run; k++ {
				i++
				if err := c.Set(ctx, fmt.Sprintf("n:%d", id), i, 0).Err(); err != nil {
					b.Error(err)
				}
			}
			return run
		})
	})

	b.Run("AutoPipelineBlocking", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379"})
		defer c.Close()
		ap, err := c.AutoPipeline() // blocking face: ap.Set blocks until executed (parallel-batch default)
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		i := 0
		drive(b, func(id int) int {
			// Each command call blocks until executed (drop-in shape, no .Result()
			// needed). We issue a small run per drive() step so the harness's
			// per-step atomic/closure cost is amortized and doesn't understate the
			// command rate — every command here still fully executes and is counted.
			const run = 50
			for k := 0; k < run; k++ {
				i++
				if err := ap.Set(ctx, fmt.Sprintf("b:%d", id), i, 0).Err(); err != nil {
					b.Error(err)
				}
			}
			return run
		})
	})

	b.Run("AutoPipelineWindowed", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379"})
		defer c.Close()
		ap, err := c.AsyncAutoPipeline(apConfig()) // deferred face: submit a window, read later
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		drive(b, func(id int) int {
			cmds := make([]*redis.StatusCmd, 0, window)
			for j := 0; j < window; j++ {
				cmds = append(cmds, ap.Set(ctx, fmt.Sprintf("w:%d", id), j, 0)) // does not block
			}
			n := 0
			for _, cmd := range cmds {
				if _, err := cmd.Result(); err != nil { // read result = executed
					b.Error(err)
				}
				n++
			}
			return n // only commands whose result was read
		})
	})

	// Windowed GET: same windowed-async pattern but read-only. SET throughput is
	// capped by Redis's write processing (~2.7M SET/sec on a typical local
	// instance, matching `redis-benchmark -t set` at its ceiling), so the SET
	// variants above are server-bound, not client-bound. GET is cheaper on the
	// server (~4M ceiling), so this variant shows the client itself clears 3M+
	// — i.e. the pipeline machinery is not the limit for the SET numbers.
	b.Run("AutoPipelineWindowedGET", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379"})
		defer c.Close()
		if err := c.Set(ctx, "bench:get", "v", 0).Err(); err != nil {
			b.Fatal(err)
		}
		ap, err := c.AsyncAutoPipeline(apConfig())
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		drive(b, func(id int) int {
			cmds := make([]*redis.StringCmd, 0, window)
			for j := 0; j < window; j++ {
				cmds = append(cmds, ap.Get(ctx, "bench:get"))
			}
			n := 0
			for _, cmd := range cmds {
				if _, err := cmd.Result(); err != nil {
					b.Error(err)
				}
				n++
			}
			return n
		})
	})
}
