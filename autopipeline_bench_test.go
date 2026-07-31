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

// benchAddr is the standalone server the benchmarks run against — the same
// server the docker-compose env exposes in CI (and `redis-benchmark`'s
// default), so numbers are comparable with the reference bands in
// autopipeline_bench_README.md.
const benchAddr = ":6379"

// skipWithoutBenchServer skips the benchmark when no server answers, instead
// of "passing" while measuring dial-failure latency.
func skipWithoutBenchServer(b *testing.B) {
	b.Helper()
	probe := redis.NewClient(&redis.Options{Addr: benchAddr})
	defer probe.Close()
	if err := probe.Ping(context.Background()).Err(); err != nil {
		b.Skipf("no redis at %s: %v", benchAddr, err)
	}
}

// BenchmarkIndividualCommands is the plain-client baseline: one blocking
// round-trip per command across GOMAXPROCS workers.
func BenchmarkIndividualCommands(b *testing.B) {
	skipWithoutBenchServer(b)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: benchAddr,
	})
	defer client.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i)
			if err := client.Set(ctx, key, i, 0).Err(); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

// BenchmarkManualPipeline benchmarks using manual pipelining. Deliberately
// sequential (one goroutine, one pipeline at a time): it isolates the
// per-batch cost of an explicit Pipeline().Exec() round-trip, so its numbers
// are not directly comparable to the parallel BenchmarkIndividualCommands
// above. For a like-for-like concurrent comparison of the client faces see
// BenchmarkAutoPipelineThroughput, whose variants all share one harness.
func BenchmarkManualPipeline(b *testing.B) {
	skipWithoutBenchServer(b)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: benchAddr,
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

// BenchmarkFutureFace measures the uniform typed future face two ways:
//   - InOrder: fap.Set(...).Err() per command (blocking-like ergonomics).
//   - Window: submit a window of typed commands, then read them (max throughput).
//
// Both use the same fap.Set typed method; only when the result is read differs.
// Fixed-duration driver: goroutines issue commands until the deadline and the
// metric divides by the TIMED region (which includes draining the windows
// already in flight at the deadline), so the reported ops/sec is honest.
// NOTE: fixed-duration drivers ignore b.N — run them with the default
// -benchtime or -benchtime=1x, not a time-based value (which would re-run the
// full window geometrically).
func BenchmarkFutureFace(b *testing.B) {
	run := func(b *testing.B, window int) {
		skipWithoutBenchServer(b)
		ctx := context.Background()
		c := redis.NewClient(&redis.Options{Addr: benchAddr, PoolSize: 250})
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
						if err := cc.Err(); err != nil {
							b.Error(err)
							return
						}
					}
					atomic.AddInt64(&count, int64(window))
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
	}

	b.Run("InOrder", func(b *testing.B) { run(b, 1) })
	b.Run("Window200", func(b *testing.B) { run(b, 200) })
}

// BenchmarkAutoPipelineSubmit measures the non-blocking Submit path. Each
// goroutine submits a window of commands without blocking, then waits on them,
// so it pays one park/unpark round-trip per window instead of per command.
// Fixed-duration driver; see the BenchmarkFutureFace note about -benchtime.
func BenchmarkAutoPipelineSubmit(b *testing.B) {
	skipWithoutBenchServer(b)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: benchAddr, PoolSize: 250})
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
					if err := futs[i].Wait(); err != nil {
						b.Error(err)
						return
					}
				}
				atomic.AddInt64(&count, window)
			}
		}(g)
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
}

// clusterBenchAddrs is the local osscluster (docker-compose `cluster` profile,
// formed with `redis-cli --cluster create ... --cluster-replicas 1`).
var clusterBenchAddrs = []string{"127.0.0.1:16600", "127.0.0.1:16601", "127.0.0.1:16602"}

func newClusterBenchClient(b *testing.B) *redis.ClusterClient {
	b.Helper()
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
// benchmark, a command is counted only after its result is read, and the metric
// divides by the timed region (including the post-deadline drain of windows
// already in flight), so the reported ops/sec is honest.
// Fixed-duration driver; see the BenchmarkFutureFace note about -benchtime.
func BenchmarkClusterAutoPipelineThroughput(b *testing.B) {
	const duration = 3 * time.Second

	b.Run("Blocking", func(b *testing.B) {
		c := newClusterBenchClient(b)
		defer c.Close()
		ap, err := c.AutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 512, MaxConcurrentBatches: 200, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		const G = 2000
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
						if err := ap.Set(ctx, key, i, 0).Err(); err != nil {
							b.Error(err)
							return
						}
					}
					atomic.AddInt64(&count, run)
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
	})

	b.Run("Windowed", func(b *testing.B) {
		c := newClusterBenchClient(b)
		defer c.Close()
		ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 300, MaxConcurrentBatches: 96, Unordered: true})
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
						if err := cm.Err(); err != nil {
							b.Error(err)
							return
						}
					}
					atomic.AddInt64(&count, int64(W))
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
	})
}

// benchWindowed measures the per-command dispatch cost of the deferred face
// with honest b.N accounting: ns/op and allocs/op are per executed command.
func benchWindowed(b *testing.B, cfg *redis.AutoPipelineOptions, goroutines, window int) {
	skipWithoutBenchServer(b)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: benchAddr, PoolSize: 100})
	defer client.Close()
	ap, err := client.AsyncAutoPipelineWithOptions(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()

	// one warmup command so pools/conns exist
	if err := ap.Set(ctx, "dispatch:warm", 1, 0).Err(); err != nil {
		b.Fatal(err)
	}

	// Exact b.N partitioning: the first b.N%goroutines workers run one extra
	// command, so the total is b.N and ns/op / allocs/op are per command at
	// any -benchtime (a floor-and-min-1 split would over-run for small b.N
	// and drop the remainder for large).
	base, extra := b.N/goroutines, b.N%goroutines
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			per := base
			if id < extra {
				per++
			}
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
					if err := futs[i].Wait(); err != nil {
						b.Error(err)
					}
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
		benchWindowed(b, &redis.AutoPipelineOptions{MaxBatchSize: 300}, 64, 100)
	})
	b.Run("unordered/g64_w100", func(b *testing.B) {
		benchWindowed(b, &redis.AutoPipelineOptions{
			MaxBatchSize: 300, MaxConcurrentBatches: 8, Unordered: true,
		}, 64, 100)
	})
	b.Run("solo/blocking", func(b *testing.B) {
		skipWithoutBenchServer(b)
		ctx := context.Background()
		client := redis.NewClient(&redis.Options{Addr: benchAddr})
		defer client.Close()
		ap, err := client.AutoPipeline()
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

// BenchmarkAutoPipelineZeroCopy compares regular Get/Set against the zero-copy
// GetToBuffer/SetFromBuffer commands, both issued through the autopipeliner.
// Each iteration is a Set+Get pair (so throughput mixes both directions).
//
// What zero-copy buys, per this benchmark's own output: B/op drops ~10x at
// 4KiB and ~90x at 64KiB (the payload is decoded into the caller's buffer
// instead of a freshly allocated string; allocs/op only goes 11 -> 10), with
// throughput at parity (the fast path reads payload+CRLF in a single socket
// read when the buffer has len >= n+2, as allocated here). The per-goroutine
// read buffers are allocated OUTSIDE the timed region so B/op reflects the
// command path, not the harness.
func BenchmarkAutoPipelineZeroCopy(b *testing.B) {
	sizes := []int{64, 4096, 65536}
	for _, sz := range sizes {
		for _, zc := range []bool{false, true} {
			name := fmt.Sprintf("size=%d/regular", sz)
			if zc {
				name = fmt.Sprintf("size=%d/zerocopy", sz)
			}
			b.Run(name, func(b *testing.B) {
				skipWithoutBenchServer(b)
				ctx := context.Background()
				c := redis.NewClient(&redis.Options{
					Addr:     benchAddr,
					PoolSize: 10,
					// Generous deadlines: at 64KiB payloads a slow CI runner
					// pushes gigabytes through few conns; the default 3s
					// write deadline trips under that load and the bench
					// (correctly) fails on the surfaced error.
					ReadTimeout:  30 * time.Second,
					WriteTimeout: 30 * time.Second,
					AutoPipelineOptions: &redis.AutoPipelineOptions{
						MaxBatchSize:         300,
						MaxConcurrentBatches: 80,
						Unordered:            true,
						// Bound each pipeline write burst: 300 x 64KiB in one
						// batch is ~19MB down a single conn before any reply
						// is read — enough to stall a constrained CI runner
						// past its write deadline.
						MaxBatchBytes: 2 << 20,
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

				// Fewer workers at large payloads: 500 goroutines x 64KiB
				// saturates a CI runner's socket budget and only measures
				// queueing on the write deadline.
				goroutines := 500
				if sz >= 65536 {
					goroutines = 100
				}
				// Exact b.N partitioning (see benchWindowed): each unit is
				// one Set+Get pair.
				base, extra := b.N/goroutines, b.N%goroutines
				var count int64

				// len >= sz+2 opts into GetToBuffer's fast path (payload+CRLF
				// in a single socket read; the 2 bytes after the payload are
				// scratch). Allocated before ResetTimer so the harness buffers
				// don't pollute B/op.
				rbufs := make([][]byte, goroutines)
				for i := range rbufs {
					rbufs[i] = make([]byte, sz+2)
				}

				b.ResetTimer()
				b.ReportAllocs()
				var wg sync.WaitGroup
				wg.Add(goroutines)
				for g := 0; g < goroutines; g++ {
					go func(id int) {
						defer wg.Done()
						per := base
						if id < extra {
							per++
						}
						rbuf := rbufs[id]
						for i := 0; i < per; i++ {
							key := fmt.Sprintf("zc:%d:%d", id, i)
							if zc {
								if err := ap.SetFromBuffer(ctx, key, payload).Err(); err != nil {
									b.Error(err)
									return
								}
								if err := ap.GetToBuffer(ctx, key, rbuf).Err(); err != nil {
									b.Error(err)
									return
								}
							} else {
								if err := ap.Set(ctx, key, pstr, 0).Err(); err != nil {
									b.Error(err)
									return
								}
								if err := ap.Get(ctx, key).Err(); err != nil {
									b.Error(err)
									return
								}
							}
							atomic.AddInt64(&count, 2)
						}
					}(g)
				}
				wg.Wait()
				b.StopTimer()
				b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
			})
		}
	}
}

// BenchmarkAutoPipelineThroughput compares executed-command throughput three
// ways. In every variant a command is counted ONLY after its result has been
// read (the command actually executed on the server) — there is no counting of
// merely-queued commands — and the metric divides by the TIMED region, which
// includes draining whatever was in flight at the deadline, so the reported
// ops/sec is real throughput.
//
//  1. Normal — a plain client. Each Set is a blocking round-trip; throughput is
//     bounded by the connection pool and Redis's non-pipelined ceiling
//     (matching redis-benchmark without -P).
//  2. AutoPipelineBlocking — ap.Set(...).Err() read immediately, the way the
//     normal client is used (drop-in, one command in flight per caller). The
//     flusher batches across the many concurrent callers into deep pipelines.
//  3. AutoPipelineWindowed — submit a window of commands per caller, then read
//     their results. Keeps each pipeline deepest; the high-throughput usage.
//
// The autopipeline variants use a parallel-batch config (MaxConcurrentBatches>1,
// Unordered) — NOT the ordered default. The default (MaxConcurrentBatches=1)
// serializes batch execution and reaches roughly half the blocking number;
// windowed submission stays in the millions even ordered.
//
// Fixed-duration drivers; see the BenchmarkFutureFace note about -benchtime.
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
	apConfig := func() *redis.AutoPipelineOptions {
		return &redis.AutoPipelineOptions{MaxBatchSize: 300, MaxConcurrentBatches: 80, Unordered: true}
	}

	// drive runs `fn` on `goroutines` goroutines until the deadline and reports
	// executed ops/sec over the timed region. fn returns how many executed
	// commands it performed in one iteration (after reading their results); it
	// must not count un-read commands.
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
		// Divide by the timed region, not the nominal window: goroutines check
		// the deadline once per run/window, so real elapsed exceeds `duration`
		// and dividing by the constant would inflate the number.
		b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "ops/sec")
	}

	ctx := context.Background()

	b.Run("Normal", func(b *testing.B) {
		skipWithoutBenchServer(b)
		c := redis.NewClient(&redis.Options{Addr: benchAddr, PoolSize: 100})
		defer c.Close()
		drive(b, func(id int) int {
			// k is only the SET payload; a counter shared across drive's
			// goroutines would race.
			const run = 50 // amortize the harness per-step cost; each Set still blocks
			for k := 0; k < run; k++ {
				if err := c.Set(ctx, fmt.Sprintf("n:%d", id), k, 0).Err(); err != nil {
					b.Error(err)
				}
			}
			return run
		})
	})

	b.Run("AutoPipelineBlocking", func(b *testing.B) {
		skipWithoutBenchServer(b)
		c := redis.NewClient(&redis.Options{Addr: benchAddr})
		defer c.Close()
		ap, err := c.AutoPipelineWithOptions(apConfig()) // blocking face, parallel batches
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		drive(b, func(id int) int {
			// Each command call blocks until executed (drop-in shape, no .Result()
			// needed). We issue a small run per drive() step so the harness's
			// per-step atomic/closure cost is amortized and doesn't understate the
			// command rate — every command here still fully executes and is counted.
			// k is only the SET payload; a shared counter would race.
			const run = 50
			for k := 0; k < run; k++ {
				if err := ap.Set(ctx, fmt.Sprintf("b:%d", id), k, 0).Err(); err != nil {
					b.Error(err)
				}
			}
			return run
		})
	})

	b.Run("AutoPipelineWindowed", func(b *testing.B) {
		skipWithoutBenchServer(b)
		c := redis.NewClient(&redis.Options{Addr: benchAddr})
		defer c.Close()
		ap, err := c.AsyncAutoPipelineWithOptions(apConfig()) // deferred face: submit a window, read later
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
	// capped by Redis's write processing, so the SET variants above are
	// server-bound, not client-bound. GET is cheaper on the server; this variant
	// shows the client machinery itself is not the limit for the SET numbers.
	b.Run("AutoPipelineWindowedGET", func(b *testing.B) {
		skipWithoutBenchServer(b)
		c := redis.NewClient(&redis.Options{Addr: benchAddr})
		defer c.Close()
		if err := c.Set(ctx, "bench:get", "v", 0).Err(); err != nil {
			b.Fatal(err)
		}
		ap, err := c.AsyncAutoPipelineWithOptions(apConfig())
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
