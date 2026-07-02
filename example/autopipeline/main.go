// Command autopipeline is a tour of go-redis automatic pipelining plus a
// runnable throughput comparison.
//
// Act 1 — usage tour: the blocking face as a drop-in client, the async face
// with a submission window, Submit/AutoFuture for raw commands, the Do escape
// hatch, and (env-gated) cluster usage.
//
// Act 2 — throughput: the same workload (N goroutines issuing SET for a fixed
// duration) run four ways:
//
//  1. normal blocking          — plain client. Each command is a synchronous
//     round-trip. Simplest; ordered; throughput bounded by the pool size (~100k).
//
//  2. AutoPipeline() blocking   — the blocking face: ap.Set(...) blocks until
//     executed, exactly like a normal client (drop-in). Per-goroutine ordered,
//     and the flusher coalesces all goroutines' commands into deep,
//     back-to-back pipelines (one ordered batch stream by default), so it
//     reaches ~1M+ ops/sec with no code change beyond the constructor.
//
//  3. AsyncAutoPipeline() ordered, read later — the deferred face: ap.Set(...)
//     returns immediately; submit a window of commands, then read their results.
//     Ordered (default) AND fastest: the window keeps each pipeline deep
//     (~2-3M). Best when you can defer reading results.
//
//  4. AsyncAutoPipeline() unordered, read later — same windowed style with a
//     parallel-batch config (MaxConcurrentBatches>1 + Unordered:true). Gives up
//     global ordering for peak throughput; use for independent / order-
//     insensitive bulk work.
//
// Decision guide:
//   - Need simplest code / low concurrency      -> 1 (normal).
//   - Want a drop-in speedup, keep ordering      -> 2 (AutoPipeline, blocking).
//   - High throughput, can read results later    -> 3 (AsyncAutoPipeline, ordered).
//   - Max throughput, order does not matter       -> 4 (AsyncAutoPipeline, unordered).
//
// Both faces return a CACHED, client-shared instance: the first call's config
// wins, later configs are ignored, and Close stops the instance for every
// caller. Close the autopipeliner (or the client) when done.
//
// Start a Redis first (e.g. `docker run --rm -p 6379:6379 redis`) then:
//
//	go run .
//
// Set REDIS_ADDR to point at a different server; set REDIS_CLUSTER_ADDRS
// (comma-separated) to also run the cluster part of the tour.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	duration   = 3 * time.Second
	goroutines = 500
	window     = 200 // commands submitted before reading results (read-later runs)
)

func addr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

func fatalf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	os.Exit(1)
}

type result struct {
	name    string
	ordered string
	ops     float64
}

func main() {
	ctx := context.Background()

	probe := redis.NewClient(&redis.Options{Addr: addr()})
	if err := probe.Ping(ctx).Err(); err != nil {
		fatalf("cannot reach redis at %s: %v", addr(), err)
	}
	probe.Close()

	usageTour(ctx)
	throughputComparison(ctx)
}

// ---------------------------------------------------------------------------
// Act 1 — usage tour
// ---------------------------------------------------------------------------

func usageTour(ctx context.Context) {
	fmt.Println("== usage tour ==")

	rdb := redis.NewClient(&redis.Options{
		Addr: addr(),
		// Optional: give pipelines their own connection pool with bigger
		// buffers. Enabled when either buffer size is set; batches then run on
		// dedicated connections instead of the main pool.
		PipelineReadBufferSize:  64 << 10,
		PipelineWriteBufferSize: 64 << 10,
		PipelinePoolSize:        10,
	})
	defer rdb.Close()
	rdb.FlushDB(ctx)

	// --- a. Blocking face: a drop-in client -------------------------------
	// Each call blocks until its command executed, exactly like a plain
	// client; the engine batches concurrent callers under the hood. Don't
	// expect windowed-throughput wins here — each caller waits per command;
	// the win is batching + far fewer connections at high concurrency.
	ap, err := rdb.AutoPipeline()
	if err != nil {
		fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("tour:blocking:%d", i)
			if err := ap.Set(ctx, key, i, 0).Err(); err != nil { // blocks until executed
				fatalf("set: %v", err)
			}
			val, err := ap.Get(ctx, key).Int() // reads exactly like a plain client
			if err != nil || val != i {
				fatalf("get %s = %d, %v", key, val, err)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("  blocking face: 4 goroutines set+get their keys (per-goroutine order held)")

	// --- b. Async face: submit a window, read results later ---------------
	// Calls return immediately; the result accessors (Err/Val/Result) block
	// until the command's batch executed. The window is what keeps pipelines
	// deep — this is the throughput pattern.
	aap, err := rdb.AsyncAutoPipeline()
	if err != nil {
		fatalf("AsyncAutoPipeline: %v", err)
	}
	defer aap.Close()

	gets := make([]*redis.StringCmd, 0, 4)
	for i := 0; i < 4; i++ {
		gets = append(gets, aap.Get(ctx, fmt.Sprintf("tour:blocking:%d", i))) // returns immediately
	}
	for i, g := range gets {
		if v, err := g.Int(); err != nil || v != i {
			fatalf("async get %d = %d, %v", i, v, err)
		}
	}
	fmt.Println("  async face: window of 4 GETs submitted, results read afterwards")

	// --- c. Submit + AutoFuture: raw commands on the async face -----------
	// Submit is the explicit form for raw Cmders. It is REJECTED on the
	// blocking face by design (Submit doesn't wait, which would break the
	// blocking face's ordering guarantees) — use it on AsyncAutoPipeline.
	cmd := redis.NewStatusCmd(ctx, "set", "tour:future", "hello")
	fut := aap.Submit(ctx, cmd)
	if err := fut.Wait(); err != nil { // or fut.WaitContext(ctx) to bound the wait
		fatalf("submit: %v", err)
	}
	fmt.Printf("  submit/future: raw SET executed, reply %q\n", cmd.Val())

	// --- d. Do: the escape hatch, NOT batched ------------------------------
	// Do runs on a normal connection outside the pipeline (plain Client.Do
	// semantics). It exists for arbitrary commands — including stateful or
	// blocking ones (SELECT, SUBSCRIBE, BLPOP, ...) that must never ride a
	// shared pipeline connection. Prefer the typed methods for data commands.
	if err := aap.Do(ctx, "echo", "outside the pipeline").Err(); err != nil {
		fatalf("do: %v", err)
	}
	fmt.Println("  do: raw command ran on a normal connection (not batched)")

	// --- e. Tuning notes (in code, so they stay honest) --------------------
	// For peak throughput on the async face give up global ordering:
	//
	//	rdb.AsyncAutoPipeline(&redis.AutoPipelineConfig{
	//		MaxConcurrentBatches: 4,   // 2-4 is the sweet spot; more mostly
	//		Unordered:            true, // fragments batches — measure first
	//	})
	//
	// Leave NumShards at 0 (one deep queue; cluster clients shard by slot
	// automatically). Remember the instance is cached per client: the FIRST
	// call's config wins.

	// --- f. Cluster (env-gated) -------------------------------------------
	if addrs := os.Getenv("REDIS_CLUSTER_ADDRS"); addrs != "" {
		clusterTour(ctx, strings.Split(addrs, ","))
	} else {
		fmt.Println("  cluster: skipped (set REDIS_CLUSTER_ADDRS=host:port,... to run)")
	}
	fmt.Println()
}

// clusterTour shows that autopipelining on a ClusterClient needs no extra
// code: commands are routed to slot-affine shards automatically, so each
// batch stays on one node and per-key ordering holds.
func clusterTour(ctx context.Context, addrs []string) {
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer cc.Close()

	ap, err := cc.AutoPipeline()
	if err != nil {
		fatalf("cluster AutoPipeline: %v", err)
	}
	defer ap.Close()

	for i := 0; i < 8; i++ { // keys spread across slots/nodes
		if err := ap.Set(ctx, fmt.Sprintf("tour:cluster:%d", i), i, 0).Err(); err != nil {
			fatalf("cluster set: %v", err)
		}
	}
	fmt.Println("  cluster: 8 multi-slot SETs batched per node (slot sharding is automatic)")
}

// ---------------------------------------------------------------------------
// Act 2 — throughput comparison
// ---------------------------------------------------------------------------

func throughputComparison(ctx context.Context) {
	results := []result{
		{"1. normal blocking", "ordered", benchNormalBlocking(ctx)},
		{"2. autopipeline ordered, blocking read", "ordered", benchOrderedBlocking(ctx)},
		{"3. autopipeline ordered, read later", "ordered", benchOrderedReadLater(ctx)},
		{"4. autopipeline unordered, read later", "UNORDERED", benchUnorderedReadLater(ctx)},
	}

	base := results[0].ops
	fmt.Printf("throughput (%d goroutines, %s):\n\n", goroutines, duration)
	fmt.Printf("  %-40s %12s  %-9s %s\n", "approach", "ops/sec", "ordering", "vs normal")
	for _, r := range results {
		fmt.Printf("  %-40s %12.0f  %-9s %.1fx\n", r.name, r.ops, r.ordered, r.ops/base)
	}
	fmt.Println(`
Notes:
  - Read-later (3,4) is far faster than blocking-read (2) because it keeps the
    pipeline deep instead of one command in flight per goroutine.
  - Unordered (4) overlaps a few batches and is typically ~10-20% faster than
    ordered read-later (3); the gap widens with more concurrent callers. Both are
    bounded by the Go client's per-command overhead (object alloc + signalling),
    so they land somewhat below what the server alone can do, and the difference
    between 3 and 4 can look small at moderate load.
  - Prefer 3 (ordered, read later) unless you've measured that 4's extra
    throughput matters for your workload and you don't need ordering.`)
}

// 1. Plain client: each Set is a synchronous round-trip. Ordered. PoolSize is
// raised to goroutine-count territory so the baseline isn't artificially
// pool-starved — that's the plain client's cost: connections scale with
// concurrency (the autopipeline cases below use the default pool and still
// hold only a handful of connections).
func benchNormalBlocking(ctx context.Context) float64 {
	rdb := redis.NewClient(&redis.Options{Addr: addr(), PoolSize: 50})
	defer rdb.Close()
	rdb.FlushDB(ctx)

	return run(func(g int, doOp func()) {
		key := fmt.Sprintf("normal:%d", g)
		rdb.Set(ctx, key, 1, 0)
		doOp()
	})
}

// 2. Blocking autopipeline: ap.Set(...) blocks until executed, exactly like a
// normal client. Drop-in; per-goroutine ordered. The flusher still batches this
// caller's command with the other goroutines' commands, so throughput is far
// above a plain client.
func benchOrderedBlocking(ctx context.Context) float64 {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()
	rdb.FlushDB(ctx)
	ap, err := rdb.AutoPipeline() // blocking face (default: single ordered batch stream)
	if err != nil {
		fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()

	return run(func(g int, doOp func()) {
		key := fmt.Sprintf("ob:%d", g)
		_ = ap.Set(ctx, key, 1, 0).Err() // the call itself blocks until executed
		doOp()
	})
}

// 3. Async autopipeline, ordered, read later: submit a window of commands without
// reading (non-blocking), then drain results. Ordered (default) and fast — the
// window keeps the pipeline deep.
func benchOrderedReadLater(ctx context.Context) float64 {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()
	rdb.FlushDB(ctx)
	// Default async config is ordered (MaxConcurrentBatches=1).
	ap, err := rdb.AsyncAutoPipeline()
	if err != nil {
		fatalf("AsyncAutoPipeline: %v", err)
	}
	return benchReadLater(ctx, ap)
}

// 4. Async autopipeline, unordered, read later: a few overlapping batches for
// peak throughput. MaxConcurrentBatches>1 gives up global ordering, so
// Unordered must be true. 2-4 concurrent batches is the sweet spot — batch N+1
// accumulates and executes while batch N's replies are in flight; much higher
// values mostly add contention (the repo's internal benches use bigger numbers
// on dedicated hardware).
func benchUnorderedReadLater(ctx context.Context) float64 {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()
	rdb.FlushDB(ctx)
	ap, err := rdb.AsyncAutoPipeline(&redis.AutoPipelineConfig{
		MaxBatchSize:         300,
		MaxConcurrentBatches: 4,
		Unordered:            true, // required for MaxConcurrentBatches > 1
	})
	if err != nil {
		fatalf("AsyncAutoPipeline: %v", err)
	}
	return benchReadLater(ctx, ap)
}

// benchReadLater submits a window of SET commands (non-blocking, returning the
// usual *StatusCmd) on the given async autopipeliner and then reads their
// results, which keeps the pipeline deep.
func benchReadLater(ctx context.Context, ap *redis.AutoPipeliner) float64 {
	defer ap.Close()

	var count int64
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("rl:%d", id)
			cmds := make([]*redis.StatusCmd, 0, window)
			for time.Now().Before(deadline) {
				cmds = cmds[:0]
				for j := 0; j < window; j++ {
					cmds = append(cmds, ap.Set(ctx, key, j, 0)) // queued, non-blocking
				}
				for _, c := range cmds {
					_ = c.Err() // blocks until executed
				}
				atomic.AddInt64(&count, window)
			}
		}(g)
	}
	wg.Wait()
	return float64(count) / duration.Seconds()
}

// run executes a per-command workload (op) on `goroutines` goroutines for the
// fixed duration and returns ops/sec. op calls doOp() once per command executed.
func run(op func(g int, doOp func())) float64 {
	var count int64
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				op(id, func() { atomic.AddInt64(&count, 1) })
			}
		}(g)
	}
	wg.Wait()
	return float64(count) / duration.Seconds()
}
