// Command autopipeline helps you choose how to issue many concurrent Redis
// commands. It runs the same workload (N goroutines issuing SET for a fixed
// duration) four ways and prints throughput plus the ordering guarantee, so
// you can pick the approach that fits your code:
//
//  1. normal blocking          — plain client. Each command is a synchronous
//     round-trip. Simplest; ordered; throughput bounded by the pool size (~100k).
//
//  2. AutoPipeline() blocking   — the blocking face: ap.Set(...) blocks until
//     executed, exactly like a normal client (drop-in). Per-goroutine ordered,
//     and the flusher batches across goroutines into parallel pipelines, so it
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
// Start a Redis first (e.g. `docker run --rm -p 6379:6379 redis`) then:
//
//	go run .
//
// Set REDIS_ADDR to point at a different server.
package main

import (
	"context"
	"fmt"
	"os"
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

type result struct {
	name    string
	ordered string
	ops     float64
}

func main() {
	ctx := context.Background()

	probe := redis.NewClient(&redis.Options{Addr: addr()})
	if err := probe.Ping(ctx).Err(); err != nil {
		fmt.Printf("cannot reach redis at %s: %v\n", addr(), err)
		os.Exit(1)
	}
	probe.Close()

	results := []result{
		{"1. normal blocking", "ordered", benchNormalBlocking(ctx)},
		{"2. autopipeline ordered, blocking read", "ordered", benchOrderedBlocking(ctx)},
		{"3. autopipeline ordered, read later", "ordered", benchOrderedReadLater(ctx)},
		{"4. autopipeline unordered, read later", "UNORDERED", benchUnorderedReadLater(ctx)},
	}

	base := results[0].ops
	fmt.Printf("\nthroughput (%d goroutines, %s):\n\n", goroutines, duration)
	fmt.Printf("  %-40s %12s  %-9s %s\n", "approach", "ops/sec", "ordering", "vs normal")
	for _, r := range results {
		fmt.Printf("  %-40s %12.0f  %-9s %.1fx\n", r.name, r.ops, r.ordered, r.ops/base)
	}
	fmt.Println(`
Notes:
  - Read-later (3,4) is far faster than blocking-read (2) because it keeps the
    pipeline deep instead of one command in flight per goroutine.
  - Unordered (4) runs batches in parallel and is typically ~10-20% faster than
    ordered read-later (3); the gap widens with more concurrent callers. Both are
    bounded by the Go client's per-command overhead (object alloc + signalling),
    so they land somewhat below what the server alone can do, and the difference
    between 3 and 4 can look small at moderate load.
  - Prefer 3 (ordered, read later) unless you've measured that 4's extra
    throughput matters for your workload and you don't need ordering.`)
}

// 1. Plain client: each Set is a synchronous round-trip. Ordered. Throughput
// bounded by the connection pool.
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
		panic(err)
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
		panic(err)
	}
	return benchReadLater(ctx, rdb, ap)
}

// 4. Async autopipeline, unordered, read later: parallel batches for peak
// throughput. MaxConcurrentBatches>1 gives up global ordering, so Unordered
// must be true.
func benchUnorderedReadLater(ctx context.Context) float64 {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()
	rdb.FlushDB(ctx)
	ap, err := rdb.AsyncAutoPipeline(&redis.AutoPipelineConfig{
		MaxBatchSize:         300,
		MaxConcurrentBatches: 80,
		Unordered:            true, // required for MaxConcurrentBatches > 1
	})
	if err != nil {
		panic(err)
	}
	return benchReadLater(ctx, rdb, ap)
}

// benchReadLater submits a window of SET commands (non-blocking, returning the
// usual *StatusCmd) on the given async autopipeliner and then reads their
// results, which keeps the pipeline deep.
func benchReadLater(ctx context.Context, rdb *redis.Client, ap *redis.AutoPipeliner) float64 {
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
