// +build ignore

// Benchmark program that works on both master and refactored branches
// Run with: go run benchmark_comparison.go

package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// Connect to Redis
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Printf("Error: Redis not available: %v\n", err)
		fmt.Println("Please start Redis with: redis-server")
		return
	}

	fmt.Println("=== go-redis Performance Benchmark ===")
	fmt.Println("Testing String Commands with Pooling Optimization")
	fmt.Println()

	// Warm up
	fmt.Println("Warming up...")
	for i := 0; i < 1000; i++ {
		client.Set(ctx, "warmup", "value", 0)
		client.Get(ctx, "warmup")
		client.Incr(ctx, "warmup:counter")
	}
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Run benchmarks
	benchmarkGet(ctx, client)
	benchmarkSet(ctx, client)
	benchmarkIncr(ctx, client)
	benchmarkMGet(ctx, client)
	benchmarkMSet(ctx, client)
	benchmarkMixed(ctx, client)
	benchmarkConcurrent(ctx, client)
}

func benchmarkGet(ctx context.Context, client *redis.Client) {
	fmt.Println("1. GET Command Benchmark")
	client.Set(ctx, "bench:get", "value", 0)

	iterations := 100000
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		client.Get(ctx, "bench:get")
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	printResults("GET", iterations, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func benchmarkSet(ctx context.Context, client *redis.Client) {
	fmt.Println("\n2. SET Command Benchmark")

	iterations := 100000
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		client.Set(ctx, "bench:set", "value", 0)
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	printResults("SET", iterations, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func benchmarkIncr(ctx context.Context, client *redis.Client) {
	fmt.Println("\n3. INCR Command Benchmark")
	client.Del(ctx, "bench:incr")

	iterations := 100000
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		client.Incr(ctx, "bench:incr")
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	printResults("INCR", iterations, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func benchmarkMGet(ctx context.Context, client *redis.Client) {
	fmt.Println("\n4. MGET Command Benchmark")
	client.MSet(ctx, "bench:mget1", "v1", "bench:mget2", "v2", "bench:mget3", "v3")

	iterations := 50000
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		client.MGet(ctx, "bench:mget1", "bench:mget2", "bench:mget3")
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	printResults("MGET", iterations, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func benchmarkMSet(ctx context.Context, client *redis.Client) {
	fmt.Println("\n5. MSET Command Benchmark")

	iterations := 50000
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		client.MSet(ctx, "bench:mset1", "v1", "bench:mset2", "v2", "bench:mset3", "v3")
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	printResults("MSET", iterations, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func benchmarkMixed(ctx context.Context, client *redis.Client) {
	fmt.Println("\n6. Mixed Workload Benchmark (GET/SET/INCR)")

	iterations := 30000
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		client.Get(ctx, "bench:mixed")
		client.Set(ctx, "bench:mixed", "value", 0)
		client.Incr(ctx, "bench:counter")
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	printResults("Mixed (3 ops)", iterations*3, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func benchmarkConcurrent(ctx context.Context, client *redis.Client) {
	fmt.Println("\n7. Concurrent GET Benchmark")
	client.Set(ctx, "bench:concurrent", "value", 0)

	workers := runtime.NumCPU()
	iterations := 10000
	totalOps := workers * iterations

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()
	done := make(chan bool, workers)
	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < iterations; i++ {
				client.Get(ctx, "bench:concurrent")
			}
			done <- true
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}
	elapsed := time.Since(start)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	fmt.Printf("  Workers: %d\n", workers)
	printResults("Concurrent GET", totalOps, elapsed, m2.Mallocs-m1.Mallocs, m2.TotalAlloc-m1.TotalAlloc)
}

func printResults(name string, iterations int, elapsed time.Duration, allocs uint64, bytes uint64) {
	nsPerOp := elapsed.Nanoseconds() / int64(iterations)
	allocsPerOp := float64(allocs) / float64(iterations)
	bytesPerOp := float64(bytes) / float64(iterations)

	fmt.Printf("  Operations:    %d\n", iterations)
	fmt.Printf("  Total time:    %v\n", elapsed)
	fmt.Printf("  ns/op:         %d\n", nsPerOp)
	fmt.Printf("  ops/sec:       %.0f\n", float64(iterations)/elapsed.Seconds())
	fmt.Printf("  allocs/op:     %.2f\n", allocsPerOp)
	fmt.Printf("  bytes/op:      %.0f\n", bytesPerOp)
}

