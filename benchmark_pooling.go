// +build ignore

package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	fmt.Println("=== Cmd Object Pooling Benchmark ===\n")

	// Benchmark 1: StringCmd pooling
	fmt.Println("1. StringCmd Allocation Benchmark")
	benchmarkStringCmd(ctx)

	// Benchmark 2: IntCmd pooling
	fmt.Println("\n2. IntCmd Allocation Benchmark")
	benchmarkIntCmd(ctx)

	// Benchmark 3: Mixed workload
	fmt.Println("\n3. Mixed Workload Benchmark (1000 ops)")
	benchmarkMixedWorkload(ctx)

	// Benchmark 4: Parallel workload
	fmt.Println("\n4. Parallel Workload Benchmark")
	benchmarkParallel(ctx)
}

func benchmarkStringCmd(ctx context.Context) {
	iterations := 1000000

	// With pooling
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	start := time.Now()

	for i := 0; i < iterations; i++ {
		cmd := redis.GetStringCmdFromPool()
		cmd.SetArgs(ctx, "get", "key")
		cmd.SetVal("value")
		_ = cmd.Val()
		redis.PutStringCmdToPool(cmd)
	}

	elapsed1 := time.Since(start)
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Without pooling
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)
	start = time.Now()

	for i := 0; i < iterations; i++ {
		cmd := redis.NewStringCmd(ctx, "get", "key")
		cmd.SetVal("value")
		_ = cmd.Val()
	}

	elapsed2 := time.Since(start)
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)

	fmt.Printf("  With Pool:    %v (%d ns/op, %d allocs)\n", elapsed1, elapsed1.Nanoseconds()/int64(iterations), m2.Mallocs-m1.Mallocs)
	fmt.Printf("  Without Pool: %v (%d ns/op, %d allocs)\n", elapsed2, elapsed2.Nanoseconds()/int64(iterations), m4.Mallocs-m3.Mallocs)
	fmt.Printf("  Speedup: %.2fx, Allocation reduction: %.2fx\n",
		float64(elapsed2)/float64(elapsed1),
		float64(m4.Mallocs-m3.Mallocs)/float64(m2.Mallocs-m1.Mallocs))
}

func benchmarkIntCmd(ctx context.Context) {
	iterations := 1000000

	// With pooling
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	start := time.Now()

	for i := 0; i < iterations; i++ {
		cmd := redis.GetIntCmdFromPool()
		cmd.SetArgs(ctx, "incr", "key")
		cmd.SetVal(42)
		_ = cmd.Val()
		redis.PutIntCmdToPool(cmd)
	}

	elapsed1 := time.Since(start)
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Without pooling
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)
	start = time.Now()

	for i := 0; i < iterations; i++ {
		cmd := redis.NewIntCmd(ctx, "incr", "key")
		cmd.SetVal(42)
		_ = cmd.Val()
	}

	elapsed2 := time.Since(start)
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)

	fmt.Printf("  With Pool:    %v (%d ns/op, %d allocs)\n", elapsed1, elapsed1.Nanoseconds()/int64(iterations), m2.Mallocs-m1.Mallocs)
	fmt.Printf("  Without Pool: %v (%d ns/op, %d allocs)\n", elapsed2, elapsed2.Nanoseconds()/int64(iterations), m4.Mallocs-m3.Mallocs)
	fmt.Printf("  Speedup: %.2fx, Allocation reduction: %.2fx\n",
		float64(elapsed2)/float64(elapsed1),
		float64(m4.Mallocs-m3.Mallocs)/float64(m2.Mallocs-m1.Mallocs))
}

func benchmarkMixedWorkload(ctx context.Context) {
	iterations := 1000

	// With pooling
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	start := time.Now()

	for i := 0; i < iterations; i++ {
		for j := 0; j < 1000; j++ {
			if j%3 == 0 {
				cmd := redis.GetStringCmdFromPool()
				cmd.SetArgs(ctx, "get", "key")
				cmd.SetVal("value")
				_ = cmd.Val()
				redis.PutStringCmdToPool(cmd)
			} else if j%3 == 1 {
				cmd := redis.GetIntCmdFromPool()
				cmd.SetArgs(ctx, "incr", "key")
				cmd.SetVal(42)
				_ = cmd.Val()
				redis.PutIntCmdToPool(cmd)
			} else {
				cmd := redis.GetStatusCmdFromPool()
				cmd.SetArgs(ctx, "set", "key", "value")
				cmd.SetVal("OK")
				_ = cmd.Val()
				redis.PutStatusCmdToPool(cmd)
			}
		}
	}

	elapsed1 := time.Since(start)
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Without pooling
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)
	start = time.Now()

	for i := 0; i < iterations; i++ {
		for j := 0; j < 1000; j++ {
			if j%3 == 0 {
				cmd := redis.NewStringCmd(ctx, "get", "key")
				cmd.SetVal("value")
				_ = cmd.Val()
			} else if j%3 == 1 {
				cmd := redis.NewIntCmd(ctx, "incr", "key")
				cmd.SetVal(42)
				_ = cmd.Val()
			} else {
				cmd := redis.NewStatusCmd(ctx, "set", "key", "value")
				cmd.SetVal("OK")
				_ = cmd.Val()
			}
		}
	}

	elapsed2 := time.Since(start)
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)

	fmt.Printf("  With Pool:    %v (%d ns/op, %d allocs)\n", elapsed1, elapsed1.Nanoseconds()/int64(iterations*1000), m2.Mallocs-m1.Mallocs)
	fmt.Printf("  Without Pool: %v (%d ns/op, %d allocs)\n", elapsed2, elapsed2.Nanoseconds()/int64(iterations*1000), m4.Mallocs-m3.Mallocs)
	fmt.Printf("  Speedup: %.2fx, Allocation reduction: %.2fx\n",
		float64(elapsed2)/float64(elapsed1),
		float64(m4.Mallocs-m3.Mallocs)/float64(m2.Mallocs-m1.Mallocs))
}

func benchmarkParallel(ctx context.Context) {
	iterations := 100000
	workers := runtime.NumCPU()

	fmt.Printf("  Using %d workers\n", workers)

	// With pooling
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	start := time.Now()

	done := make(chan bool, workers)
	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < iterations; i++ {
				cmd := redis.GetStringCmdFromPool()
				cmd.SetArgs(ctx, "get", "key")
				cmd.SetVal("value")
				_ = cmd.Val()
				redis.PutStringCmdToPool(cmd)
			}
			done <- true
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	elapsed1 := time.Since(start)
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Without pooling
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)
	start = time.Now()

	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < iterations; i++ {
				cmd := redis.NewStringCmd(ctx, "get", "key")
				cmd.SetVal("value")
				_ = cmd.Val()
			}
			done <- true
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	elapsed2 := time.Since(start)
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)

	totalOps := iterations * workers
	fmt.Printf("  With Pool:    %v (%d ns/op, %d allocs)\n", elapsed1, elapsed1.Nanoseconds()/int64(totalOps), m2.Mallocs-m1.Mallocs)
	fmt.Printf("  Without Pool: %v (%d ns/op, %d allocs)\n", elapsed2, elapsed2.Nanoseconds()/int64(totalOps), m4.Mallocs-m3.Mallocs)
	fmt.Printf("  Speedup: %.2fx, Allocation reduction: %.2fx\n",
		float64(elapsed2)/float64(elapsed1),
		float64(m4.Mallocs-m3.Mallocs)/float64(m2.Mallocs-m1.Mallocs))
}

