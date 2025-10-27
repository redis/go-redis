package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// HSET Benchmark Tests
//
// This file contains benchmark tests for Redis HSET operations with different scales:
// 1, 10, 100, 1000, 10000, 100000 operations
//
// Prerequisites:
// - Redis server running on localhost:6379
// - No authentication required
//
// Usage:
//   go test -bench=BenchmarkHSET -v ./hset_benchmark_test.go
//   go test -bench=BenchmarkHSETPipelined -v ./hset_benchmark_test.go
//   go test -bench=. -v ./hset_benchmark_test.go  # Run all benchmarks
//
// Example output:
//   BenchmarkHSET/HSET_1_operations-8         	    5000	    250000 ns/op	1000000.00 ops/sec
//   BenchmarkHSET/HSET_100_operations-8       	     100	  10000000 ns/op	 100000.00 ops/sec
//
// The benchmarks test three different approaches:
// 1. Individual HSET commands (BenchmarkHSET)
// 2. Pipelined HSET commands (BenchmarkHSETPipelined)

// BenchmarkHSET benchmarks HSET operations with different scales
func BenchmarkHSET(b *testing.B) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	// Clean up before and after tests
	defer func() {
		rdb.FlushDB(ctx)
	}()

	scales := []int{1, 10, 100, 1000, 10000, 100000}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("HSET_%d_operations", scale), func(b *testing.B) {
			benchmarkHSETOperations(b, rdb, ctx, scale)
		})
	}
}

// benchmarkHSETOperations performs the actual HSET benchmark for a given scale
func benchmarkHSETOperations(b *testing.B, rdb *redis.Client, ctx context.Context, operations int) {
	hashKey := fmt.Sprintf("benchmark_hash_%d", operations)

	b.ResetTimer()
	b.StartTimer()
	totalTimes := []time.Duration{}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Clean up the hash before each iteration
		rdb.Del(ctx, hashKey)
		b.StartTimer()

		startTime := time.Now()
		// Perform the specified number of HSET operations
		for j := 0; j < operations; j++ {
			field := fmt.Sprintf("field_%d", j)
			value := fmt.Sprintf("value_%d", j)

			err := rdb.HSet(ctx, hashKey, field, value).Err()
			if err != nil {
				b.Fatalf("HSET operation failed: %v", err)
			}
		}
		totalTimes = append(totalTimes, time.Since(startTime))
	}

	// Stop the timer to calculate metrics
	b.StopTimer()

	// Report operations per second
	opsPerSec := float64(operations*b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/sec")

	// Report average time per operation
	avgTimePerOp := b.Elapsed().Nanoseconds() / int64(operations*b.N)
	b.ReportMetric(float64(avgTimePerOp), "ns/op")
	// report average time in milliseconds from totalTimes
	sumTime := time.Duration(0)
	for _, t := range totalTimes {
		sumTime += t
	}
	avgTimePerOpMs := sumTime.Milliseconds() / int64(len(totalTimes))
	b.ReportMetric(float64(avgTimePerOpMs), "ms")
}

// benchmarkHSETOperationsConcurrent performs the actual HSET benchmark for a given scale
func benchmarkHSETOperationsConcurrent(b *testing.B, rdb *redis.Client, ctx context.Context, operations int) {
	hashKey := fmt.Sprintf("benchmark_hash_%d", operations)

	b.ResetTimer()
	b.StartTimer()
	totalTimes := []time.Duration{}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Clean up the hash before each iteration
		rdb.Del(ctx, hashKey)
		b.StartTimer()

		startTime := time.Now()
		// Perform the specified number of HSET operations

		wg := sync.WaitGroup{}
		wg.Add(operations)
		timesCh := make(chan time.Duration, operations)
		for j := 0; j < operations; j++ {
			go func(j int) {
				defer wg.Done()
				field := fmt.Sprintf("field_%d", j)
				value := fmt.Sprintf("value_%d", j)

				err := rdb.HSet(ctx, hashKey, field, value).Err()
				if err != nil {
					b.Fatalf("HSET operation failed: %v", err)
				}
				timesCh <- time.Since(startTime)
			}(j)
			wg.Wait()
			close(timesCh)
			for d := range timesCh {
				totalTimes = append(totalTimes, d)
			}
		}
	}

	// Stop the timer to calculate metrics
	b.StopTimer()

	// Report operations per second
	opsPerSec := float64(operations*b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/sec")

	// Report average time per operation
	avgTimePerOp := b.Elapsed().Nanoseconds() / int64(operations*b.N)
	b.ReportMetric(float64(avgTimePerOp), "ns/op")
	// report average time in milliseconds from totalTimes

	sumTime := time.Duration(0)
	for _, t := range totalTimes {
		sumTime += t
	}
	avgTimePerOpMs := sumTime.Milliseconds() / int64(len(totalTimes))
	b.ReportMetric(float64(avgTimePerOpMs), "ms")
}

// BenchmarkHSETPipelined benchmarks HSET operations using pipelining for better performance
func BenchmarkHSETPipelined(b *testing.B) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	// Clean up before and after tests
	defer func() {
		rdb.FlushDB(ctx)
	}()

	scales := []int{1, 10, 100, 1000, 10000, 100000}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("HSET_Pipelined_%d_operations", scale), func(b *testing.B) {
			benchmarkHSETPipelined(b, rdb, ctx, scale)
		})
	}
}

func BenchmarkHSET_Concurrent(b *testing.B) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		DB:       0,
		PoolSize: 1000,
	})
	defer rdb.Close()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	// Clean up before and after tests
	defer func() {
		rdb.FlushDB(ctx)
	}()

	scales := []int{1, 10, 100, 1000, 10000, 100000}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("HSET_%d_operations_concurrent", scale), func(b *testing.B) {
			benchmarkHSETOperationsConcurrent(b, rdb, ctx, scale)
		})
	}
}

// benchmarkHSETPipelined performs HSET benchmark using pipelining
func benchmarkHSETPipelined(b *testing.B, rdb *redis.Client, ctx context.Context, operations int) {
	hashKey := fmt.Sprintf("benchmark_hash_pipelined_%d", operations)

	b.ResetTimer()
	b.StartTimer()
	totalTimes := []time.Duration{}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Clean up the hash before each iteration
		rdb.Del(ctx, hashKey)
		b.StartTimer()

		startTime := time.Now()
		// Use pipelining for better performance
		pipe := rdb.Pipeline()

		// Add all HSET operations to the pipeline
		for j := 0; j < operations; j++ {
			field := fmt.Sprintf("field_%d", j)
			value := fmt.Sprintf("value_%d", j)
			pipe.HSet(ctx, hashKey, field, value)
		}

		// Execute all operations at once
		_, err := pipe.Exec(ctx)
		if err != nil {
			b.Fatalf("Pipeline execution failed: %v", err)
		}
		totalTimes = append(totalTimes, time.Since(startTime))
	}

	b.StopTimer()

	// Report operations per second
	opsPerSec := float64(operations*b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/sec")

	// Report average time per operation
	avgTimePerOp := b.Elapsed().Nanoseconds() / int64(operations*b.N)
	b.ReportMetric(float64(avgTimePerOp), "ns/op")
	// report average time in milliseconds from totalTimes
	sumTime := time.Duration(0)
	for _, t := range totalTimes {
		sumTime += t
	}
	avgTimePerOpMs := sumTime.Milliseconds() / int64(len(totalTimes))
	b.ReportMetric(float64(avgTimePerOpMs), "ms")
}

// add same tests but with RESP2
func BenchmarkHSET_RESP2(b *testing.B) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
		Protocol: 2,
	})
	defer rdb.Close()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	// Clean up before and after tests
	defer func() {
		rdb.FlushDB(ctx)
	}()

	scales := []int{1, 10, 100, 1000, 10000, 100000}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("HSET_RESP2_%d_operations", scale), func(b *testing.B) {
			benchmarkHSETOperations(b, rdb, ctx, scale)
		})
	}
}

func BenchmarkHSETPipelined_RESP2(b *testing.B) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
		Protocol: 2,
	})
	defer rdb.Close()

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	// Clean up before and after tests
	defer func() {
		rdb.FlushDB(ctx)
	}()

	scales := []int{1, 10, 100, 1000, 10000, 100000}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("HSET_Pipelined_RESP2_%d_operations", scale), func(b *testing.B) {
			benchmarkHSETPipelined(b, rdb, ctx, scale)
		})
	}
}
