package redis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkOTelOverhead_Baseline measures baseline performance without OTel
// This benchmark is designed to run on both the current branch and upstream/master
// On the current branch with OTel code, use the benchmarks in extra/redisotel-native/
func BenchmarkOTelOverhead_Baseline(b *testing.B) {
	ctx := context.Background()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:         ":6379",
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     10,
		MinIdleConns: 5,
	})
	defer rdb.Close()

	// Verify connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	// Clean up
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		b.Fatalf("Failed to flush DB: %v", err)
	}

	// Run sub-benchmarks for different operations
	b.Run("Ping", func(b *testing.B) {
		benchmarkPing(b, rdb, ctx)
	})

	b.Run("Set", func(b *testing.B) {
		benchmarkSet(b, rdb, ctx)
	})

	b.Run("Get", func(b *testing.B) {
		benchmarkGet(b, rdb, ctx)
	})

	b.Run("SetGet_Mixed", func(b *testing.B) {
		benchmarkSetGetMixed(b, rdb, ctx)
	})

	b.Run("Pipeline", func(b *testing.B) {
		benchmarkPipeline(b, rdb, ctx)
	})
}

// benchmarkPing measures PING command performance
func benchmarkPing(b *testing.B, rdb *redis.Client, ctx context.Context) {
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := rdb.Ping(ctx).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// benchmarkSet measures SET command performance
func benchmarkSet(b *testing.B, rdb *redis.Client, ctx context.Context) {
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key:%d", i)
			if err := rdb.Set(ctx, key, "value", 0).Err(); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// benchmarkGet measures GET command performance
func benchmarkGet(b *testing.B, rdb *redis.Client, ctx context.Context) {
	// Pre-populate keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := rdb.Set(ctx, key, "value", 0).Err(); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key:%d", i%1000)
			if err := rdb.Get(ctx, key).Err(); err != nil && err != redis.Nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// benchmarkSetGetMixed measures mixed SET/GET workload (70% GET, 30% SET)
func benchmarkSetGetMixed(b *testing.B, rdb *redis.Client, ctx context.Context) {
	// Pre-populate keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := rdb.Set(ctx, key, "value", 0).Err(); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key:%d", i%1000)

			// 70% GET, 30% SET
			if i%10 < 7 {
				if err := rdb.Get(ctx, key).Err(); err != nil && err != redis.Nil {
					b.Fatal(err)
				}
			} else {
				if err := rdb.Set(ctx, key, "value", 0).Err(); err != nil {
					b.Fatal(err)
				}
			}
			i++
		}
	})
}

// benchmarkPipeline measures pipelined operations performance
func benchmarkPipeline(b *testing.B, rdb *redis.Client, ctx context.Context) {
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pipe := rdb.Pipeline()

			// Pipeline 10 commands
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key:%d:%d", i, j)
				pipe.Set(ctx, key, "value", 0)
			}

			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkOTelOverhead_ConnectionPool measures connection pool overhead
func BenchmarkOTelOverhead_ConnectionPool(b *testing.B) {
	ctx := context.Background()

	// Create Redis client with larger pool
	rdb := redis.NewClient(&redis.Options{
		Addr:         ":6379",
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     100,
		MinIdleConns: 10,
	})
	defer rdb.Close()

	// Verify connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis server not available: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	// Stress test connection pool with high concurrency
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := rdb.Ping(ctx).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
