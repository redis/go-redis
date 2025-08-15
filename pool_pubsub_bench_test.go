// Pool and PubSub Benchmark Suite
//
// This file contains comprehensive benchmarks for both pool operations and PubSub initialization.
// It's designed to be run against different branches to compare performance.
//
// Usage Examples:
//   # Run all benchmarks
//   go test -bench=. -run='^$' -benchtime=1s pool_pubsub_bench_test.go
//
//   # Run only pool benchmarks
//   go test -bench=BenchmarkPool -run='^$' pool_pubsub_bench_test.go
//
//   # Run only PubSub benchmarks
//   go test -bench=BenchmarkPubSub -run='^$' pool_pubsub_bench_test.go
//
//   # Compare between branches
//   git checkout branch1 && go test -bench=. -run='^$' pool_pubsub_bench_test.go > branch1.txt
//   git checkout branch2 && go test -bench=. -run='^$' pool_pubsub_bench_test.go > branch2.txt
//   benchcmp branch1.txt branch2.txt
//
//   # Run with memory profiling
//   go test -bench=BenchmarkPoolGetPut -run='^$' -memprofile=mem.prof pool_pubsub_bench_test.go
//
//   # Run with CPU profiling
//   go test -bench=BenchmarkPoolGetPut -run='^$' -cpuprofile=cpu.prof pool_pubsub_bench_test.go

package redis_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/pool"
)

// dummyDialer creates a mock connection for benchmarking
func dummyDialer(ctx context.Context) (net.Conn, error) {
	return &dummyConn{}, nil
}

// dummyConn implements net.Conn for benchmarking
type dummyConn struct{}

func (c *dummyConn) Read(b []byte) (n int, err error)  { return len(b), nil }
func (c *dummyConn) Write(b []byte) (n int, err error) { return len(b), nil }
func (c *dummyConn) Close() error                      { return nil }
func (c *dummyConn) LocalAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379} }
func (c *dummyConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379}
}
func (c *dummyConn) SetDeadline(t time.Time) error      { return nil }
func (c *dummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *dummyConn) SetWriteDeadline(t time.Time) error { return nil }

// =============================================================================
// POOL BENCHMARKS
// =============================================================================

// BenchmarkPoolGetPut benchmarks the core pool Get/Put operations
func BenchmarkPoolGetPut(b *testing.B) {
	ctx := context.Background()

	poolSizes := []int{1, 2, 4, 8, 16, 32, 64, 128}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			connPool := pool.NewConnPool(&pool.Options{
				Dialer:          dummyDialer,
				PoolSize:        int32(poolSize),
				PoolTimeout:     time.Second,
				DialTimeout:     time.Second,
				ConnMaxIdleTime: time.Hour,
				MinIdleConns:    int32(0), // Start with no idle connections
			})
			defer connPool.Close()

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					connPool.Put(ctx, cn)
				}
			})
		})
	}
}

// BenchmarkPoolGetPutWithMinIdle benchmarks pool operations with MinIdleConns
func BenchmarkPoolGetPutWithMinIdle(b *testing.B) {
	ctx := context.Background()

	configs := []struct {
		poolSize     int
		minIdleConns int
	}{
		{8, 2},
		{16, 4},
		{32, 8},
		{64, 16},
	}

	for _, config := range configs {
		b.Run(fmt.Sprintf("Pool_%d_MinIdle_%d", config.poolSize, config.minIdleConns), func(b *testing.B) {
			connPool := pool.NewConnPool(&pool.Options{
				Dialer:          dummyDialer,
				PoolSize:        int32(config.poolSize),
				MinIdleConns:    int32(config.minIdleConns),
				PoolTimeout:     time.Second,
				DialTimeout:     time.Second,
				ConnMaxIdleTime: time.Hour,
			})
			defer connPool.Close()

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					connPool.Put(ctx, cn)
				}
			})
		})
	}
}

// BenchmarkPoolConcurrentGetPut benchmarks pool under high concurrency
func BenchmarkPoolConcurrentGetPut(b *testing.B) {
	ctx := context.Background()

	connPool := pool.NewConnPool(&pool.Options{
		Dialer:          dummyDialer,
		PoolSize:        int32(32),
		PoolTimeout:     time.Second,
		DialTimeout:     time.Second,
		ConnMaxIdleTime: time.Hour,
		MinIdleConns:    int32(0),
	})
	defer connPool.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Test with different levels of concurrency
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32, 64}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			b.SetParallelism(concurrency)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					connPool.Put(ctx, cn)
				}
			})
		})
	}
}

// =============================================================================
// PUBSUB BENCHMARKS
// =============================================================================

// benchmarkClient creates a Redis client for benchmarking with mock dialer
func benchmarkClient(poolSize int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "localhost:6379", // Mock address
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     poolSize,
		MinIdleConns: 0, // Start with no idle connections for consistent benchmarks
	})
}

// BenchmarkPubSubCreation benchmarks PubSub creation and subscription
func BenchmarkPubSubCreation(b *testing.B) {
	ctx := context.Background()

	poolSizes := []int{1, 4, 8, 16, 32}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			client := benchmarkClient(poolSize)
			defer client.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				pubsub := client.Subscribe(ctx, "test-channel")
				pubsub.Close()
			}
		})
	}
}

// BenchmarkPubSubPatternCreation benchmarks PubSub pattern subscription
func BenchmarkPubSubPatternCreation(b *testing.B) {
	ctx := context.Background()

	poolSizes := []int{1, 4, 8, 16, 32}

	for _, poolSize := range poolSizes {
		b.Run(fmt.Sprintf("PoolSize_%d", poolSize), func(b *testing.B) {
			client := benchmarkClient(poolSize)
			defer client.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				pubsub := client.PSubscribe(ctx, "test-*")
				pubsub.Close()
			}
		})
	}
}

// BenchmarkPubSubConcurrentCreation benchmarks concurrent PubSub creation
func BenchmarkPubSubConcurrentCreation(b *testing.B) {
	ctx := context.Background()
	client := benchmarkClient(32)
	defer client.Close()

	concurrencyLevels := []int{1, 2, 4, 8, 16}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			semaphore := make(chan struct{}, concurrency)

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				semaphore <- struct{}{}

				go func() {
					defer wg.Done()
					defer func() { <-semaphore }()

					pubsub := client.Subscribe(ctx, "test-channel")
					pubsub.Close()
				}()
			}

			wg.Wait()
		})
	}
}

// BenchmarkPubSubMultipleChannels benchmarks subscribing to multiple channels
func BenchmarkPubSubMultipleChannels(b *testing.B) {
	ctx := context.Background()
	client := benchmarkClient(16)
	defer client.Close()

	channelCounts := []int{1, 5, 10, 25, 50, 100}

	for _, channelCount := range channelCounts {
		b.Run(fmt.Sprintf("Channels_%d", channelCount), func(b *testing.B) {
			// Prepare channel names
			channels := make([]string, channelCount)
			for i := 0; i < channelCount; i++ {
				channels[i] = fmt.Sprintf("channel-%d", i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				pubsub := client.Subscribe(ctx, channels...)
				pubsub.Close()
			}
		})
	}
}

// BenchmarkPubSubReuse benchmarks reusing PubSub connections
func BenchmarkPubSubReuse(b *testing.B) {
	ctx := context.Background()
	client := benchmarkClient(16)
	defer client.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Benchmark just the creation and closing of PubSub connections
		// This simulates reuse patterns without requiring actual Redis operations
		pubsub := client.Subscribe(ctx, fmt.Sprintf("test-channel-%d", i))
		pubsub.Close()
	}
}

// =============================================================================
// COMBINED BENCHMARKS
// =============================================================================

// BenchmarkPoolAndPubSubMixed benchmarks mixed pool stats and PubSub operations
func BenchmarkPoolAndPubSubMixed(b *testing.B) {
	ctx := context.Background()
	client := benchmarkClient(32)
	defer client.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Mix of pool stats collection and PubSub creation
			if pb.Next() {
				// Pool stats operation
				stats := client.PoolStats()
				_ = stats.Hits + stats.Misses // Use the stats to prevent optimization
			}

			if pb.Next() {
				// PubSub operation
				pubsub := client.Subscribe(ctx, "test-channel")
				pubsub.Close()
			}
		}
	})
}

// BenchmarkPoolStatsCollection benchmarks pool statistics collection
func BenchmarkPoolStatsCollection(b *testing.B) {
	client := benchmarkClient(16)
	defer client.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stats := client.PoolStats()
		_ = stats.Hits + stats.Misses + stats.Timeouts // Use the stats to prevent optimization
	}
}

// BenchmarkPoolHighContention tests pool performance under high contention
func BenchmarkPoolHighContention(b *testing.B) {
	ctx := context.Background()
	client := benchmarkClient(32)
	defer client.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// High contention Get/Put operations
			pubsub := client.Subscribe(ctx, "test-channel")
			pubsub.Close()
		}
	})
}
