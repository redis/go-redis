package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// PubSub Pool Benchmark Suite
//
// This file contains comprehensive benchmarks for PubSub pool operations.
// PubSub pools have different characteristics than regular pools:
// - Connections are never pooled (always created fresh)
// - Connections are always removed (never reused)
// - No pool size limits apply
// - Focus on creation/destruction performance
//
// Usage Examples:
//   # Run all PubSub benchmarks
//   go test -bench=BenchmarkPubSub -run='^$' internal/pool/pubsub_bench_test.go internal/pool/main_test.go
//
//   # Run specific benchmark
//   go test -bench=BenchmarkPubSubGetRemove -run='^$' internal/pool/pubsub_bench_test.go internal/pool/main_test.go
//
//   # Compare with regular pool benchmarks
//   go test -bench=. -run='^$' internal/pool/

type pubsubGetRemoveBenchmark struct {
	name string
}

func (bm pubsubGetRemoveBenchmark) String() string {
	return bm.name
}

// BenchmarkPubSubGetRemove benchmarks the core PubSub pool operation:
// Get a connection and immediately remove it (PubSub connections are never pooled)
func BenchmarkPubSubGetRemove(b *testing.B) {
	ctx := context.Background()
	benchmarks := []pubsubGetRemoveBenchmark{
		{"sequential"},
		{"parallel"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			pubsubPool := pool.NewPubSubPool(&pool.Config{
				DialTimeout:     1 * time.Second,
				ConnMaxIdleTime: time.Hour,
			}, dummyDialer)
			defer pubsubPool.Close()

			b.ResetTimer()
			b.ReportAllocs()

			if bm.name == "parallel" {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						cn, err := pubsubPool.Get(ctx)
						if err != nil {
							b.Fatal(err)
						}
						pubsubPool.Remove(ctx, cn, nil)
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					cn, err := pubsubPool.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					pubsubPool.Remove(ctx, cn, nil)
				}
			}
		})
	}
}

// BenchmarkPubSubConcurrentAccess benchmarks concurrent access patterns
// typical in PubSub scenarios with multiple subscribers
func BenchmarkPubSubConcurrentAccess(b *testing.B) {
	ctx := context.Background()
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("goroutines=%d", concurrency), func(b *testing.B) {
			pubsubPool := pool.NewPubSubPool(&pool.Config{
				DialTimeout:     1 * time.Second,
				ConnMaxIdleTime: time.Hour,
			}, dummyDialer)
			defer pubsubPool.Close()

			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / concurrency
			if opsPerGoroutine == 0 {
				opsPerGoroutine = 1
			}

			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < opsPerGoroutine; j++ {
						cn, err := pubsubPool.Get(ctx)
						if err != nil {
							b.Error(err)
							return
						}
						pubsubPool.Remove(ctx, cn, nil)
					}
				}()
			}
			wg.Wait()
		})
	}
}

// BenchmarkPubSubConnectionLifecycle benchmarks the full lifecycle
// of PubSub connections including creation, usage simulation, and cleanup
func BenchmarkPubSubConnectionLifecycle(b *testing.B) {
	ctx := context.Background()
	pubsubPool := pool.NewPubSubPool(&pool.Config{
		DialTimeout:     1 * time.Second,
		ConnMaxIdleTime: time.Hour,
	}, dummyDialer)
	defer pubsubPool.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Get connection
			cn, err := pubsubPool.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}

			// Simulate some work (minimal to focus on pool overhead)
			cn.SetUsable(true)

			// Remove connection (PubSub connections are never pooled)
			pubsubPool.Remove(ctx, cn, nil)
		}
	})
}

// BenchmarkPubSubStats benchmarks statistics collection performance
func BenchmarkPubSubStats(b *testing.B) {
	ctx := context.Background()
	pubsubPool := pool.NewPubSubPool(&pool.Config{
		DialTimeout:     1 * time.Second,
		ConnMaxIdleTime: time.Hour,
	}, dummyDialer)
	defer pubsubPool.Close()

	// Pre-create some connections to have meaningful stats
	var connections []*pool.Conn
	for i := 0; i < 10; i++ {
		cn, err := pubsubPool.Get(ctx)
		if err != nil {
			b.Fatal(err)
		}
		connections = append(connections, cn)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = pubsubPool.Stats()
	}

	// Cleanup
	for _, cn := range connections {
		pubsubPool.Remove(ctx, cn, nil)
	}
}

// BenchmarkPubSubVsRegularPool compares PubSub pool performance
// with regular pool performance for similar operations
func BenchmarkPubSubVsRegularPool(b *testing.B) {
	ctx := context.Background()

	b.Run("PubSubPool", func(b *testing.B) {
		pubsubPool := pool.NewPubSubPool(&pool.Config{
			DialTimeout:     1 * time.Second,
			ConnMaxIdleTime: time.Hour,
		}, dummyDialer)
		defer pubsubPool.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			cn, err := pubsubPool.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			pubsubPool.Remove(ctx, cn, nil)
		}
	})

	b.Run("RegularPool", func(b *testing.B) {
		connPool := pool.NewConnPool(&pool.Options{
			Dialer:          dummyDialer,
			PoolSize:        1, // Small pool to force creation/removal
			PoolTimeout:     time.Second,
			DialTimeout:     1 * time.Second,
			ConnMaxIdleTime: time.Hour,
		})
		defer connPool.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			cn, err := connPool.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			connPool.Remove(ctx, cn, nil) // Force removal like PubSub
		}
	})
}

// BenchmarkPubSubMemoryUsage benchmarks memory allocation patterns
func BenchmarkPubSubMemoryUsage(b *testing.B) {
	ctx := context.Background()
	pubsubPool := pool.NewPubSubPool(&pool.Config{
		DialTimeout:     1 * time.Second,
		ConnMaxIdleTime: time.Hour,
	}, dummyDialer)
	defer pubsubPool.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Focus on memory allocations
	for i := 0; i < b.N; i++ {
		cn, err := pubsubPool.Get(ctx)
		if err != nil {
			b.Fatal(err)
		}
		pubsubPool.Put(ctx, cn) // Put calls Remove internally for PubSub
	}
}

// BenchmarkPubSubBurstLoad benchmarks handling of burst connection requests
// typical in PubSub scenarios where many subscribers connect simultaneously
func BenchmarkPubSubBurstLoad(b *testing.B) {
	ctx := context.Background()
	burstSizes := []int{10, 50, 100, 500}

	for _, burstSize := range burstSizes {
		b.Run(fmt.Sprintf("burst=%d", burstSize), func(b *testing.B) {
			pubsubPool := pool.NewPubSubPool(&pool.Config{
				DialTimeout:     1 * time.Second,
				ConnMaxIdleTime: time.Hour,
			}, dummyDialer)
			defer pubsubPool.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				var connections []*pool.Conn
				var mu sync.Mutex

				// Simulate burst of connections
				for j := 0; j < burstSize; j++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						cn, err := pubsubPool.Get(ctx)
						if err != nil {
							b.Error(err)
							return
						}
						mu.Lock()
						connections = append(connections, cn)
						mu.Unlock()
					}()
				}
				wg.Wait()

				// Clean up all connections
				for _, cn := range connections {
					pubsubPool.Remove(ctx, cn, nil)
				}
			}
		})
	}
}

// BenchmarkPubSubLongRunning benchmarks long-running PubSub connections
// that stay active for extended periods
func BenchmarkPubSubLongRunning(b *testing.B) {
	ctx := context.Background()
	pubsubPool := pool.NewPubSubPool(&pool.Config{
		DialTimeout:     1 * time.Second,
		ConnMaxIdleTime: time.Hour,
	}, dummyDialer)
	defer pubsubPool.Close()

	// Create long-running connections
	var connections []*pool.Conn
	for i := 0; i < 100; i++ {
		cn, err := pubsubPool.Get(ctx)
		if err != nil {
			b.Fatal(err)
		}
		connections = append(connections, cn)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark operations while long-running connections exist
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Create short-lived connection while long-running ones exist
			cn, err := pubsubPool.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			pubsubPool.Remove(ctx, cn, nil)
		}
	})

	// Cleanup long-running connections
	for _, cn := range connections {
		pubsubPool.Remove(ctx, cn, nil)
	}
}

// BenchmarkPubSubErrorHandling benchmarks error scenarios
func BenchmarkPubSubErrorHandling(b *testing.B) {
	ctx := context.Background()

	// Create a pool that will be closed to trigger errors
	pubsubPool := pool.NewPubSubPool(&pool.Config{
		DialTimeout:     1 * time.Second,
		ConnMaxIdleTime: time.Hour,
	}, dummyDialer)

	// Close the pool to trigger error conditions
	pubsubPool.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// This should return an error since pool is closed
		_, err := pubsubPool.Get(ctx)
		if err == nil {
			b.Fatal("Expected error from closed pool")
		}
	}
}
