package redis_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Test to trace allocations step by step
func TestTraceAllocations(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Warm up pools
	for i := 0; i < 100; i++ {
		_, _ = client.Get(ctx, "warmup")
	}

	// Force GC
	runtime.GC()

	// Measure allocations
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	_, _ = client.Get(ctx, "key")

	runtime.ReadMemStats(&m2)

	allocs := m2.Mallocs - m1.Mallocs
	bytes := m2.TotalAlloc - m1.TotalAlloc

	t.Logf("Allocations: %d", allocs)
	t.Logf("Bytes allocated: %d", bytes)
}

// Benchmark with allocation tracking
func BenchmarkGetWithAllocTrace(b *testing.B) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = client.Get(ctx, "key")
	}
}

