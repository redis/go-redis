package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Benchmark for profiling allocations
func BenchmarkProfileAllocations(b *testing.B) {
	ctx := context.Background()
	
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Get(ctx, "key")
		}
	})

	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Set(ctx, "key", "value", 0)
		}
	})

	b.Run("Incr", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Incr(ctx, "counter")
		}
	})
}

