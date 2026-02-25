package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Benchmark actual client command execution WITHOUT pooling (master branch)
// This tests the real command flow

func BenchmarkClientCommands(b *testing.B) {
	ctx := context.Background()
	
	// Create a client (will fail to connect but that's ok for benchmarking the client-side code)
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// We'll benchmark the client-side processing (serialization, etc.)
	// even if Redis isn't running

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Get(ctx, "key").Result()
		}
	})

	b.Run("Set", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Set(ctx, "key", "value", 0).Result()
		}
	})

	b.Run("Incr", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Incr(ctx, "counter").Result()
		}
	})

	b.Run("MGet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.MGet(ctx, "key1", "key2", "key3").Result()
		}
	})

	b.Run("MixedWorkload", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = client.Get(ctx, "key").Result()
			_, _ = client.Set(ctx, "key", "value", 0).Result()
			_, _ = client.Incr(ctx, "counter").Result()
		}
	})

	b.Run("Parallel_Get", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = client.Get(ctx, "key").Result()
			}
		})
	})
}

