package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func BenchmarkClientCommands(b *testing.B) {
	opt := &redis.Options{
		Addr:         ":6379",
		PoolSize:     128,
		MinIdleConns: 32,
	}
	client := redis.NewClient(opt)
	defer client.Close()

	ctx := context.Background()

	// Warmup - ensure connection is established
	if err := client.Ping(ctx).Err(); err != nil {
		b.Skipf("Redis not available: %v", err)
	}

	b.Run("Get", func(b *testing.B) {
		client.Set(ctx, "bench:get:key", "value", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			client.Get(ctx, "bench:get:key")
		}
	})

	b.Run("Incr", func(b *testing.B) {
		client.Set(ctx, "bench:incr:key", "0", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			client.Incr(ctx, "bench:incr:key")
		}
	})

	b.Run("Set", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			client.Set(ctx, "bench:set:key", "value", 0)
		}
	})

	b.Run("Set_WithTTL", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			client.Set(ctx, "bench:set:key", "value", time.Hour)
		}
	})

	b.Run("MGet", func(b *testing.B) {
		client.Set(ctx, "bench:mget:key1", "value1", 0)
		client.Set(ctx, "bench:mget:key2", "value2", 0)
		client.Set(ctx, "bench:mget:key3", "value3", 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			client.MGet(ctx, "bench:mget:key1", "bench:mget:key2", "bench:mget:key3")
		}
	})

	b.Run("Parallel_Get", func(b *testing.B) {
		client.Set(ctx, "bench:parallel:key", "value", 0)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				client.Get(ctx, "bench:parallel:key")
			}
		})
	})

	b.Run("Parallel_Set", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				client.Set(ctx, "bench:parallel:key", "value", 0)
			}
		})
	})
}

