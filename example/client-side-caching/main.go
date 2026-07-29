// Example: use Redis server-assisted client-side caching with a standalone
// go-redis client.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cached := redis.NewClient(&redis.Options{
		Addr:     addr,
		Protocol: 3,
		DB:       0,
		ClientSideCacheConfig: &redis.ClientSideCacheConfig{
			MaxEntries: 1_000,
		},
	})
	defer cached.Close()

	// Use a separate client to show that a write from another connection
	// invalidates the cached value.
	writer := redis.NewClient(&redis.Options{Addr: addr})
	defer writer.Close()

	if err := cached.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect cached client: %v", err)
	}
	if err := writer.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect writer: %v", err)
	}

	const key = "example:csc:greeting"
	if err := writer.Set(ctx, key, "hello", 0).Err(); err != nil {
		log.Fatalf("set initial value: %v", err)
	}

	first := get(ctx, cached, key)  // Cache miss: fetched from Redis.
	second := get(ctx, cached, key) // Cache hit: served from local memory.
	hits, misses := cached.CSCStats()

	fmt.Printf("first read: %s\n", first)
	fmt.Printf("second read: %s\n", second)
	fmt.Printf("cache stats: %d hit, %d miss\n", hits, misses)

	if err := writer.Set(ctx, key, "hello again", 0).Err(); err != nil {
		log.Fatalf("update value: %v", err)
	}

	// Invalidation notifications are processed asynchronously. Wait until the
	// cache observes the update rather than relying on a fixed sleep.
	deadline := time.Now().Add(2 * time.Second)
	for {
		value := get(ctx, cached, key)
		if value == "hello again" {
			fmt.Printf("after invalidation: %s\n", value)
			break
		}
		if time.Now().After(deadline) {
			log.Fatal("timed out waiting for cache invalidation")
		}
		time.Sleep(time.Millisecond)
	}
}

func get(ctx context.Context, client *redis.Client, key string) string {
	value, err := client.Get(ctx, key).Result()
	if err != nil {
		log.Fatalf("get %q: %v", key, err)
	}
	return value
}
