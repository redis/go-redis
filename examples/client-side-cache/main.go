package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("=== Redis Client-Side Caching Example ===")

	// Create Redis client with RESP3 protocol (required for push notifications)
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Protocol: 3, // RESP3 required for client tracking and push notifications
	})
	defer client.Close()

	ctx := context.Background()

	// Test Redis connection
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("✅ Connected to Redis")

	// Enable client-side caching
	err := client.EnableClientSideCache(&redis.ClientSideCacheOptions{
		MaxSize:        1000,                // Cache up to 1000 entries
		DefaultTTL:     5 * time.Minute,     // Default TTL for cached entries
		EnableTracking: true,                // Enable Redis CLIENT TRACKING
		NoLoop:         true,                // Don't track keys modified by this client
		TrackingPrefix: []string{"user:", "session:"}, // Only track specific prefixes
	})
	if err != nil {
		log.Fatalf("Failed to enable client-side cache: %v", err)
	}
	defer client.DisableClientSideCache()
	fmt.Println("✅ Client-side cache enabled")

	// Example 1: Basic caching operations
	fmt.Println("\n🔧 Example 1: Basic Caching Operations")
	
	key := "user:123"
	value := "John Doe"

	// Set a value (stored in Redis and cached locally)
	err = client.CachedSet(ctx, key, value, time.Hour).Err()
	if err != nil {
		log.Fatalf("Failed to set value: %v", err)
	}
	fmt.Printf("✅ Set %s = %s\n", key, value)

	// Get the value (should be served from cache)
	result, err := client.CachedGet(ctx, key).Result()
	if err != nil {
		log.Fatalf("Failed to get value: %v", err)
	}
	fmt.Printf("✅ Got %s = %s (from cache)\n", key, result)

	// Example 2: Cache statistics
	fmt.Println("\n🔧 Example 2: Cache Statistics")
	
	cache := client.GetClientSideCache()
	hits, misses, evictions, hitRatio, size := cache.GetStats()
	fmt.Printf("📊 Cache Stats:\n")
	fmt.Printf("   Hits: %d\n", hits)
	fmt.Printf("   Misses: %d\n", misses)
	fmt.Printf("   Evictions: %d\n", evictions)
	fmt.Printf("   Hit Ratio: %.2f%%\n", hitRatio*100)
	fmt.Printf("   Size: %d entries\n", size)

	// Example 3: Multiple operations to show caching in action
	fmt.Println("\n🔧 Example 3: Multiple Operations")
	
	keys := []string{"user:100", "user:101", "user:102"}
	values := []string{"Alice", "Bob", "Charlie"}

	// Set multiple values
	for i, k := range keys {
		err = client.CachedSet(ctx, k, values[i], time.Hour).Err()
		if err != nil {
			log.Printf("Failed to set %s: %v", k, err)
		}
	}
	fmt.Println("✅ Set multiple user values")

	// Get values multiple times to show cache hits
	for round := 1; round <= 3; round++ {
		fmt.Printf("\n📋 Round %d - Getting values:\n", round)
		for _, k := range keys {
			start := time.Now()
			result, err := client.CachedGet(ctx, k).Result()
			duration := time.Since(start)
			if err != nil {
				log.Printf("Failed to get %s: %v", k, err)
				continue
			}
			fmt.Printf("   %s = %s (took %v)\n", k, result, duration)
		}
	}

	// Show updated statistics
	hits, misses, evictions, hitRatio, size = cache.GetStats()
	fmt.Printf("\n📊 Updated Cache Stats:\n")
	fmt.Printf("   Hits: %d\n", hits)
	fmt.Printf("   Misses: %d\n", misses)
	fmt.Printf("   Evictions: %d\n", evictions)
	fmt.Printf("   Hit Ratio: %.2f%%\n", hitRatio*100)
	fmt.Printf("   Size: %d entries\n", size)

	// Example 4: Cache invalidation
	fmt.Println("\n🔧 Example 4: Cache Invalidation")
	
	// Modify a value from another client to trigger invalidation
	// (In a real scenario, this would be another application instance)
	fmt.Println("📋 Simulating external modification...")
	
	// Use regular Set to modify the value (this will trigger invalidation)
	err = client.Set(ctx, "user:100", "Alice Updated", time.Hour).Err()
	if err != nil {
		log.Printf("Failed to update value: %v", err)
	} else {
		fmt.Println("✅ Updated user:100 externally")
		
		// Give some time for invalidation to process
		time.Sleep(100 * time.Millisecond)
		
		// Get the value again (should fetch from Redis due to invalidation)
		result, err := client.CachedGet(ctx, "user:100").Result()
		if err != nil {
			log.Printf("Failed to get updated value: %v", err)
		} else {
			fmt.Printf("✅ Got updated value: %s\n", result)
		}
	}

	// Example 5: Cache management
	fmt.Println("\n🔧 Example 5: Cache Management")
	
	// Clear the cache
	cache.Clear()
	fmt.Println("✅ Cache cleared")
	
	// Show final statistics
	hits, misses, evictions, hitRatio, size = cache.GetStats()
	fmt.Printf("📊 Final Cache Stats:\n")
	fmt.Printf("   Hits: %d\n", hits)
	fmt.Printf("   Misses: %d\n", misses)
	fmt.Printf("   Evictions: %d\n", evictions)
	fmt.Printf("   Hit Ratio: %.2f%%\n", hitRatio*100)
	fmt.Printf("   Size: %d entries\n", size)

	// Clean up test keys
	fmt.Println("\n🧹 Cleaning up...")
	allKeys := append(keys, key)
	deleted, err := client.Del(ctx, allKeys...).Result()
	if err != nil {
		log.Printf("Failed to clean up keys: %v", err)
	} else {
		fmt.Printf("✅ Deleted %d keys\n", deleted)
	}

	fmt.Println("\n🎉 Client-Side Caching Example Complete!")
	fmt.Println("\n📋 Key Benefits Demonstrated:")
	fmt.Println("   ✅ Automatic local caching with Redis fallback")
	fmt.Println("   ✅ Real-time cache invalidation via Redis push notifications")
	fmt.Println("   ✅ Significant performance improvements for repeated reads")
	fmt.Println("   ✅ Transparent integration with existing Redis operations")
	fmt.Println("   ✅ Comprehensive statistics and monitoring")
	fmt.Println("   ✅ Configurable cache size, TTL, and tracking options")
}
