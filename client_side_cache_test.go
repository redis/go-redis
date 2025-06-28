package redis

import (
	"context"
	"testing"
	"time"
)

func TestClientSideCache(t *testing.T) {
	t.Run("NewClientSideCache", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3, // Required for push notifications
		})
		defer client.Close()

		// Test with default options
		cache, err := NewClientSideCache(client, nil)
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		if cache == nil {
			t.Error("NewClientSideCache should return a non-nil cache")
		}
		if cache.client != client {
			t.Error("Cache should reference the provided client")
		}
		if cache.maxSize != 10000 {
			t.Errorf("Expected default maxSize 10000, got %d", cache.maxSize)
		}
		if cache.defaultTTL != 5*time.Minute {
			t.Errorf("Expected default TTL 5 minutes, got %v", cache.defaultTTL)
		}
	})

	t.Run("NewClientSideCacheWithOptions", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		opts := &ClientSideCacheOptions{
			MaxSize:                5000,
			DefaultTTL:             10 * time.Minute,
			EnableTracking:         true,
			NoLoop:                 false,
			TrackingPrefix:         []string{"user:", "session:"},
			InvalidationBufferSize: 500,
		}

		cache, err := NewClientSideCache(client, opts)
		if err != nil {
			t.Fatalf("Failed to create client-side cache with options: %v", err)
		}
		defer cache.Close()

		if cache.maxSize != 5000 {
			t.Errorf("Expected maxSize 5000, got %d", cache.maxSize)
		}
		if cache.defaultTTL != 10*time.Minute {
			t.Errorf("Expected TTL 10 minutes, got %v", cache.defaultTTL)
		}
	})

	t.Run("CacheOperations", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		cache, err := NewClientSideCache(client, &ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Minute,
			EnableTracking: false, // Disable tracking for unit tests
		})
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()

		// Test cache miss and Redis fallback
		key := "test:cache:key1"
		value := "test_value"

		// First get should be a cache miss
		_, misses, _, _, _ := cache.GetStats()
		initialMisses := misses

		cmd := cache.Get(ctx, key)
		if cmd.Err() == nil {
			// If Redis is available and key exists, verify it's cached
			hits, misses, _, _, size := cache.GetStats()
			if misses <= initialMisses {
				t.Error("Expected cache miss on first get")
			}
			if size > 0 && cmd.Err() == nil {
				// Second get should be a cache hit
				cmd2 := cache.Get(ctx, key)
				if cmd2.Err() != nil {
					t.Errorf("Second get failed: %v", cmd2.Err())
				}
				hits2, _, _, _, _ := cache.GetStats()
				if hits2 <= hits {
					t.Error("Expected cache hit on second get")
				}
			}
		}

		// Test Set operation
		setCmd := cache.Set(ctx, key, value, time.Hour)
		if setCmd.Err() == nil {
			// Verify value is cached locally
			if cachedValue, found := cache.getFromCache(key); found {
				if cachedValue != value {
					t.Errorf("Expected cached value %s, got %v", value, cachedValue)
				}
			}
		}

		// Test Del operation
		delCmd := cache.Del(ctx, key)
		if delCmd.Err() == nil {
			// Verify value is removed from cache
			if _, found := cache.getFromCache(key); found {
				t.Error("Key should be removed from cache after Del")
			}
		}
	})

	t.Run("CacheEviction", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		cache, err := NewClientSideCache(client, &ClientSideCacheOptions{
			MaxSize:        2, // Small cache for testing eviction
			DefaultTTL:     1 * time.Hour,
			EnableTracking: false,
		})
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		// Fill cache beyond capacity
		cache.setInCache("key1", "value1", time.Hour)
		cache.setInCache("key2", "value2", time.Hour)
		cache.setInCache("key3", "value3", time.Hour) // Should trigger eviction

		// Check that cache size doesn't exceed maxSize
		_, _, _, _, size := cache.GetStats()
		if size > 2 {
			t.Errorf("Cache size %d exceeds maxSize 2", size)
		}

		// Check that eviction occurred
		_, _, evictions, _, _ := cache.GetStats()
		if evictions == 0 {
			t.Error("Expected at least one eviction")
		}
	})

	t.Run("CacheExpiration", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		cache, err := NewClientSideCache(client, &ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Hour,
			EnableTracking: false,
		})
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		// Set entry with short TTL
		cache.setInCache("expiring_key", "value", 1*time.Millisecond)

		// Wait for expiration
		time.Sleep(10 * time.Millisecond)

		// Try to get expired entry
		if value, found := cache.getFromCache("expiring_key"); found {
			t.Errorf("Expected expired entry to be removed, but found: %v", value)
		}
	})

	t.Run("InvalidationHandler", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		cache, err := NewClientSideCache(client, &ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Hour,
			EnableTracking: false,
		})
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		// Create invalidation handler
		handler := &clientSideCacheInvalidationHandler{cache: cache}

		// Add some entries to cache
		cache.setInCache("key1", "value1", time.Hour)
		cache.setInCache("key2", "value2", time.Hour)

		// Test invalidation notification
		ctx := context.Background()
		notification := []interface{}{"invalidate", []interface{}{"key1", "key2"}}

		handled := handler.HandlePushNotification(ctx, notification)
		if !handled {
			t.Error("Handler should return true for valid invalidation notification")
		}

		// Give some time for async processing
		time.Sleep(10 * time.Millisecond)

		// Verify keys are removed from cache
		if _, found := cache.getFromCache("key1"); found {
			t.Error("key1 should be invalidated")
		}
		if _, found := cache.getFromCache("key2"); found {
			t.Error("key2 should be invalidated")
		}
	})

	t.Run("CacheStats", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		cache, err := NewClientSideCache(client, &ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Hour,
			EnableTracking: false,
		})
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		// Initial stats
		hits, misses, evictions, hitRatio, size := cache.GetStats()
		if hits != 0 || misses != 0 || evictions != 0 || hitRatio != 0 || size != 0 {
			t.Error("Initial stats should be zero")
		}

		// Generate some cache activity
		cache.getFromCache("nonexistent") // miss
		cache.setInCache("key1", "value1", time.Hour)
		cache.getFromCache("key1") // hit

		hits, misses, evictions, hitRatio, size = cache.GetStats()
		if hits != 1 {
			t.Errorf("Expected 1 hit, got %d", hits)
		}
		if misses != 1 {
			t.Errorf("Expected 1 miss, got %d", misses)
		}
		if size != 1 {
			t.Errorf("Expected cache size 1, got %d", size)
		}
		if hitRatio != 0.5 {
			t.Errorf("Expected hit ratio 0.5, got %f", hitRatio)
		}
	})

	t.Run("CacheClear", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		cache, err := NewClientSideCache(client, &ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Hour,
			EnableTracking: false,
		})
		if err != nil {
			t.Fatalf("Failed to create client-side cache: %v", err)
		}
		defer cache.Close()

		// Add entries
		cache.setInCache("key1", "value1", time.Hour)
		cache.setInCache("key2", "value2", time.Hour)

		// Verify entries exist
		_, _, _, _, size := cache.GetStats()
		if size != 2 {
			t.Errorf("Expected cache size 2, got %d", size)
		}

		// Clear cache
		cache.Clear()

		// Verify cache is empty
		_, _, _, _, size = cache.GetStats()
		if size != 0 {
			t.Errorf("Expected cache size 0 after clear, got %d", size)
		}
	})
}

func TestClientSideCacheIntegration(t *testing.T) {
	t.Run("EnableDisableClientSideCache", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		// Initially no cache
		if cache := client.GetClientSideCache(); cache != nil {
			t.Error("Client should not have cache initially")
		}

		// Enable cache
		err := client.EnableClientSideCache(nil)
		if err != nil {
			t.Fatalf("Failed to enable client-side cache: %v", err)
		}

		// Verify cache is enabled
		if cache := client.GetClientSideCache(); cache == nil {
			t.Error("Client should have cache after enabling")
		}

		// Try to enable again (should fail)
		err = client.EnableClientSideCache(nil)
		if err == nil {
			t.Error("Enabling cache twice should return error")
		}

		// Disable cache
		err = client.DisableClientSideCache()
		if err != nil {
			t.Errorf("Failed to disable client-side cache: %v", err)
		}

		// Verify cache is disabled
		if cache := client.GetClientSideCache(); cache != nil {
			t.Error("Client should not have cache after disabling")
		}

		// Disable again (should not error)
		err = client.DisableClientSideCache()
		if err != nil {
			t.Errorf("Disabling cache twice should not error: %v", err)
		}
	})

	t.Run("CachedOperations", func(t *testing.T) {
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		ctx := context.Background()

		// Test operations without cache (should fallback to regular operations)
		cmd1 := client.CachedGet(ctx, "test:key")
		cmd2 := client.CachedSet(ctx, "test:key", "value", time.Hour)
		cmd3 := client.CachedDel(ctx, "test:key")

		// These should work the same as regular operations
		if cmd1 == nil || cmd2 == nil || cmd3 == nil {
			t.Error("Cached operations should return valid commands even without cache")
		}

		// Enable cache
		err := client.EnableClientSideCache(&ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Hour,
			EnableTracking: false, // Disable for unit tests
		})
		if err != nil {
			t.Fatalf("Failed to enable client-side cache: %v", err)
		}
		defer client.DisableClientSideCache()

		// Test operations with cache
		cmd4 := client.CachedGet(ctx, "test:key2")
		cmd5 := client.CachedSet(ctx, "test:key2", "value2", time.Hour)
		cmd6 := client.CachedDel(ctx, "test:key2")

		if cmd4 == nil || cmd5 == nil || cmd6 == nil {
			t.Error("Cached operations should return valid commands with cache enabled")
		}

		// Verify cache is being used
		cache := client.GetClientSideCache()
		if cache == nil {
			t.Error("Cache should be available")
		}
	})

	t.Run("CacheWithRealRedis", func(t *testing.T) {
		// This test requires a real Redis instance
		client := NewClient(&Options{
			Addr:     "localhost:6379",
			Protocol: 3,
		})
		defer client.Close()

		// Test connection
		ctx := context.Background()
		if err := client.Ping(ctx).Err(); err != nil {
			t.Skip("Redis not available, skipping integration test")
		}

		// Enable cache
		err := client.EnableClientSideCache(&ClientSideCacheOptions{
			MaxSize:        100,
			DefaultTTL:     1 * time.Minute,
			EnableTracking: true, // Enable tracking for real Redis
			NoLoop:         true,
		})
		if err != nil {
			t.Fatalf("Failed to enable client-side cache: %v", err)
		}
		defer client.DisableClientSideCache()

		// Test key
		key := "test:client_side_cache:integration"
		value := "test_value_123"

		// Clean up
		client.Del(ctx, key)

		// Set value
		err = client.CachedSet(ctx, key, value, time.Hour).Err()
		if err != nil {
			t.Fatalf("Failed to set value: %v", err)
		}

		// Get value (should be cached)
		result, err := client.CachedGet(ctx, key).Result()
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		if result != value {
			t.Errorf("Expected value %s, got %s", value, result)
		}

		// Verify cache stats
		cache := client.GetClientSideCache()
		hits, misses, _, hitRatio, size := cache.GetStats()
		if size == 0 {
			t.Error("Cache should contain entries")
		}
		if hits+misses == 0 {
			t.Error("Cache should have some activity")
		}

		t.Logf("Cache stats: hits=%d, misses=%d, hitRatio=%.2f, size=%d", hits, misses, hitRatio, size)

		// Clean up
		client.Del(ctx, key)
	})
}
