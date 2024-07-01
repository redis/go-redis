package redis_test

import (
	"log"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Testredis.NewCache tests the creation of a new cache instance
func TestNewCache(t *testing.T) {
	_, err := redis.NewCache(1000, 1<<20) // 1 MB size
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	t.Log("Cache created successfully")
}

// TestCacheSetAndGet tests setting and getting values in the cache
func TestCacheSetAndGet(t *testing.T) {
	cache, err := redis.NewCache(1000, 1<<20)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	key, value := "key1", "value1"
	setSuccess := cache.Set(key, value, 1)
	if !setSuccess {
		t.Fatalf("Failed to set key: %s", key)
	}
	log.Printf("Set operation successful for key: %s", key)

	// Allow value to pass through buffers
	time.Sleep(10 * time.Millisecond)

	getValue, found := cache.Get(key)
	if !found || getValue != value {
		t.Errorf("Failed to get key: %s, expected value: %s, got: %v", key, value, getValue)
	} else {
		log.Printf("Get operation successful for key: %s", key)
	}
}

// TestCacheClearKey tests the clearing of a specific key from the cache
func TestCacheClearKey(t *testing.T) {
	cache, err := redis.NewCache(1000, 1<<20)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	key := "key1"
	cache.Set(key, "value1", 1)
	log.Printf("Key %s set in cache", key)

	time.Sleep(10 * time.Millisecond)

	cache.ClearKey(key)
	log.Printf("Key %s cleared from cache", key)

	time.Sleep(10 * time.Millisecond)

	_, found := cache.Get(key)
	if found {
		t.Errorf("Expected key %s to be cleared", key)
	} else {
		log.Printf("ClearKey operation successful for key: %s", key)
	}
}

// TestCacheClear tests clearing all keys from the cache
func TestCacheClear(t *testing.T) {
	cache, err := redis.NewCache(1000, 1<<20)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	key := "key1"
	cache.Set(key, "value1", 1)
	log.Printf("Key %s set in cache", key)

	time.Sleep(10 * time.Millisecond)

	cache.Clear()
	t.Log("All keys cleared from cache")

	time.Sleep(10 * time.Millisecond)

	_, found := cache.Get(key)
	if found {
		t.Errorf("Expected cache to be cleared, but key %s was found", key)
	} else {
		t.Log("Clear operation successful, cache is empty")
	}
}
