package redis_test

import (
	"context"
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

// TestCacheSetKeyAndGetKey tests SetKeyting and GetKeyting values in the cache
func TestCacheSetKeyAndGetKey(t *testing.T) {
	cache, err := redis.NewCache(1000, 1<<20)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	key, value := "key1", "value1"
	SetKeySuccess := cache.SetKey(key, value, 1)
	if !SetKeySuccess {
		t.Fatalf("Failed to SetKey key: %s", key)
	}
	log.Printf("SetKey operation successful for key: %s", key)

	// Allow value to pass through buffers
	time.Sleep(10 * time.Millisecond)

	GetKeyValue, found := cache.GetKey(key)
	if !found || GetKeyValue != value {
		t.Errorf("Failed to GetKey key: %s, expected value: %s, got: %v", key, value, GetKeyValue)
	} else {
		log.Printf("GetKey operation successful for key: %s", key)
	}
}

// TestCacheClearKey tests the clearing of a specific key from the cache
func TestCacheClearKey(t *testing.T) {
	cache, err := redis.NewCache(1000, 1<<20)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	key := "key1"
	cache.SetKey(key, "value1", 1)
	log.Printf("Key %s SetKey in cache", key)

	time.Sleep(10 * time.Millisecond)

	cache.ClearKey(key)
	log.Printf("Key %s cleared from cache", key)

	time.Sleep(10 * time.Millisecond)

	_, found := cache.GetKey(key)
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
	cache.SetKey(key, "value1", 1)
	log.Printf("Key %s SetKey in cache", key)

	time.Sleep(10 * time.Millisecond)

	cache.Clear()
	t.Log("All keys cleared from cache")

	time.Sleep(10 * time.Millisecond)

	_, found := cache.GetKey(key)
	if found {
		t.Errorf("Expected cache to be cleared, but key %s was found", key)
	} else {
		t.Log("Clear operation successful, cache is empty")
	}
}

func TestSetCache(t *testing.T) {
	cache, err := redis.NewCache(1000, 1<<20)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: ":6379", CacheObject: cache})
	defer client.Close()
	ctx := context.Background()
	client.Ping(ctx)
	// TODO: fix this
	time.Sleep(1 * time.Millisecond)
	val, found := client.Options().CacheObject.GetKey("ping")
	if found {
		t.Log(val)
	} else {
		t.Error("Key not found")
	}
	ping := client.Ping(ctx)
	if ping.Val() == "PONG" {
		t.Log(ping.Val())
	} else {
		t.Error("Ping from cache failed")
	}
}
