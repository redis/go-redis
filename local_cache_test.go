package redis

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLocalCache_SetGet(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})

	if ok := cache.Set("get:foo", []string{"foo"}, []byte("bar")); !ok {
		t.Fatal("Set should cache entry")
	}

	val, ok := cache.Get("get:foo")
	if !ok {
		t.Fatal("Get should return cached value")
	}
	if string(val) != "bar" {
		t.Fatalf("Get value mismatch: got %q want %q", string(val), "bar")
	}
}

func TestLocalCache_DeleteByCacheKey(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:a", []string{"a"}, []byte("1"))
	cache.Set("get:b", []string{"b"}, []byte("2"))

	if !cache.DeleteByCacheKey("get:a") {
		t.Fatal("DeleteByCacheKey should return true for existing key")
	}
	if cache.DeleteByCacheKey("get:a") {
		t.Fatal("DeleteByCacheKey should return false for missing key")
	}
	if _, ok := cache.Get("get:a"); ok {
		t.Fatal("Deleted cache key should be absent")
	}
	if _, ok := cache.Get("get:b"); !ok {
		t.Fatal("Unrelated cache key should still exist")
	}
}

func TestLocalCache_DeleteByRedisKey_MultiKey(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("mget:a:b", []string{"a", "b"}, []byte("ab"))
	cache.Set("mget:b:c", []string{"b", "c"}, []byte("bc"))
	cache.Set("get:d", []string{"d"}, []byte("d"))

	removed := cache.DeleteByRedisKey("b")
	if removed != 2 {
		t.Fatalf("DeleteByRedisKey removed mismatch: got %d want %d", removed, 2)
	}
	if _, ok := cache.Get("mget:a:b"); ok {
		t.Fatal("entry linked to b should be removed")
	}
	if _, ok := cache.Get("mget:b:c"); ok {
		t.Fatal("entry linked to b should be removed")
	}
	if _, ok := cache.Get("get:d"); !ok {
		t.Fatal("entry unrelated to b should remain")
	}
}

func TestLocalCache_Flush(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:a", []string{"a"}, []byte("1"))
	cache.Set("get:b", []string{"b"}, []byte("2"))
	token, shouldFetch := cache.Reserve("get:c", []string{"c"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should create in-progress entry")
	}

	removed := cache.Flush()
	if removed != 3 {
		t.Fatalf("Flush removed mismatch: got %d want %d", removed, 3)
	}
	if cache.Len() != 0 {
		t.Fatalf("Len after Flush mismatch: got %d want %d", cache.Len(), 0)
	}
}

func TestLocalCache_EvictsLRU_ByMaxEntries(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 2})

	if !cache.Set("k1", []string{"k1"}, []byte("v1")) {
		t.Fatal("failed to set k1")
	}
	if !cache.Set("k2", []string{"k2"}, []byte("v2")) {
		t.Fatal("failed to set k2")
	}

	if _, ok := cache.Get("k1"); !ok {
		t.Fatal("k1 should exist and become MRU")
	}

	if !cache.Set("k3", []string{"k3"}, []byte("v3")) {
		t.Fatal("failed to set k3")
	}

	if _, ok := cache.Get("k2"); ok {
		t.Fatal("k2 should be evicted as LRU")
	}
	if _, ok := cache.Get("k1"); !ok {
		t.Fatal("k1 should remain")
	}
	if _, ok := cache.Get("k3"); !ok {
		t.Fatal("k3 should remain")
	}
}

func TestLocalCache_Evicts_ByMaxMemory(t *testing.T) {
	cache := NewLocalCache(CacheConfig{
		MaxMemoryBytes: 5,
		Sizer: func(_ string, _ []string, value []byte) int64 {
			return int64(len(value))
		},
	})

	if !cache.Set("a", []string{"a"}, []byte("aaaa")) { // 4
		t.Fatal("failed to set a")
	}
	if !cache.Set("b", []string{"b"}, []byte("bb")) { // +2 => evict a
		t.Fatal("failed to set b")
	}

	if _, ok := cache.Get("a"); ok {
		t.Fatal("a should be evicted by memory limit")
	}
	if _, ok := cache.Get("b"); !ok {
		t.Fatal("b should remain cached")
	}

	if cache.Set("big", []string{"big"}, []byte("123456")) { // 6 > 5
		t.Fatal("oversized entry should not be cached")
	}
	if _, ok := cache.Get("big"); ok {
		t.Fatal("oversized entry should not exist")
	}
}

func TestLocalCache_ReserveFulfill_WaitsOnInProgress(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	token, shouldFetch := cache.Reserve("get:wait", []string{"wait"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should return token and shouldFetch=true for new entry")
	}

	type result struct {
		val string
		ok  bool
	}
	done := make(chan result, 1)

	go func() {
		v, ok := cache.Get("get:wait")
		done <- result{val: string(v), ok: ok}
	}()

	select {
	case got := <-done:
		t.Fatalf("Get should block while IN_PROGRESS, got early result: %#v", got)
	case <-time.After(60 * time.Millisecond):
	}

	if !cache.Fulfill("get:wait", token, []byte("ready")) {
		t.Fatal("Fulfill should succeed")
	}

	select {
	case got := <-done:
		if !got.ok || got.val != "ready" {
			t.Fatalf("Get result mismatch: %#v", got)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Get should unblock after Fulfill")
	}
}

func TestLocalCache_FulfillFailsAfterDelete(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	token, shouldFetch := cache.Reserve("get:foo", []string{"foo"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should create placeholder")
	}

	if !cache.DeleteByCacheKey("get:foo") {
		t.Fatal("DeleteByCacheKey should remove placeholder")
	}

	if cache.Fulfill("get:foo", token, []byte("bar")) {
		t.Fatal("Fulfill should fail after placeholder deletion")
	}
	if _, ok := cache.Get("get:foo"); ok {
		t.Fatal("Entry should remain absent")
	}
}

func TestLocalCache_ConcurrentAccess(t *testing.T) {
	cache := NewLocalCache(CacheConfig{
		MaxEntries: 64,
		Sizer: func(cacheKey string, redisKeys []string, value []byte) int64 {
			size := int64(len(cacheKey) + len(value))
			for _, key := range redisKeys {
				size += int64(len(key))
			}
			return size
		},
		MaxMemoryBytes: 4096,
	})

	const (
		workers    = 24
		iterations = 500
		keysCount  = 16
	)

	var wg sync.WaitGroup
	wg.Add(workers)

	for workerID := 0; workerID < workers; workerID++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("k:%d", (id+i)%keysCount)
				cacheKey := "get:" + key

				switch i % 4 {
				case 0:
					token, shouldFetch := cache.Reserve(cacheKey, []string{key})
					if shouldFetch && token != 0 {
						value := []byte(fmt.Sprintf("v:%d:%d", id, i))
						if i%9 == 0 {
							cache.Cancel(cacheKey, token)
						} else {
							cache.Fulfill(cacheKey, token, value)
						}
					}
				case 1:
					cache.Set(cacheKey, []string{key}, []byte(fmt.Sprintf("set:%d:%d", id, i)))
				case 2:
					cache.Get(cacheKey)
				case 3:
					cache.DeleteByRedisKey(key)
				}
			}
		}(workerID)
	}

	wg.Wait()

	if got := cache.Len(); got > 64 {
		t.Fatalf("Len exceeds MaxEntries: got %d limit %d", got, 64)
	}

	if used := cache.MemoryUsage(); used < 0 {
		t.Fatalf("MemoryUsage should not be negative: got %d", used)
	} else if used > 4096 {
		t.Fatalf("MemoryUsage exceeds MaxMemoryBytes: got %d limit %d", used, 4096)
	}
}
