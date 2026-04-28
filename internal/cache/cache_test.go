package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLocalCache_SetGet(t *testing.T) {
	cache := New(Config{MaxEntries: 16})

	if ok := cache.Set("get:foo", []string{"foo"}, []byte("bar")); !ok {
		t.Fatal("Set should cache entry")
	}

	val, ok := cache.Get(context.Background(), "get:foo")
	if !ok {
		t.Fatal("Get should return cached value")
	}
	if string(val) != "bar" {
		t.Fatalf("Get value mismatch: got %q want %q", string(val), "bar")
	}
}

func TestLocalCache_DeleteByCacheKey(t *testing.T) {
	cache := New(Config{MaxEntries: 16})
	cache.Set("get:a", []string{"a"}, []byte("1"))
	cache.Set("get:b", []string{"b"}, []byte("2"))

	if !cache.DeleteByCacheKey("get:a") {
		t.Fatal("DeleteByCacheKey should return true for existing key")
	}
	if cache.DeleteByCacheKey("get:a") {
		t.Fatal("DeleteByCacheKey should return false for missing key")
	}
	if _, ok := cache.Get(context.Background(), "get:a"); ok {
		t.Fatal("Deleted cache key should be absent")
	}
	if _, ok := cache.Get(context.Background(), "get:b"); !ok {
		t.Fatal("Unrelated cache key should still exist")
	}
}

func TestLocalCache_DeleteByRedisKey_MultiKey(t *testing.T) {
	cache := New(Config{MaxEntries: 16})
	cache.Set("mget:a:b", []string{"a", "b"}, []byte("ab"))
	cache.Set("mget:b:c", []string{"b", "c"}, []byte("bc"))
	cache.Set("get:d", []string{"d"}, []byte("d"))

	removed := cache.DeleteByRedisKey("b")
	if removed != 2 {
		t.Fatalf("DeleteByRedisKey removed mismatch: got %d want %d", removed, 2)
	}
	if _, ok := cache.Get(context.Background(), "mget:a:b"); ok {
		t.Fatal("entry linked to b should be removed")
	}
	if _, ok := cache.Get(context.Background(), "mget:b:c"); ok {
		t.Fatal("entry linked to b should be removed")
	}
	if _, ok := cache.Get(context.Background(), "get:d"); !ok {
		t.Fatal("entry unrelated to b should remain")
	}
}

func TestLocalCache_Flush(t *testing.T) {
	cache := New(Config{MaxEntries: 16})
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
	cache := New(Config{MaxEntries: 2})

	if !cache.Set("k1", []string{"k1"}, []byte("v1")) {
		t.Fatal("failed to set k1")
	}
	if !cache.Set("k2", []string{"k2"}, []byte("v2")) {
		t.Fatal("failed to set k2")
	}

	if _, ok := cache.Get(context.Background(), "k1"); !ok {
		t.Fatal("k1 should exist and become MRU")
	}

	if !cache.Set("k3", []string{"k3"}, []byte("v3")) {
		t.Fatal("failed to set k3")
	}

	if _, ok := cache.Get(context.Background(), "k2"); ok {
		t.Fatal("k2 should be evicted as LRU")
	}
	if _, ok := cache.Get(context.Background(), "k1"); !ok {
		t.Fatal("k1 should remain")
	}
	if _, ok := cache.Get(context.Background(), "k3"); !ok {
		t.Fatal("k3 should remain")
	}
}

func TestLocalCache_Evicts_ByMaxMemory(t *testing.T) {
	cache := New(Config{
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

	if _, ok := cache.Get(context.Background(), "a"); ok {
		t.Fatal("a should be evicted by memory limit")
	}
	if _, ok := cache.Get(context.Background(), "b"); !ok {
		t.Fatal("b should remain cached")
	}

	if cache.Set("big", []string{"big"}, []byte("123456")) { // 6 > 5
		t.Fatal("oversized entry should not be cached")
	}
	if _, ok := cache.Get(context.Background(), "big"); ok {
		t.Fatal("oversized entry should not exist")
	}
}

func TestLocalCache_ReserveFulfill_WaitsOnInProgress(t *testing.T) {
	cache := New(Config{MaxEntries: 8})
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
		v, ok := cache.Get(context.Background(), "get:wait")
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
	cache := New(Config{MaxEntries: 8})
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
	if _, ok := cache.Get(context.Background(), "get:foo"); ok {
		t.Fatal("Entry should remain absent")
	}
}

func TestLocalCache_ConcurrentAccess(t *testing.T) {
	cache := New(Config{
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
					cache.Get(context.Background(), cacheKey)
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

func TestLocalCache_Get_ReturnsOnContextCancel(t *testing.T) {
	cache := New(Config{MaxEntries: 8})
	token, shouldFetch := cache.Reserve("get:abandoned", []string{"abandoned"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should create in-progress entry")
	}

	// Do NOT call Fulfill or Cancel — simulate an abandoned reservation.

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	type result struct {
		val []byte
		ok  bool
	}
	done := make(chan result, 1)

	go func() {
		v, ok := cache.Get(ctx, "get:abandoned")
		done <- result{val: v, ok: ok}
	}()

	select {
	case got := <-done:
		if got.ok {
			t.Fatalf("Get should return false on context cancellation, got val=%q", got.val)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Get should have returned within 500ms after context timeout, but blocked")
	}

	// Clean up the abandoned placeholder.
	cache.Cancel("get:abandoned", token)
}

func TestLocalCache_Reserve_TakeoverRecoversStalePlaceholder(t *testing.T) {
	// Use a very short stale timeout so the test triggers takeover quickly.
	cache := New(Config{MaxEntries: 8, StaleTimeout: 10 * time.Millisecond})

	token1, shouldFetch1 := cache.Reserve("get:x", []string{"x"})
	if !shouldFetch1 || token1 == 0 {
		t.Fatal("first Reserve should return token and shouldFetch=true")
	}

	// Immediately, second Reserve should NOT take over (entry is fresh).
	token2, shouldFetch2 := cache.Reserve("get:x", []string{"x"})
	if shouldFetch2 {
		t.Fatal("second Reserve should return shouldFetch=false while entry is fresh")
	}
	if token2 != 0 {
		t.Fatalf("second Reserve should return token=0 for fresh IN_PROGRESS, got %d", token2)
	}

	// Wait for the stale timeout to expire.
	time.Sleep(20 * time.Millisecond)

	// Now Reserve should take over the stale placeholder.
	token3, shouldFetch3 := cache.Reserve("get:x", []string{"x"})
	if !shouldFetch3 {
		t.Fatal("third Reserve should return shouldFetch=true (stale takeover)")
	}
	if token3 == 0 {
		t.Fatal("third Reserve should return a non-zero token")
	}
	if token3 == token1 {
		t.Fatal("third Reserve should return a different token than the first")
	}

	// Old token is now invalid — Fulfill with it must fail.
	if cache.Fulfill("get:x", token1, []byte("stale")) {
		t.Fatal("Fulfill with old token should fail after takeover")
	}

	// New token should work.
	if !cache.Fulfill("get:x", token3, []byte("good")) {
		t.Fatal("Fulfill with takeover token should succeed")
	}

	val, ok := cache.Get(context.Background(), "get:x")
	if !ok || string(val) != "good" {
		t.Fatalf("Get should return fulfilled value, got %q ok=%v", string(val), ok)
	}
}

func TestLocalCache_Get_NilContext(t *testing.T) {
	cache := New(Config{MaxEntries: 8})
	cache.Set("get:nilctx", []string{"nilctx"}, []byte("value"))

	// nil context must not panic on cache hit.
	val, ok := cache.Get(nil, "get:nilctx")
	if !ok || string(val) != "value" {
		t.Fatalf("Get(nil, ...) on hit should return value, got %q ok=%v", string(val), ok)
	}

	// nil context must not panic on cache miss.
	_, ok = cache.Get(nil, "get:nonexistent")
	if ok {
		t.Fatal("Get(nil, ...) on miss should return false")
	}
}

func TestLocalCache_ConcurrentReserveFulfill_NoHijack(t *testing.T) {
	// Default stale timeout (5s) ensures no takeover during this fast test.
	cache := New(Config{MaxEntries: 8})

	const goroutines = 10
	const cacheKey = "get:race"
	const redisKey = "race"
	const correctValue = "correct-value"

	type reserveResult struct {
		token       uint64
		shouldFetch bool
	}

	results := make(chan reserveResult, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// All goroutines race to Reserve the same key.
	// With time-gated takeover, only the first should win; rest see fresh IN_PROGRESS.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			tok, sf := cache.Reserve(cacheKey, []string{redisKey})
			results <- reserveResult{token: tok, shouldFetch: sf}
		}()
	}

	wg.Wait()
	close(results)

	var winners []reserveResult
	var losers []reserveResult
	for r := range results {
		if r.shouldFetch {
			winners = append(winners, r)
		} else {
			losers = append(losers, r)
		}
	}

	if len(winners) != 1 {
		t.Fatalf("expected exactly 1 winner with shouldFetch=true, got %d", len(winners))
	}
	winner := winners[0]
	if winner.token == 0 {
		t.Fatal("winner should have a non-zero token")
	}

	// All losers must have token=0.
	for i, l := range losers {
		if l.token != 0 {
			t.Fatalf("loser[%d] should have token=0, got %d", i, l.token)
		}
	}

	// Winner fulfills with the correct value.
	if !cache.Fulfill(cacheKey, winner.token, []byte(correctValue)) {
		t.Fatal("winner Fulfill should succeed")
	}

	// All goroutines should now read the correct value.
	var getWg sync.WaitGroup
	getWg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer getWg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			val, ok := cache.Get(ctx, cacheKey)
			if !ok {
				t.Error("Get should return cached value after Fulfill")
				return
			}
			if string(val) != correctValue {
				t.Errorf("Get returned %q, want %q", string(val), correctValue)
			}
		}()
	}
	getWg.Wait()
}
