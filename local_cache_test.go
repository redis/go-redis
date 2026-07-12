package redis

import (
	"context"
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

	val, ok := cache.Get(context.Background(), "get:foo")
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
	if _, ok := cache.Get(context.Background(), "get:a"); ok {
		t.Fatal("Deleted cache key should be absent")
	}
	if _, ok := cache.Get(context.Background(), "get:b"); !ok {
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
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:a", []string{"a"}, []byte("1"))
	cache.Set("get:b", []string{"b"}, []byte("2"))
	token, shouldFetch := cache.Reserve("get:c", []string{"c"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should create in-progress entry")
	}

	// Flush removes IN_PROGRESS placeholders too: invalidations can arrive on
	// a different TCP stream than the pending reply (BCAST sidecar, background
	// drainer), so a fetch spanning the flush may hold a pre-flush value.
	removed := cache.Flush()
	if removed != 3 {
		t.Fatalf("Flush removed mismatch: got %d want %d", removed, 3)
	}
	if cache.Len() != 0 {
		t.Fatalf("Len after Flush mismatch: got %d want %d", cache.Len(), 0)
	}

	if cache.Fulfill("get:c", token, []byte("3")) {
		t.Fatal("Fulfill must fail after Flush removed the placeholder")
	}
	if _, ok := cache.Get(context.Background(), "get:c"); ok {
		t.Fatal("value fulfilled across a flush must not be served")
	}
}

func TestLocalCache_DeleteByRedisKey_PoisonsInProgress(t *testing.T) {
	// Invalidations can arrive on a different TCP stream than the in-flight
	// reply (BCAST sidecar, background drainer), so a racing fetch may hold a
	// PRE-invalidation value. The placeholder must be removed so Fulfill
	// fails and the stale value is never published.
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:a", []string{"shared"}, []byte("1"))
	token, shouldFetch := cache.Reserve("get:b", []string{"shared"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should create in-progress entry")
	}

	removed := cache.DeleteByRedisKey("shared")
	if removed != 2 {
		t.Fatalf("DeleteByRedisKey removed mismatch: got %d want %d (valid entry + placeholder)", removed, 2)
	}
	if _, ok := cache.Get(context.Background(), "get:a"); ok {
		t.Fatal("valid entry should be removed")
	}

	if cache.Fulfill("get:b", token, []byte("2")) {
		t.Fatal("Fulfill must fail after the placeholder was invalidated")
	}
	if _, ok := cache.Get(context.Background(), "get:b"); ok {
		t.Fatal("invalidated in-flight fetch must not be served")
	}
}

func TestLocalCache_Flush_PoisonsInProgress(t *testing.T) {
	// Same contract for Flush (FLUSHDB invalidation, sidecar outage
	// teardown): a fetch spanning the flush must not publish its value.
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	token, shouldFetch := cache.Reserve("get:k", []string{"k"})
	if !shouldFetch || token == 0 {
		t.Fatal("Reserve should create in-progress entry")
	}

	if removed := cache.Flush(); removed != 1 {
		t.Fatalf("Flush removed mismatch: got %d want 1 (the placeholder)", removed)
	}
	if cache.Fulfill("get:k", token, []byte("v")) {
		t.Fatal("Fulfill must fail after Flush removed the placeholder")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("value fulfilled across a flush must not be served")
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

func TestLocalCache_GetTouch_SecondChanceEviction(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 3})

	for _, k := range []string{"a", "b", "c"} {
		if !cache.Set(k, []string{k}, []byte(k)) {
			t.Fatalf("failed to set %s", k)
		}
	}

	// Reads record recency via an atomic token (no write lock); eviction must
	// honor those touches and evict the untouched entry instead.
	if _, ok := cache.Get(context.Background(), "a"); !ok {
		t.Fatal("a should exist")
	}
	if _, ok := cache.Get(context.Background(), "b"); !ok {
		t.Fatal("b should exist")
	}

	if !cache.Set("d", []string{"d"}, []byte("d")) {
		t.Fatal("failed to set d")
	}

	if _, ok := cache.Get(context.Background(), "c"); ok {
		t.Fatal("c should be evicted as least recently used")
	}
	for _, k := range []string{"a", "b", "d"} {
		if _, ok := cache.Get(context.Background(), k); !ok {
			t.Fatalf("%s should remain cached", k)
		}
	}
}

func TestLocalCache_ConcurrentGetsDuringEviction(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("k%d", i%16)
				cache.Set(key, []string{key}, []byte("v"))
				cache.Get(context.Background(), key)
			}
		}(g)
	}
	wg.Wait()

	if n := cache.Len(); n > 8 {
		t.Fatalf("cache exceeded MaxEntries: %d", n)
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
	if _, ok := cache.Get(context.Background(), "get:foo"); ok {
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
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
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

func TestLocalCache_Get_TimesOutOnAbandonedReservation(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 4, StaleTimeout: 50 * time.Millisecond})

	if _, shouldFetch := cache.Reserve("k", []string{"k"}); !shouldFetch {
		t.Fatal("Reserve should grant the fetch")
	}
	// Fetcher "dies" without Fulfill/Cancel; the waiter must unblock after
	// the stale window even with an unbounded context.
	start := time.Now()
	if _, ok := cache.Get(context.Background(), "k"); ok {
		t.Fatal("Get should miss on an abandoned reservation")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Get blocked for %v, want ~StaleTimeout (50ms)", elapsed)
	}
}

func TestLocalCache_Reserve_TakeoverRecoversStalePlaceholder(t *testing.T) {
	// Use a very short stale timeout so the test triggers takeover quickly.
	cache := NewLocalCache(CacheConfig{MaxEntries: 8, StaleTimeout: 10 * time.Millisecond})

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
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
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
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})

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

func TestLocalCache_MaxStalenessBackstop(t *testing.T) {
	ctx := context.Background()

	// With MaxStaleness set, a fresh entry hits; one older than the window misses
	// and is evicted so the next access re-fetches (else Reserve suppresses it).
	c := NewLocalCache(CacheConfig{MaxStaleness: 30 * time.Millisecond})
	if !c.Set("k", []string{"rk"}, []byte("v")) {
		t.Fatal("Set failed")
	}
	if v, ok := c.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("fresh entry should hit: v=%q ok=%v", v, ok)
	}
	time.Sleep(45 * time.Millisecond)
	if _, ok := c.Get(ctx, "k"); ok {
		t.Fatal("entry past MaxStaleness should miss")
	}
	if n := c.Len(); n != 0 {
		t.Fatalf("stale entry should be evicted, got Len=%d", n)
	}

	// MaxStaleness disabled (zero) must not expire entries by age.
	c2 := NewLocalCache(CacheConfig{})
	c2.Set("k", []string{"rk"}, []byte("v"))
	time.Sleep(20 * time.Millisecond)
	if _, ok := c2.Get(ctx, "k"); !ok {
		t.Fatal("MaxStaleness=0 must not expire entries by age")
	}
}

func TestLocalCache_UnboundedConfigGetsDefaultLimit(t *testing.T) {
	// Both MaxEntries and MaxMemoryBytes unlimited: the cache must still be
	// bounded (MaxEntries defaults to defaultCacheMaxEntries).
	cache := NewLocalCache(CacheConfig{})
	for i := 0; i < defaultCacheMaxEntries+100; i++ {
		cache.Set(fmt.Sprintf("get:k%d", i), []string{fmt.Sprintf("k%d", i)}, []byte("v"))
	}
	if n := cache.Len(); n > defaultCacheMaxEntries {
		t.Fatalf("unbounded config must default to %d entries, got Len=%d",
			defaultCacheMaxEntries, n)
	}
}

func TestLocalCache_ShardedMaxEntriesIsGlobalCap(t *testing.T) {
	// MaxEntries above the sharding threshold spreads over 16 shards; the
	// per-shard caps must sum to exactly MaxEntries so total residency never
	// exceeds the configured limit (ceil-per-shard allowed 112 for 100).
	const maxEntries = 100
	cache := NewLocalCache(CacheConfig{MaxEntries: maxEntries})

	lc := cache.(*localCache)
	if lc.shardCount == 1 {
		t.Fatalf("test expects a sharded cache, got 1 shard")
	}
	sum := 0
	for i := range lc.shards {
		sum += lc.shards[i].maxEntries
	}
	if sum != maxEntries {
		t.Fatalf("per-shard caps must sum to MaxEntries: got %d want %d", sum, maxEntries)
	}

	for i := 0; i < 20*maxEntries; i++ {
		cache.Set(fmt.Sprintf("get:k%d", i), []string{fmt.Sprintf("k%d", i)}, []byte("v"))
	}
	if n := cache.Len(); n > maxEntries {
		t.Fatalf("total entries exceed MaxEntries: got %d want <= %d", n, maxEntries)
	}
}

func TestLocalCache_ShardedMaxMemoryIsGlobalCap(t *testing.T) {
	// Per-shard byte caps must also sum to exactly MaxMemoryBytes.
	const maxBytes = 1<<20 + 13 // not divisible by 16
	cache := NewLocalCache(CacheConfig{MaxMemoryBytes: maxBytes})

	lc := cache.(*localCache)
	var sum int64
	for i := range lc.shards {
		sum += lc.shards[i].maxMemoryBytes
	}
	if sum != maxBytes {
		t.Fatalf("per-shard byte caps must sum to MaxMemoryBytes: got %d want %d", sum, maxBytes)
	}
}

func TestLocalCache_PerShardByteCeilingPinned(t *testing.T) {
	// Pins the documented limitation: the cache is 16-way sharded and each
	// shard admits at most MaxMemoryBytes/16, so a 100KiB entry is rejected
	// under a 1MiB budget. If admission becomes global, update the
	// MaxMemoryBytes doc comment and CacheAdmissionRejects guidance.
	cache := NewLocalCache(CacheConfig{MaxMemoryBytes: 1 << 20})
	if ok := cache.Set("get:big", []string{"big"}, make([]byte, 100<<10)); ok {
		t.Fatal("per-shard byte cap should reject a 100KiB entry (1MiB/16 shards); docs now stale")
	}
	if ok := cache.Set("get:small", []string{"small"}, make([]byte, 4<<10)); !ok {
		t.Fatal("a 4KiB entry must be admitted under the per-shard cap")
	}
}

func TestLocalCache_ReserveHonorsCapWithoutEvictingPeers(t *testing.T) {
	// A shard full of IN_PROGRESS placeholders must stay within MaxEntries, but
	// a new Reserve must NOT abort a peer's in-flight fetch: the overflow
	// reservation is rejected (token 0, shouldFetch true) while every earlier
	// placeholder survives and can still be fulfilled.
	const maxEntries = 4 // below the sharding threshold: single shard
	cache := NewLocalCache(CacheConfig{MaxEntries: maxEntries})

	tokens := make([]uint64, maxEntries+1)
	for i := 0; i <= maxEntries; i++ {
		key := fmt.Sprintf("get:k%d", i)
		token, shouldFetch := cache.Reserve(key, []string{fmt.Sprintf("k%d", i)})
		if !shouldFetch {
			t.Fatalf("Reserve(%s) should fetch", key)
		}
		tokens[i] = token
	}

	if n := cache.Len(); n > maxEntries {
		t.Fatalf("placeholders exceeded MaxEntries: Len=%d cap=%d", n, maxEntries)
	}
	// The overflow (maxEntries-th) reservation was rejected: token 0, not cacheable.
	overflow := fmt.Sprintf("get:k%d", maxEntries)
	if tokens[maxEntries] != 0 {
		t.Fatalf("overflow Reserve should return token 0, got %d", tokens[maxEntries])
	}
	if cache.Fulfill(overflow, tokens[maxEntries], []byte("v")) {
		t.Fatal("Fulfill must fail for the rejected overflow reservation")
	}
	// Every earlier in-flight placeholder survived and can complete.
	for i := 0; i < maxEntries; i++ {
		key := fmt.Sprintf("get:k%d", i)
		if !cache.Fulfill(key, tokens[i], []byte("v")) {
			t.Fatalf("Fulfill(%s) must succeed: peer placeholder was wrongly evicted", key)
		}
	}
}
