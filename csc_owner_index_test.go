package redis

import (
	"context"
	"net"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestLocalCache_FulfillOwned_EvictByConn(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner, ok := cache.(connCacheOwner)
	if !ok {
		t.Fatal("localCache must implement connCacheOwner")
	}

	// Two entries owned by conn 1, one by conn 2.
	for _, kv := range []struct {
		key    string
		connID uint64
	}{{"get:a", 1}, {"get:b", 1}, {"get:c", 2}} {
		tok, sf := cache.Reserve(kv.key, []string{kv.key})
		if !sf {
			t.Fatalf("Reserve(%s) should fetch", kv.key)
		}
		if !owner.FulfillOwned(kv.key, tok, kv.connID, []byte("v")) {
			t.Fatalf("FulfillOwned(%s) failed", kv.key)
		}
	}

	if n := owner.EvictByConn(1); n != 2 {
		t.Fatalf("EvictByConn(1) removed %d, want 2", n)
	}
	if _, ok := cache.Get(context.Background(), "get:a"); ok {
		t.Fatal("conn-1 entry a should be evicted")
	}
	if _, ok := cache.Get(context.Background(), "get:b"); ok {
		t.Fatal("conn-1 entry b should be evicted")
	}
	if _, ok := cache.Get(context.Background(), "get:c"); !ok {
		t.Fatal("conn-2 entry c must survive")
	}
	// Idempotent: evicting again removes nothing.
	if n := owner.EvictByConn(1); n != 0 {
		t.Fatalf("second EvictByConn(1) removed %d, want 0", n)
	}
}

func TestLocalCache_OwnerIndexCleanedOnInvalidation(t *testing.T) {
	// The owning-conn index must be cleaned when an entry is removed for other
	// reasons (invalidation, LRU) so a later EvictByConn can't touch a re-used
	// cache key and the index cannot leak.
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := cache.(connCacheOwner)
	lc := cache.(*localCache)

	tok, _ := cache.Reserve("get:k", []string{"rk"})
	if !owner.FulfillOwned("get:k", tok, 7, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	// Invalidate via the redis key; the owner index for conn 7 must be gone.
	if n := cache.DeleteByRedisKey("rk"); n != 1 {
		t.Fatalf("DeleteByRedisKey removed %d, want 1", n)
	}
	shard := lc.shardFor("get:k")
	shard.mu.RLock()
	_, present := shard.byConnID[7]
	shard.mu.RUnlock()
	if present {
		t.Fatal("byConnID must be cleaned when the entry is invalidated")
	}
	if n := owner.EvictByConn(7); n != 0 {
		t.Fatalf("EvictByConn(7) after invalidation removed %d, want 0", n)
	}
}

// TestCSCEvictOnRemoveHook: the pool OnRemove hook must evict exactly the
// removed connection's owned entries and leave others intact.
func TestCSCEvictOnRemoveHook(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := cache.(connCacheOwner)

	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()
	id := cn.GetID()

	tok, _ := cache.Reserve("get:owned", []string{"owned"})
	if !owner.FulfillOwned("get:owned", tok, id, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	tok2, _ := cache.Reserve("get:other", []string{"other"})
	if !owner.FulfillOwned("get:other", tok2, id+1000, []byte("v")) {
		t.Fatal("FulfillOwned (other conn) failed")
	}

	hook := &cscEvictOnRemoveHook{evictor: owner}
	hook.OnRemove(context.Background(), cn, nil)

	if _, ok := cache.Get(context.Background(), "get:owned"); ok {
		t.Fatal("removed conn's entry must be evicted by OnRemove")
	}
	if _, ok := cache.Get(context.Background(), "get:other"); !ok {
		t.Fatal("another conn's entry must survive OnRemove")
	}
}

// TestFulfillCached_RaceWithConnRemoval: if the owning connection is removed
// around the fulfill (its OnRemove eviction ran before the entry existed),
// fulfillCached must drop the orphaned entry rather than leave it resident with
// no invalidation coverage (the Window-2 TOCTOU).
func TestFulfillCached_RaceWithConnRemoval(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache.(connCacheOwner)}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	// Model "conn 42 was just removed" by seeding the ring directly (no need for
	// a real conn whose GetID == 42): its OnRemove eviction ran before the entry
	// existed, so nothing was evicted yet.
	hook.mu.Lock()
	hook.recent[0] = connID
	hook.ridx = 1
	hook.mu.Unlock()

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	if c.fulfillCached("get:k", tok, connID, []byte("v")) {
		// FulfillOwned returns true (it did store), but the entry must then be
		// evicted; fulfillCached returns FulfillOwned's result, so true is ok
		// only if the entry is gone afterwards. Assert the eviction below.
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry owned by a just-removed conn must not remain resident")
	}
}

// TestFulfillCached_NoHookUsesPlainFulfill: without an evict-on-remove hook
// (e.g. Broadcast, or a pooler without hooks), fulfillCached must fall back to
// plain Fulfill and still cache the value.
func TestFulfillCached_NoHookUsesPlainFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache} // cscPoolHook nil

	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !c.fulfillCached("get:k", tok, 7, []byte("v")) {
		t.Fatal("fulfillCached should store via plain Fulfill when no hook")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); !ok {
		t.Fatal("value should be cached")
	}
}
