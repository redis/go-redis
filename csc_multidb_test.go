package redis_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// cscMultiDBAddr is the address used by the standalone CSC regression tests
// in this file. We bypass the Ginkgo harness (which assumes a CI cluster
// fixture) and connect directly so these tests run with just a single Redis
// instance available.
func cscMultiDBAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

// rawClientHasTrackingFlag returns true if the `flags=` field of `CLIENT INFO`
// includes 't' (tracking). We parse the raw line because the typed
// ClientInfo() decoder rejects fields a newer Redis server may emit
// (e.g. read-events).
func rawClientHasTrackingFlag(t *testing.T, c *redis.Client) bool {
	t.Helper()
	raw, err := c.Do(context.Background(), "client", "info").Text()
	if err != nil {
		t.Fatalf("CLIENT INFO: %v", err)
	}
	for _, kv := range strings.Fields(raw) {
		if strings.HasPrefix(kv, "flags=") {
			return strings.Contains(kv[6:], "t")
		}
	}
	return false
}

// TestCSCMultiDBRejected verifies that the canonical (Phase 1) CSC path
// refuses to enable when Options.DB != 0. Industry survey (redis-py /
// node-redis / Jedis / Lettuce) shows no consensus on how to handle
// multi-DB CSC: redis-py and node-redis silently allow desynchronisation,
// Jedis lets per-conn tracking break on close, and Lettuce explicitly
// forbids mid-session SELECT on tracked conns. To pick the safest stance
// we refuse to enable CSC unless DB == 0.
//
// CLIENT TRACKING is per-connection and bound to the DB the conn was on
// when tracking was enabled. A runtime SELECT changes the active DB but
// does not re-key the server's tracking table, so writes to a different
// DB silently produce stale cached reads.
func TestCSCMultiDBRejected(t *testing.T) {
	cache := redis.NewLocalCache(redis.CacheConfig{MaxEntries: 16})
	c := redis.NewClient(&redis.Options{
		Addr:            cscMultiDBAddr(),
		Protocol:        3,
		DB:              1,
		ClientSideCache: cache,
		PoolSize:        2,
	})
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", cscMultiDBAddr(), err)
	}

	// CLIENT TRACKING must NOT be enabled on the pool conn — the gate
	// fired in attachCSC and the cache is unused.
	if rawClientHasTrackingFlag(t, c) {
		t.Fatalf("CLIENT TRACKING must NOT be enabled for DB != 0")
	}

	// And the cache must remain empty after a SET/GET cycle.
	key := "csc-multidb-skip"
	if err := c.Set(ctx, key, "x", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := c.Get(ctx, key).Err(); err != nil {
		t.Fatalf("GET: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if cache.Len() != 0 {
		t.Fatalf("cache must remain empty when CSC is disabled by DB gate; got len=%d", cache.Len())
	}
}

// TestCSCReadYourWrites covers the canonical Phase 1 read-your-writes path:
// after a write to a tracked key, a subsequent roundtrip on the tracking
// conn must process the invalidate frame and the cache entry must be
// evicted. Phase 1's documented Window-1 staleness is that a hit served
// before the invalidate is consumed may be stale; the drain inside
// processCached (10µs peek) shrinks but does not eliminate this window.
// This test mirrors the Ginkgo pattern: PING after the mutator's write to
// force the invalidate through, then assert the cache has been evicted.
func TestCSCReadYourWrites(t *testing.T) {
	cache := redis.NewLocalCache(redis.CacheConfig{MaxEntries: 32})
	c := redis.NewClient(&redis.Options{
		Addr:            cscMultiDBAddr(),
		Protocol:        3,
		ClientSideCache: cache,
		PoolSize:        1, // pin all reads to the same tracking conn
		MaxRetries:      -1,
	})
	t.Cleanup(func() { _ = c.Close() })

	mutator := redis.NewClient(&redis.Options{Addr: cscMultiDBAddr()})
	t.Cleanup(func() { _ = mutator.Close() })

	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	if err := mutator.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}

	key := "csc-ryw"
	if err := mutator.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatalf("SET v1: %v", err)
	}
	if got := c.Get(ctx, key).Val(); got != "v1" {
		t.Fatalf("first GET: got %q want v1", got)
	}
	// Cache must hold the entry after Fulfill. The FLUSHDB above sends a
	// nil-payload invalidate to the tracked conn; if it races the first GET's
	// in-flight fetch, that fill is (correctly) suppressed — re-drive the GET
	// until the fill lands.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cache.Len() < 1 {
		time.Sleep(20 * time.Millisecond)
		if got := c.Get(ctx, key).Val(); got != "v1" {
			t.Fatalf("re-driven GET: got %q want v1", got)
		}
	}
	if cache.Len() < 1 {
		t.Fatalf("cache should hold the entry after first GET, len=%d", cache.Len())
	}

	// Mutate via a separate client. The tracking conn receives an
	// invalidate frame; we drive a PING roundtrip to consume it.
	if err := mutator.Set(ctx, key, "v2", 0).Err(); err != nil {
		t.Fatalf("SET v2: %v", err)
	}

	// Drive the tracking conn until the invalidate is consumed and the
	// cache entry is evicted. PING is a non-cacheable roundtrip so it
	// goes through processPendingPushNotificationWithReader.
	deadline = time.Now().Add(2 * time.Second)
	for cache.Len() != 0 {
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatalf("PING: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("invalidate never observed after PING storm: cache.Len=%d", cache.Len())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Final GET must return the fresh value.
	if got := c.Get(ctx, key).Val(); got != "v2" {
		t.Fatalf("post-invalidate GET: got %q want v2", got)
	}
}
