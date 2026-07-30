package redis_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Client-side cache (standalone)", func() {
	var (
		ctx     context.Context
		cache   *redis.LocalCache
		client  *redis.Client
		mutator *redis.Client
	)

	BeforeEach(func() {
		SkipBeforeRedisVersion("6.0", "CLIENT TRACKING requires Redis 6.0+")
		ctx = context.Background()

		cache = redis.NewLocalCache(redis.CacheConfig{MaxEntries: 128})

		// Flush BEFORE the tracked client exists: a FLUSHDB after construction
		// would push a nil-payload invalidate to the tracked connection and race
		// the first GET's fill, making cache-population assertions flaky.
		mutator = redis.NewClient(redisOptions())
		Expect(mutator.Ping(ctx).Err()).NotTo(HaveOccurred())
		skipIfClientTrackingUnavailable(ctx, mutator)
		Expect(mutator.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		opt := redisOptions()
		opt.Protocol = 3
		opt.ClientSideCache = cache
		client = redis.NewClient(opt)
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if mutator != nil {
			Expect(mutator.Close()).NotTo(HaveOccurred())
		}
		if client != nil {
			Expect(client.Close()).NotTo(HaveOccurred())
		}
	})

	It("populates the local cache after a cacheable command is issued", func() {
		key := "csc-e2e-populate"
		Expect(mutator.Set(ctx, key, "hello", 0).Err()).NotTo(HaveOccurred())
		Expect(cache.Len()).To(Equal(0))

		val, err := client.Get(ctx, key).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("hello"))

		Eventually(cache.Len, 2*time.Second, 50*time.Millisecond).
			Should(BeNumerically(">=", 1))
	})

	It("caches and invalidates negative lookups", func() {
		key := "csc-e2e-negative"

		Expect(client.Get(ctx, key).Err()).To(Equal(redis.Nil))
		Eventually(cache.Len, 2*time.Second, 50*time.Millisecond).
			Should(BeNumerically(">=", 1))

		Expect(client.Get(ctx, key).Err()).To(Equal(redis.Nil))
		Expect(mutator.Set(ctx, key, "created", 0).Err()).NotTo(HaveOccurred())

		Eventually(func() int {
			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
			return cache.Len()
		}, 2*time.Second, 50*time.Millisecond).Should(Equal(0))

		Expect(client.Get(ctx, key).Val()).To(Equal("created"))
	})

	It("removes the cached entry when another client mutates the key", func() {
		key := "csc-e2e-invalidate"
		Expect(mutator.Set(ctx, key, "v1", 0).Err()).NotTo(HaveOccurred())

		Expect(client.Get(ctx, key).Val()).To(Equal("v1"))
		Eventually(cache.Len, 2*time.Second, 50*time.Millisecond).
			Should(BeNumerically(">=", 1))

		Expect(mutator.Set(ctx, key, "v2", 0).Err()).NotTo(HaveOccurred())

		// The invalidation push notification is delivered on the next
		// interaction with the tracked connection. Drain it via PING and
		// then confirm the entry has been evicted.
		Eventually(func() int {
			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
			return cache.Len()
		}, 2*time.Second, 50*time.Millisecond).Should(Equal(0))

		Expect(client.Get(ctx, key).Val()).To(Equal("v2"))
	})

	It("flushes the entire local cache on FLUSHDB", func() {
		Expect(mutator.Set(ctx, "csc-e2e-a", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(mutator.Set(ctx, "csc-e2e-b", "2", 0).Err()).NotTo(HaveOccurred())

		Expect(client.Get(ctx, "csc-e2e-a").Err()).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "csc-e2e-b").Err()).NotTo(HaveOccurred())
		Eventually(cache.Len, 2*time.Second, 50*time.Millisecond).
			Should(BeNumerically(">=", 2))

		Expect(mutator.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		Eventually(func() int {
			Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
			return cache.Len()
		}, 2*time.Second, 50*time.Millisecond).Should(Equal(0))
	})

	It("evicts a retired connection's entries on ConnMaxLifetime so the next read is fresh", func() {
		// PoolSize 1 + a tiny ConnMaxLifetime: the single tracked connection is
		// retired via ConnPool.CloseConn (which bypasses the OnRemove pool hook).
		// Its close hook must evict its cached entries — otherwise, once its
		// server-side tracking is gone, a later external write is never seen.
		key := "csc-e2e-connmaxlifetime"
		Expect(mutator.Set(ctx, key, "v1", 0).Err()).NotTo(HaveOccurred())

		lc := redis.NewLocalCache(redis.CacheConfig{MaxEntries: 128})
		opt := redisOptions()
		opt.Protocol = 3
		opt.PoolSize = 1
		opt.ConnMaxLifetime = 100 * time.Millisecond
		opt.ClientSideCache = lc
		rotator := redis.NewClient(opt)
		defer func() { Expect(rotator.Close()).NotTo(HaveOccurred()) }()

		// Populate the cache on the single connection.
		Expect(rotator.Get(ctx, key).Val()).To(Equal("v1"))
		Eventually(lc.Len, 2*time.Second, 20*time.Millisecond).Should(BeNumerically(">=", 1))

		// Let the connection exceed ConnMaxLifetime, then force its retirement with
		// a PING (which dials a fresh connection). The retired conn's close hook
		// must have evicted its entry.
		time.Sleep(200 * time.Millisecond)
		Eventually(func() int {
			Expect(rotator.Ping(ctx).Err()).NotTo(HaveOccurred())
			return lc.Len()
		}, 2*time.Second, 20*time.Millisecond).Should(Equal(0))

		// The old connection's tracking is gone, so this write reaches no tracker.
		Expect(mutator.Set(ctx, key, "v2", 0).Err()).NotTo(HaveOccurred())

		// The next GET (on the fresh connection, cache empty) must observe v2.
		Expect(rotator.Get(ctx, key).Val()).To(Equal("v2"))
	})

	It("evicts cached entries when the tracked connection dies while reads stay cached", func() {
		key := "csc-e2e-dead-connection"
		Expect(mutator.Set(ctx, key, "v1", 0).Err()).NotTo(HaveOccurred())

		lc := redis.NewLocalCache(redis.CacheConfig{MaxEntries: 128})
		opt := redisOptions()
		opt.Protocol = 3
		opt.PoolSize = 1
		opt.ClientSideCache = lc
		tracked := redis.NewClient(opt)
		defer func() { Expect(tracked.Close()).NotTo(HaveOccurred()) }()

		Expect(tracked.Get(ctx, key).Val()).To(Equal("v1"))
		Eventually(lc.Len, 2*time.Second, 20*time.Millisecond).
			Should(BeNumerically(">=", 1))

		connID, err := tracked.ClientID(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		killed := mutator.ClientKillByFilter(ctx, "ID", strconv.FormatInt(connID, 10))
		Expect(killed.Err()).NotTo(HaveOccurred())
		Expect(killed.Val()).To(Equal(int64(1)))

		// Do not touch tracked before the mutation: the background drainer must
		// discover the dead idle socket and revoke its cache ownership itself.
		Expect(mutator.Set(ctx, key, "v2", 0).Err()).NotTo(HaveOccurred())
		Eventually(lc.Len, 2*time.Second, 20*time.Millisecond).Should(Equal(0))
		Expect(tracked.Get(ctx, key).Val()).To(Equal("v2"))
	})
})

var _ = Describe("Client-side cache strategies", func() {
	var (
		ctx     context.Context
		csc     *redis.Client
		mutator *redis.Client
	)

	BeforeEach(func() {
		SkipBeforeRedisVersion("6.0", "CLIENT TRACKING requires Redis 6.0+")
		ctx = context.Background()
		csc = nil

		mutator = redis.NewClient(redisOptions())
		Expect(mutator.Ping(ctx).Err()).NotTo(HaveOccurred())
		skipIfClientTrackingUnavailable(ctx, mutator)
		Expect(mutator.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if csc != nil {
			Expect(csc.Close()).NotTo(HaveOccurred())
		}
		if mutator != nil {
			Expect(mutator.Close()).NotTo(HaveOccurred())
		}
	})

	// roundTrip exercises the full CSC lifecycle: seed → first GET (miss, populates
	// the cache) → repeat GET (hit) → external write → invalidation propagates →
	// re-fetch observes the new value. Parameterized by strategy so the check
	// naturally extends if more strategies are added.
	roundTrip := func(strategy redis.CSCStrategy) {
		opt := redisOptions()
		opt.Protocol = 3
		opt.ClientSideCacheConfig = &redis.ClientSideCacheConfig{MaxEntries: 1024}
		opt.ClientSideCacheStrategy = strategy
		csc = redis.NewClient(opt)
		Expect(csc.Ping(ctx).Err()).NotTo(HaveOccurred())

		key := fmt.Sprintf("csc-strategy-e2e-%d", strategy)
		Expect(mutator.Set(ctx, key, "v1", 0).Err()).NotTo(HaveOccurred())

		// Repeated GETs: the first is a miss that populates the cache, a
		// subsequent one is served from it.
		Eventually(func() (uint64, error) {
			if err := csc.Get(ctx, key).Err(); err != nil {
				return 0, err
			}
			return csc.CSCStats().Hits, nil
		}, 2*time.Second, 20*time.Millisecond).Should(BeNumerically(">", 0),
			"expected at least one cache hit for strategy %d", strategy)

		// External write → invalidation → re-fetch must observe v2 within the
		// strategy's staleness bound. PING forces a connection interaction so
		// environments that hold push frames until the next command cycle still
		// drain the invalidate.
		Expect(mutator.Set(ctx, key, "v2", 0).Err()).NotTo(HaveOccurred())
		Eventually(func() (string, error) {
			if err := csc.Ping(ctx).Err(); err != nil {
				return "", err
			}
			return csc.Get(ctx, key).Result()
		}, 2*time.Second, 10*time.Millisecond).Should(Equal("v2"),
			"strategy %d still serving a stale value after the external write", strategy)
	}

	It("SharedTracking serves cache hits and observes invalidations", func() {
		roundTrip(redis.CSCStrategySharedTracking)
	})
})

// cscNativeAddr is the address used by the standalone CSC regression tests
// in this file. We bypass the Ginkgo harness (which assumes a CI cluster
// fixture) and connect directly so these tests run with just a single Redis
// instance available. REDIS_PORT (a bare port) is the env var the suite reads
// in main_test.go's BeforeSuite, so honor it as a best-effort way to follow
// the suite's server. (BeforeSuite currently overrides it back to the stack
// port, so the fallback below matches the suite's effective ":6379"; we read
// the env var rather than the suite's redisAddr package var because standard
// testing.T tests run in undefined order relative to BeforeSuite.)
// REDIS_ADDR remains as a full host:port override for local runs.
func cscNativeAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	if p := os.Getenv("REDIS_PORT"); p != "" {
		return "localhost:" + p
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

// TestCSCNonZeroDBRejected verifies that the canonical (Phase 1) CSC path
// refuses to enable when Options.DB != 0. Industry survey (redis-py /
// node-redis / Jedis / Lettuce) shows no consensus on how to handle
// CSC across logical databases: redis-py and node-redis silently allow
// desynchronisation, Jedis lets per-conn tracking break on close, and Lettuce
// explicitly forbids mid-session SELECT on tracked conns. To pick the safest
// stance we refuse to enable CSC unless DB == 0.
//
// CLIENT TRACKING is per-connection and bound to the DB the conn was on
// when tracking was enabled. A runtime SELECT changes the active DB but
// does not re-key the server's tracking table, so writes to a different
// DB silently produce stale cached reads.
func TestCSCNonZeroDBRejected(t *testing.T) {
	cache := redis.NewLocalCache(redis.CacheConfig{MaxEntries: 16})
	c := redis.NewClient(&redis.Options{
		Addr:            cscNativeAddr(),
		Protocol:        3,
		DB:              1,
		ClientSideCache: cache,
		PoolSize:        2,
	})
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", cscNativeAddr(), err)
	}

	// CLIENT TRACKING must NOT be enabled on the pool conn — the gate
	// fired in attachCSC and the cache is unused.
	if rawClientHasTrackingFlag(t, c) {
		t.Fatalf("CLIENT TRACKING must NOT be enabled for DB != 0")
	}

	// And the cache must remain empty after a SET/GET cycle. Suffix the key
	// with a per-run nonce so concurrent test processes sharing one Redis
	// cannot collide (and no real application key is ever touched).
	key := "csc-nonzero-db-skip:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() { _ = c.Del(context.Background(), key).Err() })
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
		Addr:            cscNativeAddr(),
		Protocol:        3,
		ClientSideCache: cache,
		PoolSize:        1, // pin all reads to the same tracking conn
		MaxRetries:      -1,
	})
	t.Cleanup(func() { _ = c.Close() })

	mutator := redis.NewClient(&redis.Options{Addr: cscNativeAddr()})
	t.Cleanup(func() { _ = mutator.Close() })

	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", cscNativeAddr(), err)
	}
	unavailable, err := probeClientTracking(ctx, mutator)
	if unavailable {
		t.Skipf("CLIENT TRACKING is unavailable: %v", err)
	}
	if err != nil {
		t.Fatalf("probe CLIENT TRACKING: %v", err)
	}

	// Only touch this test's own key — never FLUSHDB: the target may be a
	// shared instance the suite was never pointed at. The per-run nonce keeps
	// concurrent test processes sharing one Redis from colliding.
	key := "csc-ryw:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := mutator.Del(ctx, key).Err(); err != nil {
		t.Fatalf("DEL: %v", err)
	}
	t.Cleanup(func() { _ = mutator.Del(context.Background(), key).Err() })

	if err := mutator.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatalf("SET v1: %v", err)
	}
	if got := c.Get(ctx, key).Val(); got != "v1" {
		t.Fatalf("first GET: got %q want v1", got)
	}
	// Cache must hold the entry after Fulfill. If an invalidate (e.g. a flush
	// from an unrelated actor) races the first GET's in-flight fetch, that
	// fill is (correctly) suppressed — re-drive the GET until the fill lands.
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
	// goes through processPendingPushNotificationWithReader. The budget is
	// deliberately larger than one default read timeout (3s) so a single
	// stalled roundtrip on a loaded runner can't blow it.
	deadline = time.Now().Add(5 * time.Second)
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

func TestCSCStrategyDefaultIsSharedTracking(t *testing.T) {
	var strategy redis.CSCStrategy
	if strategy != redis.CSCStrategySharedTracking {
		t.Fatalf("zero-value CSCStrategy = %d, want %d",
			strategy, redis.CSCStrategySharedTracking)
	}
}
