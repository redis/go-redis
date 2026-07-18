package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Client-side cache (standalone)", func() {
	var (
		ctx     context.Context
		cache   redis.Cache
		client  *redis.Client
		mutator *redis.Client
	)

	BeforeEach(func() {
		SkipBeforeRedisVersion(6.0, "CLIENT TRACKING requires Redis 6.0+")
		ctx = context.Background()

		cache = redis.NewLocalCache(redis.CacheConfig{MaxEntries: 128})

		// Flush BEFORE the tracked client exists: a FLUSHDB after construction
		// would push a nil-payload invalidate to the tracked connection and race
		// the first GET's fill, making cache-population assertions flaky.
		mutator = redis.NewClient(redisOptions())
		Expect(mutator.Ping(ctx).Err()).NotTo(HaveOccurred())
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
})
