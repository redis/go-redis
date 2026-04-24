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

		opt := redisOptions()
		opt.Protocol = 3
		opt.ClientSideCache = cache
		client = redis.NewClient(opt)
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

		mutator = redis.NewClient(redisOptions())
		Expect(mutator.Ping(ctx).Err()).NotTo(HaveOccurred())
		Expect(mutator.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if mutator != nil {
			Expect(mutator.Close()).NotTo(HaveOccurred())
		}
		if client != nil {
			Expect(client.Close()).NotTo(HaveOccurred())
		}
	})

	It("enables CLIENT TRACKING on the connection during handshake", func() {
		info, err := client.ClientInfo(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(info).NotTo(BeNil())
		Expect(info.Flags&redis.ClientTracking).
			NotTo(BeZero(), "expected the tracked client to have the CLIENT TRACKING flag set")
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

	It("does not enable CLIENT TRACKING when CSC is disabled", func() {
		plain := redis.NewClient(redisOptions())
		defer plain.Close()

		info, err := plain.ClientInfo(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Flags&redis.ClientTracking).
			To(BeZero(), "a client without CSC must not have CLIENT TRACKING enabled")
	})
})
