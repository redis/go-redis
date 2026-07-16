package redis_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Client-side cache strategies", func() {
	var (
		ctx     context.Context
		csc     *redis.Client
		mutator *redis.Client
	)

	BeforeEach(func() {
		SkipBeforeRedisVersion(6.0, "CLIENT TRACKING requires Redis 6.0+")
		ctx = context.Background()
		csc = nil

		mutator = redis.NewClient(redisOptions())
		Expect(mutator.Ping(ctx).Err()).NotTo(HaveOccurred())
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
			hits, _ := csc.CSCStats()
			return hits, nil
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
