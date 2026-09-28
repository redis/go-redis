package redis_test

import (
	"context"
	"fmt"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("BLESS commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.12", "BLESS requires Redis 8.12")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("sets and gets a flag on a key", func() {
		Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

		Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Err()).NotTo(HaveOccurred())

		flags, err := client.BlessGet(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(flags).To(ContainElement(string(redis.BlessNoEvict)))
	})

	It("reports no flags on a key that never had one set", func() {
		Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

		flags, err := client.BlessGet(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(flags).To(BeEmpty())
	})

	It("clears a flag from a key", func() {
		Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())
		Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Err()).NotTo(HaveOccurred())

		Expect(client.BlessClear(ctx, "key1", redis.BlessNoEvict).Err()).NotTo(HaveOccurred())

		flags, err := client.BlessGet(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(flags).NotTo(ContainElement(string(redis.BlessNoEvict)))
	})

	It("scans keys carrying a flag across multiple pages", func() {
		for i := range 20 {
			key := fmt.Sprintf("scan:%02d", i)
			Expect(client.Set(ctx, key, "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.BlessSet(ctx, key, redis.BlessNoEvict).Err()).NotTo(HaveOccurred())
		}

		var keys []string
		cursor := uint64(0)
		for {
			page, next, err := client.BlessScan(ctx, cursor, redis.BlessNoEvict, 5).Result()
			Expect(err).NotTo(HaveOccurred())
			keys = append(keys, page...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		Expect(keys).To(HaveLen(20))
	})
})
