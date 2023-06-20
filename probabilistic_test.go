package redis_test

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Probabilistic commands", Label("probabilistic"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("bloom", Label("bloom"), func() {
		It("should BFInfo", Label("bf.info"), func() {
			err := client.BFReserve(ctx, "testbf1", 0.001, 1000).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFInfo(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]string{}))

		})
	})
})
