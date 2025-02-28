package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Do cmdble", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
	})

	AfterEach(func() {
		client.Close()
	})

	It("should pong with Do cmd", func() {
		result := client.Conn().Do(ctx, "PING")
		Expect(result.Result()).To(Equal("PONG"))
		Expect(result.Err()).ShouldNot(HaveOccurred())
	})
})
