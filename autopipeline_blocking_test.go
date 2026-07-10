package redis_test

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("AutoPipeline Blocking Commands", func() {
	ctx := context.Background()
	var client *redis.Client
	var ap *redis.AutoPipeliner

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		var err error
		ap, err = client.AutoPipeline(nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should not autopipeline blocking commands", func() {
		// Push a value to the list
		Expect(client.RPush(ctx, "list", "value").Err()).NotTo(HaveOccurred())

		// BLPOP should execute immediately without autopipelining.
		// ap.Do returns a generic *redis.Cmd; use its typed accessors.
		start := time.Now()
		result := ap.Do(ctx, "BLPOP", "list", "1")
		val, err := result.StringSlice()
		elapsed := time.Since(start)

		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]string{"list", "value"}))
		// Should complete quickly since value is available
		Expect(elapsed).To(BeNumerically("<", 100*time.Millisecond))
	})

	It("should mix blocking and non-blocking commands", func() {
		// Push values
		Expect(client.RPush(ctx, "list3", "a", "b", "c").Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "key1", "value1", 0).Err()).NotTo(HaveOccurred())

		// Mix blocking and non-blocking commands
		blpopCmd := ap.Do(ctx, "BLPOP", "list3", "1")
		getCmd := ap.Do(ctx, "GET", "key1")
		brpopCmd := ap.Do(ctx, "BRPOP", "list3", "1")

		// Get results — ap.Do returns generic *redis.Cmd; use typed accessors.
		blpopVal, err := blpopCmd.StringSlice()
		Expect(err).NotTo(HaveOccurred())
		Expect(blpopVal).To(Equal([]string{"list3", "a"}))

		getVal, err := getCmd.Text()
		Expect(err).NotTo(HaveOccurred())
		Expect(getVal).To(Equal("value1"))

		brpopVal, err := brpopCmd.StringSlice()
		Expect(err).NotTo(HaveOccurred())
		Expect(brpopVal).To(Equal([]string{"list3", "c"}))
	})
})
