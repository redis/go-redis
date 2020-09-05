package redis_test

import (
	"github.com/go-redis/redis/v8"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sentinel", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should facilitate failover", func() {
		// Set value on master.
		err := client.Set(ctx, "foo", "master", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify.
		val, err := sentinelMaster.Get(ctx, "foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))

		// Create subscription.
		ch := client.Subscribe(ctx, "foo").Channel()

		// Wait until replicated.
		Eventually(func() string {
			return sentinelSlave1.Get(ctx, "foo").Val()
		}, "15s", "100ms").Should(Equal("master"))
		Eventually(func() string {
			return sentinelSlave2.Get(ctx, "foo").Val()
		}, "15s", "100ms").Should(Equal("master"))

		// Wait until slaves are picked up by sentinel.
		Eventually(func() string {
			return sentinel1.Info(ctx).Val()
		}, "10s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel2.Info(ctx).Val()
		}, "10s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel3.Info(ctx).Val()
		}, "10s", "100ms").Should(ContainSubstring("slaves=2"))

		// Kill master.
		sentinelMaster.Shutdown(ctx)
		Eventually(func() error {
			return sentinelMaster.Ping(ctx).Err()
		}, "15s", "100ms").Should(HaveOccurred())

		// Wait for Redis sentinel to elect new master.
		Eventually(func() string {
			return sentinelSlave1.Info(ctx).Val() + sentinelSlave2.Info(ctx).Val()
		}, "30s", "100ms").Should(ContainSubstring("role:master"))

		// Check that client picked up new master.
		Eventually(func() error {
			return client.Get(ctx, "foo").Err()
		}, "15s", "100ms").ShouldNot(HaveOccurred())

		// Check if subscription is renewed.
		var msg *redis.Message
		Eventually(func() <-chan *redis.Message {
			_ = client.Publish(ctx, "foo", "hello").Err()
			return ch
		}, "15s").Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("foo"))
		Expect(msg.Payload).To(Equal("hello"))
	})

	It("supports DB selection", func() {
		Expect(client.Close()).NotTo(HaveOccurred())

		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			DB:            1,
		})
		err := client.Ping(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
	})
})
