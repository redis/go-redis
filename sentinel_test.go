package redis_test

import (
	"context"

	"github.com/kirk91/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sentinel", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: []string{":" + sentinelPort},
		})
		Expect(client.FlushDB(context.Background()).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should facilitate failover", func() {
		// Set value on master.
		err := client.Set(context.Background(), "foo", "master", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify.
		val, err := sentinelMaster.Get(context.Background(), "foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))

		// Create subscription.
		ch := client.Subscribe("foo").Channel()

		// Wait until replicated.
		Eventually(func() string {
			return sentinelSlave1.Get(context.Background(), "foo").Val()
		}, "1s", "100ms").Should(Equal("master"))
		Eventually(func() string {
			return sentinelSlave2.Get(context.Background(), "foo").Val()
		}, "1s", "100ms").Should(Equal("master"))

		// Wait until slaves are picked up by sentinel.
		Eventually(func() string {
			return sentinel.Info(context.Background()).Val()
		}, "10s", "100ms").Should(ContainSubstring("slaves=2"))

		// Kill master.
		sentinelMaster.Shutdown(context.Background())
		Eventually(func() error {
			return sentinelMaster.Ping(context.Background()).Err()
		}, "5s", "100ms").Should(HaveOccurred())

		// Wait for Redis sentinel to elect new master.
		Eventually(func() string {
			return sentinelSlave1.Info(context.Background()).Val() + sentinelSlave2.Info(context.Background()).Val()
		}, "30s", "1s").Should(ContainSubstring("role:master"))

		// Check that client picked up new master.
		Eventually(func() error {
			return client.Get(context.Background(), "foo").Err()
		}, "5s", "100ms").ShouldNot(HaveOccurred())

		// Publish message to check if subscription is renewed.
		err = client.Publish(context.Background(), "foo", "hello").Err()
		Expect(err).NotTo(HaveOccurred())

		var msg *redis.Message
		Eventually(ch).Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("foo"))
		Expect(msg.Payload).To(Equal("hello"))
	})

	It("supports DB selection", func() {
		Expect(client.Close()).NotTo(HaveOccurred())

		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: []string{":" + sentinelPort},
			DB:            1,
		})
		err := client.Ping(context.Background()).Err()
		Expect(err).NotTo(HaveOccurred())
	})
})
