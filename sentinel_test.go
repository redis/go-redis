package redis_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Sentinel", func() {

	const masterName = "mymaster"
	const masterPort = "8123"
	const sentinelPort = "8124"

	It("should facilitate failover", func() {
		master, err := startRedis(masterPort)
		Expect(err).NotTo(HaveOccurred())
		defer master.Close()

		sentinel, err := startSentinel(sentinelPort, masterName, masterPort)
		Expect(err).NotTo(HaveOccurred())
		defer sentinel.Close()

		slave1, err := startRedis("8125", "--slaveof", "127.0.0.1", masterPort)
		Expect(err).NotTo(HaveOccurred())
		defer slave1.Close()

		slave2, err := startRedis("8126", "--slaveof", "127.0.0.1", masterPort)
		Expect(err).NotTo(HaveOccurred())
		defer slave2.Close()

		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: []string{":" + sentinelPort},
		})
		defer client.Close()

		// Set value on master, verify
		err = client.Set("foo", "master").Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := master.Get("foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))

		// Wait until replicated
		Eventually(func() string {
			return slave1.Get("foo").Val()
		}, "1s", "100ms").Should(Equal("master"))
		Eventually(func() string {
			return slave2.Get("foo").Val()
		}, "1s", "100ms").Should(Equal("master"))

		// Kill master.
		master.Shutdown()
		Eventually(func() error {
			return master.Ping().Err()
		}, "5s", "100ms").Should(HaveOccurred())

		// Wait for Redis sentinel to elect new master.
		Eventually(func() string {
			return slave1.Info().Val() + slave2.Info().Val()
		}, "30s", "500ms").Should(ContainSubstring("role:master"))

		// Check that client picked up new master.
		val, err = client.Get("foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))
	})

})
