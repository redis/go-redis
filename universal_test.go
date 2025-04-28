package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("UniversalClient", func() {
	var client redis.UniversalClient

	AfterEach(func() {
		if client != nil {
			Expect(client.Close()).To(Succeed())
		}
	})

	It("should connect to failover servers", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			MasterName: sentinelName,
			Addrs:      sentinelAddrs,
		})
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	})

	It("should connect to failover cluster", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			MasterName:    sentinelName,
			RouteRandomly: true,
			Addrs:         sentinelAddrs,
		})
		_, ok := client.(*redis.ClusterClient)
		Expect(ok).To(BeTrue(), "expected a ClusterClient")
	})

	It("should connect to simple servers", func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{redisAddr},
		})
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	})

	It("should connect to clusters", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: cluster.addrs(),
		})
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	})

	It("connect to clusters with UniversalClient and UnstableResp3", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:         cluster.addrs(),
			Protocol:      3,
			UnstableResp3: true,
		})
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
		a := func() { client.FTInfo(ctx, "all").Result() }
		Expect(a).ToNot(Panic())
	})

	It("connect to clusters with ClusterClient and UnstableResp3", Label("NonRedisEnterprise"), func() {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:         cluster.addrs(),
			Protocol:      3,
			UnstableResp3: true,
		})
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
		a := func() { client.FTInfo(ctx, "all").Result() }
		Expect(a).ToNot(Panic())
	})

	It("should connect to failover servers on slaves when readonly Options is ok", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			MasterName: sentinelName,
			Addrs:      sentinelAddrs,
			ReadOnly:   true,
		})
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

		roleCmd := client.Do(ctx, "ROLE")
		role, err := roleCmd.Result()
		Expect(err).NotTo(HaveOccurred())

		roleSlice, ok := role.([]interface{})
		Expect(ok).To(BeTrue())
		Expect(roleSlice[0]).To(Equal("slave"))

		err = client.Set(ctx, "somekey", "somevalue", 0).Err()
		Expect(err).To(HaveOccurred())
	})

	It("should connect to clusters if IsClusterMode is set even if only a single address is provided", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:         []string{cluster.addrs()[0]},
			IsClusterMode: true,
		})
		_, ok := client.(*redis.ClusterClient)
		Expect(ok).To(BeTrue(), "expected a ClusterClient")
	})

	It("should return all slots after instantiating UniversalClient with IsClusterMode", Label("NonRedisEnterprise"), func() {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:         []string{cluster.addrs()[0]},
			IsClusterMode: true,
		})
		Expect(client.ClusterSlots(ctx).Val()).To(HaveLen(3))
	})
})
