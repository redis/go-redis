package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("UniversalClient", Serial, func() {
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
		// FailoverClient(ReadOnly:true) routes via sentinelFailover.RandomReplicaAddr,
		// which silently falls back to the master when Sentinel reports
		// zero usable replicas. Post-failover, replicas often linger
		// with the "disconnected" flag for several seconds, and the
		// "should facilitate failover" specs in sentinel_test.go run
		// outside any Ordered block — random spec ordering can put this
		// test right after any of them and observe the fallback.
		//
		// Retry (recreate the client each round so RandomReplicaAddr
		// is re-evaluated) until it actually picks a replica. Bounded
		// at 30s so a genuinely broken setup still fails the test.
		Eventually(func() string {
			if client != nil {
				_ = client.Close()
			}
			client = redis.NewUniversalClient(&redis.UniversalOptions{
				MasterName: sentinelName,
				Addrs:      sentinelAddrs,
				ReadOnly:   true,
			})
			if err := client.Ping(ctx).Err(); err != nil {
				return ""
			}
			role, err := client.Do(ctx, "ROLE").Result()
			if err != nil {
				return ""
			}
			roleSlice, ok := role.([]interface{})
			if !ok || len(roleSlice) == 0 {
				return ""
			}
			firstRole, _ := roleSlice[0].(string)
			return firstRole
		}, "30s", "500ms").Should(Equal("slave"))

		err := client.Set(ctx, "somekey", "somevalue", 0).Err()
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
