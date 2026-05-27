package redis_test

import (
	"fmt"

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
		// Two failure modes combine to make this spec flaky under random
		// spec ordering when any prior spec triggered a failover:
		//
		//  1. FailoverClient(ReadOnly:true) routes via RandomReplicaAddr,
		//     which silently falls back to the master when Sentinel
		//     reports zero usable replicas (post-failover replicas often
		//     linger as "disconnected" for several seconds). Observed
		//     symptom: ROLE returns "master".
		//
		//  2. The SET assertion is similarly bypassed by go-redis's
		//     retry-on-READONLY logic: SET against a replica returns
		//     READONLY, shouldRetry returns true, the conn is recycled,
		//     and the next dial goes through masterReplicaDialer again
		//     — which can fall back to master via the same path as (1).
		//     The SET then succeeds against the master. Observed symptom:
		//     ROLE returns "slave" but SET unexpectedly succeeds.
		//
		// Defenses:
		//
		//  - Disable client retries (MaxRetries: -1) so a READONLY reply
		//    surfaces as the first/only SET error instead of triggering
		//    a fresh dial that may land on the master.
		//
		//  - Run the full assertion (connect → ROLE → SET) atomically
		//    inside a bounded Eventually. If any step lands on the
		//    master (because RandomReplicaAddr fell back), close and
		//    retry with a fresh client so RandomReplicaAddr is
		//    re-evaluated against current Sentinel state.
		Eventually(func() error {
			if client != nil {
				_ = client.Close()
			}
			client = redis.NewUniversalClient(&redis.UniversalOptions{
				MasterName: sentinelName,
				Addrs:      sentinelAddrs,
				ReadOnly:   true,
				MaxRetries: -1,
			})
			if err := client.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
			role, err := client.Do(ctx, "ROLE").Result()
			if err != nil {
				return fmt.Errorf("ROLE: %w", err)
			}
			roleSlice, ok := role.([]interface{})
			if !ok || len(roleSlice) == 0 {
				return fmt.Errorf("ROLE returned unexpected shape: %v", role)
			}
			firstRole, _ := roleSlice[0].(string)
			if firstRole != "slave" {
				return fmt.Errorf("ROLE = %q, want %q (RandomReplicaAddr fell back to master)", firstRole, "slave")
			}
			if setErr := client.Set(ctx, "somekey", "somevalue", 0).Err(); setErr == nil {
				return fmt.Errorf("SET on replica unexpectedly succeeded (likely landed on master)")
			}
			return nil
		}, "30s", "500ms").Should(Succeed())
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
