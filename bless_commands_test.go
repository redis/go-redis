package redis_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// blessedKeysRe matches the top-level "blessed_keys:N" field in INFO output.
var blessedKeysRe = regexp.MustCompile(`(?m)^blessed_keys:(\d+)\r?$`)

// blessScanAll drains BLESS SCAN on a single node and returns every key it
// reports. The BLESS SCAN cursor is node-local, so this is the building block
// for scanning a ClusterClient or Ring master by master.
func blessScanAll(ctx context.Context, node *redis.Client, flag redis.BlessFlag) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		page, next, err := node.BlessScan(ctx, cursor, flag, 5).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, page...)
		if next == 0 {
			return keys, nil
		}
		cursor = next
	}
}

// blessScanEachNode runs blessScanAll on every node visited by forEach
// (ClusterClient.ForEachMaster or Ring.ForEachShard) and merges the results.
// forEach runs its callback concurrently, hence the mutex.
func blessScanEachNode(
	ctx context.Context,
	flag redis.BlessFlag,
	forEach func(context.Context, func(context.Context, *redis.Client) error) error,
) ([]string, error) {
	var mu sync.Mutex
	var all []string
	err := forEach(ctx, func(ctx context.Context, node *redis.Client) error {
		keys, err := blessScanAll(ctx, node, flag)
		if err != nil {
			return err
		}
		mu.Lock()
		all = append(all, keys...)
		mu.Unlock()
		return nil
	})
	return all, err
}

var _ = Describe("BLESS commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	const unknownFlag = redis.BlessFlag("NO-SUCH-FLAG")

	// blessedKeys returns the blessed_keys counter from INFO, failing the spec
	// if the server does not report one.
	blessedKeys := func() int {
		GinkgoHelper()
		info, err := client.Info(ctx, "everything").Result()
		Expect(err).NotTo(HaveOccurred())
		m := blessedKeysRe.FindStringSubmatch(info)
		Expect(m).NotTo(BeNil(), "INFO does not report blessed_keys")
		n, err := strconv.Atoi(m[1])
		Expect(err).NotTo(HaveOccurred())
		return n
	}

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.12", "BLESS requires Redis 8.12")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("BLESS SET", func() {
		It("returns 1 when the flag is newly set and 0 when it was already set", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

			Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(0)))

			flags, err := client.BlessGet(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(flags).To(Equal([]string{string(redis.BlessNoEvict)}))
		})

		It("rejects an unknown flag", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

			err := client.BlessSet(ctx, "key1", unknownFlag).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("ERR"))

			// A rejected flag must not leave any state behind.
			flags, err := client.BlessGet(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(flags).To(BeEmpty())
		})
	})

	Describe("BLESS GET", func() {
		It("reports no flags on a key that never had one set", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

			flags, err := client.BlessGet(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(flags).To(BeEmpty())
		})
	})

	Describe("BLESS CLEAR", func() {
		It("returns 1 when the flag was present and 0 when it was not", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(1)))

			Expect(client.BlessClear(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(client.BlessClear(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(0)))

			flags, err := client.BlessGet(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(flags).To(BeEmpty())
		})

		It("returns 0 for a key that was never blessed", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

			Expect(client.BlessClear(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(0)))
		})

		It("rejects an unknown flag", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Err()).NotTo(HaveOccurred())

			err := client.BlessClear(ctx, "key1", unknownFlag).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("ERR"))

			// The existing flag must survive a rejected CLEAR.
			flags, err := client.BlessGet(ctx, "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(flags).To(Equal([]string{string(redis.BlessNoEvict)}))
		})
	})

	Describe("BLESS SCAN", func() {
		It("scans keys carrying a flag across multiple pages", func() {
			want := make([]string, 0, 20)
			for i := range 20 {
				key := fmt.Sprintf("scan:%02d", i)
				want = append(want, key)
				Expect(client.Set(ctx, key, "v", 0).Err()).NotTo(HaveOccurred())
				Expect(client.BlessSet(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			}
			// An unblessed key must not show up in the scan.
			Expect(client.Set(ctx, "plain", "v", 0).Err()).NotTo(HaveOccurred())

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
			Expect(keys).To(ConsistOf(want))
		})

		It("returns an empty result when no key carries the flag", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())

			keys, cursor, err := client.BlessScan(ctx, 0, redis.BlessNoEvict, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(BeEmpty())
			Expect(cursor).To(Equal(uint64(0)))
		})

		It("rejects an unknown flag", func() {
			err := client.BlessScan(ctx, 0, unknownFlag, 0).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("ERR"))
		})
	})

	Describe("INFO blessed_keys", func() {
		It("tracks the number of blessed keys through SET, CLEAR and DEL", func() {
			Expect(blessedKeys()).To(Equal(0))

			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.Set(ctx, "key2", "v", 0).Err()).NotTo(HaveOccurred())

			Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(blessedKeys()).To(Equal(1))

			Expect(client.BlessSet(ctx, "key2", redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(blessedKeys()).To(Equal(2))

			// Re-setting an existing flag must not double count.
			Expect(client.BlessSet(ctx, "key2", redis.BlessNoEvict).Result()).To(Equal(int64(0)))
			Expect(blessedKeys()).To(Equal(2))

			Expect(client.BlessClear(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(blessedKeys()).To(Equal(1))

			// Deleting a blessed key releases its slot in the counter.
			Expect(client.Del(ctx, "key2").Result()).To(Equal(int64(1)))
			Expect(blessedKeys()).To(Equal(0))
		})

		It("resets to zero on FLUSHDB", func() {
			Expect(client.Set(ctx, "key1", "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.BlessSet(ctx, "key1", redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(blessedKeys()).To(Equal(1))

			Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Expect(blessedKeys()).To(Equal(0))
		})
	})
})

// BLESS SET/GET/CLEAR carry the key at position 2, so a ClusterClient routes
// them to the owning master like any other keyed command. BLESS SCAN is
// keyless and its cursor is node-local, so it has to be issued to each master
// individually; these specs pin both halves of that contract.
var _ = Describe("BLESS commands on ClusterClient", func() {
	ctx := context.TODO()
	var client *redis.ClusterClient

	// Enough keys to land on every master of the three-shard test cluster
	// with overwhelming probability.
	const numKeys = 60

	BeforeEach(func() {
		if RECluster {
			Skip("OSS cluster scenario is not configured under RE_CLUSTER")
		}
		SkipBeforeRedisVersion("8.12", "BLESS requires Redis 8.12")

		client = cluster.newClusterClient(ctx, redisClusterOptions())
		Expect(client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("routes SET, GET and CLEAR to the master owning the key", func() {
		for i := range numKeys {
			key := fmt.Sprintf("cluster:%02d", i)
			Expect(client.Set(ctx, key, "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.BlessSet(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(client.BlessGet(ctx, key).Result()).To(Equal([]string{string(redis.BlessNoEvict)}))
		}
		for i := range numKeys {
			key := fmt.Sprintf("cluster:%02d", i)
			Expect(client.BlessClear(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(client.BlessGet(ctx, key).Result()).To(BeEmpty())
		}
	})

	It("finds every blessed key only when each master is scanned separately", func() {
		want := make([]string, 0, numKeys)
		for i := range numKeys {
			key := fmt.Sprintf("cluster:%02d", i)
			want = append(want, key)
			Expect(client.Set(ctx, key, "v", 0).Err()).NotTo(HaveOccurred())
			Expect(client.BlessSet(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
		}

		// Each master only reports its own slice of the keyspace.
		var mu sync.Mutex
		perMaster := map[string][]string{}
		Expect(client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			keys, err := blessScanAll(ctx, master, redis.BlessNoEvict)
			if err != nil {
				return err
			}
			mu.Lock()
			perMaster[master.Options().Addr] = keys
			mu.Unlock()
			return nil
		})).NotTo(HaveOccurred())
		Expect(perMaster).To(HaveLen(len(cluster.masters())))
		for addr, keys := range perMaster {
			Expect(keys).NotTo(BeEmpty(), "master %s reported no blessed keys", addr)
			Expect(len(keys)).To(BeNumerically("<", numKeys), "master %s reported every key", addr)
		}

		// Merging the per-master scans yields the complete, duplicate-free set.
		all, err := blessScanEachNode(ctx, redis.BlessNoEvict, client.ForEachMaster)
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(ConsistOf(want))

		// A single BLESS SCAN through the ClusterClient lands on one arbitrary
		// master and therefore cannot see the whole keyspace, which is why the
		// per-master loop above is the supported pattern.
		page, _, err := client.BlessScan(ctx, 0, redis.BlessNoEvict, numKeys*2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(page)).To(BeNumerically("<", numKeys))
	})
})

var _ = Describe("BLESS commands on Ring", func() {
	ctx := context.TODO()
	var ring *redis.Ring

	// Enough keys to land on both ring shards with overwhelming probability.
	const numKeys = 40

	BeforeEach(func() {
		if RECluster {
			Skip("ring shards are not configured under RE_CLUSTER")
		}
		SkipBeforeRedisVersion("8.12", "BLESS requires Redis 8.12")

		ring = redis.NewRing(redisRingOptions())
		Expect(ring.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
			return shard.FlushDB(ctx).Err()
		})).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = ring.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
			return shard.FlushDB(ctx).Err()
		})
		Expect(ring.Close()).NotTo(HaveOccurred())
	})

	It("routes SET, GET and CLEAR to the shard owning the key", func() {
		for i := range numKeys {
			key := fmt.Sprintf("ring:%02d", i)
			Expect(ring.Set(ctx, key, "v", 0).Err()).NotTo(HaveOccurred())
			Expect(ring.BlessSet(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(ring.BlessGet(ctx, key).Result()).To(Equal([]string{string(redis.BlessNoEvict)}))
		}
		for i := range numKeys {
			key := fmt.Sprintf("ring:%02d", i)
			Expect(ring.BlessClear(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
			Expect(ring.BlessGet(ctx, key).Result()).To(BeEmpty())
		}
	})

	It("finds every blessed key only when each shard is scanned separately", func() {
		want := make([]string, 0, numKeys)
		for i := range numKeys {
			key := fmt.Sprintf("ring:%02d", i)
			want = append(want, key)
			Expect(ring.Set(ctx, key, "v", 0).Err()).NotTo(HaveOccurred())
			Expect(ring.BlessSet(ctx, key, redis.BlessNoEvict).Result()).To(Equal(int64(1)))
		}

		// Each shard only reports the keys hashed to it.
		var mu sync.Mutex
		perShard := map[string][]string{}
		Expect(ring.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
			keys, err := blessScanAll(ctx, shard, redis.BlessNoEvict)
			if err != nil {
				return err
			}
			mu.Lock()
			perShard[shard.Options().Addr] = keys
			mu.Unlock()
			return nil
		})).NotTo(HaveOccurred())
		Expect(perShard).To(HaveLen(ring.Len()))
		for addr, keys := range perShard {
			Expect(keys).NotTo(BeEmpty(), "shard %s reported no blessed keys", addr)
			Expect(len(keys)).To(BeNumerically("<", numKeys), "shard %s reported every key", addr)
		}

		// Merging the per-shard scans yields the complete, duplicate-free set.
		all, err := blessScanEachNode(ctx, redis.BlessNoEvict, ring.ForEachShard)
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(ConsistOf(want))

		// A single BLESS SCAN through the Ring goes to a random shard and
		// cannot see the whole keyspace.
		page, _, err := ring.BlessScan(ctx, 0, redis.BlessNoEvict, numKeys*2).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(page)).To(BeNumerically("<", numKeys))
	})
})
