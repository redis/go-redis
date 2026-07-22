package redis_test

import (
	"context"
	"fmt"
	"sync"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("HIMPORT commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.10", "HIMPORT requires Redis 8.10")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if client != nil {
			Expect(client.Close()).NotTo(HaveOccurred())
		}
	})

	It("prepares a fieldset and creates hashes from values", func() {
		prepare := client.HImportPrepare(ctx, "shared", "name", "email", "age")
		Expect(prepare.Err()).NotTo(HaveOccurred())
		Expect(prepare.Val()).To(Equal("OK"))

		set := client.HImportSet(ctx, "shared:1", "shared", "alice", "alice@example.com", "25")
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		Expect(client.HGet(ctx, "shared:1", "name").Val()).To(Equal("alice"))
		Expect(client.HGet(ctx, "shared:1", "email").Val()).To(Equal("alice@example.com"))
		Expect(client.HGet(ctx, "shared:1", "age").Val()).To(Equal("25"))
		Expect(client.HGetAll(ctx, "shared:1").Val()).To(HaveLen(3))
	})

	It("pairs values positionally with each fieldset's declared order", func() {
		Expect(client.HImportPrepare(ctx, "order1", "a", "b", "c").Err()).NotTo(HaveOccurred())
		Expect(client.HImportPrepare(ctx, "order2", "c", "b", "a").Err()).NotTo(HaveOccurred())

		Expect(client.HImportSet(ctx, "order:key1", "order1", "va1", "vb1", "vc1").Err()).NotTo(HaveOccurred())
		Expect(client.HImportSet(ctx, "order:key2", "order2", "vc2", "vb2", "va2").Err()).NotTo(HaveOccurred())

		Expect(client.HGet(ctx, "order:key1", "a").Val()).To(Equal("va1"))
		Expect(client.HGet(ctx, "order:key2", "a").Val()).To(Equal("va2"))
	})

	It("replays the prepare on every pooled connection that runs a set", func() {
		Expect(client.HImportPrepare(ctx, "pooled", "f1", "f2").Err()).NotTo(HaveOccurred())

		// Concurrent writers spread across the pool; every connection must
		// have the fieldset replayed onto its session before its first SET.
		var wg sync.WaitGroup
		errs := make(chan error, 100)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("pooled:%d", i)
				if err := client.HImportSet(ctx, key, "pooled", "v1", "v2").Err(); err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			Expect(err).NotTo(HaveOccurred())
		}

		keys, err := client.Keys(ctx, "pooled:*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(HaveLen(100))
	})

	It("fully replaces an existing hash", func() {
		Expect(client.HSet(ctx, "replace:1", "old", "x", "stale", "y").Err()).NotTo(HaveOccurred())
		Expect(client.HImportPrepare(ctx, "repl", "fresh").Err()).NotTo(HaveOccurred())
		Expect(client.HImportSet(ctx, "replace:1", "repl", "z").Err()).NotTo(HaveOccurred())

		all := client.HGetAll(ctx, "replace:1").Val()
		Expect(all).To(Equal(map[string]string{"fresh": "z"}))
	})

	It("re-prepares after a fieldset is replaced", func() {
		Expect(client.HImportPrepare(ctx, "evolving", "a").Err()).NotTo(HaveOccurred())
		Expect(client.HImportSet(ctx, "evolving:1", "evolving", "1").Err()).NotTo(HaveOccurred())

		// Preparing the same name again silently replaces it; subsequent
		// sets must use the new field list on every pooled connection.
		Expect(client.HImportPrepare(ctx, "evolving", "a", "b").Err()).NotTo(HaveOccurred())
		Expect(client.HImportSet(ctx, "evolving:2", "evolving", "1", "2").Err()).NotTo(HaveOccurred())
		Expect(client.HGet(ctx, "evolving:2", "b").Val()).To(Equal("2"))
	})

	It("discards a fieldset and stops replaying it", func() {
		Expect(client.HImportPrepare(ctx, "gone", "f").Err()).NotTo(HaveOccurred())
		Expect(client.HImportSet(ctx, "gone:1", "gone", "v").Err()).NotTo(HaveOccurred())

		discard := client.HImportDiscard(ctx, "gone")
		Expect(discard.Err()).NotTo(HaveOccurred())

		err := client.HImportSet(ctx, "gone:2", "gone", "v").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no such fieldset"))

		// Hashes created through the fieldset survive the discard.
		Expect(client.HGet(ctx, "gone:1", "f").Val()).To(Equal("v"))
	})

	It("discards all fieldsets", func() {
		Expect(client.HImportPrepare(ctx, "all1", "f").Err()).NotTo(HaveOccurred())
		Expect(client.HImportPrepare(ctx, "all2", "g").Err()).NotTo(HaveOccurred())

		count, err := client.HImportDiscardAll(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeNumerically(">=", 2))

		err = client.HImportSet(ctx, "all:1", "all1", "v").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no such fieldset"))
	})

	It("propagates server errors unchanged", func() {
		err := client.HImportSet(ctx, "nofs:1", "never-prepared", "v").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no such fieldset"))

		err = client.HImportPrepare(ctx, "dup", "f", "f").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("duplicate field name"))

		Expect(client.Set(ctx, "notahash", "s", 0).Err()).NotTo(HaveOccurred())
		Expect(client.HImportPrepare(ctx, "wrongtype", "f").Err()).NotTo(HaveOccurred())
		err = client.HImportSet(ctx, "notahash", "wrongtype", "v").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
	})

	It("counts value/field mismatches server-side", func() {
		Expect(client.HImportPrepare(ctx, "counted", "a", "b").Err()).NotTo(HaveOccurred())
		err := client.HImportSet(ctx, "counted:1", "counted", "only-one").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("value count"))
	})

	It("works on a dedicated connection", func() {
		conn := client.Conn()
		defer conn.Close()

		Expect(conn.HImportPrepare(ctx, "dedicated", "f").Err()).NotTo(HaveOccurred())
		Expect(conn.HImportSet(ctx, "dedicated:1", "dedicated", "v").Err()).NotTo(HaveOccurred())

		count, err := conn.HImportDiscard(ctx, "dedicated").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(1)))
	})

	It("prepares and sets within one pipeline", func() {
		pipe := client.Pipeline()
		prepare := pipe.HImportPrepare(ctx, "piped", "f1", "f2")
		set1 := pipe.HImportSet(ctx, "piped:1", "piped", "a", "b")
		set2 := pipe.HImportSet(ctx, "piped:2", "piped", "c", "d")
		_, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(prepare.Val()).To(Equal("OK"))
		Expect(set1.Val()).To(Equal("OK"))
		Expect(set2.Val()).To(Equal("OK"))

		Expect(client.HGet(ctx, "piped:2", "f2").Val()).To(Equal("d"))
	})

	It("replays a registered fieldset into a pipeline", func() {
		Expect(client.HImportPrepare(ctx, "pipedreg", "f").Err()).NotTo(HaveOccurred())

		pipe := client.Pipeline()
		var sets []*redis.StatusCmd
		for i := 0; i < 10; i++ {
			sets = append(sets, pipe.HImportSet(ctx, fmt.Sprintf("pipedreg:%d", i), "pipedreg", "v"))
		}
		_, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		for _, set := range sets {
			Expect(set.Val()).To(Equal("OK"))
		}
	})

	It("replays a registered fieldset into a transaction", func() {
		Expect(client.HImportPrepare(ctx, "txfs", "f").Err()).NotTo(HaveOccurred())

		tx := client.TxPipeline()
		set1 := tx.HImportSet(ctx, "txfs:1", "txfs", "a")
		set2 := tx.HImportSet(ctx, "txfs:2", "txfs", "b")
		_, err := tx.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(set1.Val()).To(Equal("OK"))
		Expect(set2.Val()).To(Equal("OK"))

		Expect(client.HGet(ctx, "txfs:1", "f").Val()).To(Equal("a"))
		Expect(client.HGet(ctx, "txfs:2", "f").Val()).To(Equal("b"))
	})
})
