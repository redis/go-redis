package redis_test

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("AutoPipeline Cmdable Interface", func() {
	ctx := context.Background()
	var client *redis.Client
	var ap *redis.AutoPipeliner

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		ap = client.AutoPipeline()
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should support string commands", func() {
		// Use autopipeline like a regular client
		setCmd := ap.Set(ctx, "key1", "value1", 0)
		getCmd := ap.Get(ctx, "key1")
		incrCmd := ap.Incr(ctx, "counter")
		decrCmd := ap.Decr(ctx, "counter")

		// Get results
		Expect(setCmd.Err()).NotTo(HaveOccurred())
		Expect(setCmd.Val()).To(Equal("OK"))

		val, err := getCmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value1"))

		Expect(incrCmd.Val()).To(Equal(int64(1)))
		Expect(decrCmd.Val()).To(Equal(int64(0)))
	})

	It("should support hash commands", func() {
		// Use hash commands
		hsetCmd := ap.HSet(ctx, "hash1", "field1", "value1", "field2", "value2")
		hgetCmd := ap.HGet(ctx, "hash1", "field1")
		hgetallCmd := ap.HGetAll(ctx, "hash1")

		// Get results
		Expect(hsetCmd.Val()).To(Equal(int64(2)))
		Expect(hgetCmd.Val()).To(Equal("value1"))
		Expect(hgetallCmd.Val()).To(Equal(map[string]string{
			"field1": "value1",
			"field2": "value2",
		}))
	})

	It("should support list commands", func() {
		// Use list commands
		rpushCmd := ap.RPush(ctx, "list1", "a", "b", "c")
		lrangeCmd := ap.LRange(ctx, "list1", 0, -1)
		lpopCmd := ap.LPop(ctx, "list1")

		// Get results
		Expect(rpushCmd.Val()).To(Equal(int64(3)))
		Expect(lrangeCmd.Val()).To(Equal([]string{"a", "b", "c"}))
		Expect(lpopCmd.Val()).To(Equal("a"))
	})

	It("should support set commands", func() {
		// Use set commands
		saddCmd := ap.SAdd(ctx, "set1", "member1", "member2", "member3")
		smembersCmd := ap.SMembers(ctx, "set1")
		sismemberCmd := ap.SIsMember(ctx, "set1", "member1")

		// Get results
		Expect(saddCmd.Val()).To(Equal(int64(3)))
		Expect(smembersCmd.Val()).To(ConsistOf("member1", "member2", "member3"))
		Expect(sismemberCmd.Val()).To(BeTrue())
	})

	It("should support sorted set commands", func() {
		// Use sorted set commands
		zaddCmd := ap.ZAdd(ctx, "zset1",
			redis.Z{Score: 1, Member: "one"},
			redis.Z{Score: 2, Member: "two"},
			redis.Z{Score: 3, Member: "three"},
		)
		zrangeCmd := ap.ZRange(ctx, "zset1", 0, -1)
		zscoreCmd := ap.ZScore(ctx, "zset1", "two")

		// Get results
		Expect(zaddCmd.Val()).To(Equal(int64(3)))
		Expect(zrangeCmd.Val()).To(Equal([]string{"one", "two", "three"}))
		Expect(zscoreCmd.Val()).To(Equal(float64(2)))
	})

	It("should support generic commands", func() {
		// Set some keys
		ap.Set(ctx, "key1", "value1", 0)
		ap.Set(ctx, "key2", "value2", 0)
		ap.Set(ctx, "key3", "value3", 0)

		// Use generic commands
		existsCmd := ap.Exists(ctx, "key1", "key2", "key3")
		delCmd := ap.Del(ctx, "key1")
		ttlCmd := ap.TTL(ctx, "key2")

		// Get results
		Expect(existsCmd.Val()).To(Equal(int64(3)))
		Expect(delCmd.Val()).To(Equal(int64(1)))
		Expect(ttlCmd.Val()).To(Equal(time.Duration(-1))) // No expiration
	})

	It("should support Do method for custom commands", func() {
		// Use Do for custom commands
		setCmd := ap.Do(ctx, "SET", "custom_key", "custom_value")
		getCmd := ap.Do(ctx, "GET", "custom_key")

		// Get results
		setVal, err := setCmd.(*redis.Cmd).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(setVal).To(Equal("OK"))

		getVal, err := getCmd.(*redis.Cmd).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(getVal).To(Equal("custom_value"))
	})

	It("should support Pipeline method", func() {
		// Get a traditional pipeline from autopipeliner
		pipe := ap.Pipeline()
		Expect(pipe).NotTo(BeNil())

		// Use the pipeline
		pipe.Set(ctx, "pipe_key", "pipe_value", 0)
		pipe.Get(ctx, "pipe_key")

		cmds, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
	})

	It("should support Pipelined method", func() {
		// Use Pipelined for convenience
		cmds, err := ap.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "pipelined_key", "pipelined_value", 0)
			pipe.Get(ctx, "pipelined_key")
			return nil
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
		Expect(cmds[0].(*redis.StatusCmd).Val()).To(Equal("OK"))
		Expect(cmds[1].(*redis.StringCmd).Val()).To(Equal("pipelined_value"))
	})

	It("should support AutoPipeline method", func() {
		// AutoPipeline should return itself
		ap2 := ap.AutoPipeline()
		Expect(ap2).To(Equal(ap))
	})

	It("should mix autopipelined and direct commands", func() {
		// Use autopipeline commands
		ap.Set(ctx, "ap_key1", "ap_value1", 0)
		ap.Set(ctx, "ap_key2", "ap_value2", 0)

		// Use traditional pipeline
		pipe := ap.Pipeline()
		pipe.Set(ctx, "pipe_key1", "pipe_value1", 0)
		pipe.Set(ctx, "pipe_key2", "pipe_value2", 0)
		_, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Verify all keys exist
		val1, _ := ap.Get(ctx, "ap_key1").Result()
		val2, _ := ap.Get(ctx, "ap_key2").Result()
		val3, _ := ap.Get(ctx, "pipe_key1").Result()
		val4, _ := ap.Get(ctx, "pipe_key2").Result()

		Expect(val1).To(Equal("ap_value1"))
		Expect(val2).To(Equal("ap_value2"))
		Expect(val3).To(Equal("pipe_value1"))
		Expect(val4).To(Equal("pipe_value2"))
	})
})

