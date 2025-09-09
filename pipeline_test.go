package redis_test

import (
	"errors"
	"strconv"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("pipelining", func() {
	var client *redis.Client
	var pipe *redis.Pipeline

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("supports block style", func() {
		var get *redis.StringCmd
		cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			get = pipe.Get(ctx, "foo")
			return nil
		})
		Expect(err).To(Equal(redis.Nil))
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0]).To(Equal(get))
		Expect(get.Err()).To(Equal(redis.Nil))
		Expect(get.Val()).To(Equal(""))
	})

	It("exports queued commands", func() {
		p := client.Pipeline()
		cmds := p.Cmds()
		Expect(cmds).To(BeEmpty())

		p.Set(ctx, "foo", "bar", 0)
		p.Get(ctx, "foo")
		cmds = p.Cmds()
		Expect(cmds).To(HaveLen(p.Len()))
		Expect(cmds[0].Name()).To(Equal("set"))
		Expect(cmds[1].Name()).To(Equal("get"))

		cmds, err := p.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
		Expect(cmds[0].Name()).To(Equal("set"))
		Expect(cmds[0].(*redis.StatusCmd).Val()).To(Equal("OK"))
		Expect(cmds[1].Name()).To(Equal("get"))
		Expect(cmds[1].(*redis.StringCmd).Val()).To(Equal("bar"))

		cmds = p.Cmds()
		Expect(cmds).To(BeEmpty())
	})

	assertPipeline := func() {
		It("returns no errors when there are no commands", func() {
			_, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("discards queued commands", func() {
			pipe.Get(ctx, "key")
			pipe.Discard()
			cmds, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(BeNil())
		})

		It("handles val/err", func() {
			err := client.Set(ctx, "key", "value", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			get := pipe.Get(ctx, "key")
			cmds, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))

			val, err := get.Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value"))
		})

		It("supports custom command", func() {
			pipe.Do(ctx, "ping")
			cmds, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
		})

		It("handles large pipelines", Label("NonRedisEnterprise"), func() {
			for callCount := 1; callCount < 16; callCount++ {
				for i := 1; i <= callCount; i++ {
					pipe.SetNX(ctx, strconv.Itoa(i)+"_key", strconv.Itoa(i)+"_value", 0)
				}

				cmds, err := pipe.Exec(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cmds).To(HaveLen(callCount))
				for _, cmd := range cmds {
					Expect(cmd).To(BeAssignableToTypeOf(&redis.BoolCmd{}))
				}
			}
		})

		It("should Exec, not Do", func() {
			err := pipe.Do(ctx).Err()
			Expect(err).To(Equal(errors.New("redis: please enter the command to be executed")))
		})

		It("should process", func() {
			err := pipe.Process(ctx, redis.NewCmd(ctx, "asking"))
			Expect(err).To(BeNil())
			Expect(pipe.Cmds()).To(HaveLen(1))
		})

		It("should batchProcess", func() {
			err := pipe.BatchProcess(ctx, redis.NewCmd(ctx, "asking"))
			Expect(err).To(BeNil())
			Expect(pipe.Cmds()).To(HaveLen(1))

			pipe.Discard()
			Expect(pipe.Cmds()).To(HaveLen(0))

			err = pipe.BatchProcess(ctx, redis.NewCmd(ctx, "asking"), redis.NewCmd(ctx, "set", "key", "value"))
			Expect(err).To(BeNil())
			Expect(pipe.Cmds()).To(HaveLen(2))
		})
	}

	Describe("Pipeline", func() {
		BeforeEach(func() {
			pipe = client.Pipeline().(*redis.Pipeline)
		})

		assertPipeline()
	})

	Describe("TxPipeline", func() {
		BeforeEach(func() {
			pipe = client.TxPipeline().(*redis.Pipeline)
		})

		assertPipeline()
	})
})
