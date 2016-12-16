package redis_test

import (
	"gopkg.in/redis.v5"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("pipelining", func() {
	var client *redis.Client
	var pipe *redis.Pipeline

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("supports block style", func() {
		var get *redis.StringCmd
		cmds, err := client.Pipelined(func(pipe *redis.Pipeline) error {
			get = pipe.Get("foo")
			return nil
		})
		Expect(err).To(Equal(redis.Nil))
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0]).To(Equal(get))
		Expect(get.Err()).To(Equal(redis.Nil))
		Expect(get.Val()).To(Equal(""))
	})

	assertPipeline := func() {
		It("returns an error when there are no commands", func() {
			_, err := pipe.Exec()
			Expect(err).To(MatchError("redis: pipeline is empty"))
		})

		It("discards queued commands", func() {
			pipe.Get("key")
			pipe.Discard()
			_, err := pipe.Exec()
			Expect(err).To(MatchError("redis: pipeline is empty"))
		})

		It("handles val/err", func() {
			err := client.Set("key", "value", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			get := pipe.Get("key")
			cmds, err := pipe.Exec()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))

			val, err := get.Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value"))
		})
	}

	Describe("Pipeline", func() {
		BeforeEach(func() {
			pipe = client.Pipeline()
		})

		assertPipeline()
	})

	Describe("TxPipeline", func() {
		BeforeEach(func() {
			pipe = client.TxPipeline()
		})

		assertPipeline()
	})
})
