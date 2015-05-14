package redis_test

import (
	"strconv"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("Pipelining", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should pipeline", func() {
		set := client.Set("key2", "hello2", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		pipeline := client.Pipeline()
		set = pipeline.Set("key1", "hello1", 0)
		get := pipeline.Get("key2")
		incr := pipeline.Incr("key3")
		getNil := pipeline.Get("key4")

		cmds, err := pipeline.Exec()
		Expect(err).To(Equal(redis.Nil))
		Expect(cmds).To(HaveLen(4))
		Expect(pipeline.Close()).NotTo(HaveOccurred())

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello2"))

		Expect(incr.Err()).NotTo(HaveOccurred())
		Expect(incr.Val()).To(Equal(int64(1)))

		Expect(getNil.Err()).To(Equal(redis.Nil))
		Expect(getNil.Val()).To(Equal(""))
	})

	It("should discard", func() {
		pipeline := client.Pipeline()

		pipeline.Get("key")
		pipeline.Discard()
		cmds, err := pipeline.Exec()
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(0))
		Expect(pipeline.Close()).NotTo(HaveOccurred())
	})

	It("should support block style", func() {
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

	It("should handle vals/err", func() {
		pipeline := client.Pipeline()

		get := pipeline.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal(""))
		Expect(pipeline.Close()).NotTo(HaveOccurred())
	})

	It("should pipeline with empty queue", func() {
		pipeline := client.Pipeline()
		cmds, err := pipeline.Exec()
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(0))
		Expect(pipeline.Close()).NotTo(HaveOccurred())
	})

	It("should increment correctly", func() {
		const N = 20000
		key := "TestPipelineIncr"
		pipeline := client.Pipeline()
		for i := 0; i < N; i++ {
			pipeline.Incr(key)
		}

		cmds, err := pipeline.Exec()
		Expect(err).NotTo(HaveOccurred())
		Expect(pipeline.Close()).NotTo(HaveOccurred())

		Expect(len(cmds)).To(Equal(20000))
		for _, cmd := range cmds {
			Expect(cmd.Err()).NotTo(HaveOccurred())
		}

		get := client.Get(key)
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal(strconv.Itoa(N)))
	})

	It("should PipelineEcho", func() {
		const N = 1000

		wg := &sync.WaitGroup{}
		wg.Add(N)
		for i := 0; i < N; i++ {
			go func(i int) {
				defer GinkgoRecover()
				defer wg.Done()

				pipeline := client.Pipeline()

				msg1 := "echo" + strconv.Itoa(i)
				msg2 := "echo" + strconv.Itoa(i+1)

				echo1 := pipeline.Echo(msg1)
				echo2 := pipeline.Echo(msg2)

				cmds, err := pipeline.Exec()
				Expect(err).NotTo(HaveOccurred())
				Expect(cmds).To(HaveLen(2))

				Expect(echo1.Err()).NotTo(HaveOccurred())
				Expect(echo1.Val()).To(Equal(msg1))

				Expect(echo2.Err()).NotTo(HaveOccurred())
				Expect(echo2.Val()).To(Equal(msg2))

				Expect(pipeline.Close()).NotTo(HaveOccurred())
			}(i)
		}
		wg.Wait()
	})

})
