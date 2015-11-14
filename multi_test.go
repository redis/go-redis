package redis_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("Multi", func() {
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

	It("should exec", func() {
		multi := client.Multi()
		defer func() {
			Expect(multi.Close()).NotTo(HaveOccurred())
		}()

		var (
			set *redis.StatusCmd
			get *redis.StringCmd
		)
		cmds, err := multi.Exec(func() error {
			set = multi.Set("key", "hello", 0)
			get = multi.Get("key")
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello"))
	})

	It("should discard", func() {
		multi := client.Multi()
		defer func() {
			Expect(multi.Close()).NotTo(HaveOccurred())
		}()

		cmds, err := multi.Exec(func() error {
			multi.Set("key1", "hello1", 0)
			multi.Discard()
			multi.Set("key2", "hello2", 0)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(1))

		get := client.Get("key1")
		Expect(get.Err()).To(Equal(redis.Nil))
		Expect(get.Val()).To(Equal(""))

		get = client.Get("key2")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello2"))
	})

	It("should exec empty", func() {
		multi := client.Multi()
		defer func() {
			Expect(multi.Close()).NotTo(HaveOccurred())
		}()

		cmds, err := multi.Exec(func() error { return nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(0))

		ping := multi.Ping()
		Expect(ping.Err()).NotTo(HaveOccurred())
		Expect(ping.Val()).To(Equal("PONG"))
	})

	It("should exec empty queue", func() {
		multi := client.Multi()
		defer func() {
			Expect(multi.Close()).NotTo(HaveOccurred())
		}()

		cmds, err := multi.Exec(func() error { return nil })
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(0))
	})

	It("should exec bulks", func() {
		multi := client.Multi()
		defer func() {
			Expect(multi.Close()).NotTo(HaveOccurred())
		}()

		cmds, err := multi.Exec(func() error {
			for i := int64(0); i < 20000; i++ {
				multi.Incr("key")
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(cmds)).To(Equal(20000))
		for _, cmd := range cmds {
			Expect(cmd.Err()).NotTo(HaveOccurred())
		}

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("20000"))
	})

	It("should recover from bad connection", func() {
		// Put bad connection in the pool.
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.SetNetConn(&badConn{})
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		multi := client.Multi()
		defer func() {
			Expect(multi.Close()).NotTo(HaveOccurred())
		}()

		_, err = multi.Exec(func() error {
			multi.Ping()
			return nil
		})
		Expect(err).To(MatchError("bad connection"))

		_, err = multi.Exec(func() error {
			multi.Ping()
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	})
})
