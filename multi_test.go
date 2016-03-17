package redis_test

import (
	"strconv"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("Multi", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should Watch", func() {
		var incr func(string) error

		// Transactionally increments key using GET and SET commands.
		incr = func(key string) error {
			tx, err := client.Watch(key)
			if err != nil {
				return err
			}
			defer tx.Close()

			n, err := tx.Get(key).Int64()
			if err != nil && err != redis.Nil {
				return err
			}

			_, err = tx.Exec(func() error {
				tx.Set(key, strconv.FormatInt(n+1, 10), 0)
				return nil
			})
			if err == redis.TxFailedErr {
				return incr(key)
			}
			return err
		}

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				err := incr("key")
				Expect(err).NotTo(HaveOccurred())
			}()
		}
		wg.Wait()

		n, err := client.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(100)))
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
		cn, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.NetConn = &badConn{}
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

	It("should recover from bad connection when there are no commands", func() {
		// Put bad connection in the pool.
		cn, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.NetConn = &badConn{}
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		{
			tx, err := client.Watch("key")
			Expect(err).To(MatchError("bad connection"))
			Expect(tx).To(BeNil())
		}

		{
			tx, err := client.Watch("key")
			Expect(err).NotTo(HaveOccurred())

			err = tx.Ping().Err()
			Expect(err).NotTo(HaveOccurred())

			err = tx.Close()
			Expect(err).NotTo(HaveOccurred())
		}
	})
})
