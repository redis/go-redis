package redis_test

import (
	"strconv"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v4"
)

var _ = Describe("Tx", func() {
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
			err := client.Watch(func(tx *redis.Tx) error {
				n, err := tx.Get(key).Int64()
				if err != nil && err != redis.Nil {
					return err
				}

				_, err = tx.MultiExec(func() error {
					tx.Set(key, strconv.FormatInt(n+1, 10), 0)
					return nil
				})
				return err
			}, key)
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
		err := client.Watch(func(tx *redis.Tx) error {
			cmds, err := tx.MultiExec(func() error {
				tx.Set("key1", "hello1", 0)
				tx.Discard()
				tx.Set("key2", "hello2", 0)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
			return err
		}, "key1", "key2")
		Expect(err).NotTo(HaveOccurred())

		get := client.Get("key1")
		Expect(get.Err()).To(Equal(redis.Nil))
		Expect(get.Val()).To(Equal(""))

		get = client.Get("key2")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello2"))
	})

	It("should exec empty", func() {
		err := client.Watch(func(tx *redis.Tx) error {
			cmds, err := tx.MultiExec(func() error { return nil })
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(0))
			return err
		})
		Expect(err).NotTo(HaveOccurred())

		v, err := client.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("PONG"))
	})

	It("should exec bulks", func() {
		const N = 20000

		err := client.Watch(func(tx *redis.Tx) error {
			cmds, err := tx.MultiExec(func() error {
				for i := 0; i < N; i++ {
					tx.Incr("key")
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cmds)).To(Equal(N))
			for _, cmd := range cmds {
				Expect(cmd.Err()).NotTo(HaveOccurred())
			}
			return err
		})
		Expect(err).NotTo(HaveOccurred())

		num, err := client.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(N)))
	})

	It("should recover from bad connection", func() {
		// Put bad connection in the pool.
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.NetConn = &badConn{}
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		do := func() error {
			err := client.Watch(func(tx *redis.Tx) error {
				_, err := tx.MultiExec(func() error {
					tx.Ping()
					return nil
				})
				return err
			})
			return err
		}

		err = do()
		Expect(err).To(MatchError("bad connection"))

		err = do()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should recover from bad connection when there are no commands", func() {
		// Put bad connection in the pool.
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.NetConn = &badConn{}
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		do := func() error {
			err := client.Watch(func(tx *redis.Tx) error {
				_, err := tx.MultiExec(func() error {
					return nil
				})
				return err
			}, "key")
			return err
		}

		err = do()
		Expect(err).To(MatchError("bad connection"))

		err = do()
		Expect(err).NotTo(HaveOccurred())
	})
})
