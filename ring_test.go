package redis_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Redis Ring", func() {
	const heartbeat = 100 * time.Millisecond

	var ring *redis.Ring

	setRingKeys := func() {
		for i := 0; i < 100; i++ {
			err := ring.Set(fmt.Sprintf("key%d", i), "value", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		}
	}

	BeforeEach(func() {
		opt := redisRingOptions()
		opt.HeartbeatFrequency = heartbeat
		ring = redis.NewRing(opt)

		err := ring.ForEachShard(func(cl *redis.Client) error {
			return cl.FlushDB().Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(ring.Close()).NotTo(HaveOccurred())
	})

	It("distributes keys", func() {
		setRingKeys()

		// Both shards should have some keys now.
		Expect(ringShard1.Info("keyspace").Val()).To(ContainSubstring("keys=57"))
		Expect(ringShard2.Info("keyspace").Val()).To(ContainSubstring("keys=43"))
	})

	It("distributes keys when using EVAL", func() {
		script := redis.NewScript(`
			local r = redis.call('SET', KEYS[1], ARGV[1])
			return r
		`)

		var key string
		for i := 0; i < 100; i++ {
			key = fmt.Sprintf("key%d", i)
			err := script.Run(ring, []string{key}, "value").Err()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(ringShard1.Info("keyspace").Val()).To(ContainSubstring("keys=57"))
		Expect(ringShard2.Info("keyspace").Val()).To(ContainSubstring("keys=43"))
	})

	It("uses single shard when one of the shards is down", func() {
		// Stop ringShard2.
		Expect(ringShard2.Close()).NotTo(HaveOccurred())

		Eventually(func() int {
			return ring.Len()
		}, "30s").Should(Equal(1))

		setRingKeys()

		// RingShard1 should have all keys.
		Expect(ringShard1.Info("keyspace").Val()).To(ContainSubstring("keys=100"))

		// Start ringShard2.
		var err error
		ringShard2, err = startRedis(ringShard2Port)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int {
			return ring.Len()
		}, "30s").Should(Equal(2))

		setRingKeys()

		// RingShard2 should have its keys.
		Expect(ringShard2.Info("keyspace").Val()).To(ContainSubstring("keys=43"))
	})

	It("supports hash tags", func() {
		for i := 0; i < 100; i++ {
			err := ring.Set(fmt.Sprintf("key%d{tag}", i), "value", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(ringShard1.Info("keyspace").Val()).ToNot(ContainSubstring("keys="))
		Expect(ringShard2.Info("keyspace").Val()).To(ContainSubstring("keys=100"))
	})

	It("propagates process for WithContext", func() {
		var fromWrap []string
		wrapper := func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
			return func(cmd redis.Cmder) error {
				fromWrap = append(fromWrap, cmd.Name())

				return oldProcess(cmd)
			}
		}

		ctx := context.Background()
		ring = ring.WithContext(ctx)
		ring.WrapProcess(wrapper)

		ring.Ping()
		Expect(fromWrap).To(Equal([]string{"ping"}))

		ring.Ping()
		Expect(fromWrap).To(Equal([]string{"ping", "ping"}))
	})

	Describe("pipeline", func() {
		It("distributes keys", func() {
			pipe := ring.Pipeline()
			for i := 0; i < 100; i++ {
				err := pipe.Set(fmt.Sprintf("key%d", i), "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}
			cmds, err := pipe.Exec()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(100))
			Expect(pipe.Close()).NotTo(HaveOccurred())

			for _, cmd := range cmds {
				Expect(cmd.Err()).NotTo(HaveOccurred())
				Expect(cmd.(*redis.StatusCmd).Val()).To(Equal("OK"))
			}

			// Both shards should have some keys now.
			Expect(ringShard1.Info().Val()).To(ContainSubstring("keys=57"))
			Expect(ringShard2.Info().Val()).To(ContainSubstring("keys=43"))
		})

		It("is consistent with ring", func() {
			var keys []string
			for i := 0; i < 100; i++ {
				key := make([]byte, 64)
				_, err := rand.Read(key)
				Expect(err).NotTo(HaveOccurred())
				keys = append(keys, string(key))
			}

			_, err := ring.Pipelined(func(pipe redis.Pipeliner) error {
				for _, key := range keys {
					pipe.Set(key, "value", 0).Err()
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			for _, key := range keys {
				val, err := ring.Get(key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal("value"))
			}
		})

		It("supports hash tags", func() {
			_, err := ring.Pipelined(func(pipe redis.Pipeliner) error {
				for i := 0; i < 100; i++ {
					pipe.Set(fmt.Sprintf("key%d{tag}", i), "value", 0).Err()
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(ringShard1.Info().Val()).ToNot(ContainSubstring("keys="))
			Expect(ringShard2.Info().Val()).To(ContainSubstring("keys=100"))
		})
	})
})

var _ = Describe("empty Redis Ring", func() {
	var ring *redis.Ring

	BeforeEach(func() {
		ring = redis.NewRing(&redis.RingOptions{})
	})

	AfterEach(func() {
		Expect(ring.Close()).NotTo(HaveOccurred())
	})

	It("returns an error", func() {
		err := ring.Ping().Err()
		Expect(err).To(MatchError("redis: all ring shards are down"))
	})

	It("pipeline returns an error", func() {
		_, err := ring.Pipelined(func(pipe redis.Pipeliner) error {
			pipe.Ping()
			return nil
		})
		Expect(err).To(MatchError("redis: all ring shards are down"))
	})
})

var _ = Describe("Ring watch", func() {
	const heartbeat = 100 * time.Millisecond

	var ring *redis.Ring

	BeforeEach(func() {
		opt := redisRingOptions()
		opt.HeartbeatFrequency = heartbeat
		ring = redis.NewRing(opt)

		err := ring.ForEachShard(func(cl *redis.Client) error {
			return cl.FlushDB().Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(ring.Close()).NotTo(HaveOccurred())
	})

	It("should Watch", func() {
		var incr func(string) error

		// Transactionally increments key using GET and SET commands.
		incr = func(key string) error {
			err := ring.Watch(func(tx *redis.Tx) error {
				n, err := tx.Get(key).Int64()
				if err != nil && err != redis.Nil {
					return err
				}

				_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
					pipe.Set(key, strconv.FormatInt(n+1, 10), 0)
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

		n, err := ring.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(100)))
	})

	It("should discard", func() {
		err := ring.Watch(func(tx *redis.Tx) error {
			cmds, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.Set("key1", "hello1", 0)
				pipe.Discard()
				pipe.Set("key2", "hello2", 0)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
			return err
		}, "key1", "key2")
		Expect(err).NotTo(HaveOccurred())

		get := ring.Get("key1")
		Expect(get.Err()).To(Equal(redis.Nil))
		Expect(get.Val()).To(Equal(""))

		get = ring.Get("key2")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello2"))
	})

	It("returns no error when there are no commands", func() {
		err := ring.Watch(func(tx *redis.Tx) error {
			_, err := tx.Pipelined(func(redis.Pipeliner) error { return nil })
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())

		v, err := ring.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("PONG"))
	})

	It("should exec bulks", func() {
		const N = 20000

		err := ring.Watch(func(tx *redis.Tx) error {
			cmds, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				for i := 0; i < N; i++ {
					pipe.Incr("key")
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cmds)).To(Equal(N))
			for _, cmd := range cmds {
				Expect(cmd.Err()).NotTo(HaveOccurred())
			}
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())

		num, err := ring.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(N)))
	})

	It("should Watch/Unwatch", func() {
		var C, N int

		err := ring.Set("key", "0", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := ring.Watch(func(tx *redis.Tx) error {
					val, err := tx.Get("key").Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(val).NotTo(Equal(redis.Nil))

					num, err := strconv.ParseInt(val, 10, 64)
					Expect(err).NotTo(HaveOccurred())

					cmds, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
						pipe.Set("key", strconv.FormatInt(num+1, 10), 0)
						return nil
					})
					Expect(cmds).To(HaveLen(1))
					return err
				}, "key")
				if err == redis.TxFailedErr {
					i--
					continue
				}
				Expect(err).NotTo(HaveOccurred())
			}
		})

		val, err := ring.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(int64(C * N)))
	})

	It("should close Tx without closing the client", func() {
		err := ring.Watch(func(tx *redis.Tx) error {
			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				pipe.Ping()
				return nil
			})
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())

		Expect(ring.Ping().Err()).NotTo(HaveOccurred())
	})

	It("respects max size on multi", func() {
		perform(1000, func(id int) {
			var ping *redis.StatusCmd

			err := ring.Watch(func(tx *redis.Tx) error {
				cmds, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
					ping = pipe.Ping()
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(cmds).To(HaveLen(1))
				return err
			}, "key")
			Expect(err).NotTo(HaveOccurred())

			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
		})

		ring.ForEachShard(func(cl *redis.Client) error {
			pool := cl.Pool()
			Expect(pool.Len()).To(BeNumerically("<=", 10))
			Expect(pool.IdleLen()).To(BeNumerically("<=", 10))
			Expect(pool.Len()).To(Equal(pool.IdleLen()))

			return nil
		})
	})
})

var _ = Describe("Ring Tx timeout", func() {
	const heartbeat = 100 * time.Millisecond

	var ring *redis.Ring

	AfterEach(func() {
		_ = ring.Close()
	})

	testTimeout := func() {
		It("Tx timeouts", func() {
			err := ring.Watch(func(tx *redis.Tx) error {
				return tx.Ping().Err()
			}, "foo")
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})

		It("Tx Pipeline timeouts", func() {
			err := ring.Watch(func(tx *redis.Tx) error {
				_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
					pipe.Ping()
					return nil
				})
				return err
			}, "foo")
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})
	}

	const pause = 5 * time.Second

	Context("read/write timeout", func() {
		BeforeEach(func() {
			opt := redisRingOptions()
			opt.ReadTimeout = 250 * time.Millisecond
			opt.WriteTimeout = 250 * time.Millisecond
			opt.HeartbeatFrequency = heartbeat
			ring = redis.NewRing(opt)

			err := ring.ForEachShard(func(client *redis.Client) error {
				return client.ClientPause(pause).Err()
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			_ = ring.ForEachShard(func(client *redis.Client) error {
				defer GinkgoRecover()
				Eventually(func() error {
					return client.Ping().Err()
				}, 2*pause).ShouldNot(HaveOccurred())
				return nil
			})
		})

		testTimeout()
	})
})
