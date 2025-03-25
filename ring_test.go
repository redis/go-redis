package redis_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Redis Ring PROTO 2", func() {
	const heartbeat = 100 * time.Millisecond

	var ring *redis.Ring

	BeforeEach(func() {
		opt := redisRingOptions()
		opt.Protocol = 2
		opt.HeartbeatFrequency = heartbeat
		ring = redis.NewRing(opt)

		err := ring.ForEachShard(ctx, func(ctx context.Context, cl *redis.Client) error {
			return cl.FlushDB(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(ring.Close()).NotTo(HaveOccurred())
	})

	It("should ring PROTO 2", func() {
		_ = ring.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			val, err := c.Do(ctx, "HELLO").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(ContainElements("proto", int64(2)))
			return nil
		})
	})
})

var _ = Describe("Redis Ring", func() {
	const heartbeat = 100 * time.Millisecond

	var ring *redis.Ring

	setRingKeys := func() {
		for i := 0; i < 100; i++ {
			err := ring.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		}
	}

	BeforeEach(func() {
		opt := redisRingOptions()
		opt.ClientName = "ring_hi"
		opt.HeartbeatFrequency = heartbeat
		ring = redis.NewRing(opt)

		err := ring.ForEachShard(ctx, func(ctx context.Context, cl *redis.Client) error {
			return cl.FlushDB(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(ring.Close()).NotTo(HaveOccurred())
	})

	It("supports context", func() {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := ring.Ping(ctx).Err()
		Expect(err).To(MatchError("context canceled"))
	})

	It("should ring client setname", func() {
		err := ring.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			return c.Ping(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())

		_ = ring.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			val, err := c.ClientList(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(ContainSubstring("name=ring_hi"))
			return nil
		})
	})

	It("should ring PROTO 3", func() {
		_ = ring.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
			val, err := c.Do(ctx, "HELLO").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(HaveKeyWithValue("proto", int64(3)))
			return nil
		})
	})

	It("distributes keys", func() {
		setRingKeys()

		// Both shards should have some keys now.
		Expect(ringShard1.Info(ctx, "keyspace").Val()).To(ContainSubstring("keys=56"))
		Expect(ringShard2.Info(ctx, "keyspace").Val()).To(ContainSubstring("keys=44"))
	})

	It("distributes keys when using EVAL", func() {
		script := redis.NewScript(`
			local r = redis.call('SET', KEYS[1], ARGV[1])
			return r
		`)

		var key string
		for i := 0; i < 100; i++ {
			key = fmt.Sprintf("key%d", i)
			err := script.Run(ctx, ring, []string{key}, "value").Err()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(ringShard1.Info(ctx, "keyspace").Val()).To(ContainSubstring("keys=56"))
		Expect(ringShard2.Info(ctx, "keyspace").Val()).To(ContainSubstring("keys=44"))
	})

	It("supports hash tags", func() {
		for i := 0; i < 100; i++ {
			err := ring.Set(ctx, fmt.Sprintf("key%d{tag}", i), "value", 0).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(ringShard1.Info(ctx, "keyspace").Val()).ToNot(ContainSubstring("keys="))
		Expect(ringShard2.Info(ctx, "keyspace").Val()).To(ContainSubstring("keys=100"))
	})

	Describe("[new] dynamic setting ring shards", func() {
		It("downscale shard and check reuse shard, upscale shard and check reuse", func() {
			Expect(ring.Len(), 2)

			wantShard := ring.ShardByName("ringShardOne")
			ring.SetAddrs(map[string]string{
				"ringShardOne": ":" + ringShard1Port,
			})
			Expect(ring.Len(), 1)
			gotShard := ring.ShardByName("ringShardOne")
			Expect(gotShard).To(BeIdenticalTo(wantShard))

			ring.SetAddrs(map[string]string{
				"ringShardOne": ":" + ringShard1Port,
				"ringShardTwo": ":" + ringShard2Port,
			})
			Expect(ring.Len(), 2)
			gotShard = ring.ShardByName("ringShardOne")
			Expect(gotShard).To(BeIdenticalTo(wantShard))
		})

		It("uses 3 shards after setting it to 3 shards", func() {
			Expect(ring.Len(), 2)

			shardName1 := "ringShardOne"
			shardAddr1 := ":" + ringShard1Port
			wantShard1 := ring.ShardByName(shardName1)
			shardName2 := "ringShardTwo"
			shardAddr2 := ":" + ringShard2Port
			wantShard2 := ring.ShardByName(shardName2)
			shardName3 := "ringShardThree"
			shardAddr3 := ":" + ringShard3Port

			ring.SetAddrs(map[string]string{
				shardName1: shardAddr1,
				shardName2: shardAddr2,
				shardName3: shardAddr3,
			})
			Expect(ring.Len(), 3)
			gotShard1 := ring.ShardByName(shardName1)
			gotShard2 := ring.ShardByName(shardName2)
			gotShard3 := ring.ShardByName(shardName3)
			Expect(gotShard1).To(BeIdenticalTo(wantShard1))
			Expect(gotShard2).To(BeIdenticalTo(wantShard2))
			Expect(gotShard3).ToNot(BeNil())

			ring.SetAddrs(map[string]string{
				shardName1: shardAddr1,
				shardName2: shardAddr2,
			})
			Expect(ring.Len(), 2)
			gotShard1 = ring.ShardByName(shardName1)
			gotShard2 = ring.ShardByName(shardName2)
			gotShard3 = ring.ShardByName(shardName3)
			Expect(gotShard1).To(BeIdenticalTo(wantShard1))
			Expect(gotShard2).To(BeIdenticalTo(wantShard2))
			Expect(gotShard3).To(BeNil())
		})
	})
	Describe("pipeline", func() {
		It("doesn't panic closed ring, returns error", func() {
			pipe := ring.Pipeline()
			for i := 0; i < 3; i++ {
				err := pipe.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			Expect(ring.Close()).NotTo(HaveOccurred())

			Expect(func() {
				_, execErr := pipe.Exec(ctx)
				Expect(execErr).To(HaveOccurred())
			}).NotTo(Panic())
		})

		It("distributes keys", func() {
			pipe := ring.Pipeline()
			for i := 0; i < 100; i++ {
				err := pipe.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}
			cmds, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(100))

			for _, cmd := range cmds {
				Expect(cmd.Err()).NotTo(HaveOccurred())
				Expect(cmd.(*redis.StatusCmd).Val()).To(Equal("OK"))
			}

			// Both shards should have some keys now.
			Expect(ringShard1.Info(ctx).Val()).To(ContainSubstring("keys=56"))
			Expect(ringShard2.Info(ctx).Val()).To(ContainSubstring("keys=44"))
		})

		It("is consistent with ring", func() {
			var keys []string
			for i := 0; i < 100; i++ {
				key := make([]byte, 64)
				_, err := rand.Read(key)
				Expect(err).NotTo(HaveOccurred())
				keys = append(keys, string(key))
			}

			_, err := ring.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, key := range keys {
					pipe.Set(ctx, key, "value", 0).Err()
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			for _, key := range keys {
				val, err := ring.Get(ctx, key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal("value"))
			}
		})

		It("supports hash tags", func() {
			_, err := ring.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for i := 0; i < 100; i++ {
					pipe.Set(ctx, fmt.Sprintf("key%d{tag}", i), "value", 0).Err()
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(ringShard1.Info(ctx).Val()).ToNot(ContainSubstring("keys="))
			Expect(ringShard2.Info(ctx).Val()).To(ContainSubstring("keys=100"))
		})
	})

	Describe("new client callback", func() {
		It("can be initialized with a new client callback", func() {
			opts := redisRingOptions()
			opts.NewClient = func(opt *redis.Options) *redis.Client {
				opt.Username = "username1"
				opt.Password = "password1"
				return redis.NewClient(opt)
			}
			ring = redis.NewRing(opts)

			err := ring.Ping(ctx).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGPASS"))
		})
	})

	Describe("Process hook", func() {
		BeforeEach(func() {
			// the health check leads to data race for variable "stack []string".
			// here, the health check time is set to 72 hours to avoid health check
			opt := redisRingOptions()
			opt.HeartbeatFrequency = 72 * time.Hour
			ring = redis.NewRing(opt)
		})
		It("supports Process hook", func() {
			err := ring.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			var stack []string

			ring.AddHook(&hook{
				processHook: func(hook redis.ProcessHook) redis.ProcessHook {
					return func(ctx context.Context, cmd redis.Cmder) error {
						Expect(cmd.String()).To(Equal("ping: "))
						stack = append(stack, "ring.BeforeProcess")

						err := hook(ctx, cmd)

						Expect(cmd.String()).To(Equal("ping: PONG"))
						stack = append(stack, "ring.AfterProcess")

						return err
					}
				},
			})

			ring.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
				shard.AddHook(&hook{
					processHook: func(hook redis.ProcessHook) redis.ProcessHook {
						return func(ctx context.Context, cmd redis.Cmder) error {
							Expect(cmd.String()).To(Equal("ping: "))
							stack = append(stack, "shard.BeforeProcess")

							err := hook(ctx, cmd)

							Expect(cmd.String()).To(Equal("ping: PONG"))
							stack = append(stack, "shard.AfterProcess")

							return err
						}
					},
				})
				return nil
			})

			err = ring.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(stack).To(Equal([]string{
				"ring.BeforeProcess",
				"shard.BeforeProcess",
				"shard.AfterProcess",
				"ring.AfterProcess",
			}))
		})

		It("supports Pipeline hook", func() {
			err := ring.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			var stack []string

			ring.AddHook(&hook{
				processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
					return func(ctx context.Context, cmds []redis.Cmder) error {
						Expect(cmds).To(HaveLen(1))
						Expect(cmds[0].String()).To(Equal("ping: "))
						stack = append(stack, "ring.BeforeProcessPipeline")

						err := hook(ctx, cmds)

						Expect(cmds).To(HaveLen(1))
						Expect(cmds[0].String()).To(Equal("ping: PONG"))
						stack = append(stack, "ring.AfterProcessPipeline")

						return err
					}
				},
			})

			ring.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
				shard.AddHook(&hook{
					processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
						return func(ctx context.Context, cmds []redis.Cmder) error {
							Expect(cmds).To(HaveLen(1))
							Expect(cmds[0].String()).To(Equal("ping: "))
							stack = append(stack, "shard.BeforeProcessPipeline")

							err := hook(ctx, cmds)

							Expect(cmds).To(HaveLen(1))
							Expect(cmds[0].String()).To(Equal("ping: PONG"))
							stack = append(stack, "shard.AfterProcessPipeline")

							return err
						}
					},
				})
				return nil
			})

			_, err = ring.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(stack).To(Equal([]string{
				"ring.BeforeProcessPipeline",
				"shard.BeforeProcessPipeline",
				"shard.AfterProcessPipeline",
				"ring.AfterProcessPipeline",
			}))
		})

		It("supports TxPipeline hook", func() {
			err := ring.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			var stack []string

			ring.AddHook(&hook{
				processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
					return func(ctx context.Context, cmds []redis.Cmder) error {
						defer GinkgoRecover()

						Expect(cmds).To(HaveLen(3))
						Expect(cmds[1].String()).To(Equal("ping: "))
						stack = append(stack, "ring.BeforeProcessPipeline")

						err := hook(ctx, cmds)

						Expect(cmds).To(HaveLen(3))
						Expect(cmds[1].String()).To(Equal("ping: PONG"))
						stack = append(stack, "ring.AfterProcessPipeline")

						return err
					}
				},
			})

			ring.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
				shard.AddHook(&hook{
					processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
						return func(ctx context.Context, cmds []redis.Cmder) error {
							defer GinkgoRecover()

							Expect(cmds).To(HaveLen(3))
							Expect(cmds[1].String()).To(Equal("ping: "))
							stack = append(stack, "shard.BeforeProcessPipeline")

							err := hook(ctx, cmds)

							Expect(cmds).To(HaveLen(3))
							Expect(cmds[1].String()).To(Equal("ping: PONG"))
							stack = append(stack, "shard.AfterProcessPipeline")

							return err
						}
					},
				})
				return nil
			})

			_, err = ring.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(stack).To(Equal([]string{
				"ring.BeforeProcessPipeline",
				"shard.BeforeProcessPipeline",
				"shard.AfterProcessPipeline",
				"ring.AfterProcessPipeline",
			}))
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
		err := ring.Ping(ctx).Err()
		Expect(err).To(MatchError("redis: all ring shards are down"))
	})

	It("pipeline returns an error", func() {
		_, err := ring.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Ping(ctx)
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

		err := ring.ForEachShard(ctx, func(ctx context.Context, cl *redis.Client) error {
			return cl.FlushDB(ctx).Err()
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
			err := ring.Watch(ctx, func(tx *redis.Tx) error {
				n, err := tx.Get(ctx, key).Int64()
				if err != nil && err != redis.Nil {
					return err
				}

				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, key, strconv.FormatInt(n+1, 10), 0)
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

		n, err := ring.Get(ctx, "key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(100)))
	})

	It("should discard", func() {
		err := ring.Watch(ctx, func(tx *redis.Tx) error {
			cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "{shard}key1", "hello1", 0)
				pipe.Discard()
				pipe.Set(ctx, "{shard}key2", "hello2", 0)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
			return err
		}, "{shard}key1", "{shard}key2")
		Expect(err).NotTo(HaveOccurred())

		get := ring.Get(ctx, "{shard}key1")
		Expect(get.Err()).To(Equal(redis.Nil))
		Expect(get.Val()).To(Equal(""))

		get = ring.Get(ctx, "{shard}key2")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello2"))
	})

	It("returns no error when there are no commands", func() {
		err := ring.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(redis.Pipeliner) error { return nil })
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())

		v, err := ring.Ping(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("PONG"))
	})

	It("should exec bulks", func() {
		const N = 20000

		err := ring.Watch(ctx, func(tx *redis.Tx) error {
			cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				for i := 0; i < N; i++ {
					pipe.Incr(ctx, "key")
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

		num, err := ring.Get(ctx, "key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(num).To(Equal(int64(N)))
	})

	It("should Watch/Unwatch", func() {
		var C, N int

		err := ring.Set(ctx, "key", "0", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := ring.Watch(ctx, func(tx *redis.Tx) error {
					val, err := tx.Get(ctx, "key").Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(val).NotTo(Equal(redis.Nil))

					num, err := strconv.ParseInt(val, 10, 64)
					Expect(err).NotTo(HaveOccurred())

					cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Set(ctx, "key", strconv.FormatInt(num+1, 10), 0)
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

		val, err := ring.Get(ctx, "key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(int64(C * N)))
	})

	It("should close Tx without closing the client", func() {
		err := ring.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			return err
		}, "key")
		Expect(err).NotTo(HaveOccurred())

		Expect(ring.Ping(ctx).Err()).NotTo(HaveOccurred())
	})

	It("respects max size on multi", func() {
		//this test checks the number of "pool.conn"
		//if the health check is performed at the same time
		//conn will be used, resulting in an abnormal number of "pool.conn".
		//
		//redis.NewRing() does not have an option to prohibit health checks.
		//set a relatively large time here to avoid health checks.
		opt := redisRingOptions()
		opt.HeartbeatFrequency = 72 * time.Hour
		ring = redis.NewRing(opt)

		perform(1000, func(id int) {
			var ping *redis.StatusCmd

			err := ring.Watch(ctx, func(tx *redis.Tx) error {
				cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					ping = pipe.Ping(ctx)
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

		ring.ForEachShard(ctx, func(ctx context.Context, cl *redis.Client) error {
			defer GinkgoRecover()

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
			err := ring.Watch(ctx, func(tx *redis.Tx) error {
				return tx.Ping(ctx).Err()
			}, "foo")
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})

		It("Tx Pipeline timeouts", func() {
			err := ring.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Ping(ctx)
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

			err := ring.ForEachShard(ctx, func(ctx context.Context, client *redis.Client) error {
				return client.ClientPause(ctx, pause).Err()
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			_ = ring.ForEachShard(ctx, func(ctx context.Context, client *redis.Client) error {
				defer GinkgoRecover()
				Eventually(func() error {
					return client.Ping(ctx).Err()
				}, 2*pause).ShouldNot(HaveOccurred())
				return nil
			})
		})

		testTimeout()
	})
})
