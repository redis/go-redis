package redis_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
)

type clusterScenario struct {
	ports   []string
	nodeIDs []string
	clients map[string]*redis.Client
}

func (s *clusterScenario) slots() []int {
	return []int{0, 5461, 10923, 16384}
}

func (s *clusterScenario) masters() []*redis.Client {
	result := make([]*redis.Client, 3)
	for pos, port := range s.ports[:3] {
		result[pos] = s.clients[port]
	}
	return result
}

func (s *clusterScenario) slaves() []*redis.Client {
	result := make([]*redis.Client, 3)
	for pos, port := range s.ports[3:] {
		result[pos] = s.clients[port]
	}
	return result
}

func (s *clusterScenario) addrs() []string {
	addrs := make([]string, len(s.ports))
	for i, port := range s.ports {
		addrs[i] = net.JoinHostPort("127.0.0.1", port)
	}
	return addrs
}

func (s *clusterScenario) newClusterClientUnstable(opt *redis.ClusterOptions) *redis.ClusterClient {
	opt.Addrs = s.addrs()
	return redis.NewClusterClient(opt)
}

func (s *clusterScenario) newClusterClient(
	ctx context.Context, opt *redis.ClusterOptions,
) *redis.ClusterClient {
	client := s.newClusterClientUnstable(opt)
	client.SetCommandInfoResolver(client.NewDynamicResolver())
	err := eventually(func() error {
		if opt.ClusterSlots != nil {
			return nil
		}

		state, err := client.LoadState(ctx)
		if err != nil {
			return err
		}

		if !state.IsConsistent(ctx) {
			return fmt.Errorf("cluster state is not consistent")
		}

		return nil
	}, 30*time.Second)
	if err != nil {
		panic(err)
	}

	return client
}

func (s *clusterScenario) Close() error {
	ctx := context.TODO()
	for _, master := range s.masters() {
		if master == nil {
			continue
		}
		err := master.FlushAll(ctx).Err()
		if err != nil {
			return err
		}

		// since 7.2 forget calls should be propagated, calling only master
		// nodes should be sufficient.
		for _, nID := range s.nodeIDs {
			master.ClusterForget(ctx, nID)
		}
	}

	return nil
}

func configureClusterTopology(ctx context.Context, scenario *clusterScenario) error {
	allowErrs := []string{
		"ERR Slot 0 is already busy",
		"ERR Slot 5461 is already busy",
		"ERR Slot 10923 is already busy",
		"ERR Slot 16384 is already busy",
	}

	err := collectNodeInformation(ctx, scenario)
	if err != nil {
		return err
	}

	// Meet cluster nodes.
	for _, client := range scenario.clients {
		err := client.ClusterMeet(ctx, "127.0.0.1", scenario.ports[0]).Err()
		if err != nil {
			return err
		}
	}

	slots := scenario.slots()
	for pos, master := range scenario.masters() {
		err := master.ClusterAddSlotsRange(ctx, slots[pos], slots[pos+1]-1).Err()
		if err != nil && slices.Contains(allowErrs, err.Error()) == false {
			return err
		}
	}

	// Bootstrap slaves.
	for idx, slave := range scenario.slaves() {
		masterID := scenario.nodeIDs[idx]

		// Wait until master is available
		err := eventually(func() error {
			s := slave.ClusterNodes(ctx).Val()
			wanted := masterID
			if !strings.Contains(s, wanted) {
				return fmt.Errorf("%q does not contain %q", s, wanted)
			}
			return nil
		}, 10*time.Second)
		if err != nil {
			return err
		}

		err = slave.ClusterReplicate(ctx, masterID).Err()
		if err != nil {
			return err
		}
	}

	// Wait until all nodes have consistent info.
	wanted := []redis.ClusterSlot{{
		Start: 0,
		End:   5460,
		Nodes: []redis.ClusterNode{{
			ID:   "",
			Addr: "127.0.0.1:16600",
		}, {
			ID:   "",
			Addr: "127.0.0.1:16603",
		}},
	}, {
		Start: 5461,
		End:   10922,
		Nodes: []redis.ClusterNode{{
			ID:   "",
			Addr: "127.0.0.1:16601",
		}, {
			ID:   "",
			Addr: "127.0.0.1:16604",
		}},
	}, {
		Start: 10923,
		End:   16383,
		Nodes: []redis.ClusterNode{{
			ID:   "",
			Addr: "127.0.0.1:16602",
		}, {
			ID:   "",
			Addr: "127.0.0.1:16605",
		}},
	}}

	for _, client := range scenario.clients {
		err := eventually(func() error {
			res, err := client.ClusterSlots(ctx).Result()
			if err != nil {
				return err
			}
			return assertSlotsEqual(res, wanted)
		}, 90*time.Second)
		if err != nil {
			return err
		}
	}

	return nil
}

func collectNodeInformation(ctx context.Context, scenario *clusterScenario) error {
	for pos, port := range scenario.ports {
		client := redis.NewClient(&redis.Options{
			Addr: ":" + port,
		})

		myID, err := client.ClusterMyID(ctx).Result()
		if err != nil {
			return err
		}

		scenario.clients[port] = client
		scenario.nodeIDs[pos] = myID
	}
	return nil
}

func assertSlotsEqual(slots, wanted []redis.ClusterSlot) error {
outerLoop:
	for _, s2 := range wanted {
		for _, s1 := range slots {
			if slotEqual(s1, s2) {
				continue outerLoop
			}
		}
		return fmt.Errorf("%v not found in %v", s2, slots)
	}
	return nil
}

func slotEqual(s1, s2 redis.ClusterSlot) bool {
	if s1.Start != s2.Start {
		return false
	}
	if s1.End != s2.End {
		return false
	}
	if len(s1.Nodes) != len(s2.Nodes) {
		return false
	}
	for i, n1 := range s1.Nodes {
		if n1.Addr != s2.Nodes[i].Addr {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------------------------

var _ = Describe("ClusterClient", func() {
	var failover bool
	var opt *redis.ClusterOptions
	var client *redis.ClusterClient

	assertClusterClient := func() {
		It("do", func() {
			val, err := client.Do(ctx, "ping").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		It("should GET/SET/DEL", func() {
			err := client.Get(ctx, "A").Err()
			Expect(err).To(Equal(redis.Nil))

			err = client.Set(ctx, "A", "VALUE", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				return client.Get(ctx, "A").Val()
			}, 30*time.Second).Should(Equal("VALUE"))

			cnt, err := client.Del(ctx, "A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(1)))
		})

		It("should follow redirects for GET", func() {
			err := client.Set(ctx, "A", "VALUE", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			if !failover {
				Eventually(func() int64 {
					nodes, err := client.Nodes(ctx, "A")
					if err != nil {
						return 0
					}
					return nodes[1].Client.DBSize(ctx).Val()
				}, 30*time.Second).Should(Equal(int64(1)))

				Eventually(func() error {
					return client.SwapNodes(ctx, "A")
				}, 30*time.Second).ShouldNot(HaveOccurred())
			}

			v, err := client.Get(ctx, "A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("VALUE"))
		})

		It("should follow redirects for SET", func() {
			if !failover {
				Eventually(func() error {
					return client.SwapNodes(ctx, "A")
				}, 30*time.Second).ShouldNot(HaveOccurred())
			}

			err := client.Set(ctx, "A", "VALUE", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.Get(ctx, "A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("VALUE"))
		})

		It("should distribute keys", func() {
			for i := 0; i < 100; i++ {
				err := client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				defer GinkgoRecover()
				Eventually(func() string {
					return master.Info(ctx, "keyspace").Val()
				}, 30*time.Second).Should(Or(
					ContainSubstring("keys=32"),
					ContainSubstring("keys=36"),
					ContainSubstring("keys=32"),
				))
				return nil
			})

			Expect(err).NotTo(HaveOccurred())
		})

		It("should distribute keys when using EVAL", func() {
			script := redis.NewScript(`
				local r = redis.call('SET', KEYS[1], ARGV[1])
				return r
			`)

			var key string
			for i := 0; i < 100; i++ {
				key = fmt.Sprintf("key%d", i)
				err := script.Run(ctx, client, []string{key}, "value").Err()
				Expect(err).NotTo(HaveOccurred())
			}

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				defer GinkgoRecover()
				Eventually(func() string {
					return master.Info(ctx, "keyspace").Val()
				}, 30*time.Second).Should(Or(
					ContainSubstring("keys=32"),
					ContainSubstring("keys=36"),
					ContainSubstring("keys=32"),
				))
				return nil
			})

			Expect(err).NotTo(HaveOccurred())
		})

		It("should distribute scripts when using Script Load", func() {
			client.ScriptFlush(ctx)

			script := redis.NewScript(`return 'Unique script'`)

			script.Load(ctx, client)

			err := client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
				defer GinkgoRecover()

				val, _ := script.Exists(ctx, shard).Result()
				Expect(val[0]).To(Equal(true))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should check all shards when using Script Exists", func() {
			client.ScriptFlush(ctx)

			script := redis.NewScript(`return 'First script'`)
			lostScriptSrc := `return 'Lost script'`
			lostScript := redis.NewScript(lostScriptSrc)

			script.Load(ctx, client)
			client.Do(ctx, "script", "load", lostScriptSrc)

			val, _ := client.ScriptExists(ctx, script.Hash(), lostScript.Hash()).Result()

			Expect(val).To(Equal([]bool{true, false}))
		})

		It("should flush scripts from all shards when using ScriptFlush", func() {
			script := redis.NewScript(`return 'Unnecessary script'`)
			script.Load(ctx, client)

			val, _ := client.ScriptExists(ctx, script.Hash()).Result()
			Expect(val).To(Equal([]bool{true}))

			client.ScriptFlush(ctx)

			val, _ = client.ScriptExists(ctx, script.Hash()).Result()
			Expect(val).To(Equal([]bool{false}))
		})

		It("should support Watch", func() {
			var incr func(string) error

			// Transactionally increments key using GET and SET commands.
			incr = func(key string) error {
				err := client.Watch(ctx, func(tx *redis.Tx) error {
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

			Eventually(func() string {
				return client.Get(ctx, "key").Val()
			}, 30*time.Second).Should(Equal("100"))
		})

		Describe("pipelining", func() {
			var pipe *redis.Pipeline

			assertPipeline := func(keys []string) {

				It("should follow redirects", func() {
					if !failover {
						for _, key := range keys {
							Eventually(func() error {
								return client.SwapNodes(ctx, key)
							}, 30*time.Second).ShouldNot(HaveOccurred())
						}
					}

					for i, key := range keys {
						pipe.Set(ctx, key, key+"_value", 0)
						pipe.Expire(ctx, key, time.Duration(i+1)*time.Hour)
					}
					cmds, err := pipe.Exec(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(cmds).To(HaveLen(14))

					// Check that all keys are set.
					for _, key := range keys {
						Eventually(func() string {
							return client.Get(ctx, key).Val()
						}, 30*time.Second).Should(Equal(key + "_value"))
					}

					if !failover {
						for _, key := range keys {
							Eventually(func() error {
								return client.SwapNodes(ctx, key)
							}, 30*time.Second).ShouldNot(HaveOccurred())
						}
					}

					for _, key := range keys {
						pipe.Get(ctx, key)
						pipe.TTL(ctx, key)
					}
					cmds, err = pipe.Exec(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(cmds).To(HaveLen(14))

					for i, key := range keys {
						get := cmds[i*2].(*redis.StringCmd)
						Expect(get.Val()).To(Equal(key + "_value"))

						ttl := cmds[(i*2)+1].(*redis.DurationCmd)
						dur := time.Duration(i+1) * time.Hour
						Expect(ttl.Val()).To(BeNumerically("~", dur, 30*time.Second))
					}
				})

				It("should work with missing keys", func() {
					pipe.Set(ctx, "A{s}", "A_value", 0)
					pipe.Set(ctx, "C{s}", "C_value", 0)
					_, err := pipe.Exec(ctx)
					Expect(err).NotTo(HaveOccurred())

					a := pipe.Get(ctx, "A{s}")
					b := pipe.Get(ctx, "B{s}")
					c := pipe.Get(ctx, "C{s}")
					cmds, err := pipe.Exec(ctx)
					Expect(err).To(Equal(redis.Nil))
					Expect(cmds).To(HaveLen(3))

					Expect(a.Err()).NotTo(HaveOccurred())
					Expect(a.Val()).To(Equal("A_value"))

					Expect(b.Err()).To(Equal(redis.Nil))
					Expect(b.Val()).To(Equal(""))

					Expect(c.Err()).NotTo(HaveOccurred())
					Expect(c.Val()).To(Equal("C_value"))
				})
			}

			Describe("with Pipeline", func() {
				BeforeEach(func() {
					pipe = client.Pipeline().(*redis.Pipeline)
				})

				AfterEach(func() {})

				keys := []string{"A", "B", "C", "D", "E", "F", "G"}
				assertPipeline(keys)

				It("should not fail node with context.Canceled error", func() {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					pipe.Set(ctx, "A", "A_value", 0)
					_, err := pipe.Exec(ctx)

					Expect(err).To(HaveOccurred())
					Expect(errors.Is(err, context.Canceled)).To(BeTrue())

					clientNodes, _ := client.Nodes(ctx, "A")

					for _, node := range clientNodes {
						Expect(node.Failing()).To(BeFalse())
					}
				})

				It("should not fail node with context.DeadlineExceeded error", func() {
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
					defer cancel()

					pipe.Set(ctx, "A", "A_value", 0)
					_, err := pipe.Exec(ctx)

					Expect(err).To(HaveOccurred())
					Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())

					clientNodes, _ := client.Nodes(ctx, "A")

					for _, node := range clientNodes {
						Expect(node.Failing()).To(BeFalse())
					}
				})
			})

			Describe("with TxPipeline", func() {
				BeforeEach(func() {
					pipe = client.TxPipeline().(*redis.Pipeline)
				})

				AfterEach(func() {})

				// TxPipeline doesn't support cross slot commands.
				// Use hashtag to force all keys to the same slot.
				keys := []string{"A{s}", "B{s}", "C{s}", "D{s}", "E{s}", "F{s}", "G{s}"}
				assertPipeline(keys)

				// make sure CrossSlot error is returned
				It("returns CrossSlot error", func() {
					pipe.Set(ctx, "A{s}", "A_value", 0)
					pipe.Set(ctx, "B{t}", "B_value", 0)
					Expect(hashtag.Slot("A{s}")).NotTo(Equal(hashtag.Slot("B{t}")))
					_, err := pipe.Exec(ctx)
					Expect(err).To(MatchError(redis.ErrCrossSlot))
				})

				It("works normally with keyless commands and no CrossSlot error", func() {
					pipe.Set(ctx, "A{s}", "A_value", 0)
					pipe.Ping(ctx)
					pipe.Set(ctx, "B{s}", "B_value", 0)
					pipe.Ping(ctx)
					_, err := pipe.Exec(ctx)
					Expect(err).To(Not(HaveOccurred()))
				})

				// doesn't fail when no commands are queued
				It("returns no error when there are no commands", func() {
					_, err := pipe.Exec(ctx)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		It("should support PubSub", func() {
			pubsub := client.Subscribe(ctx, "mychannel")
			defer pubsub.Close()

			Eventually(func() error {
				_, err := client.Publish(ctx, "mychannel", "hello").Result()
				if err != nil {
					return err
				}

				msg, err := pubsub.ReceiveTimeout(ctx, time.Second)
				if err != nil {
					return err
				}

				_, ok := msg.(*redis.Message)
				if !ok {
					return fmt.Errorf("got %T, wanted *redis.Message", msg)
				}

				return nil
			}, 30*time.Second).ShouldNot(HaveOccurred())
		})

		It("should support sharded PubSub", func() {
			pubsub := client.SSubscribe(ctx, "mychannel")
			defer pubsub.Close()

			Eventually(func() error {
				_, err := client.SPublish(ctx, "mychannel", "hello").Result()
				if err != nil {
					return err
				}

				msg, err := pubsub.ReceiveTimeout(ctx, time.Second)
				if err != nil {
					return err
				}

				_, ok := msg.(*redis.Message)
				if !ok {
					return fmt.Errorf("got %T, wanted *redis.Message", msg)
				}

				return nil
			}, 30*time.Second).ShouldNot(HaveOccurred())
		})

		It("should support PubSub.Ping without channels", func() {
			pubsub := client.Subscribe(ctx)
			defer pubsub.Close()

			err := pubsub.Ping(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	}

	Describe("ClusterClient PROTO 2", func() {
		BeforeEach(func() {
			opt = redisClusterOptions()
			opt.Protocol = 2
			client = cluster.newClusterClient(ctx, opt)

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			_ = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should CLUSTER PROTO 2", func() {
			_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
				val, err := c.Do(ctx, "HELLO").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).Should(ContainElements("proto", int64(2)))
				return nil
			})
		})
	})

	Describe("ClusterClient", func() {
		BeforeEach(func() {
			opt = redisClusterOptions()
			opt.ClientName = "cluster_hi"
			client = cluster.newClusterClient(ctx, opt)

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			_ = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should return pool stats", func() {
			stats := client.PoolStats()
			Expect(stats).To(BeAssignableToTypeOf(&redis.PoolStats{}))
		})

		It("should return an error when there are no attempts left", func() {
			opt := redisClusterOptions()
			opt.MaxRedirects = -1
			client := cluster.newClusterClient(ctx, opt)

			Eventually(func() error {
				return client.SwapNodes(ctx, "A")
			}, 30*time.Second).ShouldNot(HaveOccurred())

			err := client.Get(ctx, "A").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("MOVED"))

			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should determine hash slots correctly for generic commands", func() {
			opt := redisClusterOptions()
			opt.MaxRedirects = -1
			client := cluster.newClusterClient(ctx, opt)

			err := client.Do(ctx, "GET", "A").Err()
			Expect(err).To(Equal(redis.Nil))

			err = client.Do(ctx, []byte("GET"), []byte("A")).Err()
			Expect(err).To(Equal(redis.Nil))

			Eventually(func() error {
				return client.SwapNodes(ctx, "A")
			}, 30*time.Second).ShouldNot(HaveOccurred())

			err = client.Do(ctx, "GET", "A").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("MOVED"))

			err = client.Do(ctx, []byte("GET"), []byte("A")).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("MOVED"))

			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should follow node redirection immediately", func() {
			// Configure retry backoffs far in excess of the expected duration of redirection
			opt := redisClusterOptions()
			opt.MinRetryBackoff = 10 * time.Minute
			opt.MaxRetryBackoff = 20 * time.Minute
			client := cluster.newClusterClient(ctx, opt)

			Eventually(func() error {
				return client.SwapNodes(ctx, "A")
			}, 30*time.Second).ShouldNot(HaveOccurred())

			// Note that this context sets a deadline more aggressive than the lowest possible bound
			// of the retry backoff; this verifies that redirection completes immediately.
			redirCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			err := client.Set(redirCtx, "A", "VALUE", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.Get(redirCtx, "A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("VALUE"))

			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should call fn for every master node", func() {
			for i := 0; i < 10; i++ {
				Expect(client.Set(ctx, strconv.Itoa(i), "", 0).Err()).NotTo(HaveOccurred())
			}

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			size, err := client.DBSize(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(0)))
		})

		It("should CLUSTER SLOTS", func() {
			res, err := client.ClusterSlots(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))

			wanted := []redis.ClusterSlot{{
				Start: 0,
				End:   5460,
				Nodes: []redis.ClusterNode{{
					ID:   "",
					Addr: "127.0.0.1:16600",
				}, {
					ID:   "",
					Addr: "127.0.0.1:16603",
				}},
			}, {
				Start: 5461,
				End:   10922,
				Nodes: []redis.ClusterNode{{
					ID:   "",
					Addr: "127.0.0.1:16601",
				}, {
					ID:   "",
					Addr: "127.0.0.1:16604",
				}},
			}, {
				Start: 10923,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					ID:   "",
					Addr: "127.0.0.1:16602",
				}, {
					ID:   "",
					Addr: "127.0.0.1:16605",
				}},
			}}
			Expect(assertSlotsEqual(res, wanted)).NotTo(HaveOccurred())
		})

		It("should CLUSTER SHARDS", func() {
			res, err := client.ClusterShards(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).NotTo(BeEmpty())

			// Iterate over the ClusterShard results and validate the fields.
			for _, shard := range res {
				Expect(shard.Slots).NotTo(BeEmpty())
				for _, slotRange := range shard.Slots {
					Expect(slotRange.Start).To(BeNumerically(">=", 0))
					Expect(slotRange.End).To(BeNumerically(">=", slotRange.Start))
				}

				Expect(shard.Nodes).NotTo(BeEmpty())
				for _, node := range shard.Nodes {
					Expect(node.ID).NotTo(BeEmpty())
					Expect(node.Endpoint).NotTo(BeEmpty())
					Expect(node.IP).NotTo(BeEmpty())
					Expect(node.Port).To(BeNumerically(">", 0))

					validRoles := []string{"master", "slave", "replica"}
					Expect(validRoles).To(ContainElement(node.Role))

					Expect(node.ReplicationOffset).To(BeNumerically(">=", 0))

					validHealthStatuses := []string{"online", "failed", "loading"}
					Expect(validHealthStatuses).To(ContainElement(node.Health))
				}
			}
		})

		It("should CLUSTER LINKS", func() {
			res, err := client.ClusterLinks(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).NotTo(BeEmpty())

			// Iterate over the ClusterLink results and validate the map keys.
			for _, link := range res {

				Expect(link.Direction).NotTo(BeEmpty())
				Expect([]string{"from", "to"}).To(ContainElement(link.Direction))
				Expect(link.Node).NotTo(BeEmpty())
				Expect(link.CreateTime).To(BeNumerically(">", 0))

				Expect(link.Events).NotTo(BeEmpty())
				validEventChars := []rune{'r', 'w'}
				for _, eventChar := range link.Events {
					Expect(validEventChars).To(ContainElement(eventChar))
				}

				Expect(link.SendBufferAllocated).To(BeNumerically(">=", 0))
				Expect(link.SendBufferUsed).To(BeNumerically(">=", 0))
			}
		})

		It("should cluster client setname", func() {
			err := client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
				return c.Ping(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
				val, err := c.ClientList(ctx).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).Should(ContainSubstring("name=cluster_hi"))
				return nil
			})
		})

		It("should CLUSTER PROTO 3", func() {
			_ = client.ForEachShard(ctx, func(ctx context.Context, c *redis.Client) error {
				val, err := c.Do(ctx, "HELLO").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).Should(HaveKeyWithValue("proto", int64(3)))
				return nil
			})
		})

		It("should CLUSTER MYSHARDID", func() {
			shardID, err := client.ClusterMyShardID(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(shardID).ToNot(BeEmpty())
		})

		It("should CLUSTER NODES", func() {
			res, err := client.ClusterNodes(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(BeNumerically(">", 400))
		})

		It("should CLUSTER INFO", func() {
			res, err := client.ClusterInfo(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainSubstring("cluster_known_nodes:6"))
		})

		It("should CLUSTER KEYSLOT", func() {
			hashSlot, err := client.ClusterKeySlot(ctx, "somekey").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(hashSlot).To(Equal(int64(hashtag.Slot("somekey"))))
		})

		It("should CLUSTER GETKEYSINSLOT", func() {
			keys, err := client.ClusterGetKeysInSlot(ctx, hashtag.Slot("somekey"), 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(keys)).To(Equal(0))
		})

		It("should CLUSTER COUNT-FAILURE-REPORTS", func() {
			n, err := client.ClusterCountFailureReports(ctx, cluster.nodeIDs[0]).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER COUNTKEYSINSLOT", func() {
			n, err := client.ClusterCountKeysInSlot(ctx, 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER SAVECONFIG", func() {
			res, err := client.ClusterSaveConfig(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
		})

		It("should CLUSTER SLAVES", func() {
			nodesList, err := client.ClusterSlaves(ctx, cluster.nodeIDs[0]).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nodesList).Should(ContainElement(ContainSubstring("slave")))
			Expect(nodesList).Should(HaveLen(1))
		})

		It("should RANDOMKEY", func() {
			const nkeys = 100

			for i := 0; i < nkeys; i++ {
				err := client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			var keys []string
			addKey := func(key string) {
				for _, k := range keys {
					if k == key {
						return
					}
				}
				keys = append(keys, key)
			}

			for i := 0; i < nkeys*10; i++ {
				key := client.RandomKey(ctx).Val()
				addKey(key)
			}

			Expect(len(keys)).To(BeNumerically("~", nkeys, nkeys/10))
		})

		It("should support Process hook", func() {
			testCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			err := client.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
				return node.Ping(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			var stack []string

			clusterHook := &hook{
				processHook: func(hook redis.ProcessHook) redis.ProcessHook {
					return func(ctx context.Context, cmd redis.Cmder) error {
						select {
						case <-testCtx.Done():
							return hook(ctx, cmd)
						default:
						}

						Expect(cmd.String()).To(Equal("ping: "))
						mu.Lock()
						stack = append(stack, "cluster.BeforeProcess")
						mu.Unlock()

						err := hook(ctx, cmd)

						Expect(cmd.String()).To(Equal("ping: PONG"))
						mu.Lock()
						stack = append(stack, "cluster.AfterProcess")
						mu.Unlock()

						return err
					}
				},
			}
			client.AddHook(clusterHook)

			nodeHook := &hook{
				processHook: func(hook redis.ProcessHook) redis.ProcessHook {
					return func(ctx context.Context, cmd redis.Cmder) error {
						select {
						case <-testCtx.Done():
							return hook(ctx, cmd)
						default:
						}

						Expect(cmd.String()).To(Equal("ping: "))
						mu.Lock()
						stack = append(stack, "shard.BeforeProcess")
						mu.Unlock()

						err := hook(ctx, cmd)

						Expect(cmd.String()).To(Equal("ping: PONG"))
						mu.Lock()
						stack = append(stack, "shard.AfterProcess")
						mu.Unlock()

						return err
					}
				},
			}

			_ = client.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
				node.AddHook(nodeHook)
				return nil
			})

			err = client.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			mu.Lock()
			finalStack := make([]string, len(stack))
			copy(finalStack, stack)
			mu.Unlock()

			Expect(finalStack).To(ContainElements([]string{
				"cluster.BeforeProcess",
				"shard.BeforeProcess",
				"shard.AfterProcess",
				"cluster.AfterProcess",
			}))
		})

		It("should support Pipeline hook", func() {
			err := client.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
				return node.Ping(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			var stack []string

			client.AddHook(&hook{
				processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
					return func(ctx context.Context, cmds []redis.Cmder) error {
						Expect(cmds).To(HaveLen(1))
						cmdStr := cmds[0].String()

						// Handle SET command (should succeed)
						if cmdStr == "set pipeline_test_key pipeline_test_value: " {
							mu.Lock()
							stack = append(stack, "cluster.BeforeProcessPipeline")
							mu.Unlock()

							err := hook(ctx, cmds)

							Expect(cmds).To(HaveLen(1))
							Expect(cmds[0].String()).To(Equal("set pipeline_test_key pipeline_test_value: OK"))
							mu.Lock()
							stack = append(stack, "cluster.AfterProcessPipeline")
							mu.Unlock()

							return err
						}

						// For other commands (like ping), just pass through without expectations
						// since they might fail before reaching this point
						return hook(ctx, cmds)
					}
				},
			})

			_ = client.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
				node.AddHook(&hook{
					processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
						return func(ctx context.Context, cmds []redis.Cmder) error {
							Expect(cmds).To(HaveLen(1))
							cmdStr := cmds[0].String()

							// Handle SET command (should succeed)
							if cmdStr == "set pipeline_test_key pipeline_test_value: " {
								mu.Lock()
								stack = append(stack, "shard.BeforeProcessPipeline")
								mu.Unlock()

								err := hook(ctx, cmds)

								Expect(cmds).To(HaveLen(1))
								Expect(cmds[0].String()).To(Equal("set pipeline_test_key pipeline_test_value: OK"))
								mu.Lock()
								stack = append(stack, "shard.AfterProcessPipeline")
								mu.Unlock()

								return err
							}

							// For other commands (like ping), just pass through without expectations
							return hook(ctx, cmds)
						}
					},
				})
				return nil
			})

			_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "pipeline_test_key", "pipeline_test_value", 0)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			mu.Lock()
			finalStack := make([]string, len(stack))
			copy(finalStack, stack)
			mu.Unlock()

			Expect(finalStack).To(Equal([]string{
				"cluster.BeforeProcessPipeline",
				"shard.BeforeProcessPipeline",
				"shard.AfterProcessPipeline",
				"cluster.AfterProcessPipeline",
			}))
		})

		It("should reject ping command in pipeline", func() {
			// Test that ping command fails in pipeline as expected
			_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("redis: cannot pipeline command \"ping\" with request policy ReqAllNodes/ReqAllShards/ReqMultiShard"))
		})

		It("should support TxPipeline hook", func() {
			err := client.Ping(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
				return node.Ping(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			var stack []string

			client.AddHook(&hook{
				processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
					return func(ctx context.Context, cmds []redis.Cmder) error {
						Expect(cmds).To(HaveLen(3))
						Expect(cmds[1].String()).To(Equal("ping: "))
						mu.Lock()
						stack = append(stack, "cluster.BeforeProcessPipeline")
						mu.Unlock()

						err := hook(ctx, cmds)

						Expect(cmds).To(HaveLen(3))
						Expect(cmds[1].String()).To(Equal("ping: PONG"))
						mu.Lock()
						stack = append(stack, "cluster.AfterProcessPipeline")
						mu.Unlock()

						return err
					}
				},
			})

			_ = client.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
				node.AddHook(&hook{
					processPipelineHook: func(hook redis.ProcessPipelineHook) redis.ProcessPipelineHook {
						return func(ctx context.Context, cmds []redis.Cmder) error {
							Expect(cmds).To(HaveLen(3))
							Expect(cmds[1].String()).To(Equal("ping: "))
							mu.Lock()
							stack = append(stack, "shard.BeforeProcessPipeline")
							mu.Unlock()

							err := hook(ctx, cmds)

							Expect(cmds).To(HaveLen(3))
							Expect(cmds[1].String()).To(Equal("ping: PONG"))
							mu.Lock()
							stack = append(stack, "shard.AfterProcessPipeline")
							mu.Unlock()

							return err
						}
					},
				})
				return nil
			})

			_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			mu.Lock()
			finalStack := make([]string, len(stack))
			copy(finalStack, stack)
			mu.Unlock()

			Expect(finalStack).To(Equal([]string{
				"cluster.BeforeProcessPipeline",
				"shard.BeforeProcessPipeline",
				"shard.AfterProcessPipeline",
				"cluster.AfterProcessPipeline",
			}))
		})

		It("should return correct replica for key", func() {
			client, err := client.SlaveForKey(ctx, "test")
			Expect(err).ToNot(HaveOccurred())
			info := client.Info(ctx, "server")
			Expect(info.Val()).Should(ContainSubstring("tcp_port:16604"))
		})

		It("should return correct master for key", func() {
			client, err := client.MasterForKey(ctx, "test")
			Expect(err).ToNot(HaveOccurred())
			info := client.Info(ctx, "server")
			Expect(info.Val()).Should(ContainSubstring("tcp_port:16601"))
		})

		assertClusterClient()
	})

	Describe("ClusterClient with RouteByLatency", func() {
		BeforeEach(func() {
			opt = redisClusterOptions()
			opt.RouteByLatency = true
			client = cluster.newClusterClient(ctx, opt)

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachSlave(ctx, func(ctx context.Context, slave *redis.Client) error {
				Eventually(func() int64 {
					return client.DBSize(ctx).Val()
				}, 30*time.Second).Should(Equal(int64(0)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := client.ForEachSlave(ctx, func(ctx context.Context, slave *redis.Client) error {
				return slave.ReadWrite(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			err = client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		assertClusterClient()
	})

	Describe("ClusterClient with ClusterSlots", func() {
		BeforeEach(func() {
			failover = true

			opt = redisClusterOptions()
			opt.ClusterSlots = func(ctx context.Context) ([]redis.ClusterSlot, error) {
				slots := []redis.ClusterSlot{{
					Start: 0,
					End:   5460,
					Nodes: []redis.ClusterNode{{
						Addr: ":" + ringShard1Port,
					}},
				}, {
					Start: 5461,
					End:   10922,
					Nodes: []redis.ClusterNode{{
						Addr: ":" + ringShard2Port,
					}},
				}, {
					Start: 10923,
					End:   16383,
					Nodes: []redis.ClusterNode{{
						Addr: ":" + ringShard3Port,
					}},
				}}
				return slots, nil
			}
			client = cluster.newClusterClient(ctx, opt)
			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachSlave(ctx, func(ctx context.Context, slave *redis.Client) error {
				Eventually(func() int64 {
					return client.DBSize(ctx).Val()
				}, 30*time.Second).Should(Equal(int64(0)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			failover = false

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		assertClusterClient()
	})

	Describe("ClusterClient with RouteRandomly and ClusterSlots", func() {
		BeforeEach(func() {
			failover = true

			opt = redisClusterOptions()
			opt.RouteRandomly = true
			opt.ClusterSlots = func(ctx context.Context) ([]redis.ClusterSlot, error) {
				slots := []redis.ClusterSlot{{
					Start: 0,
					End:   5460,
					Nodes: []redis.ClusterNode{{
						Addr: ":" + ringShard1Port,
					}},
				}, {
					Start: 5461,
					End:   10922,
					Nodes: []redis.ClusterNode{{
						Addr: ":" + ringShard2Port,
					}},
				}, {
					Start: 10923,
					End:   16383,
					Nodes: []redis.ClusterNode{{
						Addr: ":" + ringShard3Port,
					}},
				}}
				return slots, nil
			}
			client = cluster.newClusterClient(ctx, opt)

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachSlave(ctx, func(ctx context.Context, slave *redis.Client) error {
				Eventually(func() int64 {
					return client.DBSize(ctx).Val()
				}, 30*time.Second).Should(Equal(int64(0)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			failover = false

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		assertClusterClient()
	})

	Describe("ClusterClient with ClusterSlots with multiple nodes per slot", func() {
		BeforeEach(func() {
			failover = true

			opt = redisClusterOptions()
			opt.ReadOnly = true
			opt.ClusterSlots = func(ctx context.Context) ([]redis.ClusterSlot, error) {
				slots := []redis.ClusterSlot{{
					Start: 0,
					End:   5460,
					Nodes: []redis.ClusterNode{{
						Addr: ":16600",
					}, {
						Addr: ":16603",
					}},
				}, {
					Start: 5461,
					End:   10922,
					Nodes: []redis.ClusterNode{{
						Addr: ":16601",
					}, {
						Addr: ":16604",
					}},
				}, {
					Start: 10923,
					End:   16383,
					Nodes: []redis.ClusterNode{{
						Addr: ":16602",
					}, {
						Addr: ":16605",
					}},
				}}
				return slots, nil
			}
			client = cluster.newClusterClient(ctx, opt)

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.FlushDB(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			err = client.ForEachSlave(ctx, func(ctx context.Context, slave *redis.Client) error {
				Eventually(func() int64 {
					return client.DBSize(ctx).Val()
				}, 30*time.Second).Should(Equal(int64(0)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			failover = false

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		assertClusterClient()
	})
})

var _ = Describe("ClusterClient without nodes", func() {
	var client *redis.ClusterClient

	BeforeEach(func() {
		client = redis.NewClusterClient(&redis.ClusterOptions{})
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should return an error for Ping", func() {
		err := client.Ping(ctx).Err()
		Expect(err).To(MatchError("redis: cluster has no nodes"))
	})

	It("should return an error for pipeline", func() {
		_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Ping(ctx)
			return nil
		})
		Expect(err).To(MatchError("redis: cluster has no nodes"))
	})
})

var _ = Describe("ClusterClient without valid nodes", func() {
	var client *redis.ClusterClient

	BeforeEach(func() {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{redisAddr},
		})
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should return an error when cluster support is disabled", func() {
		err := client.Ping(ctx).Err()
		Expect(err).To(MatchError("ERR This instance has cluster support disabled"))
	})

	It("should return an error for pipeline when cluster support is disabled", func() {
		_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Ping(ctx)
			return nil
		})
		Expect(err).To(MatchError("ERR This instance has cluster support disabled"))
	})
})

var _ = Describe("ClusterClient with unavailable Cluster", func() {
	var client *redis.ClusterClient

	BeforeEach(func() {
		opt := redisClusterOptions()
		opt.ReadTimeout = 250 * time.Millisecond
		opt.WriteTimeout = 250 * time.Millisecond
		opt.MaxRedirects = 1
		client = cluster.newClusterClientUnstable(opt)
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())

		for _, node := range cluster.clients {
			err := node.ClientPause(ctx, 5*time.Second).Err()
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should recover when Cluster recovers", func() {
		err := client.Ping(ctx).Err()
		Expect(err).To(HaveOccurred())

		Eventually(func() error {
			return client.Ping(ctx).Err()
		}, "30s").ShouldNot(HaveOccurred())
	})
})

var _ = Describe("ClusterClient timeout", func() {
	var client *redis.ClusterClient

	AfterEach(func() {
		_ = client.Close()
	})

	testTimeout := func() {
		It("should timeout Ping", func() {
			err := client.Ping(ctx).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})

		It("should timeout Pipeline", func() {
			_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})

		It("should timeout Tx", func() {
			err := client.Watch(ctx, func(tx *redis.Tx) error {
				return tx.Ping(ctx).Err()
			}, "foo")
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})

		It("should timeout Tx Pipeline", func() {
			err := client.Watch(ctx, func(tx *redis.Tx) error {
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
			opt := redisClusterOptions()
			client = cluster.newClusterClient(ctx, opt)

			err := client.ForEachShard(ctx, func(ctx context.Context, client *redis.Client) error {
				err := client.ClientPause(ctx, pause).Err()

				opt := client.Options()
				opt.ReadTimeout = time.Nanosecond
				opt.WriteTimeout = time.Nanosecond

				return err
			})
			Expect(err).NotTo(HaveOccurred())

			// Overwrite timeouts after the client is initialized.
			opt.ReadTimeout = time.Nanosecond
			opt.WriteTimeout = time.Nanosecond
			opt.MaxRedirects = 0
		})

		AfterEach(func() {
			_ = client.ForEachShard(ctx, func(ctx context.Context, client *redis.Client) error {
				defer GinkgoRecover()

				opt := client.Options()
				opt.ReadTimeout = time.Second
				opt.WriteTimeout = time.Second

				Eventually(func() error {
					return client.Ping(ctx).Err()
				}, 2*pause).ShouldNot(HaveOccurred())
				return nil
			})

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		testTimeout()
	})
})

var _ = Describe("Command Tips tests", func() {
	var client *redis.ClusterClient

	BeforeEach(func() {
		opt := redisClusterOptions()
		client = cluster.newClusterClient(ctx, opt)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should verify COMMAND tips match router policy types", func() {
		SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")
		expectedPolicies := map[string]struct {
			RequestPolicy  string
			ResponsePolicy string
		}{
			"touch": {
				RequestPolicy:  "multi_shard",
				ResponsePolicy: "agg_sum",
			},
			"flushall": {
				RequestPolicy:  "all_shards",
				ResponsePolicy: "all_succeeded",
			},
		}

		cmds, err := client.Command(ctx).Result()
		Expect(err).NotTo(HaveOccurred())

		for cmdName, expected := range expectedPolicies {
			actualCmd := cmds[cmdName]

			Expect(actualCmd.CommandPolicy).NotTo(BeNil())

			// Verify request_policy from COMMAND matches router policy
			actualRequestPolicy := actualCmd.CommandPolicy.Request.String()
			Expect(actualRequestPolicy).To(Equal(expected.RequestPolicy))

			// Verify response_policy from COMMAND matches router policy
			actualResponsePolicy := actualCmd.CommandPolicy.Response.String()
			Expect(actualResponsePolicy).To(Equal(expected.ResponsePolicy))
		}
	})

	Describe("Explicit Routing Policy Tests", func() {
		It("should test explicit routing policy for TOUCH", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Verify TOUCH command has multi_shard policy
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			touchCmd := cmds["touch"]

			Expect(touchCmd.CommandPolicy).NotTo(BeNil())
			Expect(touchCmd.CommandPolicy.Request.String()).To(Equal("multi_shard"))
			Expect(touchCmd.CommandPolicy.Response.String()).To(Equal("agg_sum"))

			keys := []string{"key1", "key2", "key3", "key4", "key5"}
			for _, key := range keys {
				err := client.Set(ctx, key, "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			result := client.Touch(ctx, keys...)
			Expect(result.Err()).NotTo(HaveOccurred())
			Expect(result.Val()).To(Equal(int64(len(keys))))
		})

		It("should test explicit routing policy for FLUSHALL", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Verify FLUSHALL command has all_shards policy
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			flushallCmd := cmds["flushall"]

			Expect(flushallCmd.CommandPolicy).NotTo(BeNil())
			Expect(flushallCmd.CommandPolicy.Request.String()).To(Equal("all_shards"))
			Expect(flushallCmd.CommandPolicy.Response.String()).To(Equal("all_succeeded"))

			testKeys := []string{"test1", "test2", "test3"}
			for _, key := range testKeys {
				err := client.Set(ctx, key, "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			err = client.FlushAll(ctx).Err()
			Expect(err).NotTo(HaveOccurred())

			for _, key := range testKeys {
				exists := client.Exists(ctx, key)
				Expect(exists.Val()).To(Equal(int64(0)))
			}
		})

		It("should test explicit routing policy for PING", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Verify PING command has all_shards policy
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			pingCmd := cmds["ping"]
			Expect(pingCmd.CommandPolicy).NotTo(BeNil())
			Expect(pingCmd.CommandPolicy.Request.String()).To(Equal("all_shards"))
			Expect(pingCmd.CommandPolicy.Response.String()).To(Equal("all_succeeded"))

			result := client.Ping(ctx)
			Expect(result.Err()).NotTo(HaveOccurred())
			Expect(result.Val()).To(Equal("PONG"))
		})

		It("should test explicit routing policy for DBSIZE", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Verify DBSIZE command has all_shards policy with agg_sum response
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			dbsizeCmd := cmds["dbsize"]
			Expect(dbsizeCmd.CommandPolicy).NotTo(BeNil())
			Expect(dbsizeCmd.CommandPolicy.Request.String()).To(Equal("all_shards"))
			Expect(dbsizeCmd.CommandPolicy.Response.String()).To(Equal("agg_sum"))

			testKeys := []string{"dbsize_test1", "dbsize_test2", "dbsize_test3"}
			for _, key := range testKeys {
				err := client.Set(ctx, key, "value", 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			size := client.DBSize(ctx)
			Expect(size.Err()).NotTo(HaveOccurred())
			Expect(size.Val()).To(BeNumerically(">=", int64(len(testKeys))))
		})
	})

	Describe("DDL Commands Routing Policy Tests", func() {
		BeforeEach(func() {
			info := client.Info(ctx, "modules")
			if info.Err() != nil || !strings.Contains(info.Val(), "search") {
				Skip("Search module not available")
			}
		})

		It("should test DDL commands routing policy for FT.CREATE", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Verify FT.CREATE command routing policy
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			ftCreateCmd, exists := cmds["ft.create"]
			if !exists || ftCreateCmd.CommandPolicy == nil {
				Skip("FT.CREATE command or tips not available")
			}

			// DDL commands should NOT be broadcasted - they should go to coordinator only
			Expect(ftCreateCmd.CommandPolicy).NotTo(BeNil())
			requestPolicy := ftCreateCmd.CommandPolicy.Request.String()
			Expect(requestPolicy).NotTo(Equal("all_shards"))
			Expect(requestPolicy).NotTo(Equal("all_nodes"))

			indexName := "test_index_create"
			client.FTDropIndex(ctx, indexName)

			result := client.FTCreate(ctx, indexName,
				&redis.FTCreateOptions{
					OnHash: true,
					Prefix: []interface{}{"doc:"},
				},
				&redis.FieldSchema{
					FieldName: "title",
					FieldType: redis.SearchFieldTypeText,
				})
			Expect(result.Err()).NotTo(HaveOccurred())
			Expect(result.Val()).To(Equal("OK"))

			infoResult := client.FTInfo(ctx, indexName)
			Expect(infoResult.Err()).NotTo(HaveOccurred())
			Expect(infoResult.Val().IndexName).To(Equal(indexName))
			client.FTDropIndex(ctx, indexName)
		})

		It("should test DDL commands routing policy for FT.ALTER", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Verify FT.ALTER command routing policy
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			ftAlterCmd, exists := cmds["ft.alter"]
			if !exists || ftAlterCmd.CommandPolicy == nil {
				Skip("FT.ALTER command or tips not available")
			}

			Expect(ftAlterCmd.CommandPolicy).NotTo(BeNil())
			requestPolicy := ftAlterCmd.CommandPolicy.Request.String()
			Expect(requestPolicy).NotTo(Equal("all_shards"))
			Expect(requestPolicy).NotTo(Equal("all_nodes"))

			indexName := "test_index_alter"
			client.FTDropIndex(ctx, indexName)

			result := client.FTCreate(ctx, indexName,
				&redis.FTCreateOptions{
					OnHash: true,
					Prefix: []interface{}{"doc:"},
				},
				&redis.FieldSchema{
					FieldName: "title",
					FieldType: redis.SearchFieldTypeText,
				})
			Expect(result.Err()).NotTo(HaveOccurred())

			alterResult := client.FTAlter(ctx, indexName, false,
				[]interface{}{"description", redis.SearchFieldTypeText.String()})
			Expect(alterResult.Err()).NotTo(HaveOccurred())
			Expect(alterResult.Val()).To(Equal("OK"))
			client.FTDropIndex(ctx, indexName)
		})

		It("should route keyed commands to correct shard based on hash slot", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			type masterNode struct {
				client *redis.Client
				addr   string
			}
			var masterNodes []masterNode
			var mu sync.Mutex

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				addr := master.Options().Addr
				mu.Lock()
				masterNodes = append(masterNodes, masterNode{
					client: master,
					addr:   addr,
				})
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(masterNodes)).To(BeNumerically(">", 1))

			// Single keyed command should go to exactly one shard - determined by hash slot
			testKey := "test_key_12345"
			testValue := "test_value"

			result := client.Set(ctx, testKey, testValue, 0)
			Expect(result.Err()).NotTo(HaveOccurred())
			Expect(result.Val()).To(Equal("OK"))

			time.Sleep(200 * time.Millisecond)

			var targetNodeAddr string
			foundNodes := 0

			for _, node := range masterNodes {
				getResult := node.client.Get(ctx, testKey)
				if getResult.Err() == nil && getResult.Val() == testValue {
					foundNodes++
					targetNodeAddr = node.addr
				} else {
				}
			}

			Expect(foundNodes).To(Equal(1))
			Expect(targetNodeAddr).NotTo(BeEmpty())

			// Multiple commands with same key should go to same shard
			finalValue := ""
			for i := 0; i < 5; i++ {
				finalValue = fmt.Sprintf("value_%d", i)
				result := client.Set(ctx, testKey, finalValue, 0)
				Expect(result.Err()).NotTo(HaveOccurred())
				Expect(result.Val()).To(Equal("OK"))
			}

			time.Sleep(200 * time.Millisecond)

			var currentTargetNode string
			foundNodesAfterUpdate := 0

			for _, node := range masterNodes {
				getResult := node.client.Get(ctx, testKey)
				if getResult.Err() == nil && getResult.Val() == finalValue {
					foundNodesAfterUpdate++
					currentTargetNode = node.addr
				} else {
				}
			}

			// All commands with same key should go to same shard
			Expect(foundNodesAfterUpdate).To(Equal(1))
			Expect(currentTargetNode).To(Equal(targetNodeAddr))
		})

		It("should aggregate responses according to explicit aggregation policies", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			type masterNode struct {
				client *redis.Client
				addr   string
			}
			var masterNodes []masterNode
			var mu sync.Mutex

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				addr := master.Options().Addr
				mu.Lock()
				masterNodes = append(masterNodes, masterNode{
					client: master,
					addr:   addr,
				})
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(masterNodes)).To(BeNumerically(">", 1))

			// verify TOUCH command has agg_sum policy
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			touchCmd, exists := cmds["touch"]
			if !exists || touchCmd.CommandPolicy == nil {
				Skip("TOUCH command or tips not available")
			}

			Expect(touchCmd.CommandPolicy.Response.String()).To(Equal("agg_sum"))

			testKeys := []string{
				"touch_test_key_1111", // These keys should map to different hash slots
				"touch_test_key_2222",
				"touch_test_key_3333",
				"touch_test_key_4444",
				"touch_test_key_5555",
			}

			// Set keys on different shards
			keysPerShard := make(map[string][]string)
			for _, key := range testKeys {
				result := client.Set(ctx, key, "test_value", 0)
				Expect(result.Err()).NotTo(HaveOccurred())

				// Find which shard contains this key
				for _, node := range masterNodes {
					getResult := node.client.Get(ctx, key)
					if getResult.Err() == nil {
						keysPerShard[node.addr] = append(keysPerShard[node.addr], key)
						break
					}
				}
			}

			// Verify keys are distributed across multiple shards
			shardsWithKeys := len(keysPerShard)
			Expect(shardsWithKeys).To(BeNumerically(">", 1))

			// Execute TOUCH command on all keys - this should aggregate results using agg_sum
			touchResult := client.Touch(ctx, testKeys...)
			Expect(touchResult.Err()).NotTo(HaveOccurred())

			totalTouched := touchResult.Val()
			Expect(totalTouched).To(Equal(int64(len(testKeys))))

			totalKeysOnShards := 0
			for _, keys := range keysPerShard {
				totalKeysOnShards += len(keys)
			}

			Expect(totalKeysOnShards).To(Equal(len(testKeys)))

			// FLUSHALL command with all_succeeded aggregation policy
			flushallCmd, exists := cmds["flushall"]
			if !exists || flushallCmd.CommandPolicy == nil {
				Skip("FLUSHALL command or tips not available")
			}

			Expect(flushallCmd.CommandPolicy.Response.String()).To(Equal("all_succeeded"))

			for i := 0; i < len(masterNodes); i++ {
				testKey := fmt.Sprintf("flush_test_key_%d_%d", i, time.Now().UnixNano())
				result := client.Set(ctx, testKey, "test_data", 0)
				Expect(result.Err()).NotTo(HaveOccurred())
			}

			flushResult := client.FlushAll(ctx)
			Expect(flushResult.Err()).NotTo(HaveOccurred())
			Expect(flushResult.Val()).To(Equal("OK"))

			for _, node := range masterNodes {
				dbSizeResult := node.client.DBSize(ctx)
				Expect(dbSizeResult.Err()).NotTo(HaveOccurred())
				Expect(dbSizeResult.Val()).To(Equal(int64(0)))
			}

			// WAIT command aggregation policy - verify agg_min policy
			waitCmd, exists := cmds["wait"]
			if !exists || waitCmd.CommandPolicy == nil {
				Skip("WAIT command or tips not available")
			}

			Expect(waitCmd.CommandPolicy.Response.String()).To(Equal("agg_min"))

			// Set up some data to replicate
			testKey := "wait_test_key_1111"
			result := client.Set(ctx, testKey, "test_value", 0)
			Expect(result.Err()).NotTo(HaveOccurred())

			// Execute WAIT command - should aggregate using agg_min across all shards
			// WAIT waits for a given number of replicas to acknowledge writes
			// With agg_min policy, it returns the minimum number of replicas that acknowledged
			waitResult := client.Wait(ctx, 0, 1000) // Wait for 0 replicas with 1 second timeout
			Expect(waitResult.Err()).NotTo(HaveOccurred())

			// The result should be the minimum number of replicas across all shards
			// Since we're asking for 0 replicas, all shards should return 0, so min is 0
			minReplicas := waitResult.Val()
			Expect(minReplicas).To(BeNumerically(">=", 0))

			// SCRIPT EXISTS command aggregation policy - verify agg_logical_and policy
			scriptExistsCmd, exists := cmds["script exists"]
			if !exists || scriptExistsCmd.CommandPolicy == nil {
				Skip("SCRIPT EXISTS command or tips not available")
			}

			Expect(scriptExistsCmd.CommandPolicy.Response.String()).To(Equal("agg_logical_and"))

			// Load a script on all shards
			testScript := "return 'hello'"
			scriptLoadResult := client.ScriptLoad(ctx, testScript)
			Expect(scriptLoadResult.Err()).NotTo(HaveOccurred())
			scriptSHA := scriptLoadResult.Val()

			// Verify script exists on all shards using SCRIPT EXISTS
			// With agg_logical_and policy, it should return true only if script exists on ALL shards
			scriptExistsResult := client.ScriptExists(ctx, scriptSHA)
			Expect(scriptExistsResult.Err()).NotTo(HaveOccurred())

			existsResults := scriptExistsResult.Val()
			Expect(len(existsResults)).To(Equal(1))
			Expect(existsResults[0]).To(BeTrue()) // Script should exist on all shards

			// Test with a non-existent script SHA
			nonExistentSHA := "0000000000000000000000000000000000000000"
			scriptExistsResult2 := client.ScriptExists(ctx, nonExistentSHA)
			Expect(scriptExistsResult2.Err()).NotTo(HaveOccurred())

			existsResults2 := scriptExistsResult2.Val()
			Expect(len(existsResults2)).To(Equal(1))
			Expect(existsResults2[0]).To(BeFalse()) // Script should not exist on any shard

			// Test with mixed scenario - flush scripts from one shard manually
			// This is harder to test in practice since SCRIPT FLUSH affects all shards
			// So we'll just verify the basic functionality works
		})

		It("should verify command aggregation policies", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			commandPolicies := map[string]string{
				"touch":         "agg_sum",
				"flushall":      "all_succeeded",
				"pfcount":       "default(hashslot)",
				"exists":        "agg_sum",
				"script exists": "agg_logical_and",
				"wait":          "agg_min",
			}

			for cmdName, expectedPolicy := range commandPolicies {
				cmd, exists := cmds[cmdName]
				if !exists {
					continue
				}

				if cmd.CommandPolicy == nil {
					continue
				}

				actualPolicy := cmd.CommandPolicy.Response.String()
				Expect(actualPolicy).To(Equal(expectedPolicy))
			}
		})

		It("should properly aggregate responses from keyless commands executed on multiple shards", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			type masterNode struct {
				client *redis.Client
				addr   string
			}
			var masterNodes []masterNode
			var mu sync.Mutex

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				addr := master.Options().Addr
				mu.Lock()
				masterNodes = append(masterNodes, masterNode{
					client: master,
					addr:   addr,
				})
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(masterNodes)).To(BeNumerically(">", 1))

			// PING command with all_shards policy - should aggregate responses
			cmds, err := client.Command(ctx).Result()
			Expect(err).NotTo(HaveOccurred())

			pingCmd, exists := cmds["ping"]
			if exists && pingCmd.CommandPolicy != nil {
			}

			pingResult := client.Ping(ctx)
			Expect(pingResult.Err()).NotTo(HaveOccurred())
			Expect(pingResult.Val()).To(Equal("PONG"))

			// Verify PING was executed on all shards by checking individual nodes
			for _, node := range masterNodes {
				nodePingResult := node.client.Ping(ctx)
				Expect(nodePingResult.Err()).NotTo(HaveOccurred())
				Expect(nodePingResult.Val()).To(Equal("PONG"))
			}

			// Test 2: DBSIZE command aggregation across shards - verify agg_sum policy
			testKeys := []string{
				"dbsize_test_key_1111",
				"dbsize_test_key_2222",
				"dbsize_test_key_3333",
				"dbsize_test_key_4444",
			}

			for _, key := range testKeys {
				result := client.Set(ctx, key, "test_value", 0)
				Expect(result.Err()).NotTo(HaveOccurred())
			}

			dbSizeResult := client.DBSize(ctx)
			Expect(dbSizeResult.Err()).NotTo(HaveOccurred())

			totalSize := dbSizeResult.Val()
			Expect(totalSize).To(BeNumerically(">=", int64(len(testKeys))))

			// Verify aggregation by manually getting sizes from each shard
			totalManualSize := int64(0)

			for _, node := range masterNodes {
				nodeDbSizeResult := node.client.DBSize(ctx)
				Expect(nodeDbSizeResult.Err()).NotTo(HaveOccurred())

				nodeSize := nodeDbSizeResult.Val()
				totalManualSize += nodeSize
			}

			// Verify aggregation worked correctly
			Expect(totalSize).To(Equal(totalManualSize))
		})

		It("should properly aggregate responses from keyed commands executed on multiple shards", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")
			type masterNode struct {
				client *redis.Client
				addr   string
			}
			var masterNodes []masterNode
			var mu sync.Mutex

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				addr := master.Options().Addr
				mu.Lock()
				masterNodes = append(masterNodes, masterNode{
					client: master,
					addr:   addr,
				})
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(masterNodes)).To(BeNumerically(">", 1))

			// MGET command with keys on different shards
			testData := map[string]string{
				"mget_test_key_1111": "value1",
				"mget_test_key_2222": "value2",
				"mget_test_key_3333": "value3",
				"mget_test_key_4444": "value4",
				"mget_test_key_5555": "value5",
			}

			keyLocations := make(map[string]string)
			for key, value := range testData {
				result := client.Set(ctx, key, value, 0)
				Expect(result.Err()).NotTo(HaveOccurred())

				for _, node := range masterNodes {
					getResult := node.client.Get(ctx, key)
					if getResult.Err() == nil && getResult.Val() == value {
						keyLocations[key] = node.addr
						break
					}
				}
			}

			shardsUsed := make(map[string]bool)
			for _, shardAddr := range keyLocations {
				shardsUsed[shardAddr] = true
			}
			Expect(len(shardsUsed)).To(BeNumerically(">", 1))

			keys := make([]string, 0, len(testData))
			expectedValues := make([]interface{}, 0, len(testData))
			for key, value := range testData {
				keys = append(keys, key)
				expectedValues = append(expectedValues, value)
			}

			mgetResult := client.MGet(ctx, keys...)
			Expect(mgetResult.Err()).NotTo(HaveOccurred())

			actualValues := mgetResult.Val()
			Expect(len(actualValues)).To(Equal(len(expectedValues)))
			for i, value := range actualValues {
				if value != nil {
					Expect(value).To(Equal(expectedValues[i]))
				} else {
					Expect(value).To(BeNil())
				}
			}

			// EXISTS command aggregation across multiple keys
			existsTestData := map[string]string{
				"exists_agg_key_1111": "value1",
				"exists_agg_key_2222": "value2",
				"exists_agg_key_3333": "value3",
			}

			existsKeys := make([]string, 0, len(existsTestData))
			for key, value := range existsTestData {
				result := client.Set(ctx, key, value, 0)
				Expect(result.Err()).NotTo(HaveOccurred())
				existsKeys = append(existsKeys, key)
			}

			// Add a non-existent key to the list
			nonExistentKey := "non_existent_key_9999"
			existsKeys = append(existsKeys, nonExistentKey)

			existsResult := client.Exists(ctx, existsKeys...)
			Expect(existsResult.Err()).NotTo(HaveOccurred())

			existsCount := existsResult.Val()
			Expect(existsCount).To(Equal(int64(len(existsTestData))))
		})

		It("should propagate coordinator errors to client without modification", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			type masterNode struct {
				client *redis.Client
				addr   string
			}
			var masterNodes []masterNode
			var mu sync.Mutex

			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				addr := master.Options().Addr
				mu.Lock()
				masterNodes = append(masterNodes, masterNode{
					client: master,
					addr:   addr,
				})
				mu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(masterNodes)).To(BeNumerically(">", 0))

			invalidSlotResult := client.ClusterAddSlotsRange(ctx, 99999, 100000)
			coordinatorErr := invalidSlotResult.Err()

			if coordinatorErr != nil {
				// Verify the error is a Redis error
				var redisErr redis.Error
				Expect(errors.As(coordinatorErr, &redisErr)).To(BeTrue())

				// Verify error message is preserved exactly as returned by coordinator
				errorMsg := coordinatorErr.Error()
				Expect(errorMsg).To(SatisfyAny(
					ContainSubstring("slot"),
					ContainSubstring("ERR"),
					ContainSubstring("Invalid"),
				))

				// Test that the same error occurs when calling coordinator directly
				coordinatorNode := masterNodes[0]
				directResult := coordinatorNode.client.ClusterAddSlotsRange(ctx, 99999, 100000)
				directErr := directResult.Err()

				if directErr != nil {
					Expect(coordinatorErr.Error()).To(Equal(directErr.Error()))
				}
			}

			// Try cluster forget with invalid node ID
			invalidNodeID := "invalid_node_id_12345"
			forgetResult := client.ClusterForget(ctx, invalidNodeID)
			forgetErr := forgetResult.Err()

			if forgetErr != nil {
				var redisErr redis.Error
				Expect(errors.As(forgetErr, &redisErr)).To(BeTrue())

				errorMsg := forgetErr.Error()
				Expect(errorMsg).To(SatisfyAny(
					ContainSubstring("Unknown node"),
					ContainSubstring("Invalid node"),
					ContainSubstring("ERR"),
				))

				coordinatorNode := masterNodes[0]
				directForgetResult := coordinatorNode.client.ClusterForget(ctx, invalidNodeID)
				directForgetErr := directForgetResult.Err()

				if directForgetErr != nil {
					Expect(forgetErr.Error()).To(Equal(directForgetErr.Error()))
				}
			}

			// Test error type preservation and format
			keySlotResult := client.ClusterKeySlot(ctx, "")
			keySlotErr := keySlotResult.Err()

			if keySlotErr != nil {
				var redisErr redis.Error
				Expect(errors.As(keySlotErr, &redisErr)).To(BeTrue())

				errorMsg := keySlotErr.Error()
				Expect(len(errorMsg)).To(BeNumerically(">", 0))
				Expect(errorMsg).NotTo(ContainSubstring("wrapped"))
				Expect(errorMsg).NotTo(ContainSubstring("context"))
			}

			// Verify error propagation consistency
			clusterInfoResult := client.ClusterInfo(ctx)
			clusterInfoErr := clusterInfoResult.Err()

			if clusterInfoErr != nil {
				var redisErr redis.Error
				Expect(errors.As(clusterInfoErr, &redisErr)).To(BeTrue())

				coordinatorNode := masterNodes[0]
				directInfoResult := coordinatorNode.client.ClusterInfo(ctx)
				directInfoErr := directInfoResult.Err()

				if directInfoErr != nil {
					Expect(clusterInfoErr.Error()).To(Equal(directInfoErr.Error()))
				}
			}

			// Verify no error modification in router
			invalidReplicateResult := client.ClusterReplicate(ctx, "00000000000000000000000000000000invalid00")
			invalidReplicateErr := invalidReplicateResult.Err()

			if invalidReplicateErr != nil {
				var redisErr redis.Error
				Expect(errors.As(invalidReplicateErr, &redisErr)).To(BeTrue())

				errorMsg := invalidReplicateErr.Error()
				Expect(errorMsg).NotTo(ContainSubstring("router"))
				Expect(errorMsg).NotTo(ContainSubstring("cluster client"))
				Expect(errorMsg).NotTo(ContainSubstring("failed to execute"))

				Expect(errorMsg).To(SatisfyAny(
					HavePrefix("ERR"),
					ContainSubstring("Invalid"),
					ContainSubstring("Unknown"),
				))
			}
		})

		Describe("Routing Policies Comprehensive Test Suite", func() {
			It("should test MGET command with multi-slot routing and key order preservation", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Set up test data across multiple shards
				testData := map[string]string{
					"mget_test_key_1111": "value1",
					"mget_test_key_2222": "value2",
					"mget_test_key_3333": "value3",
					"mget_test_key_4444": "value4",
					"mget_test_key_5555": "value5",
				}

				// Set all keys
				for key, value := range testData {
					Expect(client.Set(ctx, key, value, 0).Err()).NotTo(HaveOccurred())
				}

				// Verify keys are distributed across multiple shards
				slotMap := make(map[int]bool)
				for key := range testData {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				// Test MGET with specific key order
				keys := []string{
					"mget_test_key_3333",
					"mget_test_key_1111",
					"mget_test_key_5555",
					"mget_test_key_2222",
					"mget_test_key_4444",
				}

				result := client.MGet(ctx, keys...)
				Expect(result.Err()).NotTo(HaveOccurred())

				// Verify values are returned in the same order as keys
				values := result.Val()
				Expect(len(values)).To(Equal(len(keys)))
				for i, key := range keys {
					Expect(values[i]).To(Equal(testData[key]))
				}
			})

			It("should test MGET with non-existent keys across multiple shards", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Set up some keys
				Expect(client.Set(ctx, "mget_exists_1111", "value1", 0).Err()).NotTo(HaveOccurred())
				Expect(client.Set(ctx, "mget_exists_3333", "value3", 0).Err()).NotTo(HaveOccurred())

				// MGET with mix of existing and non-existing keys
				keys := []string{
					"mget_exists_1111",
					"mget_nonexist_2222",
					"mget_exists_3333",
					"mget_nonexist_4444",
				}

				result := client.MGet(ctx, keys...)
				Expect(result.Err()).ToNot(HaveOccurred())

				// MGET returns nil for non-existent keys (not errors)
				// Values should be in the same order as requested keys
				values := result.Val()
				Expect(len(values)).To(Equal(4))
				Expect(values[0]).To(Equal("value1")) // existing key
				Expect(values[1]).To(BeNil())         // non-existent key returns nil
				Expect(values[2]).To(Equal("value3")) // existing key
				Expect(values[3]).To(BeNil())         // non-existent key returns nil
			})

			It("should test TOUCH command with multi-slot routing", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Set up keys across multiple shards
				keys := []string{
					"touch_test_key_1111",
					"touch_test_key_2222",
					"touch_test_key_3333",
					"touch_test_key_4444",
				}

				// Set all keys
				for _, key := range keys {
					Expect(client.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}

				// Verify keys are on different shards
				slotMap := make(map[int]bool)
				for _, key := range keys {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				// TOUCH should work across multiple shards
				result := client.Touch(ctx, keys...)
				Expect(result.Err()).NotTo(HaveOccurred())
				Expect(result.Val()).To(Equal(int64(len(keys))))
			})

			It("should test DEL command with multi-slot routing", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Set up keys across multiple shards
				keys := []string{
					"del_test_key_1111",
					"del_test_key_2222",
					"del_test_key_3333",
				}

				// Set all keys
				for _, key := range keys {
					Expect(client.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}

				// Verify keys are on different shards
				slotMap := make(map[int]bool)
				for _, key := range keys {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				// DEL should work across multiple shards
				result := client.Del(ctx, keys...)
				Expect(result.Err()).NotTo(HaveOccurred())
				Expect(result.Val()).To(Equal(int64(len(keys))))

				// Verify all keys were deleted
				for _, key := range keys {
					val := client.Get(ctx, key)
					Expect(val.Err()).To(Equal(redis.Nil))
				}
			})

			It("should test DBSIZE command with agg_sum aggregation across all shards", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Set keys across multiple shards
				keys := []string{
					"dbsize_test_1111",
					"dbsize_test_2222",
					"dbsize_test_3333",
					"dbsize_test_4444",
					"dbsize_test_5555",
				}

				// Clean up first
				client.Del(ctx, keys...)

				// Set all keys
				for _, key := range keys {
					Expect(client.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}

				// DBSIZE should aggregate results from all shards
				result := client.DBSize(ctx)
				Expect(result.Err()).NotTo(HaveOccurred())
				Expect(result.Val()).To(BeNumerically(">=", int64(len(keys))))
			})

			It("should test PING command with all_shards routing", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// PING should be sent to all shards and return one successful response
				result := client.Ping(ctx)
				Expect(result.Err()).NotTo(HaveOccurred())
				Expect(result.Val()).To(Equal("PONG"))
			})

			It("should test MGET with single shard optimization", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Use hash tags to ensure all keys are on the same shard
				keys := []string{
					"{sameslot}key1",
					"{sameslot}key2",
					"{sameslot}key3",
				}

				// Verify all keys hash to the same slot
				slot := hashtag.Slot(keys[0])
				for _, key := range keys {
					Expect(hashtag.Slot(key)).To(Equal(slot))
				}

				// Set all keys
				for i, key := range keys {
					Expect(client.Set(ctx, key, fmt.Sprintf("value%d", i+1), 0).Err()).NotTo(HaveOccurred())
				}

				// MGET should work even with single shard
				result := client.MGet(ctx, keys...)
				Expect(result.Err()).NotTo(HaveOccurred())

				values := result.Val()
				Expect(len(values)).To(Equal(3))
				Expect(values[0]).To(Equal("value1"))
				Expect(values[1]).To(Equal("value2"))
				Expect(values[2]).To(Equal("value3"))
			})

			It("should test empty MGET command returns error", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// MGET with no keys should return an error
				result := client.MGet(ctx)
				Expect(result.Err()).To(HaveOccurred())
				Expect(result.Err().Error()).To(ContainSubstring("multi-shard command mget has no key arguments"))
			})

			It("should test MGET integration with MSET across multiple shards", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Create test data
				testData := map[string]string{
					"integration_key_1111": "alpha",
					"integration_key_2222": "beta",
					"integration_key_3333": "gamma",
					"integration_key_4444": "delta",
					"integration_key_5555": "epsilon",
				}

				// Verify keys are on different shards
				slotMap := make(map[int]bool)
				for key := range testData {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				// Use individual SET commands instead of MSET
				keys := make([]string, 0, len(testData))
				for key := range testData {
					keys = append(keys, key)
				}

				msetResult := client.MSet(ctx, testData)
				Expect(msetResult.Err()).NotTo(HaveOccurred())
				Expect(msetResult.Val()).To(Equal("OK"))

				// Execute MGET
				mgetResult := client.MGet(ctx, keys...)
				Expect(mgetResult.Err()).NotTo(HaveOccurred())

				// Verify all values match
				values := mgetResult.Val()
				Expect(len(values)).To(Equal(len(keys)))
				for i, key := range keys {
					Expect(values[i]).To(Equal(testData[key]))
				}
			})

			It("should test multi-shard commands cannot be used in pipeline", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Create keys across multiple shards
				keys := []string{
					"pipeline_test_1111",
					"pipeline_test_2222",
					"pipeline_test_3333",
				}

				// Verify keys are on different shards
				slotMap := make(map[int]bool)
				for _, key := range keys {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				// Try to use MGET in pipeline - should fail
				pipe := client.Pipeline()
				pipe.MGet(ctx, keys...)
				_, err := pipe.Exec(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("cannot pipeline command"))
			})

			It("should test DisableRoutingPolicies option disables routing policies", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Test 1: With routing policies enabled (default), MGET should work across slots
				testData := map[string]string{
					"disable_routing_key_1111": "value1",
					"disable_routing_key_2222": "value2",
					"disable_routing_key_3333": "value3",
				}

				// Set keys
				for key, value := range testData {
					Expect(client.Set(ctx, key, value, 0).Err()).NotTo(HaveOccurred())
				}

				// Verify keys are on different shards
				slotMap := make(map[int]bool)
				for key := range testData {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				keys := make([]string, 0, len(testData))
				for key := range testData {
					keys = append(keys, key)
				}

				// With routing policies enabled, MGET should work
				mgetResult := client.MGet(ctx, keys...)
				Expect(mgetResult.Err()).NotTo(HaveOccurred())
				Expect(len(mgetResult.Val())).To(Equal(len(keys)))

				// Test 2: With routing policies disabled, MGET should fail with CROSSSLOT error
				opt := redisClusterOptions()
				opt.DisableRoutingPolicies = true
				clientWithoutPolicies := cluster.newClusterClient(ctx, opt)
				defer clientWithoutPolicies.Close()

				// Try MGET with routing policies disabled - should fail with CROSSSLOT error
				mgetResultDisabled := clientWithoutPolicies.MGet(ctx, keys...)
				Expect(mgetResultDisabled.Err()).To(HaveOccurred())
				Expect(mgetResultDisabled.Err().Error()).To(ContainSubstring("CROSSSLOT"))
			})

			It("should test large MGET with many keys across all shards", func() {
				SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

				// Create many keys to ensure coverage across all shards
				numKeys := 100
				keys := make([]string, numKeys)
				values := make(map[string]string)

				for i := 0; i < numKeys; i++ {
					key := fmt.Sprintf("large_mget_key_%d", i)
					value := fmt.Sprintf("value_%d", i)
					keys[i] = key
					values[key] = value
					Expect(client.Set(ctx, key, value, 0).Err()).NotTo(HaveOccurred())
				}

				// Verify keys are distributed across multiple shards
				slotMap := make(map[int]bool)
				for _, key := range keys {
					slot := hashtag.Slot(key)
					slotMap[slot] = true
				}
				Expect(len(slotMap)).To(BeNumerically(">", 1))

				// Execute MGET
				result := client.MGet(ctx, keys...)
				Expect(result.Err()).NotTo(HaveOccurred())

				// Verify all values are correct
				resultValues := result.Val()
				Expect(len(resultValues)).To(Equal(numKeys))
				for i, key := range keys {
					Expect(resultValues[i]).To(Equal(values[key]))
				}
			})
		})

		It("should route keyless commands to arbitrary shards using round robin", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			var numMasters int
			var numMastersMu sync.Mutex
			err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				numMastersMu.Lock()
				numMasters++
				numMastersMu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(numMasters).To(BeNumerically(">", 1))

			err = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.ConfigResetStat(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			// Helper function to get ECHO command counts from all nodes
			getEchoCounts := func() map[string]int {
				echoCounts := make(map[string]int)
				var echoCountsMu sync.Mutex
				err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
					info := master.Info(ctx, "server")
					Expect(info.Err()).NotTo(HaveOccurred())

					serverInfo := info.Val()
					portStart := strings.Index(serverInfo, "tcp_port:")
					portLine := serverInfo[portStart:]
					portEnd := strings.Index(portLine, "\r\n")
					if portEnd == -1 {
						portEnd = len(portLine)
					}
					port := strings.TrimPrefix(portLine[:portEnd], "tcp_port:")

					commandStats := master.Info(ctx, "commandstats")
					count := 0
					if commandStats.Err() == nil {
						stats := commandStats.Val()
						cmdStatKey := "cmdstat_echo:"
						if strings.Contains(stats, cmdStatKey) {
							statStart := strings.Index(stats, cmdStatKey)
							statLine := stats[statStart:]
							statEnd := strings.Index(statLine, "\r\n")
							if statEnd == -1 {
								statEnd = len(statLine)
							}
							statLine = statLine[:statEnd]

							callsStart := strings.Index(statLine, "calls=")
							if callsStart != -1 {
								callsStr := statLine[callsStart+6:]
								callsEnd := strings.Index(callsStr, ",")
								if callsEnd == -1 {
									callsEnd = strings.Index(callsStr, "\r")
									if callsEnd == -1 {
										callsEnd = len(callsStr)
									}
								}
								if callsCount, err := strconv.Atoi(callsStr[:callsEnd]); err == nil {
									count = callsCount
								}
							}
						}
					}

					echoCountsMu.Lock()
					echoCounts[port] = count
					echoCountsMu.Unlock()
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				return echoCounts
			}

			// Single ECHO command should go to exactly one shard
			result := client.Echo(ctx, "single_test")
			Expect(result.Err()).NotTo(HaveOccurred())
			Expect(result.Val()).To(Equal("single_test"))

			time.Sleep(200 * time.Millisecond)

			// Verify single command went to exactly one shard
			echoCounts := getEchoCounts()
			shardsWithEcho := 0
			for _, count := range echoCounts {
				if count > 0 {
					shardsWithEcho++
					Expect(count).To(Equal(1))
				}
			}
			Expect(shardsWithEcho).To(Equal(1))

			// Test Multiple ECHO commands should distribute across all shards using round robin
			numCommands := numMasters * 5

			for i := 0; i < numCommands; i++ {
				result := client.Echo(ctx, fmt.Sprintf("multi_test_%d", i))
				Expect(result.Err()).NotTo(HaveOccurred())
				Expect(result.Val()).To(Equal(fmt.Sprintf("multi_test_%d", i)))
			}

			time.Sleep(200 * time.Millisecond)

			echoCounts = getEchoCounts()
			totalEchos := 0
			shardsWithEchos := 0
			for _, count := range echoCounts {
				if count > 0 {
					shardsWithEchos++
				}
				totalEchos += count
			}

			// All shards should now have some ECHO commands
			Expect(shardsWithEchos).To(Equal(numMasters))

			expectedTotal := 1 + numCommands
			Expect(totalEchos).To(Equal(expectedTotal))
		})
	})

	var _ = Describe("ClusterClient ParseURL", func() {
		cases := []struct {
			test string
			url  string
			o    *redis.ClusterOptions // expected value
			err  error
		}{
			{
				test: "ParseRedisURL",
				url:  "redis://localhost:123",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}},
			}, {
				test: "ParseRedissURL",
				url:  "rediss://localhost:123",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, TLSConfig: &tls.Config{ServerName: "localhost"}},
			}, {
				test: "MissingRedisPort",
				url:  "redis://localhost",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:6379"}},
			}, {
				test: "MissingRedissPort",
				url:  "rediss://localhost",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:6379"}, TLSConfig: &tls.Config{ServerName: "localhost"}},
			}, {
				test: "MultipleRedisURLs",
				url:  "redis://localhost:123?addr=localhost:1234&addr=localhost:12345",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123", "localhost:1234", "localhost:12345"}},
			}, {
				test: "MultipleRedissURLs",
				url:  "rediss://localhost:123?addr=localhost:1234&addr=localhost:12345",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123", "localhost:1234", "localhost:12345"}, TLSConfig: &tls.Config{ServerName: "localhost"}},
			}, {
				test: "OnlyPassword",
				url:  "redis://:bar@localhost:123",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, Password: "bar"},
			}, {
				test: "OnlyUser",
				url:  "redis://foo@localhost:123",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, Username: "foo"},
			}, {
				test: "RedisUsernamePassword",
				url:  "redis://foo:bar@localhost:123",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, Username: "foo", Password: "bar"},
			}, {
				test: "RedissUsernamePassword",
				url:  "rediss://foo:bar@localhost:123?addr=localhost:1234",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123", "localhost:1234"}, Username: "foo", Password: "bar", TLSConfig: &tls.Config{ServerName: "localhost"}},
			}, {
				test: "QueryParameters",
				url:  "redis://localhost:123?read_timeout=2&pool_fifo=true&addr=localhost:1234",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123", "localhost:1234"}, ReadTimeout: 2 * time.Second, PoolFIFO: true},
			}, {
				test: "DisabledTimeout",
				url:  "redis://localhost:123?conn_max_idle_time=0",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, ConnMaxIdleTime: -1},
			}, {
				test: "DisabledTimeoutNeg",
				url:  "redis://localhost:123?conn_max_idle_time=-1",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, ConnMaxIdleTime: -1},
			}, {
				test: "UseDefault",
				url:  "redis://localhost:123?conn_max_idle_time=",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, ConnMaxIdleTime: 0},
			}, {
				test: "Protocol",
				url:  "redis://localhost:123?protocol=2",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, Protocol: 2},
			}, {
				test: "ClientName",
				url:  "redis://localhost:123?client_name=cluster_hi",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, ClientName: "cluster_hi"},
			}, {
				test: "UseDefaultMissing=",
				url:  "redis://localhost:123?conn_max_idle_time",
				o:    &redis.ClusterOptions{Addrs: []string{"localhost:123"}, ConnMaxIdleTime: 0},
			}, {
				test: "InvalidQueryAddr",
				url:  "rediss://foo:bar@localhost:123?addr=rediss://foo:barr@localhost:1234",
				err:  errors.New(`redis: unable to parse addr param: rediss://foo:barr@localhost:1234`),
			}, {
				test: "InvalidInt",
				url:  "redis://localhost?pool_size=five",
				err:  errors.New(`redis: invalid pool_size number: strconv.Atoi: parsing "five": invalid syntax`),
			}, {
				test: "InvalidBool",
				url:  "redis://localhost?pool_fifo=yes",
				err:  errors.New(`redis: invalid pool_fifo boolean: expected true/false/1/0 or an empty string, got "yes"`),
			}, {
				test: "UnknownParam",
				url:  "redis://localhost?abc=123",
				err:  errors.New("redis: unexpected option: abc"),
			}, {
				test: "InvalidScheme",
				url:  "https://google.com",
				err:  errors.New("redis: invalid URL scheme: https"),
			},
		}

		It("should match ParseClusterURL", func() {
			for i := range cases {
				tc := cases[i]
				actual, err := redis.ParseClusterURL(tc.url)
				if tc.err != nil {
					Expect(err).Should(MatchError(tc.err))
				} else {
					Expect(err).NotTo(HaveOccurred())
				}

				if err == nil {
					Expect(tc.o).NotTo(BeNil())

					Expect(tc.o.Addrs).To(Equal(actual.Addrs))
					Expect(tc.o.TLSConfig).To(Equal(actual.TLSConfig))
					Expect(tc.o.Username).To(Equal(actual.Username))
					Expect(tc.o.Password).To(Equal(actual.Password))
					Expect(tc.o.MaxRetries).To(Equal(actual.MaxRetries))
					Expect(tc.o.MinRetryBackoff).To(Equal(actual.MinRetryBackoff))
					Expect(tc.o.MaxRetryBackoff).To(Equal(actual.MaxRetryBackoff))
					Expect(tc.o.DialTimeout).To(Equal(actual.DialTimeout))
					Expect(tc.o.ReadTimeout).To(Equal(actual.ReadTimeout))
					Expect(tc.o.WriteTimeout).To(Equal(actual.WriteTimeout))
					Expect(tc.o.PoolFIFO).To(Equal(actual.PoolFIFO))
					Expect(tc.o.PoolSize).To(Equal(actual.PoolSize))
					Expect(tc.o.MinIdleConns).To(Equal(actual.MinIdleConns))
					Expect(tc.o.ConnMaxLifetime).To(Equal(actual.ConnMaxLifetime))
					Expect(tc.o.ConnMaxIdleTime).To(Equal(actual.ConnMaxIdleTime))
					Expect(tc.o.PoolTimeout).To(Equal(actual.PoolTimeout))
				}
			}
		})

		It("should distribute keyless commands randomly across shards using random shard picker", func() {
			SkipBeforeRedisVersion(7.9, "The tips are included from Redis 8")

			// Create a cluster client with random shard picker
			opt := redisClusterOptions()
			opt.ShardPicker = &routing.RandomPicker{}
			randomClient := cluster.newClusterClient(ctx, opt)
			defer randomClient.Close()

			Eventually(func() error {
				return randomClient.Ping(ctx).Err()
			}, 30*time.Second).ShouldNot(HaveOccurred())

			var numMasters int
			var numMastersMu sync.Mutex
			err := randomClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				numMastersMu.Lock()
				numMasters++
				numMastersMu.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(numMasters).To(BeNumerically(">", 1))

			// Reset command statistics on all masters
			err = randomClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
				return master.ConfigResetStat(ctx).Err()
			})
			Expect(err).NotTo(HaveOccurred())

			// Helper function to get ECHO command counts from all nodes
			getEchoCounts := func() map[string]int {
				echoCounts := make(map[string]int)
				var echoCountsMu sync.Mutex
				err := randomClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
					addr := master.Options().Addr
					port := addr[strings.LastIndex(addr, ":")+1:]

					info, err := master.Info(ctx, "commandstats").Result()
					if err != nil {
						return err
					}

					count := 0
					if strings.Contains(info, "cmdstat_echo:") {
						lines := strings.Split(info, "\n")
						for _, line := range lines {
							if strings.HasPrefix(line, "cmdstat_echo:") {
								parts := strings.Split(line, ",")
								if len(parts) > 0 {
									callsPart := strings.Split(parts[0], "=")
									if len(callsPart) > 1 {
										if parsedCount, parseErr := strconv.Atoi(callsPart[1]); parseErr == nil {
											count = parsedCount
										}
									}
								}
								break
							}
						}
					}

					echoCountsMu.Lock()
					echoCounts[port] = count
					echoCountsMu.Unlock()
					return nil
				})
				Expect(err).NotTo(HaveOccurred())
				return echoCounts
			}

			// Execute multiple ECHO commands and measure distribution
			numCommands := 100
			for i := 0; i < numCommands; i++ {
				result := randomClient.Echo(ctx, fmt.Sprintf("random_test_%d", i))
				Expect(result.Err()).NotTo(HaveOccurred())
			}

			echoCounts := getEchoCounts()

			totalEchos := 0
			shardsWithEchos := 0

			for _, count := range echoCounts {
				if count > 0 {
					shardsWithEchos++
				}
				totalEchos += count
			}

			Expect(totalEchos).To(Equal(numCommands))
			Expect(shardsWithEchos).To(BeNumerically(">=", 2))
		})
	})
})

var _ = Describe("ClusterClient FailingTimeoutSeconds", func() {
	var client *redis.ClusterClient

	AfterEach(func() {
		if client != nil {
			_ = client.Close()
		}
	})

	It("should use default failing timeout of 15 seconds", func() {
		opt := redisClusterOptions()
		client = cluster.newClusterClient(ctx, opt)

		// Default should be 15 seconds
		Expect(opt.FailingTimeoutSeconds).To(Equal(15))
	})

	It("should use custom failing timeout", func() {
		opt := redisClusterOptions()
		opt.FailingTimeoutSeconds = 30
		client = cluster.newClusterClient(ctx, opt)

		// Should use custom value
		Expect(opt.FailingTimeoutSeconds).To(Equal(30))
	})

	It("should parse failing_timeout_seconds from URL", func() {
		url := "redis://localhost:16600?failing_timeout_seconds=25"
		opt, err := redis.ParseClusterURL(url)
		Expect(err).NotTo(HaveOccurred())
		Expect(opt.FailingTimeoutSeconds).To(Equal(25))
	})

	It("should handle node failing timeout correctly", func() {
		opt := redisClusterOptions()
		opt.FailingTimeoutSeconds = 2 // Short timeout for testing
		client = cluster.newClusterClient(ctx, opt)

		// Get a node and mark it as failing
		nodes, err := client.Nodes(ctx, "A")
		Expect(err).NotTo(HaveOccurred())
		Expect(len(nodes)).To(BeNumerically(">", 0))

		node := nodes[0]

		// Initially not failing
		Expect(node.Failing()).To(BeFalse())

		// Mark as failing
		node.MarkAsFailing()
		Expect(node.Failing()).To(BeTrue())

		// Should still be failing after 1 second (less than timeout)
		time.Sleep(1 * time.Second)
		Expect(node.Failing()).To(BeTrue())

		// Should not be failing after timeout expires
		time.Sleep(2 * time.Second) // Total 3 seconds > 2 second timeout
		Expect(node.Failing()).To(BeFalse())
	})

	It("should handle zero timeout by using default", func() {
		opt := redisClusterOptions()
		opt.FailingTimeoutSeconds = 0 // Should use default
		client = cluster.newClusterClient(ctx, opt)

		// After initialization, should be set to default
		Expect(opt.FailingTimeoutSeconds).To(Equal(15))
	})
})
