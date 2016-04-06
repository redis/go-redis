package redis_test

import (
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
	"gopkg.in/redis.v3/internal/hashtag"
)

type clusterScenario struct {
	ports     []string
	nodeIds   []string
	processes map[string]*redisProcess
	clients   map[string]*redis.Client
}

func (s *clusterScenario) primary() *redis.Client {
	return s.clients[s.ports[0]]
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

func (s *clusterScenario) clusterClient(opt *redis.ClusterOptions) *redis.ClusterClient {
	addrs := make([]string, len(s.ports))
	for i, port := range s.ports {
		addrs[i] = net.JoinHostPort("127.0.0.1", port)
	}
	if opt == nil {
		opt = &redis.ClusterOptions{
			DialTimeout:        10 * time.Second,
			ReadTimeout:        30 * time.Second,
			WriteTimeout:       30 * time.Second,
			PoolSize:           10,
			PoolTimeout:        30 * time.Second,
			IdleTimeout:        time.Second,
			IdleCheckFrequency: time.Second,
		}
	}
	opt.Addrs = addrs
	return redis.NewClusterClient(opt)
}

func startCluster(scenario *clusterScenario) error {
	// Start processes and collect node ids
	for pos, port := range scenario.ports {
		process, err := startRedis(port, "--cluster-enabled", "yes")
		if err != nil {
			return err
		}

		client := redis.NewClient(&redis.Options{
			Addr: ":" + port,
		})

		info, err := client.ClusterNodes().Result()
		if err != nil {
			return err
		}

		scenario.processes[port] = process
		scenario.clients[port] = client
		scenario.nodeIds[pos] = info[:40]
	}

	// Meet cluster nodes
	for _, client := range scenario.clients {
		err := client.ClusterMeet("127.0.0.1", scenario.ports[0]).Err()
		if err != nil {
			return err
		}
	}

	// Bootstrap masters
	slots := []int{0, 5000, 10000, 16384}
	for pos, master := range scenario.masters() {
		err := master.ClusterAddSlotsRange(slots[pos], slots[pos+1]-1).Err()
		if err != nil {
			return err
		}
	}

	// Bootstrap slaves
	for idx, slave := range scenario.slaves() {
		masterId := scenario.nodeIds[idx]

		// Wait until master is available
		err := eventually(func() error {
			s := slave.ClusterNodes().Val()
			wanted := masterId
			if !strings.Contains(s, wanted) {
				return fmt.Errorf("%q does not contain %q", s, wanted)
			}
			return nil
		}, 10*time.Second)
		if err != nil {
			return err
		}

		err = slave.ClusterReplicate(masterId).Err()
		if err != nil {
			return err
		}
	}

	// Wait until all nodes have consistent info
	for _, client := range scenario.clients {
		err := eventually(func() error {
			res, err := client.ClusterSlots().Result()
			if err != nil {
				return err
			}
			wanted := []redis.ClusterSlotInfo{
				{0, 4999, []string{"127.0.0.1:8220", "127.0.0.1:8223"}},
				{5000, 9999, []string{"127.0.0.1:8221", "127.0.0.1:8224"}},
				{10000, 16383, []string{"127.0.0.1:8222", "127.0.0.1:8225"}},
			}
		loop:
			for _, info := range res {
				for _, info2 := range wanted {
					if reflect.DeepEqual(info, info2) {
						continue loop
					}
				}
				return fmt.Errorf("cluster did not reach consistent state (%v)", res)
			}
			return nil
		}, 30*time.Second)
		if err != nil {
			return err
		}
	}

	return nil
}

func stopCluster(scenario *clusterScenario) error {
	for _, client := range scenario.clients {
		if err := client.Close(); err != nil {
			return err
		}
	}
	for _, process := range scenario.processes {
		if err := process.Close(); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

var _ = Describe("Cluster", func() {
	Describe("HashSlot", func() {

		It("should calculate hash slots", func() {
			tests := []struct {
				key  string
				slot int
			}{
				{"123456789", 12739},
				{"{}foo", 9500},
				{"foo{}", 5542},
				{"foo{}{bar}", 8363},
				{"", 10503},
				{"", 5176},
				{string([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}), 5463},
			}
			// Empty keys receive random slot.
			rand.Seed(100)

			for _, test := range tests {
				Expect(hashtag.Slot(test.key)).To(Equal(test.slot), "for %s", test.key)
			}
		})

		It("should extract keys from tags", func() {
			tests := []struct {
				one, two string
			}{
				{"foo{bar}", "bar"},
				{"{foo}bar", "foo"},
				{"{user1000}.following", "{user1000}.followers"},
				{"foo{{bar}}zap", "{bar"},
				{"foo{bar}{zap}", "bar"},
			}

			for _, test := range tests {
				Expect(hashtag.Slot(test.one)).To(Equal(hashtag.Slot(test.two)), "for %s <-> %s", test.one, test.two)
			}
		})

	})

	Describe("Commands", func() {

		It("should CLUSTER SLOTS", func() {
			res, err := cluster.primary().ClusterSlots().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))
			Expect(res).To(ConsistOf([]redis.ClusterSlotInfo{
				{0, 4999, []string{"127.0.0.1:8220", "127.0.0.1:8223"}},
				{5000, 9999, []string{"127.0.0.1:8221", "127.0.0.1:8224"}},
				{10000, 16383, []string{"127.0.0.1:8222", "127.0.0.1:8225"}},
			}))
		})

		It("should CLUSTER NODES", func() {
			res, err := cluster.primary().ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(BeNumerically(">", 400))
		})

		It("should CLUSTER INFO", func() {
			res, err := cluster.primary().ClusterInfo().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainSubstring("cluster_known_nodes:6"))
		})

		It("should CLUSTER KEYSLOT", func() {
			hashSlot, err := cluster.primary().ClusterKeySlot("somekey").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(hashSlot).To(Equal(int64(hashtag.Slot("somekey"))))
		})

		It("should CLUSTER COUNT-FAILURE-REPORTS", func() {
			n, err := cluster.primary().ClusterCountFailureReports(cluster.nodeIds[0]).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER COUNTKEYSINSLOT", func() {
			n, err := cluster.primary().ClusterCountKeysInSlot(10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER DELSLOTS", func() {
			res, err := cluster.primary().ClusterDelSlotsRange(16000, 16384-1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
			cluster.primary().ClusterAddSlotsRange(16000, 16384-1)
		})

		It("should CLUSTER SAVECONFIG", func() {
			res, err := cluster.primary().ClusterSaveConfig().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
		})

		It("should CLUSTER SLAVES", func() {
			nodesList, err := cluster.primary().ClusterSlaves(cluster.nodeIds[0]).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nodesList).Should(ContainElement(ContainSubstring("slave")))
			Expect(nodesList).Should(HaveLen(1))
		})

		It("should CLUSTER READONLY", func() {
			res, err := cluster.primary().Readonly().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
		})

		It("should CLUSTER READWRITE", func() {
			res, err := cluster.primary().ReadWrite().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
		})
	})

	Describe("Client", func() {
		var client *redis.ClusterClient

		BeforeEach(func() {
			client = cluster.clusterClient(nil)
		})

		AfterEach(func() {
			for _, client := range cluster.masters() {
				Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
			}
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should GET/SET/DEL", func() {
			val, err := client.Get("A").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))

			val, err = client.Set("A", "VALUE", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))

			val, err = client.Get("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("VALUE"))

			cnt, err := client.Del("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(1)))
		})

		It("should return pool stats", func() {
			Expect(client.PoolStats()).To(BeAssignableToTypeOf(&redis.PoolStats{}))
		})

		It("should follow redirects", func() {
			Expect(client.Set("A", "VALUE", 0).Err()).NotTo(HaveOccurred())

			slot := hashtag.Slot("A")
			Expect(client.SwapSlot(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			val, err := client.Get("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("VALUE"))

			Eventually(func() []string {
				return client.SlotAddrs(slot)
			}, "5s").Should(Equal([]string{"127.0.0.1:8221", "127.0.0.1:8224"}))
		})

		It("should return error when there are no attempts left", func() {
			Expect(client.Close()).NotTo(HaveOccurred())
			client = cluster.clusterClient(&redis.ClusterOptions{
				MaxRedirects: -1,
			})

			slot := hashtag.Slot("A")
			Expect(client.SwapSlot(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			err := client.Get("A").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("MOVED"))
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
	})

	Describe("pipeline", func() {
		var client *redis.ClusterClient

		BeforeEach(func() {
			client = cluster.clusterClient(nil)
		})

		AfterEach(func() {
			for _, client := range cluster.masters() {
				Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
			}
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("performs multi-pipelines", func() {
			slot := hashtag.Slot("A")
			Expect(client.SwapSlot(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			pipe := client.Pipeline()
			defer pipe.Close()

			keys := []string{"A", "B", "C", "D", "E", "F", "G"}
			for i, key := range keys {
				pipe.Set(key, key+"_value", 0)
				pipe.Expire(key, time.Duration(i+1)*time.Hour)
			}
			for _, key := range keys {
				pipe.Get(key)
				pipe.TTL(key)
			}

			cmds, err := pipe.Exec()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(28))
			Expect(cmds[14].(*redis.StringCmd).Val()).To(Equal("A_value"))
			Expect(cmds[15].(*redis.DurationCmd).Val()).To(BeNumerically("~", 1*time.Hour, time.Second))
			Expect(cmds[20].(*redis.StringCmd).Val()).To(Equal("D_value"))
			Expect(cmds[21].(*redis.DurationCmd).Val()).To(BeNumerically("~", 4*time.Hour, time.Second))
			Expect(cmds[26].(*redis.StringCmd).Val()).To(Equal("G_value"))
			Expect(cmds[27].(*redis.DurationCmd).Val()).To(BeNumerically("~", 7*time.Hour, time.Second))
		})

		It("works with missing keys", func() {
			Expect(client.Set("A", "A_value", 0).Err()).NotTo(HaveOccurred())
			Expect(client.Set("C", "C_value", 0).Err()).NotTo(HaveOccurred())

			var a, b, c *redis.StringCmd
			cmds, err := client.Pipelined(func(pipe *redis.ClusterPipeline) error {
				a = pipe.Get("A")
				b = pipe.Get("B")
				c = pipe.Get("C")
				return nil
			})
			Expect(err).To(Equal(redis.Nil))
			Expect(cmds).To(HaveLen(3))

			Expect(a.Err()).NotTo(HaveOccurred())
			Expect(a.Val()).To(Equal("A_value"))

			Expect(b.Err()).To(Equal(redis.Nil))
			Expect(b.Val()).To(Equal(""))

			Expect(c.Err()).NotTo(HaveOccurred())
			Expect(c.Val()).To(Equal("C_value"))
		})
	})
})

//------------------------------------------------------------------------------

func BenchmarkRedisClusterPing(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping in short mode")
	}

	cluster := &clusterScenario{
		ports:     []string{"8220", "8221", "8222", "8223", "8224", "8225"},
		nodeIds:   make([]string, 6),
		processes: make(map[string]*redisProcess, 6),
		clients:   make(map[string]*redis.Client, 6),
	}
	if err := startCluster(cluster); err != nil {
		b.Fatal(err)
	}
	defer stopCluster(cluster)
	client := cluster.clusterClient(nil)
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Ping().Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
