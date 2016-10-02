package redis_test

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v4"
	"gopkg.in/redis.v4/internal/hashtag"
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
			wanted := []redis.ClusterSlot{
				{0, 4999, []redis.ClusterNode{{"", "127.0.0.1:8220"}, {"", "127.0.0.1:8223"}}},
				{5000, 9999, []redis.ClusterNode{{"", "127.0.0.1:8221"}, {"", "127.0.0.1:8224"}}},
				{10000, 16383, []redis.ClusterNode{{"", "127.0.0.1:8222"}, {"", "127.0.0.1:8225"}}},
			}
			return assertSlotsEqual(res, wanted)
		}, 30*time.Second)
		if err != nil {
			return err
		}
	}

	return nil
}

func assertSlotsEqual(slots, wanted []redis.ClusterSlot) error {
outer_loop:
	for _, s2 := range wanted {
		for _, s1 := range slots {
			if slotEqual(s1, s2) {
				continue outer_loop
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
	for i, n1 := range s1.Nodes {
		if n1.Addr != s2.Nodes[i].Addr {
			return false
		}
	}
	return true
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

var _ = Describe("ClusterClient", func() {
	var client *redis.ClusterClient

	describeClusterClient := func() {
		It("should CLUSTER SLOTS", func() {
			res, err := client.ClusterSlots().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))

			wanted := []redis.ClusterSlot{
				{0, 4999, []redis.ClusterNode{{"", "127.0.0.1:8220"}, {"", "127.0.0.1:8223"}}},
				{5000, 9999, []redis.ClusterNode{{"", "127.0.0.1:8221"}, {"", "127.0.0.1:8224"}}},
				{10000, 16383, []redis.ClusterNode{{"", "127.0.0.1:8222"}, {"", "127.0.0.1:8225"}}},
			}
			Expect(assertSlotsEqual(res, wanted)).NotTo(HaveOccurred())
		})

		It("should CLUSTER NODES", func() {
			res, err := client.ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(BeNumerically(">", 400))
		})

		It("should CLUSTER INFO", func() {
			res, err := client.ClusterInfo().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainSubstring("cluster_known_nodes:6"))
		})

		It("should CLUSTER KEYSLOT", func() {
			hashSlot, err := client.ClusterKeySlot("somekey").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(hashSlot).To(Equal(int64(hashtag.Slot("somekey"))))
		})

		It("should CLUSTER COUNT-FAILURE-REPORTS", func() {
			n, err := client.ClusterCountFailureReports(cluster.nodeIds[0]).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER COUNTKEYSINSLOT", func() {
			n, err := client.ClusterCountKeysInSlot(10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER SAVECONFIG", func() {
			res, err := client.ClusterSaveConfig().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
		})

		It("should CLUSTER SLAVES", func() {
			nodesList, err := client.ClusterSlaves(cluster.nodeIds[0]).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(nodesList).Should(ContainElement(ContainSubstring("slave")))
			Expect(nodesList).Should(HaveLen(1))
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

		It("returns pool stats", func() {
			Expect(client.PoolStats()).To(BeAssignableToTypeOf(&redis.PoolStats{}))
		})

		It("removes idle connections", func() {
			stats := client.PoolStats()
			Expect(stats.TotalConns).NotTo(BeZero())
			Expect(stats.FreeConns).NotTo(BeZero())

			time.Sleep(2 * time.Second)

			stats = client.PoolStats()
			Expect(stats.TotalConns).To(BeZero())
			Expect(stats.FreeConns).To(BeZero())
		})

		It("follows redirects", func() {
			Expect(client.Set("A", "VALUE", 0).Err()).NotTo(HaveOccurred())

			slot := hashtag.Slot("A")
			Expect(client.SwapSlotNodes(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			val, err := client.Get("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("VALUE"))
		})

		It("returns an error when there are no attempts left", func() {
			opt := redisClusterOptions()
			opt.MaxRedirects = -1
			client := cluster.clusterClient(opt)

			slot := hashtag.Slot("A")
			Expect(client.SwapSlotNodes(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			err := client.Get("A").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("MOVED"))

			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("supports Watch", func() {
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

		It("supports pipeline", func() {
			slot := hashtag.Slot("A")
			Expect(client.SwapSlotNodes(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			pipe := client.Pipeline()
			defer pipe.Close()

			keys := []string{"A", "B", "C", "D", "E", "F", "G"}

			for i, key := range keys {
				pipe.Set(key, key+"_value", 0)
				pipe.Expire(key, time.Duration(i+1)*time.Hour)
			}
			cmds, err := pipe.Exec()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(14))

			for _, key := range keys {
				pipe.Get(key)
				pipe.TTL(key)
			}
			cmds, err = pipe.Exec()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(14))
			Expect(cmds[0].(*redis.StringCmd).Val()).To(Equal("A_value"))
			Expect(cmds[1].(*redis.DurationCmd).Val()).To(BeNumerically("~", 1*time.Hour, time.Second))
			Expect(cmds[6].(*redis.StringCmd).Val()).To(Equal("D_value"))
			Expect(cmds[7].(*redis.DurationCmd).Val()).To(BeNumerically("~", 4*time.Hour, time.Second))
			Expect(cmds[12].(*redis.StringCmd).Val()).To(Equal("G_value"))
			Expect(cmds[13].(*redis.DurationCmd).Val()).To(BeNumerically("~", 7*time.Hour, time.Second))
		})

		It("supports pipeline with missing keys", func() {
			Expect(client.Set("A", "A_value", 0).Err()).NotTo(HaveOccurred())
			Expect(client.Set("C", "C_value", 0).Err()).NotTo(HaveOccurred())

			var a, b, c *redis.StringCmd
			cmds, err := client.Pipelined(func(pipe *redis.Pipeline) error {
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

		It("calls fn for every master node", func() {
			for i := 0; i < 10; i++ {
				Expect(client.Set(strconv.Itoa(i), "", 0).Err()).NotTo(HaveOccurred())
			}

			err := client.ForEachMaster(func(master *redis.Client) error {
				return master.FlushDb().Err()
			})
			Expect(err).NotTo(HaveOccurred())

			for _, client := range cluster.masters() {
				keys, err := client.Keys("*").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(keys).To(HaveLen(0))
			}
		})
	}

	Describe("default ClusterClient", func() {
		BeforeEach(func() {
			client = cluster.clusterClient(redisClusterOptions())

			_ = client.ForEachMaster(func(master *redis.Client) error {
				return master.FlushDb().Err()
			})
		})

		AfterEach(func() {
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		describeClusterClient()
	})

	Describe("ClusterClient with RouteByLatency", func() {
		BeforeEach(func() {
			opt := redisClusterOptions()
			opt.RouteByLatency = true
			client = cluster.clusterClient(opt)

			_ = client.ForEachMaster(func(master *redis.Client) error {
				return master.FlushDb().Err()
			})
		})

		AfterEach(func() {
			client.FlushDb()
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		describeClusterClient()
	})

	Describe("ClusterClient without valid nodes", func() {
		BeforeEach(func() {
			client = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{redisAddr},
			})
		})

		It("returns an error", func() {
			err := client.Ping().Err()
			Expect(err).To(MatchError("ERR This instance has cluster support disabled"))
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

	client := cluster.clusterClient(redisClusterOptions())
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
