package redis_test

import (
	"math/rand"
	"net"

	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
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
		opt = &redis.ClusterOptions{}
	}
	opt.Addrs = addrs
	return redis.NewClusterClient(opt)
}

func startCluster(scenario *clusterScenario) error {
	// Start processes, connect individual clients
	for pos, port := range scenario.ports {
		process, err := startRedis(port, "--cluster-enabled", "yes")
		if err != nil {
			return err
		}

		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})
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
	for pos, client := range scenario.masters() {
		err := client.ClusterAddSlotsRange(slots[pos], slots[pos+1]-1).Err()
		if err != nil {
			return err
		}
	}

	// Bootstrap slaves
	for pos, client := range scenario.slaves() {
		masterId := scenario.nodeIds[pos]

		// Wait for masters
		err := waitForSubstring(func() string {
			return client.ClusterNodes().Val()
		}, masterId, 10*time.Second)
		if err != nil {
			return err
		}

		err = client.ClusterReplicate(masterId).Err()
		if err != nil {
			return err
		}

		// Wait for slaves
		err = waitForSubstring(func() string {
			return scenario.primary().ClusterNodes().Val()
		}, "slave "+masterId, 10*time.Second)
		if err != nil {
			return err
		}
	}

	// Wait for cluster state to turn OK
	for _, client := range scenario.clients {
		err := waitForSubstring(func() string {
			return client.ClusterInfo().Val()
		}, "cluster_state:ok", 10*time.Second)
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
			rand.Seed(100)

			for _, test := range tests {
				Expect(redis.HashSlot(test.key)).To(Equal(test.slot), "for %s", test.key)
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
				Expect(redis.HashSlot(test.one)).To(Equal(redis.HashSlot(test.two)), "for %s <-> %s", test.one, test.two)
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

		It("should follow redirects", func() {
			Expect(client.Set("A", "VALUE", 0).Err()).NotTo(HaveOccurred())

			slot := redis.HashSlot("A")
			Expect(client.SwapSlot(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			val, err := client.Get("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("VALUE"))

			Eventually(func() []string {
				return client.SlotAddrs(slot)
			}, "5s").Should(Equal([]string{"127.0.0.1:8221", "127.0.0.1:8224"}))
		})

		It("should perform multi-pipelines", func() {
			slot := redis.HashSlot("A")
			Expect(client.SlotAddrs(slot)).To(Equal([]string{"127.0.0.1:8221", "127.0.0.1:8224"}))
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

		It("should return error when there are no attempts left", func() {
			client = cluster.clusterClient(&redis.ClusterOptions{
				MaxRedirects: -1,
			})

			slot := redis.HashSlot("A")
			Expect(client.SwapSlot(slot)).To(Equal([]string{"127.0.0.1:8224", "127.0.0.1:8221"}))

			err := client.Get("A").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("MOVED"))
		})
	})
})

//------------------------------------------------------------------------------

func BenchmarkRedisClusterPing(b *testing.B) {
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
