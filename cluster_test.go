package redis_test

import (
	"math/rand"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Cluster", func() {
	var scenario = struct {
		ports     []string
		nodeIDs   []string
		processes map[string]*redisProcess
		clients   map[string]*redis.Client
		master    *redis.Client
	}{
		ports:     []string{"8220", "8221", "8222", "8223", "8224", "8225"},
		nodeIDs:   make([]string, 6),
		processes: make(map[string]*redisProcess, 6),
		clients:   make(map[string]*redis.Client, 6),
	}

	BeforeSuite(func() {
		// Start processes, connect individual clients
		for pos, port := range scenario.ports {
			process, err := startRedis(port, "--cluster-enabled", "yes")
			Expect(err).NotTo(HaveOccurred())

			client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})
			info, err := client.ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())

			scenario.processes[port] = process
			scenario.clients[port] = client
			scenario.nodeIDs[pos] = info[:40]
		}
		scenario.master = scenario.clients[scenario.ports[0]]

		// Meet cluster nodes
		for _, port := range scenario.ports {
			client := scenario.clients[port]
			err := client.ClusterMeet("127.0.0.1", scenario.ports[0]).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		// Bootstrap masters
		slots := []int{0, 5000, 10000, redis.HashSlots}
		for pos, port := range scenario.ports[:3] {
			client := scenario.clients[port]
			err := client.ClusterAddSlotsRange(slots[pos], slots[pos+1]-1).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		// Bootstrap slaves
		for pos, port := range scenario.ports[3:] {
			client := scenario.clients[port]
			masterID := scenario.nodeIDs[pos]

			Eventually(func() string { // Wait for masters
				return client.ClusterNodes().Val()
			}, "10s").Should(ContainSubstring(masterID))

			err := client.ClusterReplicate(masterID).Err()
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string { // Wait for slaves
				return scenario.master.ClusterNodes().Val()
			}, "10s").Should(ContainSubstring("slave " + masterID))
		}
	})

	AfterSuite(func() {
		for _, client := range scenario.clients {
			client.Close()
		}
		for _, process := range scenario.processes {
			process.Close()
		}
	})

	Describe("HashSlot", func() {

		It("should calculate hash slots", func() {
			rand.Seed(100)

			Expect(redis.HashSlot("123456789")).To(Equal(12739))
			Expect(redis.HashSlot("{}foo")).To(Equal(9500))
			Expect(redis.HashSlot("foo{}")).To(Equal(5542))
			Expect(redis.HashSlot("foo{}{bar}")).To(Equal(8363))
			Expect(redis.HashSlot("")).To(Equal(10503))
			Expect(redis.HashSlot("")).To(Equal(5176))
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
			res, err := scenario.master.ClusterSlots().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))
			Expect(res).To(ConsistOf([]redis.ClusterSlotInfo{
				{0, 4999, []string{"127.0.0.1:8220", "127.0.0.1:8223"}},
				{5000, 9999, []string{"127.0.0.1:8221", "127.0.0.1:8224"}},
				{10000, 16383, []string{"127.0.0.1:8222", "127.0.0.1:8225"}},
			}))
		})

		It("should CLUSTER NODES", func() {
			res, err := scenario.master.ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(BeNumerically(">", 400))
		})

	})
})
