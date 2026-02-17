package redis

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var ctx = context.TODO()

var _ = Describe("newClusterState", func() {
	var state *clusterState

	createClusterState := func(slots []ClusterSlot) *clusterState {
		opt := &ClusterOptions{}
		opt.init()
		nodes := newClusterNodes(opt)
		state, err := newClusterState(nodes, slots, "10.10.10.10:1234")
		Expect(err).NotTo(HaveOccurred())
		return state
	}

	Describe("sorting", func() {
		BeforeEach(func() {
			state = createClusterState([]ClusterSlot{{
				Start: 1000,
				End:   1999,
			}, {
				Start: 0,
				End:   999,
			}, {
				Start: 2000,
				End:   2999,
			}})
		})

		It("sorts slots", func() {
			Expect(state.slots).To(Equal([]*clusterSlot{
				{start: 0, end: 999, nodes: nil},
				{start: 1000, end: 1999, nodes: nil},
				{start: 2000, end: 2999, nodes: nil},
			}))
		})
	})

	Describe("loopback", func() {
		BeforeEach(func() {
			state = createClusterState([]ClusterSlot{{
				Nodes: []ClusterNode{{Addr: "127.0.0.1:7001"}},
			}, {
				Nodes: []ClusterNode{{Addr: "127.0.0.1:7002"}},
			}, {
				Nodes: []ClusterNode{{Addr: "1.2.3.4:1234"}},
			}, {
				Nodes: []ClusterNode{{Addr: ":1234"}},
			}})
		})

		It("replaces loopback hosts in addresses", func() {
			slotAddr := func(slot *clusterSlot) string {
				return slot.nodes[0].Client.Options().Addr
			}

			Expect(slotAddr(state.slots[0])).To(Equal("10.10.10.10:7001"))
			Expect(slotAddr(state.slots[1])).To(Equal("10.10.10.10:7002"))
			Expect(slotAddr(state.slots[2])).To(Equal("1.2.3.4:1234"))
			Expect(slotAddr(state.slots[3])).To(Equal(":1234"))
		})
	})

	Describe("NodeAddress", func() {
		BeforeEach(func() {
			state = createClusterState([]ClusterSlot{{
				Nodes: []ClusterNode{{Addr: "127.0.0.1:7001"}},
			}, {
				Nodes: []ClusterNode{{Addr: "127.0.0.1:7002"}},
			}, {
				Nodes: []ClusterNode{{Addr: "1.2.3.4:1234"}},
			}, {
				Nodes: []ClusterNode{{Addr: ":1234"}},
			}})
		})

		It("preserves node address from ClusterSlots before loopback replacement", func() {
			slotNodeAddress := func(slot *clusterSlot) string {
				return slot.nodes[0].Client.NodeAddress()
			}

			// Node addresses should be preserved even when Addr is transformed
			Expect(slotNodeAddress(state.slots[0])).To(Equal("127.0.0.1:7001"))
			Expect(slotNodeAddress(state.slots[1])).To(Equal("127.0.0.1:7002"))
			Expect(slotNodeAddress(state.slots[2])).To(Equal("1.2.3.4:1234"))
			Expect(slotNodeAddress(state.slots[3])).To(Equal(":1234"))
		})

		It("has different Addr and NodeAddress when loopback is replaced", func() {
			// For loopback addresses, Addr should be transformed but NodeAddress preserved
			slot0 := state.slots[0]
			Expect(slot0.nodes[0].Client.Options().Addr).To(Equal("10.10.10.10:7001"))
			Expect(slot0.nodes[0].Client.NodeAddress()).To(Equal("127.0.0.1:7001"))

			// For non-loopback addresses, Addr and NodeAddress should be the same
			slot2 := state.slots[2]
			Expect(slot2.nodes[0].Client.Options().Addr).To(Equal("1.2.3.4:1234"))
			Expect(slot2.nodes[0].Client.NodeAddress()).To(Equal("1.2.3.4:1234"))
		})
	})

	Describe("NodeAddress with FQDN", func() {
		BeforeEach(func() {
			// Simulate FQDN endpoints that might be resolved/transformed
			state = createClusterState([]ClusterSlot{{
				Nodes: []ClusterNode{{Addr: "redis-master.example.com:6379"}},
			}, {
				Nodes: []ClusterNode{{Addr: "redis-replica-1.example.com:6379"}},
			}, {
				Nodes: []ClusterNode{{Addr: "redis-replica-2.example.com:6379"}},
			}})
		})

		It("preserves FQDN node addresses", func() {
			slotNodeAddress := func(slot *clusterSlot) string {
				return slot.nodes[0].Client.NodeAddress()
			}

			Expect(slotNodeAddress(state.slots[0])).To(Equal("redis-master.example.com:6379"))
			Expect(slotNodeAddress(state.slots[1])).To(Equal("redis-replica-1.example.com:6379"))
			Expect(slotNodeAddress(state.slots[2])).To(Equal("redis-replica-2.example.com:6379"))
		})
	})

	Describe("NodeAddress with multiple nodes per slot", func() {
		BeforeEach(func() {
			state = createClusterState([]ClusterSlot{{
				Start: 0,
				End:   5460,
				Nodes: []ClusterNode{
					{Addr: "127.0.0.1:7001"}, // master
					{Addr: "127.0.0.1:7004"}, // replica
				},
			}, {
				Start: 5461,
				End:   10922,
				Nodes: []ClusterNode{
					{Addr: "master-2.redis.local:6379"},  // master
					{Addr: "replica-2.redis.local:6379"}, // replica
				},
			}})
		})

		It("preserves node addresses for all nodes in a slot", func() {
			// First slot - loopback addresses
			slot0 := state.slots[0]
			Expect(slot0.nodes[0].Client.NodeAddress()).To(Equal("127.0.0.1:7001"))
			Expect(slot0.nodes[1].Client.NodeAddress()).To(Equal("127.0.0.1:7004"))

			// Verify Addr is transformed for loopback
			Expect(slot0.nodes[0].Client.Options().Addr).To(Equal("10.10.10.10:7001"))
			Expect(slot0.nodes[1].Client.Options().Addr).To(Equal("10.10.10.10:7004"))

			// Second slot - FQDN addresses (no transformation)
			slot1 := state.slots[1]
			Expect(slot1.nodes[0].Client.NodeAddress()).To(Equal("master-2.redis.local:6379"))
			Expect(slot1.nodes[1].Client.NodeAddress()).To(Equal("replica-2.redis.local:6379"))

			// Verify Addr is same as NodeAddress for non-loopback
			Expect(slot1.nodes[0].Client.Options().Addr).To(Equal("master-2.redis.local:6379"))
			Expect(slot1.nodes[1].Client.Options().Addr).To(Equal("replica-2.redis.local:6379"))
		})
	})
})

// TestNodeAddress tests that NodeAddress is correctly preserved
// when cluster nodes are created from ClusterSlots responses.
func TestNodeAddress(t *testing.T) {
	t.Run("preserves node address when loopback is replaced", func(t *testing.T) {
		opt := &ClusterOptions{}
		opt.init()
		nodes := newClusterNodes(opt)
		defer nodes.Close()

		// Create cluster state with loopback addresses
		// Origin is non-loopback, so loopback addresses will be replaced
		slots := []ClusterSlot{{
			Start: 0,
			End:   5460,
			Nodes: []ClusterNode{{Addr: "127.0.0.1:7001"}},
		}, {
			Start: 5461,
			End:   10922,
			Nodes: []ClusterNode{{Addr: "127.0.0.1:7002"}},
		}}

		state, err := newClusterState(nodes, slots, "10.10.10.10:1234")
		if err != nil {
			t.Fatalf("newClusterState failed: %v", err)
		}

		// Verify Addr is transformed (loopback replaced with origin host)
		if got := state.slots[0].nodes[0].Client.Options().Addr; got != "10.10.10.10:7001" {
			t.Errorf("Addr = %q, want %q", got, "10.10.10.10:7001")
		}
		if got := state.slots[1].nodes[0].Client.Options().Addr; got != "10.10.10.10:7002" {
			t.Errorf("Addr = %q, want %q", got, "10.10.10.10:7002")
		}

		// Verify NodeAddress is preserved (original from ClusterSlots)
		if got := state.slots[0].nodes[0].Client.NodeAddress(); got != "127.0.0.1:7001" {
			t.Errorf("NodeAddress = %q, want %q", got, "127.0.0.1:7001")
		}
		if got := state.slots[1].nodes[0].Client.NodeAddress(); got != "127.0.0.1:7002" {
			t.Errorf("NodeAddress = %q, want %q", got, "127.0.0.1:7002")
		}
	})

	t.Run("preserves FQDN node addresses", func(t *testing.T) {
		opt := &ClusterOptions{}
		opt.init()
		nodes := newClusterNodes(opt)
		defer nodes.Close()

		slots := []ClusterSlot{{
			Start: 0,
			End:   5460,
			Nodes: []ClusterNode{{Addr: "redis-master.example.com:6379"}},
		}, {
			Start: 5461,
			End:   10922,
			Nodes: []ClusterNode{{Addr: "redis-replica.example.com:6379"}},
		}}

		state, err := newClusterState(nodes, slots, "10.10.10.10:1234")
		if err != nil {
			t.Fatalf("newClusterState failed: %v", err)
		}

		// For non-loopback addresses, Addr and NodeAddress should be the same
		if got := state.slots[0].nodes[0].Client.Options().Addr; got != "redis-master.example.com:6379" {
			t.Errorf("Addr = %q, want %q", got, "redis-master.example.com:6379")
		}
		if got := state.slots[0].nodes[0].Client.NodeAddress(); got != "redis-master.example.com:6379" {
			t.Errorf("NodeAddress = %q, want %q", got, "redis-master.example.com:6379")
		}

		if got := state.slots[1].nodes[0].Client.Options().Addr; got != "redis-replica.example.com:6379" {
			t.Errorf("Addr = %q, want %q", got, "redis-replica.example.com:6379")
		}
		if got := state.slots[1].nodes[0].Client.NodeAddress(); got != "redis-replica.example.com:6379" {
			t.Errorf("NodeAddress = %q, want %q", got, "redis-replica.example.com:6379")
		}
	})

	t.Run("preserves node address for multiple nodes per slot", func(t *testing.T) {
		opt := &ClusterOptions{}
		opt.init()
		nodes := newClusterNodes(opt)
		defer nodes.Close()

		slots := []ClusterSlot{{
			Start: 0,
			End:   5460,
			Nodes: []ClusterNode{
				{Addr: "127.0.0.1:7001"}, // master
				{Addr: "127.0.0.1:7004"}, // replica
			},
		}}

		state, err := newClusterState(nodes, slots, "10.10.10.10:1234")
		if err != nil {
			t.Fatalf("newClusterState failed: %v", err)
		}

		// Master node
		if got := state.slots[0].nodes[0].Client.Options().Addr; got != "10.10.10.10:7001" {
			t.Errorf("Master Addr = %q, want %q", got, "10.10.10.10:7001")
		}
		if got := state.slots[0].nodes[0].Client.NodeAddress(); got != "127.0.0.1:7001" {
			t.Errorf("Master NodeAddress = %q, want %q", got, "127.0.0.1:7001")
		}

		// Replica node
		if got := state.slots[0].nodes[1].Client.Options().Addr; got != "10.10.10.10:7004" {
			t.Errorf("Replica Addr = %q, want %q", got, "10.10.10.10:7004")
		}
		if got := state.slots[0].nodes[1].Client.NodeAddress(); got != "127.0.0.1:7004" {
			t.Errorf("Replica NodeAddress = %q, want %q", got, "127.0.0.1:7004")
		}
	})

	t.Run("GetOrCreate without node address defaults to Addr", func(t *testing.T) {
		opt := &ClusterOptions{}
		opt.init()
		nodes := newClusterNodes(opt)
		defer nodes.Close()

		// GetOrCreate without node address (e.g., from MOVED/ASK error)
		node, err := nodes.GetOrCreate("10.10.10.10:7001")
		if err != nil {
			t.Fatalf("GetOrCreate failed: %v", err)
		}

		// NodeAddress should default to Addr when not provided
		if got := node.Client.NodeAddress(); got != "10.10.10.10:7001" {
			t.Errorf("NodeAddress = %q, want %q", got, "10.10.10.10:7001")
		}

		// Addr should be set correctly
		if got := node.Client.Options().Addr; got != "10.10.10.10:7001" {
			t.Errorf("Addr = %q, want %q", got, "10.10.10.10:7001")
		}
	})

	t.Run("GetOrCreateWithNodeAddress sets node address", func(t *testing.T) {
		opt := &ClusterOptions{}
		opt.init()
		nodes := newClusterNodes(opt)
		defer nodes.Close()

		// GetOrCreateWithNodeAddress with node address
		node, err := nodes.GetOrCreateWithNodeAddress("10.10.10.10:7001", "127.0.0.1:7001")
		if err != nil {
			t.Fatalf("GetOrCreateWithNodeAddress failed: %v", err)
		}

		// NodeAddress should be set
		if got := node.Client.NodeAddress(); got != "127.0.0.1:7001" {
			t.Errorf("NodeAddress = %q, want %q", got, "127.0.0.1:7001")
		}

		// Addr should be the transformed address
		if got := node.Client.Options().Addr; got != "10.10.10.10:7001" {
			t.Errorf("Addr = %q, want %q", got, "10.10.10.10:7001")
		}
	})
}

type fixedHash string

func (h fixedHash) Get(string) string {
	return string(h)
}

func TestRingSetAddrsAndRebalanceRace(t *testing.T) {
	const (
		ringShard1Name = "ringShardOne"
		ringShard2Name = "ringShardTwo"

		ringShard1Port = "6390"
		ringShard2Port = "6391"
	)

	ring := NewRing(&RingOptions{
		Addrs: map[string]string{
			ringShard1Name: ":" + ringShard1Port,
		},
		// Disable heartbeat
		HeartbeatFrequency: 1 * time.Hour,
		NewConsistentHash: func(shards []string) ConsistentHash {
			switch len(shards) {
			case 1:
				return fixedHash(ringShard1Name)
			case 2:
				return fixedHash(ringShard2Name)
			default:
				t.Fatalf("Unexpected number of shards: %v", shards)
				return nil
			}
		},
	})
	defer ring.Close()

	// Continuously update addresses by adding and removing one address
	updatesDone := make(chan struct{})
	defer func() { close(updatesDone) }()
	go func() {
		for i := 0; ; i++ {
			select {
			case <-updatesDone:
				return
			default:
				if i%2 == 0 {
					ring.SetAddrs(map[string]string{
						ringShard1Name: ":" + ringShard1Port,
					})
				} else {
					ring.SetAddrs(map[string]string{
						ringShard1Name: ":" + ringShard1Port,
						ringShard2Name: ":" + ringShard2Port,
					})
				}
			}
		}
	}()

	timer := time.NewTimer(1 * time.Second)
	for running := true; running; {
		select {
		case <-timer.C:
			running = false
		default:
			shard, err := ring.sharding.GetByKey("whatever")
			if err == nil && shard == nil {
				t.Fatal("shard is nil")
			}
		}
	}
}

func BenchmarkRingShardingRebalanceLocked(b *testing.B) {
	opts := &RingOptions{
		Addrs: make(map[string]string),
		// Disable heartbeat
		HeartbeatFrequency: 1 * time.Hour,
	}
	for i := 0; i < 100; i++ {
		opts.Addrs[fmt.Sprintf("shard%d", i)] = fmt.Sprintf(":63%02d", i)
	}

	ring := NewRing(opts)
	defer ring.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.sharding.rebalanceLocked()
	}
}

type testCounter struct {
	mu sync.Mutex
	t  *testing.T
	m  map[string]int
}

func newTestCounter(t *testing.T) *testCounter {
	return &testCounter{t: t, m: make(map[string]int)}
}

func (ct *testCounter) increment(key string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.m[key]++
}

func (ct *testCounter) expect(values map[string]int) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.t.Helper()
	if !reflect.DeepEqual(values, ct.m) {
		ct.t.Errorf("expected %v != actual %v", values, ct.m)
	}
}

func TestRingShardsCleanup(t *testing.T) {
	const (
		ringShard1Name = "ringShardOne"
		ringShard2Name = "ringShardTwo"

		ringShard1Addr = "shard1.test"
		ringShard2Addr = "shard2.test"
	)

	t.Run("closes unused shards", func(t *testing.T) {
		closeCounter := newTestCounter(t)

		ring := NewRing(&RingOptions{
			Addrs: map[string]string{
				ringShard1Name: ringShard1Addr,
				ringShard2Name: ringShard2Addr,
			},
			NewClient: func(opt *Options) *Client {
				c := NewClient(opt)
				c.baseClient.onClose = c.baseClient.wrappedOnClose(func() error {
					closeCounter.increment(opt.Addr)
					return nil
				})
				return c
			},
		})
		closeCounter.expect(map[string]int{})

		// no change due to the same addresses
		ring.SetAddrs(map[string]string{
			ringShard1Name: ringShard1Addr,
			ringShard2Name: ringShard2Addr,
		})
		closeCounter.expect(map[string]int{})

		ring.SetAddrs(map[string]string{
			ringShard1Name: ringShard1Addr,
		})
		closeCounter.expect(map[string]int{ringShard2Addr: 1})

		ring.SetAddrs(map[string]string{
			ringShard2Name: ringShard2Addr,
		})
		closeCounter.expect(map[string]int{ringShard1Addr: 1, ringShard2Addr: 1})

		ring.Close()
		closeCounter.expect(map[string]int{ringShard1Addr: 1, ringShard2Addr: 2})
	})

	t.Run("closes created shards if ring was closed", func(t *testing.T) {
		createCounter := newTestCounter(t)
		closeCounter := newTestCounter(t)

		var (
			ring        *Ring
			shouldClose int32
		)

		ring = NewRing(&RingOptions{
			Addrs: map[string]string{
				ringShard1Name: ringShard1Addr,
			},
			NewClient: func(opt *Options) *Client {
				if atomic.LoadInt32(&shouldClose) != 0 {
					ring.Close()
				}
				createCounter.increment(opt.Addr)
				c := NewClient(opt)
				c.baseClient.onClose = c.baseClient.wrappedOnClose(func() error {
					closeCounter.increment(opt.Addr)
					return nil
				})
				return c
			},
		})
		createCounter.expect(map[string]int{ringShard1Addr: 1})
		closeCounter.expect(map[string]int{})

		atomic.StoreInt32(&shouldClose, 1)

		ring.SetAddrs(map[string]string{
			ringShard2Name: ringShard2Addr,
		})
		createCounter.expect(map[string]int{ringShard1Addr: 1, ringShard2Addr: 1})
		closeCounter.expect(map[string]int{ringShard1Addr: 1, ringShard2Addr: 1})
	})
}

//------------------------------------------------------------------------------

type timeoutErr struct {
	error
}

func (e timeoutErr) Timeout() bool {
	return true
}

func (e timeoutErr) Temporary() bool {
	return true
}

func (e timeoutErr) Error() string {
	return "i/o timeout"
}

var _ = Describe("withConn", func() {
	var client *Client

	BeforeEach(func() {
		client = NewClient(&Options{
			PoolSize: 1,
		})
	})

	AfterEach(func() {
		client.Close()
	})

	It("should replace the connection in the pool when there is no error", func() {
		var conn *pool.Conn

		client.withConn(ctx, func(ctx context.Context, c *pool.Conn) error {
			conn = c
			return nil
		})

		newConn, err := client.connPool.Get(ctx)
		Expect(err).To(BeNil())
		Expect(newConn).To(Equal(conn))
	})

	It("should replace the connection in the pool when there is an error not related to a bad connection", func() {
		var conn *pool.Conn

		client.withConn(ctx, func(ctx context.Context, c *pool.Conn) error {
			conn = c
			return proto.RedisError("LOADING")
		})

		newConn, err := client.connPool.Get(ctx)
		Expect(err).To(BeNil())
		Expect(newConn).To(Equal(conn))
	})

	It("should remove the connection from the pool when it times out", func() {
		var conn *pool.Conn

		client.withConn(ctx, func(ctx context.Context, c *pool.Conn) error {
			conn = c
			return timeoutErr{}
		})

		newConn, err := client.connPool.Get(ctx)
		Expect(err).To(BeNil())
		Expect(newConn).NotTo(Equal(conn))
		Expect(client.connPool.Len()).To(Equal(1))
	})
})

var _ = Describe("ClusterClient", func() {
	var client *ClusterClient

	BeforeEach(func() {
		client = &ClusterClient{}
	})

	Describe("cmdSlot", func() {
		It("select slot from args for GETKEYSINSLOT command", func() {
			cmd := NewStringSliceCmd(ctx, "cluster", "getkeysinslot", 100, 200)

			slot := client.cmdSlot(cmd, -1)
			Expect(slot).To(Equal(100))
		})

		It("select slot from args for COUNTKEYSINSLOT command", func() {
			cmd := NewStringSliceCmd(ctx, "cluster", "countkeysinslot", 100)

			slot := client.cmdSlot(cmd, -1)
			Expect(slot).To(Equal(100))
		})

		It("follows preferred random slot", func() {
			cmd := NewStatusCmd(ctx, "ping")

			slot := client.cmdSlot(cmd, 101)
			Expect(slot).To(Equal(101))
		})
	})
})

var _ = Describe("isLoopback", func() {
	DescribeTable("should correctly identify loopback addresses",
		func(host string, expected bool) {
			result := isLoopback(host)
			Expect(result).To(Equal(expected))
		},
		// IP addresses
		Entry("IPv4 loopback", "127.0.0.1", true),
		Entry("IPv6 loopback", "::1", true),
		Entry("IPv4 non-loopback", "192.168.1.1", false),
		Entry("IPv6 non-loopback", "2001:db8::1", false),

		// Well-known loopback hostnames
		Entry("localhost lowercase", "localhost", true),
		Entry("localhost uppercase", "LOCALHOST", true),
		Entry("localhost mixed case", "LocalHost", true),

		// Docker-specific loopbacks
		Entry("host.docker.internal", "host.docker.internal", true),
		Entry("HOST.DOCKER.INTERNAL", "HOST.DOCKER.INTERNAL", true),
		Entry("custom.docker.internal", "custom.docker.internal", true),
		Entry("app.docker.internal", "app.docker.internal", true),

		// Non-loopback hostnames
		Entry("redis hostname", "redis-cluster", false),
		Entry("FQDN", "redis.example.com", false),
		Entry("docker but not internal", "redis.docker.com", false),

		// Edge cases
		Entry("empty string", "", false),
		Entry("invalid IP", "256.256.256.256", false),
		Entry("partial docker internal", "docker.internal", false),
	)
})
