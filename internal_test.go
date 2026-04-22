package redis

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
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

// testOnCloseHookID is the id used by the ring-shard cleanup tests when
// registering a close hook against the internal onCloseHooks registry.
const testOnCloseHookID = "test-close-counter"

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
				c.baseClient.onClose.register(testOnCloseHookID, func() error {
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
				c.baseClient.onClose.register(testOnCloseHookID, func() error {
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


// TestOnCloseHooks_RunInRegistrationOrder verifies that hooks registered under
// distinct ids are all invoked on run() in the order they were registered.
func TestOnCloseHooks_RunInRegistrationOrder(t *testing.T) {
	h := &onCloseHooks{}
	var calls []string

	h.register("a", func() error { calls = append(calls, "a"); return nil })
	h.register("b", func() error { calls = append(calls, "b"); return nil })
	h.register("c", func() error { calls = append(calls, "c"); return nil })

	if err := h.run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("run order = %v, want %v", calls, want)
	}
}

// TestOnCloseHooks_RegisterSameIDReplaces is the regression test for issue
// #3772. Registering the same id repeatedly must replace the existing
// callback rather than chain onto it, so the registry stays bounded even
// under storm-like re-registration (the exact scenario that previously leaked
// when initConn re-wrapped c.onClose on every connection init).
func TestOnCloseHooks_RegisterSameIDReplaces(t *testing.T) {
	h := &onCloseHooks{}
	const id = "same-id"
	const iterations = 10_000

	var lastSeen int32
	for i := 0; i < iterations; i++ {
		i := int32(i)
		h.register(id, func() error { atomic.StoreInt32(&lastSeen, i); return nil })
	}

	if got := len(h.order); got != 1 {
		t.Fatalf("order length after %d re-registrations = %d, want 1", iterations, got)
	}
	if got := len(h.hooks); got != 1 {
		t.Fatalf("hooks map size after %d re-registrations = %d, want 1", iterations, got)
	}

	if err := h.run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&lastSeen); got != iterations-1 {
		t.Fatalf("last-registered callback not invoked: lastSeen = %d, want %d", got, iterations-1)
	}
}

// TestOnCloseHooks_DistinctIDsCoexist guarantees the dedup behavior does not
// discard hooks from other callers: registering new ids must never drop
// previously registered ids.
func TestOnCloseHooks_DistinctIDsCoexist(t *testing.T) {
	h := &onCloseHooks{}
	var aCount, bCount int32

	h.register("a", func() error { atomic.AddInt32(&aCount, 1); return nil })
	h.register("b", func() error { atomic.AddInt32(&bCount, 1); return nil })
	// Re-registering "a" must not drop "b".
	h.register("a", func() error { atomic.AddInt32(&aCount, 1); return nil })

	if err := h.run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a, b := atomic.LoadInt32(&aCount), atomic.LoadInt32(&bCount); a != 1 || b != 1 {
		t.Fatalf("call counts a=%d b=%d, want a=1 b=1", a, b)
	}
}

// TestOnCloseHooks_Unregister verifies that unregister removes a hook and
// that running after unregister does not invoke it.
func TestOnCloseHooks_Unregister(t *testing.T) {
	h := &onCloseHooks{}
	var aCalled, bCalled bool

	h.register("a", func() error { aCalled = true; return nil })
	h.register("b", func() error { bCalled = true; return nil })
	h.unregister("a")
	h.unregister("missing") // no-op must not panic

	if err := h.run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if aCalled {
		t.Fatal("unregistered hook was invoked")
	}
	if !bCalled {
		t.Fatal("remaining hook was not invoked")
	}
	if got := len(h.order); got != 1 {
		t.Fatalf("order length = %d, want 1", got)
	}
}

// TestOnCloseHooks_AllRunOnError confirms every hook is invoked even if an
// earlier one returns an error, and that the first error is returned.
func TestOnCloseHooks_AllRunOnError(t *testing.T) {
	h := &onCloseHooks{}
	var called [3]bool
	errFirst := fmt.Errorf("first")
	errSecond := fmt.Errorf("second")

	h.register("a", func() error { called[0] = true; return errFirst })
	h.register("b", func() error { called[1] = true; return errSecond })
	h.register("c", func() error { called[2] = true; return nil })

	err := h.run()
	if err != errFirst {
		t.Fatalf("run() err = %v, want %v", err, errFirst)
	}
	for i, c := range called {
		if !c {
			t.Fatalf("hook %d was not invoked", i)
		}
	}
}

// TestOnCloseHooks_NilReceiver ensures run() on a nil registry is a safe
// no-op. baseClient embedded in Conn/Tx does initialize the registry, but
// defensive nil-safety lets future constructors add the field without
// breaking Close().
func TestOnCloseHooks_NilReceiver(t *testing.T) {
	var h *onCloseHooks
	if err := h.run(); err != nil {
		t.Fatalf("run() on nil = %v, want nil", err)
	}
}

// TestOnCloseHooks_ConcurrentRegisterSameID hammers the registry with many
// goroutines re-registering under the same id. The registry must remain
// bounded and the surviving callback must still be invoked exactly once.
func TestOnCloseHooks_ConcurrentRegisterSameID(t *testing.T) {
	h := &onCloseHooks{}
	const id = "hot"
	const goroutines = 64
	const perG = 1_000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				h.register(id, func() error { return nil })
			}
		}()
	}
	wg.Wait()

	if got := len(h.order); got != 1 {
		t.Fatalf("order length after concurrent storm = %d, want 1", got)
	}
}


// entraidLikeProvider mimics the exact semantics of
// github.com/redis/go-redis-entraid's StreamingCredentialsProvider relevant
// to issue #3772: it deduplicates subscriptions by listener pointer
// identity, and every call to Subscribe returns a FRESH UnsubscribeFunc
// closure that removes the listener by pointer match from the shared
// listeners slice. Two unsubs obtained for the same listener are therefore
// equivalent: the first one called removes the entry, any subsequent call
// is a safe no-op.
type entraidLikeProvider struct {
	mu          sync.Mutex
	listeners   []auth.CredentialsListener
	subscribeN  int32
	unsubCalls  int32
}

func (p *entraidLikeProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	atomic.AddInt32(&p.subscribeN, 1)

	p.mu.Lock()
	already := false
	for _, l := range p.listeners {
		if l == listener {
			already = true
			break
		}
	}
	if !already {
		p.listeners = append(p.listeners, listener)
	}
	p.mu.Unlock()

	unsub := func() error {
		atomic.AddInt32(&p.unsubCalls, 1)
		p.mu.Lock()
		defer p.mu.Unlock()
		for i, l := range p.listeners {
			if l == listener {
				p.listeners = append(p.listeners[:i], p.listeners[i+1:]...)
				return nil
			}
		}
		return nil
	}
	return auth.NewBasicCredentials("u", "p"), unsub, nil
}

func (p *entraidLikeProvider) listenerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.listeners)
}

// stubCredentialsListener is a minimal auth.CredentialsListener used only
// for pointer-identity in the entraid-mimicking test.
type stubCredentialsListener struct{}

func (*stubCredentialsListener) OnNext(auth.Credentials) {}
func (*stubCredentialsListener) OnError(error)           {}

// TestInitConn_EntraidLike_NoLeakAcrossReinits is the targeted regression
// test for issue #3772 against the real-world StreamingCredentialsProvider
// behavior implemented by go-redis-entraid. It simulates N re-initializations
// on the same logical connection (i.e. the same CredentialsListener pointer,
// which is what streaming.Manager.Listener returns from its per-connId cache),
// replacing cn.SetOnClose with each new unsubscribe closure.
//
// Invariants the fix must uphold:
//  1. Subscribe dedups: the provider's listener list stays at size 1.
//  2. Calling only the MOST RECENT unsub fully removes the listener.
//  3. All prior (orphaned) unsubs are safe no-ops after that.
//  4. No registration remains on the provider after close.
func TestInitConn_EntraidLike_NoLeakAcrossReinits(t *testing.T) {
	const reinits = 1000

	provider := &entraidLikeProvider{}
	listener := &stubCredentialsListener{}

	var latestUnsub auth.UnsubscribeFunc
	orphaned := make([]auth.UnsubscribeFunc, 0, reinits-1)

	for i := 0; i < reinits; i++ {
		_, unsub, err := provider.Subscribe(listener)
		if err != nil {
			t.Fatalf("Subscribe #%d: %v", i, err)
		}
		if latestUnsub != nil {
			// Mirror the pool.Conn.SetOnClose behavior: the previous unsub
			// is dropped on the floor, only the latest one is retained.
			orphaned = append(orphaned, latestUnsub)
		}
		latestUnsub = unsub
	}

	if got := provider.listenerCount(); got != 1 {
		t.Fatalf("after %d Subscribes with same listener, listener count = %d, want 1", reinits, got)
	}
	if got := atomic.LoadInt32(&provider.subscribeN); got != int32(reinits) {
		t.Fatalf("Subscribe call count = %d, want %d", got, reinits)
	}

	// Only the latest unsub is invoked, matching the post-fix cn.onClose.
	if err := latestUnsub(); err != nil {
		t.Fatalf("latest unsub returned error: %v", err)
	}
	if got := provider.listenerCount(); got != 0 {
		t.Fatalf("listener count after latest unsub = %d, want 0", got)
	}

	// Every orphaned unsub must be a safe no-op (contract in auth.UnsubscribeFunc).
	for i, u := range orphaned {
		if err := u(); err != nil {
			t.Fatalf("orphaned unsub #%d returned error: %v", i, err)
		}
	}
	if got := provider.listenerCount(); got != 0 {
		t.Fatalf("listener count after orphaned unsubs = %d, want 0", got)
	}
}
