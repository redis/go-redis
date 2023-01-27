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
})

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
				c.baseClient.onClose = func() error {
					closeCounter.increment(opt.Addr)
					return nil
				}
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
				c.baseClient.onClose = func() error {
					closeCounter.increment(opt.Addr)
					return nil
				}
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
