package redis

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.sharding.rebalanceLocked()
	}
}
