package redis

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/routing"
)

func toleranceTestNode(t *testing.T, latency time.Duration, failing bool) *clusterNode {
	t.Helper()

	n := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
	t.Cleanup(func() { _ = n.Client.Close() })

	n.latency.Store(uint32(latency / time.Microsecond))
	if failing {
		n.MarkAsFailing()
	}

	return n
}

func toleranceTestState(nodes ...*clusterNode) *clusterState {
	return &clusterState{slots: []*clusterSlot{{start: 0, end: 16383, nodes: nodes}}}
}

func TestSlotClosestNodeToleranceZeroKeepsStrictMinimum(t *testing.T) {
	fastest := toleranceTestNode(t, 100*time.Microsecond, false)
	state := toleranceTestState(
		fastest,
		toleranceTestNode(t, 120*time.Microsecond, false),
		toleranceTestNode(t, 900*time.Microsecond, false),
	)

	picker := &routing.RoundRobinPicker{}

	for i := 0; i < 20; i++ {
		got, err := state.slotClosestNode(0, 0, picker)
		if err != nil {
			t.Fatalf("slotClosestNode: %v", err)
		}
		if got != fastest {
			t.Fatalf("iteration %d: expected the fastest node with zero tolerance", i)
		}
	}
}

func TestSlotClosestNodeToleranceSpreadsAcrossCloseNodes(t *testing.T) {
	a := toleranceTestNode(t, 100*time.Microsecond, false)
	b := toleranceTestNode(t, 120*time.Microsecond, false)
	far := toleranceTestNode(t, 900*time.Microsecond, false)
	state := toleranceTestState(a, b, far)

	counts := map[*clusterNode]int{}
	picker := &routing.RoundRobinPicker{}

	for i := 0; i < 100; i++ {
		got, err := state.slotClosestNode(0, 50*time.Microsecond, picker)
		if err != nil {
			t.Fatalf("slotClosestNode: %v", err)
		}
		counts[got]++
	}

	if counts[far] != 0 {
		t.Fatalf("node outside the tolerance received %d requests", counts[far])
	}
	if counts[a] == 0 || counts[b] == 0 {
		t.Fatalf("expected both close nodes to be used, got a=%d b=%d", counts[a], counts[b])
	}
	if diff := counts[a] - counts[b]; diff > 1 || diff < -1 {
		t.Fatalf("expected an even split across close nodes, got a=%d b=%d", counts[a], counts[b])
	}
}

func TestSlotClosestNodeToleranceSkipsFailingNodes(t *testing.T) {
	healthy := toleranceTestNode(t, 120*time.Microsecond, false)
	state := toleranceTestState(toleranceTestNode(t, 100*time.Microsecond, true), healthy)

	picker := &routing.RoundRobinPicker{}

	for i := 0; i < 10; i++ {
		got, err := state.slotClosestNode(0, 50*time.Microsecond, picker)
		if err != nil {
			t.Fatalf("slotClosestNode: %v", err)
		}
		if got != healthy {
			t.Fatal("a failing node was selected")
		}
	}
}

// The fastest node being unhealthy must not stop a slower healthy one from being chosen.
// Previously the healthy candidate was only recorded while scanning for a new minimum, so
// a healthy-but-slower node could be skipped and selection fell through to the
// all-nodes-failing path.
func TestSlotClosestNodePrefersSlowerHealthyNode(t *testing.T) {
	healthy := toleranceTestNode(t, 800*time.Microsecond, false)
	state := toleranceTestState(toleranceTestNode(t, 100*time.Microsecond, true), healthy)

	got, err := state.slotClosestNode(0, 0, nil)
	if err != nil {
		t.Fatalf("slotClosestNode: %v", err)
	}
	if got != healthy {
		t.Fatal("expected the slower healthy node rather than the failing fastest one")
	}
}

func TestSlotClosestNodeToleranceNilPicker(t *testing.T) {
	a := toleranceTestNode(t, 100*time.Microsecond, false)
	state := toleranceTestState(a, toleranceTestNode(t, 120*time.Microsecond, false))

	got, err := state.slotClosestNode(0, 50*time.Microsecond, nil)
	if err != nil {
		t.Fatalf("slotClosestNode: %v", err)
	}
	if got != a {
		t.Fatal("expected the first candidate when no picker is configured")
	}
}
