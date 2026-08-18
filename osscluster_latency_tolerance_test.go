package redis

import (
	"math"
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

// The candidate set is built from latencies captured in the same pass that computes the
// minimum. Re-reading Latency() would let the background probe empty the set between the
// two reads, and slotClosestNode would then return (nil, nil) for callers to dereference.
func TestSlotClosestNodeToleranceNeverReturnsNilWithHealthyNode(t *testing.T) {
	nodes := []*clusterNode{
		toleranceTestNode(t, 100*time.Microsecond, false),
		toleranceTestNode(t, 120*time.Microsecond, false),
		toleranceTestNode(t, 900*time.Microsecond, false),
	}
	state := toleranceTestState(nodes...)
	picker := &routing.RoundRobinPicker{}

	stop := make(chan struct{})
	defer close(stop)

	// Churn the latencies while selection runs, including pushing the fastest node above
	// the band it established.
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			nodes[i%len(nodes)].latency.Store(uint32(100 + (i*37)%5000))
		}
	}()

	for i := 0; i < 5000; i++ {
		got, err := state.slotClosestNode(0, 50*time.Microsecond, picker)
		if err != nil {
			t.Fatalf("slotClosestNode: %v", err)
		}
		if got == nil {
			t.Fatal("returned nil despite healthy nodes being available")
		}
	}
}

func TestSlotClosestNodeToleranceHandlesHugeTolerance(t *testing.T) {
	a := toleranceTestNode(t, 100*time.Microsecond, false)
	state := toleranceTestState(a, toleranceTestNode(t, 900*time.Microsecond, false))

	// A tolerance near the maximum must not overflow into a negative comparison.
	got, err := state.slotClosestNode(0, time.Duration(math.MaxInt64), &routing.RoundRobinPicker{})
	if err != nil {
		t.Fatalf("slotClosestNode: %v", err)
	}
	if got == nil {
		t.Fatal("returned nil for a very large tolerance")
	}
}

func TestRouteByLatencyToleranceFromClusterURL(t *testing.T) {
	o, err := ParseClusterURL("redis://localhost:6379?route_by_latency=true&route_by_latency_tolerance=250us")
	if err != nil {
		t.Fatalf("ParseClusterURL: %v", err)
	}
	if !o.RouteByLatency {
		t.Fatal("RouteByLatency not parsed")
	}
	if o.RouteByLatencyTolerance != 250*time.Microsecond {
		t.Fatalf("tolerance = %v, want 250us", o.RouteByLatencyTolerance)
	}
}

func TestRouteByLatencyToleranceFromFailoverURL(t *testing.T) {
	o, err := ParseFailoverURL("redis://localhost:26379?master_name=mymaster&route_by_latency=true&route_by_latency_tolerance=1ms")
	if err != nil {
		t.Fatalf("ParseFailoverURL: %v", err)
	}
	if o.RouteByLatencyTolerance != time.Millisecond {
		t.Fatalf("tolerance = %v, want 1ms", o.RouteByLatencyTolerance)
	}
}

func TestRouteByLatencyToleranceFromUniversalOptions(t *testing.T) {
	o := &UniversalOptions{
		Addrs:                   []string{"localhost:6379"},
		RouteByLatency:          true,
		RouteByLatencyTolerance: 300 * time.Microsecond,
	}

	if got := o.Cluster().RouteByLatencyTolerance; got != 300*time.Microsecond {
		t.Fatalf("Cluster() tolerance = %v, want 300us", got)
	}

	o.MasterName = "mymaster"
	if got := o.Failover().RouteByLatencyTolerance; got != 300*time.Microsecond {
		t.Fatalf("Failover() tolerance = %v, want 300us", got)
	}
}
