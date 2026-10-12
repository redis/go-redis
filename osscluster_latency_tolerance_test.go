package redis

import (
	"math"
	"testing"
	"time"
)

func toleranceTestNode(t *testing.T, latency time.Duration, failing bool) *clusterNode {
	t.Helper()

	n := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
	t.Cleanup(func() { _ = n.Client.Close() })

	// Steady state: the node has answered a PING at least once, so Loading() takes its
	// cached path instead of dialling a dead address for 100ms per candidate.
	n.loaded.Store(1)
	n.latency.Store(uint32(latency / time.Microsecond))
	if failing {
		n.MarkAsFailing()
	}

	return n
}

func toleranceTestState(nodes ...*clusterNode) *clusterState {
	return &clusterState{slots: []*clusterSlot{{start: 0, end: 16383, nodes: nodes}}}
}

func TestRouteByLatencyToleranceZeroKeepsStrictMinimum(t *testing.T) {
	fastest := toleranceTestNode(t, 100*time.Microsecond, false)
	state := toleranceTestState(
		fastest,
		toleranceTestNode(t, 120*time.Microsecond, false),
		toleranceTestNode(t, 900*time.Microsecond, false),
	)

	for i := 0; i < 20; i++ {
		got, err := state.slotClosestNode(0)
		if err != nil {
			t.Fatalf("slotClosestNode: %v", err)
		}
		if got != fastest {
			t.Fatalf("iteration %d: expected the fastest node with zero tolerance", i)
		}
	}
}

func TestSlotNodeWithinLatencySpreadsAcrossCloseNodes(t *testing.T) {
	a := toleranceTestNode(t, 100*time.Microsecond, false)
	b := toleranceTestNode(t, 120*time.Microsecond, false)
	far := toleranceTestNode(t, 900*time.Microsecond, false)
	state := toleranceTestState(a, b, far)

	counts := map[*clusterNode]int{}

	for i := 0; i < 100; i++ {
		got, err := state.slotNodeWithinLatency(0, 50*time.Microsecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
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

func TestSlotNodeWithinLatencySkipsFailingNodes(t *testing.T) {
	healthy := toleranceTestNode(t, 120*time.Microsecond, false)
	state := toleranceTestState(toleranceTestNode(t, 100*time.Microsecond, true), healthy)

	for i := 0; i < 10; i++ {
		got, err := state.slotNodeWithinLatency(0, 50*time.Microsecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
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
// Exercises slotNodeWithinLatency rather than slotClosestNode: preferring a slower healthy
// node over a faster failing one is behaviour of the new path. slotClosestNode is left as it
// was on master, latent bug included - see the PR discussion.
func TestSlotNodeWithinLatencyPrefersSlowerHealthyNode(t *testing.T) {
	healthy := toleranceTestNode(t, 800*time.Microsecond, false)
	state := toleranceTestState(toleranceTestNode(t, 100*time.Microsecond, true), healthy)

	got, err := state.slotNodeWithinLatency(0, 0)
	if err != nil {
		t.Fatalf("slotNodeWithinLatency: %v", err)
	}
	if got != healthy {
		t.Fatal("expected the slower healthy node rather than the failing fastest one")
	}
}

func TestSlotNodeWithinLatencyNeverReturnsNilWithHealthyNode(t *testing.T) {
	nodes := []*clusterNode{
		toleranceTestNode(t, 100*time.Microsecond, false),
		toleranceTestNode(t, 120*time.Microsecond, false),
		toleranceTestNode(t, 900*time.Microsecond, false),
	}
	state := toleranceTestState(nodes...)

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
		got, err := state.slotNodeWithinLatency(0, 50*time.Microsecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		if got == nil {
			t.Fatal("returned nil despite healthy nodes being available")
		}
	}
}

func TestSlotNodeWithinLatencyHandlesHugeTolerance(t *testing.T) {
	a := toleranceTestNode(t, 100*time.Microsecond, false)
	state := toleranceTestState(a, toleranceTestNode(t, 900*time.Microsecond, false))

	// A tolerance near the maximum must not overflow into a negative comparison.
	got, err := state.slotNodeWithinLatency(0, time.Duration(math.MaxInt64))
	if err != nil {
		t.Fatalf("slotNodeWithinLatency: %v", err)
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

// With the option unset, selection must stay on the pre-existing hot path: no candidate
// slice, no allocation per command.
func TestSlotClosestNodeStillDoesNotAllocate(t *testing.T) {
	state := toleranceTestState(
		toleranceTestNode(t, 100*time.Microsecond, false),
		toleranceTestNode(t, 120*time.Microsecond, false),
		toleranceTestNode(t, 900*time.Microsecond, false),
	)

	allocs := testing.AllocsPerRun(200, func() {
		if _, err := state.slotClosestNode(0); err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
	})

	if allocs != 0 {
		t.Fatalf("zero tolerance allocated %.0f times per call, want 0", allocs)
	}
}

// Rotation state is per slot. A counter shared across slots is advanced by the other slots
// between two visits to this one, so with a regular interleaving each slot keeps landing on
// the same candidate index and one replica per shard takes all of that shard's reads.
func TestSlotNodeWithinLatencyRotatesPerSlot(t *testing.T) {
	nodesFor := func() []*clusterNode {
		return []*clusterNode{
			toleranceTestNode(t, 100*time.Microsecond, false),
			toleranceTestNode(t, 120*time.Microsecond, false),
		}
	}
	a, b := nodesFor(), nodesFor()
	state := &clusterState{slots: []*clusterSlot{
		{start: 0, end: 100, nodes: a},
		{start: 101, end: 200, nodes: b},
	}}

	counts := map[*clusterNode]int{}
	for i := 0; i < 100; i++ {
		for _, slot := range []int{50, 150} { // alternate slots, the interleaving that breaks a shared counter
			got, err := state.slotNodeWithinLatency(slot, 50*time.Microsecond)
			if err != nil {
				t.Fatalf("slotNodeWithinLatency: %v", err)
			}
			counts[got]++
		}
	}

	for name, nodes := range map[string][]*clusterNode{"slot 0-100": a, "slot 101-200": b} {
		if counts[nodes[0]] == 0 || counts[nodes[1]] == 0 {
			t.Fatalf("%s did not rotate: %d / %d", name, counts[nodes[0]], counts[nodes[1]])
		}
	}
}

// The rotation cursor is a uint32 and wraps. Reducing it to an index must stay in the unsigned
// domain: on 32-bit builds int is 32 bits, so converting a cursor past 2^31 before the modulo
// yields a negative index and panics. Run with GOARCH=386 to exercise that.
func TestSlotNodeWithinLatencyRotationPastIntMax(t *testing.T) {
	first := toleranceTestNode(t, 100*time.Microsecond, false)
	second := toleranceTestNode(t, 110*time.Microsecond, false)
	state := toleranceTestState(first, second)
	state.slots[0].latencyBandNodeCursor.Store(math.MaxInt32)

	seen := map[*clusterNode]int{}
	for i := 0; i < 8; i++ {
		got, err := state.slotNodeWithinLatency(0, 50*time.Microsecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		if got != first && got != second {
			t.Fatalf("unexpected node %p", got)
		}
		seen[got]++
	}

	if len(seen) != 2 {
		t.Fatalf("rotation stalled across the uint32 boundary: %v", seen)
	}
}

// A node that has not yet been confirmed loaded must still be selectable: Loading() only
// reports true for an actual LOADING reply, and a connection error is not one. Guards
// against the tolerance filter over-excluding and shrinking the rotation to nothing.
func TestSlotNodeWithinLatencyKeepsUnconfirmedNode(t *testing.T) {
	fast := toleranceTestNode(t, 100*time.Microsecond, false)
	unconfirmed := toleranceTestNode(t, 110*time.Microsecond, false)
	unconfirmed.loaded.Store(0)

	state := toleranceTestState(fast, unconfirmed)

	seen := map[*clusterNode]bool{}
	for i := 0; i < 8; i++ {
		got, err := state.slotNodeWithinLatency(0, 50*time.Microsecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		if got == nil {
			t.Fatal("slotNodeWithinLatency returned nil")
		}
		seen[got] = true
	}

	if !seen[unconfirmed] {
		t.Fatal("node with loaded=0 was excluded from the rotation; a connection error is not a LOADING reply")
	}
}

// Before the background probes finish, every node still carries the math.MaxUint32 sentinel
// from newClusterNodeWithNodeAddress. All differences are then zero, so a positive tolerance
// would admit the entire slot and round-robin startup reads across distant zones. Until one
// measurement lands, the tolerance path must behave exactly like the strict one.
func TestSlotNodeWithinLatencyKeepsStrictOrderBeforeFirstProbe(t *testing.T) {
	unprobed := func() *clusterNode {
		n := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
		t.Cleanup(func() { _ = n.Client.Close() })
		n.loaded.Store(1)
		n.latency.Store(unmeasuredNodeLatencyMicros) // as newClusterNodeWithNodeAddress does
		return n
	}

	first, second, third := unprobed(), unprobed(), unprobed()
	state := toleranceTestState(first, second, third)

	for i := 0; i < 50; i++ {
		got, err := state.slotNodeWithinLatency(0, time.Second)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		if got != first {
			t.Fatalf("iteration %d: rotated across unmeasured nodes; want the strict pick", i)
		}
	}

	strict, err := state.slotClosestNode(0)
	if err != nil {
		t.Fatalf("slotClosestNode: %v", err)
	}
	if strict != first {
		t.Fatalf("slotClosestNode disagrees with the tolerance path before the first probe")
	}
}

// Once any healthy node has a real measurement the band is meaningful again, and an
// unmeasured node must stay out of it - its sentinel latency is ~4295s, far beyond any
// sane tolerance.
func TestSlotNodeWithinLatencyExcludesUnmeasuredOnceProbed(t *testing.T) {
	measured := toleranceTestNode(t, 100*time.Microsecond, false)
	alsoMeasured := toleranceTestNode(t, 130*time.Microsecond, false)

	unprobed := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
	t.Cleanup(func() { _ = unprobed.Client.Close() })
	unprobed.loaded.Store(1)
	unprobed.latency.Store(unmeasuredNodeLatencyMicros)

	state := toleranceTestState(measured, alsoMeasured, unprobed)

	for i := 0; i < 60; i++ {
		got, err := state.slotNodeWithinLatency(0, 50*time.Microsecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		if got == unprobed {
			t.Fatalf("iteration %d: unmeasured node entered the tolerance band", i)
		}
	}
}

// updateLatency publishes the latency and its timestamp as two separate stores, so a probe can
// land between selection sampling a node's latency and reading its measurement timestamp. That
// interleaving is observable as "sentinel latency, timestamp set" - and must still count as
// unmeasured, or the band widens to the whole slot while every sampled latency is a sentinel.
func TestSlotNodeWithinLatencyIgnoresTimestampWithoutLatency(t *testing.T) {
	interleaved := func() *clusterNode {
		n := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
		t.Cleanup(func() { _ = n.Client.Close() })
		n.loaded.Store(1)
		n.latency.Store(unmeasuredNodeLatencyMicros)
		// The half-published state: timestamp visible, latency not yet stored.
		n.SetLastLatencyMeasurement(time.Now())
		return n
	}

	first, second, third := interleaved(), interleaved(), interleaved()
	state := toleranceTestState(first, second, third)

	for i := 0; i < 50; i++ {
		got, err := state.slotNodeWithinLatency(0, time.Second)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		if got != first {
			t.Fatalf("iteration %d: a set timestamp widened the band while latencies were sentinels", i)
		}
	}
}

// maximumNodeLatency (all pings failed) must not be mistaken for the unmeasured sentinel: such a
// node has a real measurement, so the band applies normally rather than collapsing to strict.
func TestSlotNodeWithinLatencyTreatsFailedPingsAsMeasured(t *testing.T) {
	a := toleranceTestNode(t, maximumNodeLatency, false)
	b := toleranceTestNode(t, maximumNodeLatency, false)
	state := toleranceTestState(a, b)

	counts := map[*clusterNode]int{}
	for i := 0; i < 80; i++ {
		got, err := state.slotNodeWithinLatency(0, time.Millisecond)
		if err != nil {
			t.Fatalf("slotNodeWithinLatency: %v", err)
		}
		counts[got]++
	}
	if counts[a] == 0 || counts[b] == 0 {
		t.Fatalf("expected rotation across both measured nodes, got a=%d b=%d", counts[a], counts[b])
	}
}
