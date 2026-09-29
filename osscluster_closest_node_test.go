package redis

import (
	"testing"
	"time"
)

func closestNodeTestNode(t *testing.T, latency time.Duration, failing bool) *clusterNode {
	t.Helper()

	n := &clusterNode{Client: NewClient(&Options{Addr: "127.0.0.1:0"})}
	t.Cleanup(func() { _ = n.Client.Close() })

	n.loaded.Store(1)
	n.latency.Store(uint32(latency / time.Microsecond))
	if failing {
		n.MarkAsFailing()
	}

	return n
}

// A failing node with the lowest latency used to hide every healthy node behind it: the
// healthy check sat inside the "new minimum" branch, so a healthy node that was not also
// the outright fastest never became closestNonFailingNode. The slot then fell through to
// the all-failing path and served the read from the failing node.
//
// This is not a rare shape - a refused connection fails fast, so a failing node often has
// the lowest measured latency in its slot.
func TestSlotClosestNodePrefersHealthyBehindFasterFailingNode(t *testing.T) {
	failingFast := closestNodeTestNode(t, 10*time.Millisecond, true)
	healthySlow := closestNodeTestNode(t, 20*time.Millisecond, false)

	state := &clusterState{slots: []*clusterSlot{{start: 0, end: 16383, nodes: []*clusterNode{failingFast, healthySlow}}}}

	got, err := state.slotClosestNode(0)
	if err != nil {
		t.Fatalf("slotClosestNode: %v", err)
	}
	if got == failingFast {
		t.Fatal("returned the failing node while a healthy one was available")
	}
	if got != healthySlow {
		t.Fatalf("expected the healthy node, got %v", got)
	}
}

// The healthy node must be the fastest healthy one, not merely the last seen.
func TestSlotClosestNodePicksFastestHealthyNode(t *testing.T) {
	failingFast := closestNodeTestNode(t, 1*time.Millisecond, true)
	healthyFast := closestNodeTestNode(t, 5*time.Millisecond, false)
	healthySlow := closestNodeTestNode(t, 50*time.Millisecond, false)

	// healthySlow last, so a "last healthy wins" implementation would return it.
	state := &clusterState{slots: []*clusterSlot{{start: 0, end: 16383, nodes: []*clusterNode{failingFast, healthyFast, healthySlow}}}}

	for i := 0; i < 10; i++ {
		got, err := state.slotClosestNode(0)
		if err != nil {
			t.Fatalf("slotClosestNode: %v", err)
		}
		if got != healthyFast {
			t.Fatalf("iteration %d: expected the fastest healthy node", i)
		}
	}
}

// With every node failing the original behaviour still applies: the least-slow one is
// served rather than failing the read.
func TestSlotClosestNodeAllFailingPicksLeastSlow(t *testing.T) {
	slow := closestNodeTestNode(t, 30*time.Millisecond, true)
	fast := closestNodeTestNode(t, 3*time.Millisecond, true)

	state := &clusterState{slots: []*clusterSlot{{start: 0, end: 16383, nodes: []*clusterNode{slow, fast}}}}

	got, err := state.slotClosestNode(0)
	if err != nil {
		t.Fatalf("slotClosestNode: %v", err)
	}
	if got != fast {
		t.Fatal("expected the least-slow failing node")
	}
}
