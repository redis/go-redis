package redis

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func newSlotTableTestPipeliner(t *testing.T) (*ClusterClient, *AutoPipeliner) {
	t.Helper()
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = ap.Close() })
	if ap.clusterFD == nil {
		t.Fatal("cluster full duplex did not engage")
	}
	return cc, ap
}

// After traffic, every slot entry points at the FD child of the slot's master.
func TestClusterFDSlotTableMapsSlotsToOwners(t *testing.T) {
	cc, ap := newSlotTableTestPipeliner(t)
	ctx := context.Background()
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("slottable:%d", i)
		if err := ap.Set(ctx, keys[i], i, time.Minute).Err(); err != nil {
			t.Fatalf("SET %s: %v", keys[i], err)
		}
	}
	r := ap.clusterFD
	table := r.slots.Load()
	if table == nil {
		t.Fatal("no slot table after traffic")
	}
	for _, k := range keys {
		owner, err := cc.MasterForKey(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		r.mu.RLock()
		want := r.children[owner]
		r.mu.RUnlock()
		if got := table.child[clusterKeySlot(k)].Load(); got != want {
			t.Fatalf("slot of %s: child %p, want the owner's child %p", k, got, want)
		}
	}
}

// A closed child is never returned: the command still succeeds and the entry is
// replaced by an open child.
func TestClusterFDSlotTableSkipsClosedChild(t *testing.T) {
	_, ap := newSlotTableTestPipeliner(t)
	ctx := context.Background()
	const key = "slottable:closed"
	if err := ap.Set(ctx, key, "v1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	slot := clusterKeySlot(key)
	old := ap.clusterFD.slots.Load().child[slot].Load()
	if old == nil {
		t.Fatal("no slot entry")
	}
	_ = old.Close()
	if v, err := ap.Get(ctx, key).Result(); err != nil || v != "v1" {
		t.Fatalf("GET after the child closed: %q, %v", v, err)
	}
	now := ap.clusterFD.slots.Load().child[slot].Load()
	if now == nil || now == old || now.IsClosed() {
		t.Fatalf("slot entry after close: %p (closed=%v), old %p", now, now != nil && now.IsClosed(), old)
	}
}

// A new cluster state makes the table stale; the next command builds a new one.
func TestClusterFDSlotTableFollowsNewState(t *testing.T) {
	cc, ap := newSlotTableTestPipeliner(t)
	ctx := context.Background()
	if err := ap.Set(ctx, "slottable:state", "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	before := ap.clusterFD.slots.Load()
	if before == nil {
		t.Fatal("no slot table")
	}
	cc.ReloadState(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := cc.state.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st != before.state {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cluster state did not reload")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := ap.Get(ctx, "slottable:state").Err(); err != nil {
		t.Fatal(err)
	}
	after := ap.clusterFD.slots.Load()
	if after == nil || after == before || after.state == before.state {
		t.Fatal("the slot table was not rebuilt for the new cluster state")
	}
}

// A slot with no owner in the cluster state is not stored: slotMasterNode picks a
// master for it on every call (skipping failing or loading nodes), which a fixed
// table entry would bypass. A covered slot is stored as usual.
func TestClusterFDSlotTableSkipsUncoveredSlot(t *testing.T) {
	addr := clusterFDTestAddrs[0]
	probe := NewClient(&Options{Addr: addr})
	defer probe.Close()
	if err := probe.Ping(context.Background()).Err(); err != nil {
		t.Skipf("local cluster not reachable at %s: %v", addr, err)
	}
	// Slots 8192-16383 have no owner in this topology.
	cc := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
	})
	t.Cleanup(func() { _ = cc.Close() })
	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ap.Close() })
	r := ap.clusterFD
	if r == nil {
		t.Fatal("cluster full duplex did not engage")
	}
	ctx := context.Background()
	var covered, uncovered string
	for i := 0; covered == "" || uncovered == ""; i++ {
		k := fmt.Sprintf("slottable:uncovered:%d", i)
		if clusterKeySlot(k) < 8192 {
			covered = k
		} else {
			uncovered = k
		}
	}
	for _, k := range []string{covered, uncovered} {
		if ch, closed := r.childFor(ctx, NewStringCmd(ctx, "get", k)); ch == nil || closed {
			t.Fatalf("%s: no FD child (closed=%v)", k, closed)
		}
	}
	table := r.slots.Load()
	if table == nil || table.child[clusterKeySlot(covered)].Load() == nil {
		t.Fatal("the covered slot was not stored")
	}
	if table.child[clusterKeySlot(uncovered)].Load() != nil {
		t.Fatal("the uncovered slot was stored with a fixed fallback master")
	}
}
