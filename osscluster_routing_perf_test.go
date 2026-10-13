package redis

import (
	"context"
	"reflect"
	"testing"
)

func newRoutingPerfTestClient(t testing.TB) *ClusterClient {
	t.Helper()
	slots := []ClusterSlot{
		{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: "127.0.0.1:1"}}},
		{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: "127.0.0.1:2"}}},
	}
	c := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) { return slots, nil },
	})
	if _, err := c.state.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

// The simple-first-key fast path must build exactly the decision the full
// path builds, for every snapshot command it applies to.
func TestRoutingSimpleFirstKeyMatchesFullPath(t *testing.T) {
	c := newRoutingPerfTestClient(t)
	defer c.Close()
	ctx := context.Background()
	view := defaultCommandMetadataView()
	shapes := [][]interface{}{
		{},
		{"k"},
		{"{a}k", "v"},
		{"k", "v", "w"},
		{"k", 1, 2, 3},
		{7},
		{[]byte("bk"), "v"},
		{"", "v"},
		{nil},
		{struct{}{}},
		{"k1", "k2", "k3", "k4"},
	}
	checked := 0
	for name, meta := range view.routingTable {
		if !meta.simpleFirstKey {
			continue
		}
		slow := *meta
		slow.simpleFirstKey = false
		for _, shape := range shapes {
			args := append([]interface{}{name}, shape...)
			cmd := NewCmd(ctx, args...)
			fast := c.routingDecisionWithMeta(cmd, view, nil, false, meta, true)
			full := c.routingDecisionWithMeta(cmd, view, nil, false, &slow, true)
			full.meta = meta
			if !reflect.DeepEqual(fast, full) {
				t.Fatalf("%v: fast %+v, full %+v", args, fast, full)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no command uses the fast path")
	}
	for _, name := range []string{"get", "set", "hget", "mget", "del", "incr"} {
		if meta := view.routingTable[name]; meta == nil || !meta.simpleFirstKey {
			t.Fatalf("%s should use the fast path", name)
		}
	}
	for _, name := range []string{"migrate", "xread", "eval", "json.debug"} {
		if meta := view.routingTable[name]; meta != nil && meta.simpleFirstKey {
			t.Fatalf("%s must not use the fast path", name)
		}
	}
}

// Allocation guards: routing a plain keyed command and AutoPipeline admission
// must not allocate beyond the command itself.
func TestClusterRoutingAllocs(t *testing.T) {
	if raceEnabledForRoutingPerf {
		t.Skip("allocation counts differ under -race")
	}
	c := newRoutingPerfTestClient(t)
	defer c.Close()
	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "key")

	if n := testing.AllocsPerRun(1000, func() {
		d := c.commandRoutingDecision(ctx, cmd)
		slot := c.cmdSlotWithDecision(cmd, &d, -1)
		if _, err := c.cmdNodeWithDecision(ctx, slot, &d); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("single command routing allocates %v times", n)
	}

	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{Unordered: true, MaxConcurrentBatches: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	// Warm the decision pool.
	for i := 0; i < 10; i++ {
		_ = ap.preflight(ctx, cmd)
		cmd.setClusterRoutingDecision(nil)
	}
	if n := testing.AllocsPerRun(1000, func() {
		if ap.mustDivert(ctx, cmd) {
			t.Fatal("diverted")
		}
		if err := ap.preflight(ctx, cmd); err != nil {
			t.Fatal(err)
		}
		_ = ap.shardFn(cmd)
		cmd.setClusterRoutingDecision(nil)
	}); n != 0 {
		t.Fatalf("AutoPipeline admission allocates %v times", n)
	}

	cmds := make([]Cmder, 100)
	for i := range cmds {
		cmds[i] = NewStringCmd(ctx, "get", "key"+string(rune('a'+i%26)))
	}
	if n := testing.AllocsPerRun(100, func() {
		if err := c.mapCmdsByNode(ctx, newCmdsMap(), cmds); err != nil {
			t.Fatal(err)
		}
	}); n > 20 { // cmdsMap growth only; base (12281823) also makes ~22
		t.Fatalf("mapping 100 commands allocates %v times", n)
	}
}
