package redis

import (
	"context"
	"testing"
)

// AutoPipeline admission pins the shared template for plain keyed commands and
// carries the slot on the command; flush and Process read the same slot.
func TestAutoPipelineAdmissionPinsTemplate(t *testing.T) {
	c := newRoutingPerfTestClient(t)
	defer c.Close()
	ctx := context.Background()
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{Unordered: true, MaxConcurrentBatches: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	for _, key := range []string{"a", "{user}1", "", "zzz"} {
		cmd := NewStringCmd(ctx, "get", key)
		if err := ap.preflight(ctx, cmd); err != nil {
			t.Fatal(err)
		}
		pin := cmd.clusterRoutingDecision()
		if pin == nil || !pin.template || pin.pooled {
			t.Fatalf("%q: pin %+v, want the shared template", key, pin)
		}
		want := clusterKeySlot(key)
		if got := c.cmdSlotWithDecision(cmd, pin, -1); got != want {
			t.Fatalf("%q: slot %d, want %d", key, got, want)
		}
		if got := ap.shardFn(cmd); got != want*ap.numShards()/16384 {
			t.Fatalf("%q: shard %d", key, got)
		}
		// Process takes the pin and must route by the carried slot.
		taken := c.commandRoutingDecision(ctx, cmd)
		if cmd.clusterRoutingDecision() != nil || c.cmdSlotWithDecision(cmd, &taken, -1) != want {
			t.Fatalf("%q: take kept the pin or lost the slot", key)
		}
		// Re-admitting a command after a different key's admission keeps its own slot.
		other := NewStringCmd(ctx, "get", key+"x")
		if err := ap.preflight(ctx, other); err != nil {
			t.Fatal(err)
		}
		if got := c.cmdSlotWithDecision(other, other.clusterRoutingDecision(), -1); got != clusterKeySlot(key+"x") {
			t.Fatalf("%q: second command slot %d", key, got)
		}
		other.setClusterRoutingDecision(nil)
	}

	// A command with a resolver policy keeps its own decision.
	ftCmd := NewCmd(ctx, "ft.search", "idx", "*")
	if err := ap.preflight(ctx, ftCmd); err == nil {
		if pin := ftCmd.clusterRoutingDecision(); pin == nil || pin.template {
			t.Fatalf("ft.search pin %+v, want an own decision", pin)
		}
	}
	ftCmd.setClusterRoutingDecision(nil)

	// Metadata-derived policies keep a pooled decision.
	c.SetCommandInfoResolver(NewCommandMetadataResolver())
	cmd := NewStringCmd(ctx, "get", "k")
	if err := ap.preflight(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if pin := cmd.clusterRoutingDecision(); pin == nil || pin.template || !pin.pooled {
		t.Fatalf("metadata resolver pin %+v, want a pooled decision", pin)
	}
	cmd.setClusterRoutingDecision(nil)
}

// A plain pipeline pins templates; mapping must group by the slots the full
// decision computes, and route.clear must drop every pin.
func TestPipelineMappingWithTemplates(t *testing.T) {
	c := newRoutingPerfTestClient(t)
	defer c.Close()
	ctx := context.Background()
	cmds := []Cmder{
		NewStringCmd(ctx, "get", "a"), NewStatusCmd(ctx, "set", "{b}1", "v"),
		NewIntCmd(ctx, "del", "c1", "c2"), NewStringCmd(ctx, "get", 12345),
		NewCmd(ctx, "object", "encoding", "d"),
	}
	for round := 0; round < 2; round++ { // the second round reuses the commands
		m := newCmdsMap()
		if err := c.mapCmdsByNode(ctx, m, cmds); err != nil {
			t.Fatal(err)
		}
		state, _ := c.state.Get(ctx)
		for _, cmd := range cmds {
			if cmd.clusterRoutingDecision() != nil {
				t.Fatalf("%v: pin kept after mapping", cmd.Args())
			}
			d := c.routingDecisionInView(ctx, cmd, c.metadataView(), nil)
			want, _ := state.slotMasterNode(d.naturalSlot)
			found := false
			for _, mapped := range m.m[want] {
				found = found || mapped == cmd
			}
			if !found {
				t.Fatalf("%v: not mapped to %v", cmd.Args(), want)
			}
		}
	}
}
