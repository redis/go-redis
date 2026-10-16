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
		if _, err := ap.routeCommand(ctx, cmd); err != nil {
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
		if _, err := ap.routeCommand(ctx, other); err != nil {
			t.Fatal(err)
		}
		if got := c.cmdSlotWithDecision(other, other.clusterRoutingDecision(), -1); got != clusterKeySlot(key+"x") {
			t.Fatalf("%q: second command slot %d", key, got)
		}
		other.setClusterRoutingDecision(nil)
	}

	// A command with a resolver policy keeps its own decision.
	ftCmd := NewCmd(ctx, "ft.search", "idx", "*")
	if _, err := ap.routeCommand(ctx, ftCmd); err == nil {
		if pin := ftCmd.clusterRoutingDecision(); pin == nil || pin.template {
			t.Fatalf("ft.search pin %+v, want an own decision", pin)
		}
	}
	ftCmd.setClusterRoutingDecision(nil)

	// Metadata-derived policies keep a pooled decision.
	c.SetCommandInfoResolver(NewCommandMetadataResolver())
	cmd := NewStringCmd(ctx, "get", "k")
	if _, err := ap.routeCommand(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if pin := cmd.clusterRoutingDecision(); pin == nil || pin.template || !pin.pooled {
		t.Fatalf("metadata resolver pin %+v, want a pooled decision", pin)
	}
	cmd.setClusterRoutingDecision(nil)
}

// sameRoutingDecision compares decisions, with errors compared by text.
func sameRoutingDecision(a, b clusterRoutingDecision) bool {
	if (a.policyErr == nil) != (b.policyErr == nil) ||
		a.policyErr != nil && a.policyErr.Error() != b.policyErr.Error() {
		return false
	}
	a.policyErr, b.policyErr = nil, nil
	return a == b
}

// fastRoutingTemplate must agree with the full resolver path for every
// snapshot command and argument shape: same template and slot when it
// applies, and the same value decision through commandRoutingDecision.
func TestFastRoutingTemplateMatchesFullPath(t *testing.T) {
	c := newRoutingPerfTestClient(t)
	defer c.Close()
	ctx := context.Background()
	view := c.metadataView()
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
		{"sub", "k"},
		{"info", "k"},
	}
	names := []string{"GET", "Get", "ft.search", "json.get", "unknown.cmd", "nosuchcmd"}
	for name := range view.records {
		names = append(names, name)
	}
	hits := 0
	for _, name := range names {
		for _, shape := range shapes {
			args := append([]interface{}{name}, shape...)
			cmd := NewCmd(ctx, args...)
			full := c.resolveCommandRoutingDecision(ctx, cmd)
			template, slot := c.fastRoutingTemplate(cmd)
			if template != nil {
				hits++
				if pinned := pinRoutingTemplate(NewCmd(ctx, args...), &full); pinned != template || full.naturalSlot != slot {
					t.Fatalf("%v: fast (%p, %d), full %+v", args, template, slot, full)
				}
			}
			if got := c.commandRoutingDecision(ctx, cmd); !sameRoutingDecision(got, full) {
				t.Fatalf("%v: commandRoutingDecision %+v, full %+v", args, got, full)
			}
		}
	}
	if hits == 0 {
		t.Fatal("fast path never applied")
	}
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
	}
	for i := 0; i < 300; i++ { // runs of same and different slot ranges
		cmds = append(cmds, NewStringCmd(ctx, "get", "k"+string(rune('a'+i%7))+string(rune('a'+i/7%26))))
	}
	route := c.resolvePipelineRouting(ctx, cmds)
	if !route.allTemplates {
		t.Fatal("plain pipeline should be all templates")
	}
	route.clear(cmds)
	// A subcommand keeps its parent's readonly flag, so it differs from its
	// leaf template and takes the full path.
	mixed := append(append([]Cmder(nil), cmds...), NewCmd(ctx, "object", "encoding", "d"))
	route = c.resolvePipelineRouting(ctx, mixed)
	if route.allTemplates {
		t.Fatal("OBJECT ENCODING should not pin a template")
	}
	route.clear(mixed)
	for round := 0; round < 3; round++ { // later rounds reuse the commands
		if round == 2 {
			cmds = mixed
		}
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
