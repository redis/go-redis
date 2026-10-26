package redis

import (
	"context"
	"fmt"
	"testing"
)

// The second GET reuses the memoized template: same decision, the right slot cached
// on the command, and no per-command decision stored.
func TestClusterFastRouteReusesTemplate(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	first := NewStringCmd(ctx, "get", "{a}key")
	tmpl := c.autoPipelineRoutingDecision(ctx, first)
	if tmpl == nil || !tmpl.template {
		t.Fatalf("first GET: decision = %+v, want a shared template", tmpl)
	}
	if c.fastRoute.Load() == nil {
		t.Fatal("template was not memoized")
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%d", i)
		cmd := NewStringCmd(ctx, "get", key)
		got := c.autoPipelineRoutingDecision(ctx, cmd)
		if got != tmpl {
			t.Fatalf("GET %s: decision %p, want the memoized template %p", key, got, tmpl)
		}
		slot, ok := cmd.cachedSlot()
		if !ok || slot != clusterKeySlot(key) {
			t.Fatalf("GET %s: cached slot = %d (%v), want %d", key, slot, ok, clusterKeySlot(key))
		}
		if cmd.clusterRoutingDecision() != nil {
			t.Fatalf("GET %s: a fast-route hit stored a per-command decision", key)
		}
	}
}

// The memo is used only with the default resolver and routing policies on.
func TestClusterFastRouteEligibility(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name  string
		setup func(c *ClusterClient)
	}{
		{"metadata resolver", func(c *ClusterClient) { c.SetCommandInfoResolver(NewCommandMetadataResolver()) }},
		{"routing policies disabled", func(c *ClusterClient) { c.opt.DisableRoutingPolicies = true }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
			t.Cleanup(func() { _ = c.Close() })
			tt.setup(c)
			for i := 0; i < 3; i++ {
				c.autoPipelineRoutingDecision(ctx, NewStringCmd(ctx, "get", fmt.Sprintf("k%d", i)))
			}
			if c.fastRouteEligible() || c.fastRoute.Load() != nil {
				t.Fatal("fast route used outside the default resolver")
			}
		})
	}
}

// A command whose name has subcommands is never memoized: its decision depends on
// more than the name.
func TestClusterFastRouteSkipsSubcommandParents(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	c.autoPipelineRoutingDecision(ctx, NewIntCmd(ctx, "object", "freq", "key"))
	if memo := c.fastRoute.Load(); memo != nil {
		if mp := memo.m.Load(); mp != nil {
			if _, ok := (*mp)["object"]; ok {
				t.Fatal("OBJECT (a subcommand parent) was memoized")
			}
		}
	}
}

// A new metadata view starts a new memo, so a template of the old view is not used.
func TestClusterFastRouteNewViewStartsNewMemo(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	c.autoPipelineRoutingDecision(ctx, NewStringCmd(ctx, "get", "a"))
	memo := c.fastRoute.Load()
	if memo == nil {
		t.Fatal("no memo")
	}
	stale := &fastRouteMemo{view: &commandMetadataView{}}
	stale.m.Store(memo.m.Load())
	c.fastRoute.Store(stale)
	cmd := NewStringCmd(ctx, "get", "b")
	if got := c.fastRouteLookup(cmd); got != nil {
		t.Fatal("a memo of another metadata view was used")
	}
}
