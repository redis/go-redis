package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// After the first GET resolves through the full path, later GETs reuse the view's
// template: same decision, the right slot cached on the command, and no
// per-command decision stored by autoPipelineRoutingDecision.
func TestClusterFastRouteReusesTemplate(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	first := NewStringCmd(ctx, "get", "{a}key")
	tmpl := c.autoPipelineRoutingDecision(ctx, first)
	if tmpl == nil || !tmpl.template {
		t.Fatalf("first GET: decision = %+v, want a shared template", tmpl)
	}
	meta := c.metadataView().routingTable["get"]
	if meta == nil || meta.template != tmpl || atomic.LoadUint32(&meta.defaultTemplate) == 0 {
		t.Fatal("the full path did not mark GET's view template as the default resolver's decision")
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key:%d", i)
		cmd := NewStringCmd(ctx, "get", key)
		got := c.autoPipelineRoutingDecision(ctx, cmd)
		if got != tmpl {
			t.Fatalf("GET %s: decision %p, want the view template %p", key, got, tmpl)
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

// The fast path is used only with the default resolver and routing policies on,
// even when the view's template is already marked by another client.
func TestClusterFastRouteEligibility(t *testing.T) {
	ctx := context.Background()
	warm := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = warm.Close() })
	warm.autoPipelineRoutingDecision(ctx, NewStringCmd(ctx, "get", "warm"))

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
			if c.fastRouteEligible() {
				t.Fatal("fast route eligible outside the default resolver")
			}
			if got := c.fastRouteLookup(NewStringCmd(ctx, "get", "k")); got != nil {
				t.Fatal("fast route used outside the default resolver")
			}
		})
	}
}

// A command whose name has subcommands never takes the fast path: its decision
// depends on more than the name.
func TestClusterFastRouteSkipsSubcommandParents(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		c.autoPipelineRoutingDecision(ctx, NewIntCmd(ctx, "object", "freq", "key"))
	}
	if got := c.fastRouteLookup(NewIntCmd(ctx, "object", "freq", "key")); got != nil {
		t.Fatal("OBJECT (a subcommand parent) took the fast path")
	}
}

// A new metadata view starts with no template marked, so a mark made in an older
// view is never used for it.
func TestClusterFastRouteNewViewStartsUnmarked(t *testing.T) {
	view := buildCommandMetadataView(nil, nil)
	meta := view.routingTable["get"]
	if meta == nil || meta.template == nil {
		t.Fatal("GET has no template in a fresh view")
	}
	if atomic.LoadUint32(&meta.defaultTemplate) != 0 {
		t.Fatal("a fresh view starts with GET's template marked")
	}
}

// fastRouteToken is a command token whose String() is a known command name but
// whose wire value differs. Normal metadata lookup rejects such tokens.
type fastRouteToken struct{}

func (fastRouteToken) String() string                 { return "get" }
func (fastRouteToken) MarshalBinary() ([]byte, error) { return []byte("set"), nil }

// A command whose first argument is not a plain string token never takes the
// fast path, even when the name it reports has a marked template.
func TestClusterFastRouteRejectsUnverifiableToken(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	c.autoPipelineRoutingDecision(ctx, NewStringCmd(ctx, "get", "warm"))

	cmd := NewStringCmd(ctx, fastRouteToken{}, "key")
	if cmd.Name() != "get" {
		t.Fatalf("test token reports name %q, want get", cmd.Name())
	}
	if got := c.fastRouteLookup(cmd); got != nil {
		t.Fatal("a command with an unverifiable token took the fast path")
	}
}

// The half-duplex autopipeliner routes again at flush, so admission pins the
// fast-route template on the command; the full-duplex router routes inside submit
// and does not.
func TestClusterFastRoutePinnedForHalfDuplex(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		opt  *AutoPipelineOptions
		pin  bool
	}{
		{"half duplex", &AutoPipelineOptions{MaxBatchSize: 64}, true},
		{"full duplex", &AutoPipelineOptions{FullDuplex: true}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
			t.Cleanup(func() { _ = c.Close() })
			ap, err := c.AsyncAutoPipelineWithOptions(tt.opt)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ap.Close() })
			if (ap.clusterFD != nil) == tt.pin {
				t.Fatalf("clusterFD set = %v", ap.clusterFD != nil)
			}
			c.autoPipelineRoutingDecision(ctx, NewStringCmd(ctx, "get", "warm"))

			cmd := NewStringCmd(ctx, "get", "key")
			if divert, err := ap.routeCommand(ctx, cmd); divert || err != nil {
				t.Fatalf("routeCommand: divert=%v err=%v", divert, err)
			}
			if pinned := cmd.clusterRoutingDecision() != nil; pinned != tt.pin {
				t.Fatalf("decision pinned on the command = %v, want %v", pinned, tt.pin)
			}
		})
	}
}
