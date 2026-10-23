package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestMigrateUnsupportedOptionFailsBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	newCmd := func() *Cmd {
		return NewCmd(ctx, "migrate", "host", 6379, "", 0, 1000, clusterBinaryKey("KEYS"), "key")
	}
	if pos := cmdFirstKeyPosWithInfo(newCmd(), nil); pos != -1 {
		t.Fatalf("unsupported MIGRATE option key position=%d, want invalid (-1)", pos)
	}
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cluster-disabled=%v", disabled), func(t *testing.T) {
			c := NewClusterClient(&ClusterOptions{DisableRoutingPolicies: disabled})
			defer c.Close()
			if err := c.Process(ctx, newCmd()); err == nil || !strings.Contains(err.Error(), "routing key") {
				t.Fatalf("direct MIGRATE error=%v", err)
			}
			missing := c.routingDecisionInView(ctx, newCmd(), &commandMetadataView{}, nil)
			if missing.policyErr == nil {
				t.Fatal("missing metadata accepted unsupported MIGRATE option")
			}
			ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ap.routeCommand(ctx, newCmd()); err == nil {
				t.Fatal("AutoPipeline accepted unsupported MIGRATE option")
			}
		})
	}
	r := NewRing(&RingOptions{})
	defer r.Close()
	if _, err := r.cmdShard(newCmd()); err == nil {
		t.Fatal("Ring accepted unsupported MIGRATE option")
	}
	if err := r.generalProcessPipeline(ctx, []Cmder{newCmd()}, false); err == nil || !strings.Contains(err.Error(), "routing key") {
		t.Fatalf("Ring pipeline MIGRATE error=%v", err)
	}
}

// Raw MIGRATE in its KEYS form: the options before KEYS take operands (AUTH
// password, AUTH2 username password), so a scan for the first "keys" token
// took a password that happens to be "KEYS" for the clause and routed by the
// token after it. The resolver walks the option grammar instead.
func TestRawMigrateKeysSkipsOptionOperands(t *testing.T) {
	ctx := context.Background()
	const K = "KEY_SENTINEL"
	cases := [][]interface{}{
		{"migrate", "h", 1, "", 0, 1000, "keys", K},
		{"migrate", "h", 1, "", 0, 1000, "copy", "replace", "keys", K},
		{"migrate", "h", 1, "", 0, 1000, "AUTH", "KEYS", "KEYS", K},
		{"migrate", "h", 1, "", 0, 1000, "auth2", "keys", "KEYS", "keys", K},
		{"migrate", "h", 1, "", 0, 1000, "copy", "auth", "pw", "keys", K, "b"},
	}
	for _, args := range cases {
		want := -1
		for i := len(args) - 1; i >= 0; i-- {
			if s, ok := args[i].(string); ok && s == K {
				want = i
				break
			}
		}
		if pos := cmdFirstKeyPosWithInfo(NewCmd(ctx, args...), nil); pos != want {
			t.Errorf("%v: resolver picks position %d, key is at %d", args, pos, want)
		}
	}
	// No KEYS clause, or nothing after it: keyless.
	for _, args := range [][]interface{}{
		{"migrate", "h", 1, "", 0, 1000},
		{"migrate", "h", 1, "", 0, 1000, "auth", "keys"},
		{"migrate", "h", 1, "", 0, 1000, "keys"},
	} {
		if pos := cmdFirstKeyPosWithInfo(NewCmd(ctx, args...), nil); pos != 0 {
			t.Errorf("%v: resolver picks position %d, want 0", args, pos)
		}
	}
}
