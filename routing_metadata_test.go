package redis

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9/internal/routing"
)

func TestRoutingMetadataDerivesPoliciesFromSharedRecords(t *testing.T) {
	tests := []struct {
		name     string
		request  routing.RequestPolicy
		response routing.ResponsePolicy
		readonly bool
	}{
		{"get", routing.ReqDefault, routing.RespDefaultHashSlot, true},
		{"touch", routing.ReqMultiShard, routing.RespAggSum, true},
		{"flushall", routing.ReqAllShards, routing.RespAllSucceeded, false},
		{"dbsize", routing.ReqAllShards, routing.RespAggSum, true},
		{"ping", routing.ReqAllShards, routing.RespAllSucceeded, false},
		{"ft.search", routing.ReqDefault, routing.RespDefaultKeyless, true},
		{"ft.create", routing.ReqDefault, routing.RespDefaultKeyless, false},
		{"ft.sugget", routing.ReqDefault, routing.RespDefaultHashSlot, true},
		{"ft.sugadd", routing.ReqDefault, routing.RespDefaultHashSlot, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, ok := defaultCommandMetadataView().routingTable[tt.name]
			if !ok {
				t.Fatalf("missing routing metadata for %s", tt.name)
			}
			policy, ok := routingPolicyFor(meta)
			if !ok {
				t.Fatalf("routing policy for %s is unavailable", tt.name)
			}
			if policy.Request != tt.request || policy.Response != tt.response {
				t.Fatalf("%s policy = (%s, %s), want (%s, %s)", tt.name,
					policy.Request, policy.Response, tt.request, tt.response)
			}
			if policy.IsReadOnly() != tt.readonly {
				t.Fatalf("%s readonly = %v, want %v", tt.name, policy.IsReadOnly(), tt.readonly)
			}
		})
	}
}

func TestRoutingMetadataResolvesContainerInvocation(t *testing.T) {
	resolver := NewDefaultCommandPolicyResolver()

	for _, child := range []string{"READ", "del"} {
		cmd := NewCmd(context.Background(), "FT.CURSOR", child, "idx", "42")
		policy := resolver.GetCommandPolicy(context.Background(), cmd)
		if policy == nil || policy.Request != routing.ReqSpecial {
			t.Fatalf("FT.CURSOR %s policy = %#v, want request special", child, policy)
		}
	}

	gc := NewCmd(context.Background(), "FT.CURSOR", "GC", "idx")
	policy := resolver.GetCommandPolicy(context.Background(), gc)
	if policy == nil || policy.Request != routing.ReqDefault || policy.Response != routing.RespDefaultKeyless {
		t.Fatalf("FT.CURSOR GC policy = %#v, want ordinary keyless", policy)
	}

	unknown := NewCmd(context.Background(), "FT.CURSOR", "future")
	if policy := resolver.GetCommandPolicy(context.Background(), unknown); policy != nil {
		t.Fatalf("unknown child policy = %#v, want nil", policy)
	}
	unsafeChild := NewCmd(context.Background(), "FT.CURSOR", struct{}{})
	if policy := resolver.GetCommandPolicy(context.Background(), unsafeChild); policy != nil {
		t.Fatalf("unsafe child policy = %#v, want nil", policy)
	}
}

func TestRoutingMetadataResolvesBareContainerInvocation(t *testing.T) {
	ctx := context.Background()

	bare, ok := routingLookupMeta(defaultCommandMetadataView(), NewCmd(ctx, "command"))
	if !ok || bare.name != "command" {
		t.Fatalf("bare COMMAND metadata = (%#v, %v), want command", bare, ok)
	}
	child, ok := routingLookupMeta(defaultCommandMetadataView(), NewCmd(ctx, "command", "info", "get"))
	if !ok || child.name != "command|info" {
		t.Fatalf("COMMAND INFO metadata = (%#v, %v), want command|info", child, ok)
	}
	if _, ok := routingLookupMeta(defaultCommandMetadataView(), NewCmd(ctx, "command", "future")); ok {
		t.Fatal("unknown COMMAND child fell back to the bare parent")
	}
}

func TestMetadataResolverDoesNotExposeImmutablePolicy(t *testing.T) {
	resolver := NewDefaultCommandPolicyResolver()
	ctx := context.Background()
	cmd := NewCmd(ctx, "get", "key")

	first := resolver.GetCommandPolicy(ctx, cmd)
	if first == nil || !first.IsReadOnly() {
		t.Fatalf("first GET policy = %#v, want readonly", first)
	}
	first.Request = routing.ReqAllNodes
	delete(first.Tips, routing.ReadOnlyCMD)

	second := resolver.GetCommandPolicy(ctx, cmd)
	if second == nil || second.Request != routing.ReqDefault || !second.IsReadOnly() {
		t.Fatalf("mutating returned policy changed shared metadata: %#v", second)
	}
}

func TestRoutingMetadataFailsClosedOnUnknownPoliciesAndKeySpecs(t *testing.T) {
	records := map[string]*CommandInfo{
		"future-request": {
			Name: "future-request", Tips: []string{"request_policy:future"},
		},
		"future-response": {
			Name: "future-response", Tips: []string{"response_policy:future"},
		},
		"incomplete": {
			Name: "incomplete", KeySpecs: []KeySpec{{
				Flags: []string{"RO", "access", "incomplete"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
		"not-key": {
			Name: "not-key", KeySpecs: []KeySpec{{
				Flags: []string{"not_key"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
		"unknown-begin": {
			Name: "unknown-begin", KeySpecs: []KeySpec{{
				Flags: []string{"RO", "access"}, BeginSearch: "future", FindKeys: "range", KeyStep: 1,
			}},
		},
		"prefix": {
			Name: "prefix", KeySpecs: []KeySpec{{
				Flags: []string{"RO", "access", "prefix"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
		"future-key-flag": {
			Name: "future-key-flag", KeySpecs: []KeySpec{{
				Flags: []string{"RO", "access", "future_key_flag"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
		"conflicting-key-mode": {
			Name: "conflicting-key-mode", KeySpecs: []KeySpec{{
				Flags: []string{"RO", "RW", "access"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", KeyStep: 1,
			}},
		},
	}
	table := deriveRoutingTable(records, nil)
	for _, name := range []string{"future-request", "future-response"} {
		if _, ok := table[name]; ok {
			t.Errorf("malformed %s unexpectedly produced routing metadata", name)
		}
	}
	if meta, ok := table["incomplete"]; !ok || meta.keyState != routingKeysKnown || meta.keyPlanComplete {
		t.Errorf("incomplete metadata = %#v, want usable first key but incomplete plan", meta)
	}
	for _, name := range []string{"unknown-begin", "prefix", "future-key-flag", "conflicting-key-mode"} {
		if meta, ok := table[name]; !ok || meta.keyState != routingKeysUnknown || meta.keyPlanComplete {
			t.Errorf("%s metadata = %#v, want retained policy with unknown keys", name, meta)
		}
	}
	for _, name := range []string{"incomplete", "unknown-begin", "prefix", "future-key-flag", "conflicting-key-mode"} {
		if _, planOK := routingResolveKeyPlan(table[name], NewCmd(context.Background(), name, "key")); planOK {
			t.Errorf("%s unexpectedly produced an exact key plan", name)
		}
	}
	if meta, ok := table["not-key"]; !ok || meta.keyState != routingKeysKnown {
		t.Errorf("not_key slot metadata = %#v, want known routing key", meta)
	}
}

func TestRoutingMetadataNoMatchingKeySpecIsNotKeyless(t *testing.T) {
	meta := deriveRoutingCommandMeta("keyword", &CommandInfo{
		Name: "keyword",
		KeySpecs: []KeySpec{{
			Flags: []string{"RO", "access"}, BeginSearch: "keyword", Keyword: "KEYS", StartFrom: 1,
			FindKeys: "range", LastKey: -1, KeyStep: 1,
		}},
	})
	if pos, ok := routingFirstKeyPos(meta, NewCmd(context.Background(), "keyword", "arg")); ok || pos != 0 {
		t.Fatalf("unmatched keyed invocation = (%d, %v), want unresolved", pos, ok)
	}
}

func TestRoutingMetadataKeyPlans(t *testing.T) {
	tests := []struct {
		name       string
		args       []interface{}
		positions  []int
		keyArgsEnd int
		step       int
		numKeysPos int
		splittable bool
	}{
		{"spublish", []interface{}{"spublish", "channel", "message"}, []int{1}, 2, 1, -1, true},
		{"mget", []interface{}{"mget", "a", "b"}, []int{1, 2}, 3, 1, -1, true},
		{"mset", []interface{}{"mset", "a", "1", "b", "2"}, []int{1, 3}, 5, 2, -1, true},
		{"msetex", []interface{}{"msetex", 2, "a", "1", "b", "2", "px", 10}, []int{2, 4}, 6, 2, 1, true},
		{"lcs", []interface{}{"lcs", "a", "b"}, []int{1, 2}, 3, 1, -1, true},
		{"eval", []interface{}{"eval", "return 1", 0}, nil, 3, 1, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := defaultCommandMetadataView().routingTable[tt.name]
			cmd := NewCmd(context.Background(), tt.args...)
			plan, ok := routingResolveKeyPlan(meta, cmd)
			if !ok {
				t.Fatal("key plan unavailable")
			}
			if !reflect.DeepEqual(plan.positions, tt.positions) || plan.keyArgsEnd != tt.keyArgsEnd ||
				plan.step != tt.step || plan.numKeysPos != tt.numKeysPos || plan.splittable != tt.splittable {
				t.Fatalf("plan = %#v, want positions=%v end=%d step=%d numkeys=%d splittable=%v",
					plan, tt.positions, tt.keyArgsEnd, tt.step, tt.numKeysPos, tt.splittable)
			}
			first, firstOK := routingFirstKeyPos(meta, cmd)
			wantFirst := 0
			if len(tt.positions) > 0 {
				wantFirst = tt.positions[0]
			}
			if !firstOK || first != wantFirst {
				t.Fatalf("first key = (%d, %v), want (%d, true)", first, firstOK, wantFirst)
			}
		})
	}
}

func TestRoutingMetadataMultipleAndKeywordKeySpecs(t *testing.T) {
	bitop := defaultCommandMetadataView().routingTable["bitop"]
	plan, ok := routingResolveKeyPlan(bitop, NewCmd(context.Background(), "bitop", "and", "dst", "a", "b"))
	if !ok || !reflect.DeepEqual(plan.positions, []int{2, 3, 4}) || plan.splittable {
		t.Fatalf("BITOP plan = %#v, ok=%v", plan, ok)
	}

	jsonDebug := defaultCommandMetadataView().routingTable["json.debug"]
	plan, ok = routingResolveKeyPlan(jsonDebug, NewCmd(context.Background(), "json.debug", "memory", "doc"))
	if !ok || !reflect.DeepEqual(plan.positions, []int{2}) {
		t.Fatalf("JSON.DEBUG MEMORY plan = %#v, ok=%v", plan, ok)
	}
}

func TestRoutingMetadataKeywordSearchesBackwardForNegativeStart(t *testing.T) {
	info := &CommandInfo{Name: "keyword-backward", KeySpecs: []KeySpec{{
		Flags:       []string{"RO", "access"},
		BeginSearch: "keyword", Keyword: "KEYS", StartFrom: -2,
		FindKeys: "range", LastKey: -1, KeyStep: 1,
	}}}
	meta := deriveRoutingCommandMeta(info.Name, info)
	cmd := NewCmd(context.Background(), "keyword-backward", "KEYS", "early", "value", "KEYS", "late", "tail")
	plan, ok := routingResolveKeyPlan(meta, cmd)
	if !ok || !reflect.DeepEqual(plan.positions, []int{5, 6}) {
		t.Fatalf("backward keyword plan = %#v, ok=%v", plan, ok)
	}
}

// Partial metadata may prove a routing key without authorizing multi-shard splitting.
func TestRoutingMetadataPartialKeyPlans(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []interface{}
		first int
	}{
		{"limited", []interface{}{"limited", "a", "b"}, 1},
		{"xread", []interface{}{"xread", "streams", "key", "0"}, 2},
		{"georadius", []interface{}{"georadius", "source", 1, 2, 3, "km", "store", "destination"}, 1},
		{"georadiusbymember", []interface{}{"georadiusbymember", "source", "member", 3, "km", "store", "destination"}, 1},
		{"sort_ro", []interface{}{"sort_ro", "source", "alpha"}, 1},
		{"xreadgroup", []interface{}{"xreadgroup", "group", "g", "c", "streams", "stream", ">"}, 5},
		{"migrate", []interface{}{"migrate", "host", 6379, "key", 0, 1000}, 3},
		{"migrate", []interface{}{"migrate", "host", 6379, "", 0, 1000, "keys", "one", "two"}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := defaultCommandMetadataView().routingTable[tc.name]
			if tc.name == "limited" {
				meta = deriveRoutingCommandMeta(tc.name, &CommandInfo{KeySpecs: []KeySpec{{
					Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1,
					FindKeys: "range", LastKey: -1, KeyStep: 1, Limit: 2,
				}}})
			}
			if !meta.valid || meta.keyState != routingKeysKnown || meta.keyPlanComplete {
				t.Fatalf("metadata=%#v, want usable first key but incomplete plan", meta)
			}
			cmd := NewCmd(context.Background(), tc.args...)
			if first, ok := routingFirstKeyPos(meta, cmd); !ok || first != tc.first {
				t.Fatalf("first key=(%d, %v), want (%d, true)", first, ok, tc.first)
			}
			if _, ok := routingResolveKeyPlan(meta, cmd); ok {
				t.Fatal("partial metadata authorized a complete key plan")
			}
			_, txOK := routingResolveTransactionKeyPlan(meta, cmd)
			if want := tc.name == "xread" || tc.name == "xreadgroup"; txOK != want {
				t.Fatalf("transaction plan=%v, want %v", txOK, want)
			}
			if policy, ok := routingPolicyFor(meta); !ok || policy.Request != routing.ReqDefault {
				t.Fatalf("partial key metadata lost its routing policy: %#v", policy)
			}
		})
	}
}

func TestRoutingStreamAdaptationRejectsChangedMetadata(t *testing.T) {
	for _, specs := range [][]KeySpec{
		{{Flags: []string{"RO", "incomplete"}, BeginSearch: "index", Index: 2, FindKeys: "range", LastKey: -1, KeyStep: 1, Limit: 2}},
		append(append([]KeySpec(nil), commandInfoSnapshotByName()["xread"].KeySpecs...), KeySpec{BeginSearch: "unknown", FindKeys: "unknown"}),
	} {
		meta := deriveRoutingCommandMeta("xread", &CommandInfo{Name: "xread", KeySpecs: specs})
		if _, ok := routingResolveTransactionKeyPlan(meta, makeCmd("xread", "streams", "key", "0")); ok {
			t.Fatal("changed stream metadata authorized an incomplete transaction plan")
		}
	}
}

func TestCommandInfoResolverDoesNotPrepareUnusedMetadataFallback(t *testing.T) {
	view := defaultCommandMetadataView()
	ensures, customCalls, fallbackCaptures := 0, 0, 0
	metadata := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return view },
		func(context.Context) error { ensures++; return nil },
	)
	customPolicy := &routing.CommandPolicy{Request: routing.ReqAllNodes}
	custom := NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		customCalls++
		return customPolicy
	})
	custom.SetFallbackResolver(metadata)

	resolution, captured, err := custom.resolveCommandRoutingWithView(
		context.Background(),
		NewCmd(context.Background(), "get", "key"),
		func() *commandMetadataView { fallbackCaptures++; return view },
	)
	if err != nil {
		t.Fatal(err)
	}
	if resolution.policy != customPolicy || captured != view {
		t.Fatalf("resolved (%p, %p), want (%p, %p)", resolution.policy, captured, customPolicy, view)
	}
	if customCalls != 1 || ensures != 0 || fallbackCaptures != 1 {
		t.Fatalf("calls custom=%d ensure=%d capture=%d, want 1/0/1", customCalls, ensures, fallbackCaptures)
	}
}

func TestCommandInfoResolverUsesStaticViewAfterEnsureFailure(t *testing.T) {
	wantErr := errors.New("COMMAND denied")
	metadata := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return defaultCommandMetadataView() },
		func(context.Context) error { return wantErr },
	)
	resolution, view, err := metadata.resolveCommandRoutingWithView(
		context.Background(),
		NewCmd(context.Background(), "get", "key"),
		func() *commandMetadataView { return nil },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if view != defaultCommandMetadataView() || resolution.policy == nil || resolution.policy.Response != routing.RespDefaultHashSlot {
		t.Fatalf("static fallback = (%#v, %p), want hash-slot/%p", resolution.policy, view, defaultCommandMetadataView())
	}
}

func TestCommandInfoResolverFallbackRefreshKeepsPolicyAndKeysTogether(t *testing.T) {
	ctx := context.Background()
	before := buildCommandMetadataView(nil, nil)
	after := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"module.read": {
			Name: "module.read", Flags: []string{"readonly"},
			FirstKeyPos: 2, LastKeyPos: 2, StepCount: 1,
		},
	})
	current := before
	static := newCommandMetadataPolicyResolver(func() *commandMetadataView { return current })
	dynamic := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return current },
		func(context.Context) error { current = after; return nil },
	)
	static.SetFallbackResolver(dynamic)

	resolution, view, err := static.resolveCommandRoutingWithView(
		ctx, NewCmd(ctx, "module.read", "option", "key"),
		func() *commandMetadataView { return current },
	)
	if err != nil {
		t.Fatal(err)
	}
	if view != after || !resolution.metaOK || resolution.policy == nil {
		t.Fatalf("fallback did not resolve the refreshed metadata: view=%p resolution=%+v", view, resolution)
	}
	if !resolution.meta.readOnly || len(resolution.meta.keySpecs) != 1 || resolution.meta.keySpecs[0].index != 2 {
		t.Fatalf("fallback used keys or flags from another view: %+v", resolution.meta)
	}
}

func TestCommandInfoResolverBatchPreparesDynamicFallback(t *testing.T) {
	ctx := context.Background()
	before := buildCommandMetadataView(nil, nil)
	after := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"module.read": {
			Name: "module.read", Flags: []string{"readonly"},
			FirstKeyPos: 2, LastKeyPos: 2, StepCount: 1,
		},
		"get": {
			Name: "get", Flags: []string{"readonly"},
			FirstKeyPos: 2, LastKeyPos: 2, StepCount: 1,
		},
	})
	current := before
	ensureCalls, customCalls := 0, 0
	dynamic := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return current },
		func(context.Context) error { ensureCalls++; current = after; return nil },
	)
	static := newCommandMetadataPolicyResolver(func() *commandMetadataView { return current })
	static.SetFallbackResolver(dynamic)
	customPolicy := &routing.CommandPolicy{Request: routing.ReqAllNodes}
	custom := NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
		customCalls++
		if cmd.Name() == "custom" {
			return customPolicy
		}
		return nil
	})
	custom.SetFallbackResolver(static)
	cmds := []Cmder{NewCmd(ctx, "get", "option", "key"), NewCmd(ctx, "module.read", "option", "key"), NewCmd(ctx, "custom")}
	resolutions, view, err := custom.resolveCommandRoutingsWithView(ctx, cmds, func() *commandMetadataView { return current })
	if err != nil {
		t.Fatal(err)
	}
	if view != after || ensureCalls != 1 || customCalls != len(cmds) {
		t.Fatalf("view=%p ensure calls=%d custom calls=%d, want %p/1/%d", view, ensureCalls, customCalls, after, len(cmds))
	}
	if resolutions[2].policy != customPolicy {
		t.Fatal("metadata refresh replaced the custom policy")
	}
	for i, resolution := range resolutions[:2] {
		if !resolution.metaOK || resolution.policy == nil || len(resolution.meta.keySpecs) != 1 || resolution.meta.keySpecs[0].index != 2 {
			t.Errorf("command %d did not use the final shared view: %+v", i, resolution)
		}
	}
}
