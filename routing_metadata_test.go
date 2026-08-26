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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, ok := defaultCommandMetadataView.routingTable[tt.name]
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

func TestRoutingMetadataOldFTDefaultsRemainEquivalent(t *testing.T) {
	keyless := []string{
		"ft.create", "ft.search", "ft.aggregate", "ft.dictadd", "ft.dictdump",
		"ft.dictdel", "ft.spellcheck", "ft.explain", "ft.explaincli", "ft.aliasadd",
		"ft.aliasupdate", "ft.aliasdel", "ft.aliaslist", "ft.info", "ft.tagvals",
		"ft.syndump", "ft.synupdate", "ft.profile", "ft.alter", "ft.dropindex", "ft.drop",
	}
	readonly := map[string]bool{
		"ft.search": true, "ft.aggregate": true, "ft.dictdump": true,
		"ft.spellcheck": true, "ft.explain": true, "ft.explaincli": true,
		"ft.aliaslist": true, "ft.info": true, "ft.tagvals": true,
		"ft.syndump": true, "ft.profile": true,
	}
	for _, name := range keyless {
		meta, ok := defaultCommandMetadataView.routingTable[name]
		if !ok {
			t.Fatalf("missing routing metadata for %s", name)
		}
		policy, ok := routingPolicyFor(meta)
		if !ok {
			t.Fatalf("missing routing policy for %s", name)
		}
		if policy.Request != routing.ReqDefault || policy.Response != routing.RespDefaultKeyless {
			t.Errorf("%s policy = (%s, %s), want default/keyless", name, policy.Request, policy.Response)
		}
		if policy.IsReadOnly() != readonly[name] {
			t.Errorf("%s readonly = %v, want %v", name, policy.IsReadOnly(), readonly[name])
		}
	}

	keyed := map[string]bool{
		"ft.suglen": true,
		"ft.sugadd": false,
		"ft.sugget": true,
		"ft.sugdel": false,
	}
	for name, wantReadOnly := range keyed {
		meta := defaultCommandMetadataView.routingTable[name]
		policy, ok := routingPolicyFor(meta)
		if !ok || policy.Request != routing.ReqDefault || policy.Response != routing.RespDefaultHashSlot {
			t.Errorf("%s did not retain default/hash-slot policy: %#v", name, policy)
		}
		if ok && policy.IsReadOnly() != wantReadOnly {
			t.Errorf("%s readonly = %v, want %v", name, policy.IsReadOnly(), wantReadOnly)
		}
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

	bare, ok := routingLookupMeta(defaultCommandMetadataView, NewCmd(ctx, "command"))
	if !ok || bare.name != "command" {
		t.Fatalf("bare COMMAND metadata = (%#v, %v), want command", bare, ok)
	}
	child, ok := routingLookupMeta(defaultCommandMetadataView, NewCmd(ctx, "command", "info", "get"))
	if !ok || child.name != "command|info" {
		t.Fatalf("COMMAND INFO metadata = (%#v, %v), want command|info", child, ok)
	}
	if _, ok := routingLookupMeta(defaultCommandMetadataView, NewCmd(ctx, "command", "future")); ok {
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
		{"mget", []interface{}{"mget", "a", "b"}, []int{1, 2}, 3, 1, -1, true},
		{"mset", []interface{}{"mset", "a", "1", "b", "2"}, []int{1, 3}, 5, 2, -1, true},
		{"msetex", []interface{}{"msetex", 2, "a", "1", "b", "2", "px", 10}, []int{2, 4}, 6, 2, 1, true},
		{"lcs", []interface{}{"lcs", "a", "b"}, []int{1, 2}, 3, 1, -1, true},
		{"eval", []interface{}{"eval", "return 1", 0}, nil, 3, 1, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := defaultCommandMetadataView.routingTable[tt.name]
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
	bitop := defaultCommandMetadataView.routingTable["bitop"]
	plan, ok := routingResolveKeyPlan(bitop, NewCmd(context.Background(), "bitop", "and", "dst", "a", "b"))
	if !ok || !reflect.DeepEqual(plan.positions, []int{2, 3, 4}) || plan.splittable {
		t.Fatalf("BITOP plan = %#v, ok=%v", plan, ok)
	}

	jsonDebug := defaultCommandMetadataView.routingTable["json.debug"]
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

func TestRoutingMetadataRejectsUnimplementedRangeLimit(t *testing.T) {
	info := &CommandInfo{Name: "limited", KeySpecs: []KeySpec{{
		Flags:       []string{"RO", "access"},
		BeginSearch: "index", Index: 1,
		FindKeys: "range", LastKey: -1, KeyStep: 1, Limit: 2,
	}}}
	meta := deriveRoutingCommandMeta(info.Name, info)
	if !meta.valid || meta.keyState != routingKeysKnown || meta.keyPlanComplete {
		t.Fatalf("range-limit metadata = %#v, want usable first key but incomplete plan", meta)
	}
	cmd := NewCmd(context.Background(), "limited", "a", "b")
	if first, ok := routingFirstKeyPos(meta, cmd); !ok || first != 1 {
		t.Fatalf("range-limit first key=(%d, %v), want (1, true)", first, ok)
	}
	if _, ok := routingResolveKeyPlan(meta, cmd); ok {
		t.Fatal("nonzero range limit unexpectedly produced an exact key plan")
	}
}

func TestRoutingMetadataNotKeyStillSelectsClusterSlot(t *testing.T) {
	meta := defaultCommandMetadataView.routingTable["spublish"]
	cmd := NewCmd(context.Background(), "spublish", "channel", "message")
	first, ok := routingFirstKeyPos(meta, cmd)
	if !ok || first != 1 {
		t.Fatalf("SPUBLISH first routing key = (%d, %v), want (1, true)", first, ok)
	}
	plan, ok := routingResolveKeyPlan(meta, cmd)
	if !ok || !reflect.DeepEqual(plan.positions, []int{1}) {
		t.Fatalf("SPUBLISH plan = %#v, ok=%v", plan, ok)
	}
}

func TestRoutingMetadataIncompleteKeysRetainPolicy(t *testing.T) {
	meta, ok := defaultCommandMetadataView.routingTable["xread"]
	if !ok || meta.keyState != routingKeysKnown || meta.keyPlanComplete {
		t.Fatalf("XREAD metadata = %#v, want first-key-only record", meta)
	}
	policy, ok := routingPolicyFor(meta)
	if !ok || !policy.IsReadOnly() || policy.Request != routing.ReqDefault {
		t.Fatalf("XREAD policy = %#v, ok=%v", policy, ok)
	}
	cmd := NewCmd(context.Background(), "xread", "streams", "key", "0")
	if first, firstOK := routingFirstKeyPos(meta, cmd); !firstOK || first != 2 {
		t.Fatalf("XREAD first key=(%d, %v), want (2, true)", first, firstOK)
	}
	if _, planOK := routingResolveKeyPlan(meta, cmd); planOK {
		t.Fatal("XREAD incomplete key spec unexpectedly produced a plan")
	}
}

func TestRoutingMetadataUsesUsableSpecsBesideIncompleteSiblings(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		cmd  Cmder
		want int
	}{
		{"georadius", NewCmd(ctx, "georadius", "source", 1, 2, 3, "km", "store", "destination"), 1},
		{"georadiusbymember", NewCmd(ctx, "georadiusbymember", "source", "member", 3, "km", "store", "destination"), 1},
		{"sort_ro", NewCmd(ctx, "sort_ro", "source", "alpha"), 1},
		{"xreadgroup", NewCmd(ctx, "xreadgroup", "group", "g", "c", "streams", "stream", ">"), 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := defaultCommandMetadataView.routingTable[tt.name]
			first, ok := routingFirstKeyPos(meta, tt.cmd)
			if !ok || first != tt.want {
				t.Fatalf("first key=(%d, %v), want (%d, true)", first, ok, tt.want)
			}
			if _, planOK := routingResolveKeyPlan(meta, tt.cmd); planOK {
				t.Fatal("incomplete sibling unexpectedly authorized a complete key plan")
			}
		})
	}
}

func TestRoutingMetadataMigrateSelectsActiveKeyForm(t *testing.T) {
	meta := defaultCommandMetadataView.routingTable["migrate"]
	ordinary := NewCmd(context.Background(), "migrate", "host", 6379, "key", 0, 1000)
	if first, ok := routingFirstKeyPos(meta, ordinary); !ok || first != 3 {
		t.Fatalf("ordinary MIGRATE first key=(%d, %v), want (3, true)", first, ok)
	}
	keys := NewCmd(context.Background(), "migrate", "host", 6379, "", 0, 1000, "keys", "one", "two")
	if first, ok := routingFirstKeyPos(meta, keys); !ok || first != 7 {
		t.Fatalf("MIGRATE KEYS first key=(%d, %v), want (7, true)", first, ok)
	}
	if _, planOK := routingResolveKeyPlan(meta, keys); planOK {
		t.Fatal("incomplete MIGRATE KEYS metadata unexpectedly produced a full plan")
	}
}

func TestRoutingMetadataSpecialPoliciesAreExplicit(t *testing.T) {
	for name, info := range commandInfoSnapshot {
		meta := deriveRoutingCommandMeta(name, info)
		if !meta.valid || meta.policy == nil {
			continue
		}
		if meta.policy.Request == routing.ReqSpecial && meta.special&routingSpecialRequestDeclared == 0 {
			t.Errorf("%s request special is not declared", name)
		}
		if meta.policy.Response == routing.RespSpecial && meta.special&routingSpecialResponseDeclared == 0 {
			t.Errorf("%s response special is not declared", name)
		}
	}

	for _, name := range []string{"info", "scan", "hotkeys|get"} {
		meta := defaultCommandMetadataView.routingTable[name]
		if _, ok := routingPolicyFor(meta); ok {
			t.Errorf("unsupported special policy for %s was enabled", name)
		}
		if err := routingSpecialPolicyError(meta); err != errUnsupportedRoutingPolicy {
			t.Errorf("%s special error = %v", name, err)
		}
	}
	randomKey := defaultCommandMetadataView.routingTable["randomkey"]
	if policy, ok := routingPolicyFor(randomKey); !ok || policy.Response != routing.RespSpecial {
		t.Fatalf("RANDOMKEY special response handler was not enabled: policy=%#v ok=%v", policy, ok)
	}

	liveOnly := deriveRoutingCommandMeta("module.future", &CommandInfo{
		Name: "module.future", Tips: []string{"request_policy:special"},
	})
	if _, ok := routingPolicyFor(liveOnly); ok {
		t.Fatal("undeclared live special policy was enabled")
	}
	if err := routingSpecialPolicyError(liveOnly); err != errUnsupportedRoutingPolicy {
		t.Fatalf("live special error = %v", err)
	}
}

func TestRoutingMetadataTransactionAdaptationsAreExplicit(t *testing.T) {
	if len(routingTransactionPolicies) != 1 ||
		routingTransactionPolicies["ping"] != routingTransactionSingleNode {
		t.Fatalf("transaction adaptations=%v, want only connection-local PING", routingTransactionPolicies)
	}
	if meta := defaultCommandMetadataView.routingTable["ping"]; meta.tx != routingTransactionSingleNode {
		t.Fatalf("PING transaction support=%v, want single-node", meta.tx)
	}
	if meta := defaultCommandMetadataView.routingTable["flushall"]; meta.tx != 0 {
		t.Fatalf("FLUSHALL transaction support=%v, want unsupported", meta.tx)
	}
}

func TestRoutingMetadataTombstonesAndShadowedParents(t *testing.T) {
	records := map[string]*CommandInfo{
		"container":       {Name: "container"},
		"container|child": {Name: "container|child"},
		"gone":            nil,
	}
	table := deriveRoutingTable(records, map[string]struct{}{"container": {}})
	if _, ok := table["container"]; ok {
		t.Fatal("shadowed parent was emitted")
	}
	if _, ok := table["container|child"]; !ok {
		t.Fatal("child was not emitted")
	}
	if _, ok := table["gone"]; ok {
		t.Fatal("tombstone was emitted")
	}
}

func TestCommandInfoResolverUsesOneSuppliedMetadataView(t *testing.T) {
	makeView := func(request string) *commandMetadataView {
		records := map[string]*CommandInfo{
			"probe": {Name: "probe", Tips: []string{"request_policy:" + request}},
		}
		return &commandMetadataView{
			records:      records,
			routingTable: deriveRoutingTable(records, nil),
		}
	}
	loaded := makeView("all_nodes")
	captured := makeView("all_shards")
	ensures := 0
	metadata := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return loaded },
		func(context.Context) error { ensures++; return nil },
	)
	custom := NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy { return nil })
	custom.SetFallbackResolver(metadata)

	ctx := context.Background()
	cmd := NewCmd(ctx, "probe")
	resolution, view, err := custom.resolveCommandRoutingWithView(ctx, cmd, func() *commandMetadataView { return captured })
	if err != nil {
		t.Fatal(err)
	}
	if ensures != 1 {
		t.Fatalf("ensure calls = %d, want 1", ensures)
	}
	if view != captured || resolution.policy == nil || resolution.policy.Request != routing.ReqAllShards {
		t.Fatalf("metadata policy/view = (%#v, %p), want all_shards/%p", resolution.policy, view, captured)
	}
	policy := custom.GetCommandPolicy(ctx, cmd)
	if policy == nil || policy.Request != routing.ReqAllNodes {
		t.Fatalf("ordinary policy = %#v, want all_nodes", policy)
	}
	if ensures != 2 {
		t.Fatalf("ensure calls after direct GetCommandPolicy = %d, want 2", ensures)
	}
}

func TestCommandInfoResolverDoesNotPrepareUnusedMetadataFallback(t *testing.T) {
	view := defaultCommandMetadataView
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
		func() *commandMetadataView { return defaultCommandMetadataView },
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
	if view != defaultCommandMetadataView || resolution.policy == nil || resolution.policy.Response != routing.RespDefaultHashSlot {
		t.Fatalf("static fallback = (%#v, %p), want hash-slot/%p", resolution.policy, view, defaultCommandMetadataView)
	}
}

func TestCommandInfoResolverBatchUsesOneViewAndLazyMetadata(t *testing.T) {
	before := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"fallback": {Name: "fallback", Tips: []string{"request_policy:all_nodes"}},
	})
	after := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"fallback": {Name: "fallback", Tips: []string{"request_policy:all_shards"}},
	})
	current := before
	ensureCalls, captureCalls := 0, 0
	metadata := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return current },
		func(context.Context) error { ensureCalls++; current = after; return nil },
	)
	customCalls := map[string]int{}
	customPolicy := &routing.CommandPolicy{Request: routing.ReqAllNodes}
	custom := NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
		customCalls[cmd.Name()]++
		if cmd.Name() == "custom" {
			return customPolicy
		}
		return nil
	})
	custom.SetFallbackResolver(metadata)

	ctx := context.Background()
	cmds := []Cmder{NewCmd(ctx, "custom"), NewCmd(ctx, "fallback")}
	resolutions, view, err := custom.resolveCommandRoutingsWithView(ctx, cmds, func() *commandMetadataView {
		captureCalls++
		return current
	})
	if err != nil {
		t.Fatal(err)
	}
	if view != after || captureCalls != 1 || ensureCalls != 1 {
		t.Fatalf("batch view=%p captures=%d ensures=%d, want %p/1/1", view, captureCalls, ensureCalls, after)
	}
	if resolutions[0].policy != customPolicy || resolutions[1].policy == nil || resolutions[1].policy.Request != routing.ReqAllShards {
		t.Fatalf("batch resolutions = %#v", resolutions)
	}
	if customCalls["custom"] != 1 || customCalls["fallback"] != 1 {
		t.Fatalf("custom calls = %#v, want each once", customCalls)
	}
}

func TestCommandInfoResolverBatchSkipsUnusedMetadata(t *testing.T) {
	ensureCalls, captureCalls := 0, 0
	metadata := newCommandMetadataPolicyResolverWithEnsure(
		func() *commandMetadataView { return defaultCommandMetadataView },
		func(context.Context) error { ensureCalls++; return nil },
	)
	custom := NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{Request: routing.ReqDefault}
	})
	custom.SetFallbackResolver(metadata)

	ctx := context.Background()
	_, _, err := custom.resolveCommandRoutingsWithView(ctx, []Cmder{
		NewCmd(ctx, "get", "a"), NewCmd(ctx, "get", "b"),
	}, func() *commandMetadataView {
		captureCalls++
		return defaultCommandMetadataView
	})
	if err != nil {
		t.Fatal(err)
	}
	if ensureCalls != 0 || captureCalls != 1 {
		t.Fatalf("ensure/capture = %d/%d, want 0/1", ensureCalls, captureCalls)
	}
}
