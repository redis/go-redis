package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
)

type clusterBinaryKey string

func (k clusterBinaryKey) MarshalBinary() ([]byte, error) {
	return []byte(k), nil
}

type countingClusterShardPicker struct {
	calls int
	index int
}

func (p *countingClusterShardPicker) Next(total int) int {
	p.calls++
	if p.index >= total {
		return 0
	}
	return p.index
}

type clusterRoutingShortCircuitHook struct{}

func (clusterRoutingShortCircuitHook) DialHook(next DialHook) DialHook { return next }

func (clusterRoutingShortCircuitHook) ProcessHook(ProcessHook) ProcessHook {
	return func(context.Context, Cmder) error { return nil }
}

func (clusterRoutingShortCircuitHook) ProcessPipelineHook(ProcessPipelineHook) ProcessPipelineHook {
	return func(context.Context, []Cmder) error { return nil }
}

type clusterMetadataNodeHook struct {
	process  func(context.Context, Cmder) error
	pipeline func(context.Context, []Cmder) error
}

func (h clusterMetadataNodeHook) DialHook(next DialHook) DialHook { return next }

func (h clusterMetadataNodeHook) ProcessHook(next ProcessHook) ProcessHook {
	if h.process == nil {
		return next
	}
	return h.process
}

func (h clusterMetadataNodeHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	if h.pipeline == nil {
		return next
	}
	return h.pipeline
}

func newMetadataTestCluster(t *testing.T, cfg *CommandMetadataConfig) *ClusterClient {
	t.Helper()
	c := NewClusterClient(&ClusterOptions{
		Addrs:           []string{"127.0.0.1:1"},
		CommandMetadata: cfg,
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestUniversalCommandMetadataPropagatesToClusterVariants(t *testing.T) {
	cfg := &CommandMetadataConfig{Mode: CommandMetadataPreferLive}
	opt := (&UniversalOptions{CommandMetadata: cfg}).Cluster()
	if opt.CommandMetadata != cfg {
		t.Fatal("UniversalOptions.Cluster dropped CommandMetadata")
	}
	failover := (&UniversalOptions{CommandMetadata: cfg}).Failover()
	if failover.CommandMetadata != cfg || failover.clusterOptions().CommandMetadata != cfg ||
		failover.clientOptions().CommandMetadata != cfg {
		t.Fatal("UniversalOptions.Failover dropped CommandMetadata")
	}
}

func TestClusterRoutingUsesCommandMetadataOverride(t *testing.T) {
	c := newMetadataTestCluster(t, &CommandMetadataConfig{Overrides: map[string]*CommandInfo{
		"GET": {
			Name:  "get",
			Flags: []string{"readonly"},
			KeySpecs: []KeySpec{{
				Flags:       []string{"RO", "access"},
				BeginSearch: "index",
				Index:       2,
				FindKeys:    "range",
				LastKey:     0,
				KeyStep:     1,
			}},
		},
	}})

	cmd := NewStringCmd(context.Background(), "get", "ignored", []byte("actual"))
	cmd.SetFirstKeyPos(1) // A constructor hint must not override shared metadata.
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if decision.firstKey != 2 || decision.keyless || !decision.readOnly {
		t.Fatalf("unexpected decision: first=%d keyless=%v readonly=%v",
			decision.firstKey, decision.keyless, decision.readOnly)
	}
	if decision.policy == nil || decision.policy.Response != routing.RespDefaultHashSlot {
		t.Fatalf("metadata policy was not derived: %#v", decision.policy)
	}
	if got, want := c.cmdSlotWithDecision(cmd, decision, -1), hashtag.Slot("actual"); got != want {
		t.Fatalf("slot=%d, want %d", got, want)
	}
}

func TestClusterRoutingDecisionUsesCapturedMetadataGeneration(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	cmd := NewStringCmd(context.Background(), "get", "first", "second")

	firstView := c.metadataView()
	firstDecision := c.routingDecisionInView(context.Background(), cmd, firstView,
		firstView.routingTable["get"].policy)
	if got, want := c.cmdSlotWithDecision(cmd, firstDecision, -1), hashtag.Slot("first"); got != want {
		t.Fatalf("first slot=%d, want %d", got, want)
	}

	secondView := buildCommandMetadataView(nil, map[string]*CommandInfo{
		"get": {
			Name: "get", Flags: []string{"readonly"},
			KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 2, FindKeys: "range", LastKey: 0, KeyStep: 1}},
		},
	})
	secondDecision := c.routingDecisionInView(context.Background(), cmd, secondView,
		secondView.routingTable["get"].policy)
	if got, want := c.cmdSlotWithDecision(cmd, secondDecision, -1), hashtag.Slot("second"); got != want {
		t.Fatalf("refreshed slot=%d, want %d", got, want)
	}
}

func TestClusterRoutingDerivesFirstKeyWithoutConstructorHints(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	tests := []struct {
		name string
		cmd  Cmder
		want int
	}{
		{
			name: "XREAD limited incomplete range",
			cmd:  NewCmd(ctx, "xread", "count", 1, "streams", "stream", "0"),
			want: 4,
		},
		{
			name: "SORT_RO usable spec beside unknown sibling",
			cmd:  NewCmd(ctx, "sort_ro", "source", "alpha"),
			want: 1,
		},
		{
			name: "MIGRATE KEYS alternative",
			cmd:  NewCmd(ctx, "migrate", "host", 6379, "", 0, 1000, "keys", "one", "two"),
			want: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Metadata routing ignores constructor key hints.
			tt.cmd.SetFirstKeyPos(2)
			decision := c.commandRoutingDecision(ctx, tt.cmd)
			if decision.policyErr != nil || decision.firstKey != tt.want {
				t.Fatalf("routing decision first=%d err=%v, want first=%d",
					decision.firstKey, decision.policyErr, tt.want)
			}
		})
	}
}

func TestClusterMSetEXSplitPreservesSuffixAndWireArgs(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	cmd := NewIntCmd(
		ctx,
		"msetex", 2,
		[]byte("{one}key"), 42,
		"{two}key", []byte("value"),
		"px", int64(10),
	)
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policy == nil || decision.policy.Request != routing.ReqMultiShard ||
		!decision.planOK || !decision.plan.splittable {
		t.Fatalf("unexpected MSETEX decision: policy=%#v plan=%#v ok=%v",
			decision.policy, decision.plan, decision.planOK)
	}

	sub, err := c.createSlotSpecificCommand(ctx, cmd,
		[]interface{}{cmd.Args()[2], cmd.Args()[3]}, 1, decision.plan)
	if err != nil {
		t.Fatal(err)
	}
	want := []interface{}{"msetex", 1, []byte("{one}key"), 42, "px", int64(10)}
	if !reflect.DeepEqual(sub.Args(), want) {
		t.Fatalf("subcommand args=%#v, want %#v", sub.Args(), want)
	}
}

func TestClusterMultiShardAllowsBinaryMarshalerValues(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	for _, cmd := range []Cmder{
		NewStatusCmd(context.Background(), "mset", "{same}one", clusterBinaryKey("one"), "{same}two", clusterBinaryKey("two")),
		NewStatusCmd(context.Background(), "mset", "{one}key", clusterBinaryKey("one"), "{two}key", clusterBinaryKey("two")),
	} {
		decision := c.commandRoutingDecision(context.Background(), cmd)
		if decision.policyErr != nil {
			t.Fatalf("MSET with BinaryMarshaler values was rejected: %v", decision.policyErr)
		}
	}
}

func TestClusterConditionalMSetEXFailsBeforeCrossSlotDispatch(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	for _, condition := range []string{"NX", "XX"} {
		cmd := NewIntCmd(
			ctx, "msetex", 2,
			"{one}key", "one", "{two}key", "two", condition, "px", 10,
		)
		decision := c.commandRoutingDecision(ctx, cmd)
		if err := c.executeMultiShard(ctx, cmd, decision.policy, decision); !errors.Is(err, ErrCrossSlot) {
			t.Fatalf("condition %s error=%v, want ErrCrossSlot", condition, err)
		}
	}
}

func TestClusterMultiShardSumAggregatesOncePerShard(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	original := NewIntCmd(ctx, "exists", "a", "b", "c")
	first := NewIntCmd(ctx, "exists", "a", "b")
	first.SetVal(2)
	second := NewIntCmd(ctx, "exists", "c")
	second.SetVal(1)
	results := make(chan slotResult, 2)
	results <- slotResult{cmd: first, keys: []string{"a", "b"}}
	results <- slotResult{cmd: second, keys: []string{"c"}}
	close(results)

	policy := &routing.CommandPolicy{Response: routing.RespAggSum}
	if err := c.aggregateMultiSlotResults(ctx, original, results, nil, policy); err != nil {
		t.Fatal(err)
	}
	if got := original.Val(); got != 3 {
		t.Fatalf("aggregated sum=%d, want 3 (one contribution per shard)", got)
	}
}

func TestClusterSpecialPoliciesFailClosed(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	unsupported := c.commandRoutingDecision(ctx, NewCmd(ctx, "info"))
	if !errors.Is(unsupported.policyErr, errUnsupportedRoutingPolicy) {
		t.Fatalf("INFO special policy error=%v, want %v", unsupported.policyErr, errUnsupportedRoutingPolicy)
	}

	supported := c.commandRoutingDecision(ctx, NewCmd(ctx, "ft.cursor", "read", "idx", "1"))
	if supported.policyErr != nil || supported.policy == nil || supported.policy.Request != routing.ReqSpecial {
		t.Fatalf("FT.CURSOR READ should retain its handler: policy=%#v err=%v",
			supported.policy, supported.policyErr)
	}
}

func TestClusterPipelineRejectsSpecialRequestBeforeDispatch(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.state.state.Store(&clusterState{generation: 1, nodes: c.nodes})
	ctx := context.Background()
	cmd := NewMapStringInterfaceCmd(ctx, "ft.cursor", "read", "idx", 42)
	route := c.resolvePipelineRouting(ctx, []Cmder{cmd})
	err := c.mapCmdsByNodeInView(ctx, newCmdsMap(), []Cmder{cmd}, route)
	if err == nil || !errors.Is(cmd.Err(), err) {
		t.Fatalf("special request pipeline error=%v cmd error=%v", err, cmd.Err())
	}
}

func TestClusterPipelineAllowsOnlySingleSlotMultiShardInvocations(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()

	singleSlot := NewIntCmd(ctx, "del", "{same}one", "{same}two")
	decision := c.commandRoutingDecision(ctx, singleSlot)
	if decision.policy == nil || decision.policy.Request != routing.ReqMultiShard {
		t.Fatalf("DEL policy=%#v, want multi_shard", decision.policy)
	}
	if err := c.pipelineRoutingError(singleSlot, decision); err != nil {
		t.Fatalf("single-slot DEL was rejected from pipeline: %v", err)
	}

	crossSlot := NewIntCmd(ctx, "del", "{one}key", "{two}key")
	decision = c.commandRoutingDecision(ctx, crossSlot)
	if err := c.pipelineRoutingError(crossSlot, decision); err == nil {
		t.Fatal("cross-slot DEL was admitted to a single-node pipeline")
	}

	constructorConflict := NewStatusCmd(ctx, "mset", "{same}one", "other-slot", "{same}two", "value")
	constructorConflict.SetFirstKeyPos(2)
	decision = c.commandRoutingDecision(ctx, constructorConflict)
	if decision.firstKey != 1 {
		t.Fatalf("constructor position overrode metadata: first key=%d, want 1", decision.firstKey)
	}
	if err := c.pipelineRoutingError(constructorConflict, decision); err != nil {
		t.Fatalf("metadata-consistent MSET was rejected because of a constructor hint: %v", err)
	}

	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{Request: routing.ReqMultiShard}
	}))
	custom := NewIntCmd(ctx, "del", "{same}one", "{same}two")
	decision = c.commandRoutingDecision(ctx, custom)
	if err := c.pipelineRoutingError(custom, decision); err == nil {
		t.Fatal("custom multi-shard DEL policy was weakened by matching static metadata")
	}
}

func TestClusterTxRoutingOnlyAdaptsConnectionLocalPing(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()

	ping := NewStatusCmd(ctx, "ping")
	decision := c.commandRoutingDecision(ctx, ping)
	if decision.policy == nil || decision.policy.Request != routing.ReqAllShards {
		t.Fatalf("PING policy=%#v, want all_shards", decision.policy)
	}
	if err := c.txRoutingError(ping, decision); err != nil {
		t.Fatalf("transaction-local PING was rejected: %v", err)
	}

	flushAll := NewStatusCmd(ctx, "flushall")
	decision = c.commandRoutingDecision(ctx, flushAll)
	if decision.policy == nil || decision.policy.Request != routing.ReqAllShards {
		t.Fatalf("FLUSHALL policy=%#v, want all_shards", decision.policy)
	}
	if err := c.txRoutingError(flushAll, decision); err == nil {
		t.Fatal("transaction-local FLUSHALL unexpectedly bypassed its all-shards policy")
	}
}

func TestClusterCursorRoutingAcceptsTypedIntegerID(t *testing.T) {
	cmd := NewMapStringInterfaceCmd(context.Background(), "ft.cursor", "read", "idx", 42)
	if got, err := cursorRoutingKey(cmd); err != nil || got != "42" {
		t.Fatalf("cursor routing key=%q err=%v, want 42", got, err)
	}
}

func TestClusterCursorRoutingSelectsOnlyCursorOwner(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	picker := &countingClusterShardPicker{}
	c.opt.ShardPicker = picker

	discarded, _ := c.nodes.GetOrCreate("127.0.0.1:7051")
	owner, _ := c.nodes.GetOrCreate("127.0.0.1:7052")
	cursorID := 42
	slot := clusterKeySlot("42")
	installMetadataClusterState(
		c, []*clusterNode{discarded, owner},
		&clusterSlot{start: slot, end: slot, nodes: []*clusterNode{owner}},
	)

	discardedCalls, ownerCalls := 0, 0
	discarded.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
		discardedCalls++
		return nil
	}})
	owner.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		ownerCalls++
		cmd.(*MapStringInterfaceCmd).SetVal(map[string]interface{}{})
		return nil
	}})

	cmd := NewMapStringInterfaceCmd(ctx, "ft.cursor", "read", "idx", cursorID)
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if discardedCalls != 0 || ownerCalls != 1 || picker.calls != 0 {
		t.Fatalf("discarded=%d owner=%d picker=%d, want 0/1/0", discardedCalls, ownerCalls, picker.calls)
	}
}

func TestClusterCursorRoutingHonorsMovedTarget(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	oldOwner, _ := c.nodes.GetOrCreate("127.0.0.1:7061")
	newOwner, _ := c.nodes.GetOrCreate("127.0.0.1:7062")
	cursorID := 42
	slot := clusterKeySlot("42")
	installMetadataClusterState(
		c, []*clusterNode{oldOwner},
		&clusterSlot{start: slot, end: slot, nodes: []*clusterNode{oldOwner}},
	)

	oldCalls, newCalls := 0, 0
	oldOwner.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
		oldCalls++
		return fmt.Errorf("MOVED %d 127.0.0.1:7062", slot)
	}})
	newOwner.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		newCalls++
		cmd.(*MapStringInterfaceCmd).SetVal(map[string]interface{}{})
		return nil
	}})

	cmd := NewMapStringInterfaceCmd(ctx, "ft.cursor", "read", "idx", cursorID)
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if oldCalls != 1 || newCalls != 1 {
		t.Fatalf("old=%d new=%d, want 1/1", oldCalls, newCalls)
	}
}

func TestClusterUnknownMultiShardPolicyDoesNotDispatch(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespAggSum}
	}))
	ctx := context.Background()
	cmd := NewIntCmd(ctx, "unknown", "a", "b")
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.planOK {
		t.Fatal("unknown command unexpectedly produced an exact key plan")
	}
	if err := c.executeMultiShard(ctx, cmd, decision.policy, decision); err == nil {
		t.Fatal("unknown multi-shard command was not rejected before dispatch")
	}
}

func TestClusterMissingMetadataUsesLegacyKeyHints(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()

	cmd := NewCmd(ctx, "module.future", "ignored", "key")
	cmd.SetFirstKeyPos(2)
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policyErr != nil || decision.metaOK || decision.firstKey != 2 {
		t.Fatalf("unknown command decision=%#v, want explicit first key 2", decision)
	}
	if got, want := decision.naturalSlot, hashtag.Slot("key"); got != want {
		t.Fatalf("unknown command slot=%d, want %d", got, want)
	}

	raw := c.commandRoutingDecision(ctx, NewCmd(ctx, "module.future", "key"))
	if raw.policyErr != nil || raw.firstKey != 1 {
		t.Fatalf("raw unknown command decision=%#v, want legacy first key 1", raw)
	}
}

func TestClusterDefaultRoutingFailsClosedForUnusableMetadata(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		view    *commandMetadataView
		cmd     Cmder
		wantErr error
	}{
		{
			name: "live tombstone",
			view: buildCommandMetadataView(
				map[string]*CommandInfo{"flushall": nil},
				nil,
			),
			cmd:     NewStatusCmd(ctx, "flushall"),
			wantErr: errClusterCommandMetadataUnusable,
		},
		{
			name: "malformed resolved record",
			view: buildCommandMetadataView(nil, map[string]*CommandInfo{
				"get": {
					Name: "get",
					Tips: []string{"request_policy:all_shards", "request_policy:all_nodes"},
				},
			}),
			cmd:     NewStringCmd(ctx, "get", "key"),
			wantErr: errClusterCommandMetadataUnusable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			c.cmdMeta.current.Store(tt.view)
			decision := c.commandRoutingDecision(ctx, tt.cmd)
			if !errors.Is(decision.policyErr, tt.wantErr) {
				t.Fatalf("routing error=%v, want %v", decision.policyErr, tt.wantErr)
			}
			// Unusable records fail before topology lookup or dispatch.
			if err := c.process(ctx, tt.cmd); !errors.Is(err, tt.wantErr) {
				t.Fatalf("process error=%v, want %v", err, tt.wantErr)
			}
			if c.state.state.Load() != nil {
				t.Fatal("metadata failure unexpectedly reached topology routing")
			}
		})
	}
}

func TestClusterCustomRoutingAlgorithmMayClassifyUnknownCommandAsKeyless(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		}
	}))
	cmd := NewCmd(context.Background(), "module.future", "key")
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if decision.policyErr != nil || decision.metaOK || decision.firstKey != 0 || !decision.keyless {
		t.Fatalf("custom unknown-command decision=%#v", decision)
	}
}

func TestClusterCustomRoutingPolicyControlsReplicaEligibility(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		cmd      Cmder
		policy   *routing.CommandPolicy
		readOnly bool
	}{
		{
			name: "remove readonly from GET",
			cmd:  NewStringCmd(ctx, "get", "key"),
			policy: &routing.CommandPolicy{
				Request: routing.ReqDefault, Response: routing.RespDefaultHashSlot,
			},
		},
		{
			name: "add readonly to SET",
			cmd:  NewStatusCmd(ctx, "set", "key", "value"),
			policy: &routing.CommandPolicy{
				Request: routing.ReqDefault, Response: routing.RespDefaultHashSlot,
				Tips: map[string]string{routing.ReadOnlyCMD: ""},
			},
			readOnly: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
				return tt.policy
			}))
			decision := c.commandRoutingDecision(ctx, tt.cmd)
			if decision.readOnly != tt.readOnly {
				t.Fatalf("readOnly=%v, want %v", decision.readOnly, tt.readOnly)
			}
		})
	}
}

func TestClusterKeylessReadOnlyCommandSelectsNodeOnce(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.opt.ReadOnly = true
	picker := &countingClusterShardPicker{index: 1}
	c.opt.ShardPicker = picker
	master, _ := c.nodes.GetOrCreate("127.0.0.1:7651")
	replica, _ := c.nodes.GetOrCreate("127.0.0.1:7652")
	state := installMetadataClusterState(c, []*clusterNode{master})
	state.Slaves = []*clusterNode{replica}
	masterCalls, replicaCalls := 0, 0
	master.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
		masterCalls++
		return nil
	}})
	replica.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		replicaCalls++
		cmd.(*StatusCmd).SetVal("PONG")
		return nil
	}})
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{
			Request: routing.ReqDefault, Response: routing.RespDefaultKeyless,
			Tips: map[string]string{routing.ReadOnlyCMD: ""},
		}
	}))
	if err := c.process(context.Background(), NewStatusCmd(context.Background(), "ping")); err != nil {
		t.Fatal(err)
	}
	if picker.calls != 1 || masterCalls != 0 || replicaCalls != 1 {
		t.Fatalf("picker/master/replica calls=%d/%d/%d, want 1/0/1", picker.calls, masterCalls, replicaCalls)
	}
}

func TestClusterCustomHashSlotPolicyCannotInventUnknownKeyPosition(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		}
	}))
	cmd := NewCmd(context.Background(), "module.future", "key")
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if decision.policyErr == nil || decision.firstKey != -1 {
		t.Fatalf("custom keyed unknown-command decision=%#v, want unresolved-key error", decision)
	}
}

func TestClusterPre810LiveMetadataStillRoutesButDoesNotEnableCSC(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	view := buildCommandMetadataViewForServer(map[string]*CommandInfo{
		"future.read": {
			Name: "future.read", Flags: []string{"readonly"},
			KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1, FindKeys: "range", LastKey: 0, KeyStep: 1}},
		},
	}, nil, "7.2.0")
	view.live = true
	c.cmdMeta.current.Store(view)

	cmd := NewStringCmd(context.Background(), "future.read", "key")
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if decision.policy == nil || !decision.readOnly || decision.firstKey != 1 {
		t.Fatalf("older live metadata was not usable for routing: %#v", decision)
	}
	if isCacheableInView(view, cmd) {
		t.Fatal("pre-8.10 live-only command unexpectedly became CSC eligible")
	}
}

func TestClusterReplicaEligibilityUsesReadonlyFlagOnly(t *testing.T) {
	c := newMetadataTestCluster(t, &CommandMetadataConfig{Overrides: map[string]*CommandInfo{
		"module.write": {
			Name: "module.write", Tips: []string{"readonly"},
			KeySpecs: []KeySpec{{
				Flags: []string{"RW", "update"}, BeginSearch: "index", Index: 1,
				FindKeys: "range", LastKey: 0, KeyStep: 1,
			}},
		},
	}})
	c.opt.ReadOnly = true
	ctx := context.Background()
	master, _ := c.nodes.GetOrCreate("127.0.0.1:7081")
	replica, _ := c.nodes.GetOrCreate("127.0.0.1:7082")
	slot := clusterKeySlot("key")
	state := installMetadataClusterState(
		c, []*clusterNode{master},
		&clusterSlot{start: slot, end: slot, nodes: []*clusterNode{master, replica}},
	)
	state.Slaves = []*clusterNode{replica}

	masterCalls, replicaCalls := 0, 0
	master.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		masterCalls++
		cmd.(*StatusCmd).SetVal("OK")
		return nil
	}})
	replica.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
		replicaCalls++
		return nil
	}})

	cmd := NewStatusCmd(ctx, "module.write", "key")
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.readOnly {
		t.Fatal("a readonly tip without the authoritative command flag enabled replica routing")
	}
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if masterCalls != 1 || replicaCalls != 0 {
		t.Fatalf("master=%d replica=%d, want 1/0", masterCalls, replicaCalls)
	}
}

func TestClusterDynamicResolverFallsBackAndRetries(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.cmdMeta.stopAndJoin()
	calls := 0
	c.cmdMeta = newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
		calls++
		if calls == 1 {
			return commandMetadataFetchResult{}, fmt.Errorf("COMMAND denied")
		}
		return commandMetadataFetchResult{
			records: map[string]*CommandInfo{
				"get": {
					Name: "get", Flags: []string{"readonly"},
					KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 2, FindKeys: "range", LastKey: 0, KeyStep: 1}},
				},
			},
			serverVersion:     "8.10.0",
			serverFingerprint: "8.10.0",
		}, nil
	})
	c.SetCommandInfoResolver(c.NewDynamicResolver())

	first := c.commandRoutingDecision(context.Background(), NewStringCmd(context.Background(), "get", "one", "two"))
	if first.firstKey != 1 || calls != 1 {
		t.Fatalf("first fallback: key=%d calls=%d, want key=1 calls=1", first.firstKey, calls)
	}
	second := c.commandRoutingDecision(context.Background(), NewStringCmd(context.Background(), "get", "one", "two"))
	if second.firstKey != 2 || calls != 2 || !second.view.live {
		t.Fatalf("retry upgrade: key=%d calls=%d live=%v, want key=2 calls=2 live",
			second.firstKey, calls, second.view.live)
	}
}

func TestClusterTopologyGenerationValidation(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.state.state.Store(&clusterState{generation: 41})
	if !c.isTopologyGeneration(41) {
		t.Fatal("current topology generation was rejected")
	}
	c.state.state.Store(&clusterState{generation: 42})
	if c.isTopologyGeneration(41) {
		t.Fatal("metadata fetch generation survived a topology change")
	}
}

func TestClusterMetadataFingerprintValidation(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		actual   string
		match    bool
		wantErr  error
	}{
		{name: "missing identity", expected: "8.10.0", wantErr: errClusterMetadataMissingFingerprint},
		{name: "first identity", actual: "8.10.0", match: true},
		{name: "same identity", expected: "8.10.0", actual: "8.10.0", match: true},
		{name: "changed identity", expected: "8.10.0", actual: "8.10.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := clusterMetadataFingerprintMatches(tt.expected, tt.actual)
			if match != tt.match || !errors.Is(err, tt.wantErr) {
				t.Fatalf("match=%v err=%v, want %v/%v", match, err, tt.match, tt.wantErr)
			}
		})
	}
}

func TestClusterTransactionChecksEveryMetadataKey(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()

	tests := []struct {
		name      string
		cmd       Cmder
		wantSlots int
	}{
		{
			name:      "same-slot range keys",
			cmd:       NewStringSliceCmd(ctx, "mget", "{one}first", "{one}second"),
			wantSlots: 1,
		},
		{
			name:      "cross-slot range keys",
			cmd:       NewStringSliceCmd(ctx, "mget", "{one}first", "{two}second"),
			wantSlots: 2,
		},
		{
			name: "cross-slot keynum keys with suffix",
			cmd: NewIntCmd(ctx, "msetex", 2,
				"{one}first", "one", "{two}second", "two", "px", 10),
			wantSlots: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmds := []Cmder{tt.cmd}
			got, err := c.slottedKeyedCommandsInRouting(ctx, cmds, c.resolvePipelineRouting(ctx, cmds))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tt.wantSlots {
				t.Fatalf("slots=%d, want %d", len(got), tt.wantSlots)
			}
		})
	}
}

func TestClusterTransactionPreparesDynamicMetadataBeforeKeyValidation(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.cmdMeta.stopAndJoin()
	called := 0
	c.cmdMeta = newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
		called++
		return commandMetadataFetchResult{
			records: map[string]*CommandInfo{
				"future.multi": {
					Name: "future.multi",
					KeySpecs: []KeySpec{{
						Flags:       []string{"RW", "access"},
						BeginSearch: "index", Index: 1,
						FindKeys: "range", LastKey: -1, KeyStep: 1,
					}},
				},
			},
			serverVersion:     "8.10.0",
			serverFingerprint: "8.10.0",
		}, nil
	})
	c.SetCommandInfoResolver(c.NewDynamicResolver())

	ctx := context.Background()
	cmd := NewStringSliceCmd(ctx, "future.multi", "{one}first", "{two}second")
	err := c.processTxPipeline(ctx, wrapMultiExec(ctx, []Cmder{cmd}))
	if !errors.Is(err, ErrCrossSlot) || called != 1 {
		t.Fatalf("transaction error=%v fetches=%d, want CROSSSLOT after one live fetch", err, called)
	}
}

func TestClusterTransactionRejectsMalformedCompleteKeyPlan(t *testing.T) {
	c := newMetadataTestCluster(t, &CommandMetadataConfig{Overrides: map[string]*CommandInfo{
		"broken": {
			Name: "broken",
			KeySpecs: []KeySpec{{
				Flags:       []string{"RW", "access"},
				BeginSearch: "index", Index: 1,
				FindKeys: "range", LastKey: -1, KeyStep: 1,
			}},
		},
	}})
	ctx := context.Background()
	cmd := NewCmd(ctx, "broken")
	err := c.processTxPipeline(ctx, wrapMultiExec(ctx, []Cmder{cmd}))
	if err == nil || !errors.Is(cmd.Err(), err) {
		t.Fatalf("malformed complete key plan error=%v cmd error=%v", err, cmd.Err())
	}
}

func TestClusterTransactionRejectsNonLocalRoutingPolicies(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	tests := []Cmder{
		NewStatusCmd(ctx, "flushall"),
		NewStatusCmd(ctx, "acl", "save"),
		NewMapStringInterfaceCmd(ctx, "ft.cursor", "read", "idx", 42),
		NewCmd(ctx, "info"),
	}
	for _, cmd := range tests {
		t.Run(cmd.FullName(), func(t *testing.T) {
			err := c.processTxPipeline(ctx, wrapMultiExec(ctx, []Cmder{cmd}))
			if err == nil || !errors.Is(cmd.Err(), err) {
				t.Fatalf("transaction policy error=%v cmd error=%v", err, cmd.Err())
			}
		})
	}
}

func TestClusterTransactionRoutingPoliciesCanBeDisabled(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.opt.DisableRoutingPolicies = true
	ctx := context.Background()
	cmd := NewStatusCmd(ctx, "flushall")
	route := c.resolvePipelineRouting(ctx, []Cmder{cmd})
	if _, err := c.slottedKeyedCommandsInRouting(ctx, []Cmder{cmd}, route); err != nil {
		t.Fatalf("disabled routing policies still rejected transaction command: %v", err)
	}
}

func TestClusterDisabledRoutingPoliciesUseLegacyMetadataFreeRoute(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{
		Addrs:                  []string{"127.0.0.1:1"},
		DisableRoutingPolicies: true,
		CommandMetadata: &CommandMetadataConfig{
			Mode: CommandMetadataPreferLive,
			Overrides: map[string]*CommandInfo{
				"ft.search": {
					Name:  "ft.search",
					Flags: []string{"readonly"},
					KeySpecs: []KeySpec{{
						Flags:       []string{"RO", "access"},
						BeginSearch: "index", Index: 1,
						FindKeys: "range", LastKey: 0, KeyStep: 1,
					}},
				},
			},
		},
	})
	t.Cleanup(func() { _ = c.Close() })

	resolverCalls := 0
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		resolverCalls++
		return &routing.CommandPolicy{Request: routing.ReqAllShards}
	}))
	cmd := NewSliceCmd(context.Background(), "ft.search", "index", "query")
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if resolverCalls != 0 {
		t.Fatalf("disabled routing invoked resolver %d times", resolverCalls)
	}
	if !decision.keyless || decision.firstKey != 0 || decision.policy != nil || decision.naturalSlot != -1 {
		t.Fatalf("disabled route=%#v, want legacy keyless FT.SEARCH", decision)
	}
	c.cmdMeta.mu.Lock()
	mode, started := c.cmdMeta.mode, c.cmdMeta.started
	c.cmdMeta.mu.Unlock()
	if mode != CommandMetadataStatic || started {
		t.Fatalf("disabled metadata store mode=%d started=%v, want inert static", mode, started)
	}

	batchRoute := c.resolvePipelineRouting(context.Background(), []Cmder{cmd})
	if got := batchRoute.decisions[cmd]; !got.keyless || got.firstKey != 0 || got.policy != nil {
		t.Fatalf("disabled pipeline route=%#v, want legacy keyless", got)
	}
	if resolverCalls != 0 {
		t.Fatalf("disabled pipeline invoked resolver %d times", resolverCalls)
	}
	autoDecision := c.autoPipelineRoutingDecision(context.Background(), cmd)
	if !autoDecision.keyless || autoDecision.firstKey != 0 || autoDecision.view != nil {
		t.Fatalf("disabled AutoPipeline route=%#v, want legacy keyless", autoDecision)
	}
	if _, cached := c.peekAutoPipelineRoutingDecision(cmd); cached {
		t.Fatal("disabled AutoPipeline retained a metadata admission decision")
	}
	if resolverCalls != 0 {
		t.Fatalf("disabled AutoPipeline invoked resolver %d times", resolverCalls)
	}
}

func TestClusterStaticMetadataStoreIsInert(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	if c.cmdMeta == nil || c.cmdMeta.view() != defaultCommandMetadataView {
		t.Fatal("default cluster does not share the static metadata view")
	}
	c.cmdMeta.mu.Lock()
	started := c.cmdMeta.started
	c.cmdMeta.mu.Unlock()
	if started {
		t.Fatal("default static cluster started a metadata worker")
	}
	if c.state.state.Load() != nil {
		t.Fatal("default static metadata construction performed network topology work")
	}
}

func TestClusterMetadataStoreCleanupDoesNotRetainDroppedClient(t *testing.T) {
	makeDroppedClient := func() *commandMetadataStore {
		client := NewClusterClient(&ClusterOptions{
			Addrs: []string{"127.0.0.1:1"},
			CommandMetadata: &CommandMetadataConfig{
				Mode: CommandMetadataPreferLive,
			},
		})
		return client.cmdMeta
	}
	store := makeDroppedClient()
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-store.stop:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("dropped ClusterClient remained rooted by metadata fetch worker")
		}
		runtime.GC()
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClusterPipelineResolvesCustomPoliciesOnceAndSkipsUnusedDynamicFallback(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.cmdMeta.stopAndJoin()
	fetches := 0
	c.cmdMeta = newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
		fetches++
		return commandMetadataFetchResult{}, fmt.Errorf("unexpected live fetch")
	})

	customCalls := 0
	custom := NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		customCalls++
		return &routing.CommandPolicy{Request: routing.ReqDefault, Response: routing.RespDefaultHashSlot}
	})
	custom.SetFallbackResolver(c.NewDynamicResolver())
	c.SetCommandInfoResolver(custom)

	ctx := context.Background()
	cmds := []Cmder{
		NewStringCmd(ctx, "get", "one"),
		NewStringCmd(ctx, "get", "two"),
	}
	route := c.resolvePipelineRouting(ctx, cmds)
	if customCalls != len(cmds) || fetches != 0 {
		t.Fatalf("custom calls=%d fetches=%d, want %d/0", customCalls, fetches, len(cmds))
	}
	for _, cmd := range cmds {
		_ = c.pipelineDecision(ctx, cmd, route)
		_ = c.pipelineDecision(ctx, cmd, route)
	}
	if customCalls != len(cmds) {
		t.Fatalf("cached pipeline decisions re-invoked custom resolver: calls=%d", customCalls)
	}
}

func TestClusterAutoPipelineReusesAndCleansAdmissionDecision(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.AddHook(clusterRoutingShortCircuitHook{})

	calls := 0
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		calls++
		return &routing.CommandPolicy{Request: routing.ReqDefault, Response: routing.RespDefaultHashSlot}
	}))
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{NumShards: 2})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "key")
	if err := ap.Process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("custom resolver calls=%d, want 1 across admission, sharding, and flush", calls)
	}
	if _, cached := c.peekAutoPipelineRoutingDecision(cmd); cached {
		t.Fatal("completed AutoPipeline command retained its admission decision")
	}

	// Preflight releases admission state because rejected commands never dispatch.
	c.SetCommandInfoResolver(newCommandMetadataPolicyResolver(c.metadataView))
	rejected := NewStringSliceCmd(ctx, "mget", "one", "two")
	if err := ap.Process(ctx, rejected); err == nil {
		t.Fatal("cross-slot AutoPipeline command was not rejected")
	}
	if _, cached := c.peekAutoPipelineRoutingDecision(rejected); cached {
		t.Fatal("rejected AutoPipeline command retained its admission decision")
	}

	sameSlot := NewStringSliceCmd(ctx, "mget", "{same}one", "{same}two")
	if err := ap.Process(ctx, sameSlot); err != nil {
		t.Fatalf("same-slot AutoPipeline command was rejected: %v", err)
	}
	if _, cached := c.peekAutoPipelineRoutingDecision(sameSlot); cached {
		t.Fatal("completed same-slot AutoPipeline command retained its admission decision")
	}
}

func TestClusterAutoPipelinePinsAdmissionMetadataGeneration(t *testing.T) {
	for _, disablePolicies := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable-policies=%v", disablePolicies), func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			c.opt.DisableRoutingPolicies = disablePolicies
			first := buildCommandMetadataView(nil, map[string]*CommandInfo{
				"get": {
					Name: "get", Flags: []string{"readonly"},
					KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 1, FindKeys: "range", LastKey: 0, KeyStep: 1}},
				},
			})
			second := buildCommandMetadataView(nil, map[string]*CommandInfo{
				"get": {
					Name: "get", Flags: []string{"readonly"},
					KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 2, FindKeys: "range", LastKey: 0, KeyStep: 1}},
				},
			})
			c.cmdMeta.current.Store(first)
			ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{NumShards: 4})
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			cmd := NewStringCmd(ctx, "get", "first", "second")
			if ap.mustDivert(ctx, cmd) {
				t.Fatal("ordinary GET was unexpectedly diverted")
			}
			if err := ap.preflight(ctx, cmd); err != nil {
				t.Fatal(err)
			}
			c.cmdMeta.current.Store(second)

			wantShard := hashtag.Slot("first") * ap.numShards() / 16384
			if got := ap.shardFn(cmd); got != wantShard {
				t.Fatalf("shard=%d, want admission-generation shard %d", got, wantShard)
			}
			route := c.resolvePipelineRouting(ctx, []Cmder{cmd})
			decision := route.decisions[cmd]
			wantView := first
			if disablePolicies {
				wantView = nil
			}
			if decision.view != wantView || decision.firstKey != 1 {
				t.Fatalf("flush decision view=%p first=%d, want view=%p first=1",
					decision.view, decision.firstKey, wantView)
			}
		})
	}
}

func TestClusterDynamicResolverRetiresLiveViewOnTopologyReload(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.cmdMeta.stopAndJoin()
	calls := 0
	c.cmdMeta = newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
		calls++
		if calls == 1 {
			// Initial topology publication must not invalidate its live fetch.
			c.state.onReload(&clusterState{}, nil)
		}
		keyPos := 2
		if calls > 1 {
			keyPos = 3
		}
		return commandMetadataFetchResult{
			records: map[string]*CommandInfo{
				"get": {
					Name: "get", Flags: []string{"readonly"},
					KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: keyPos, FindKeys: "range", LastKey: 0, KeyStep: 1}},
				},
			},
			serverVersion:     "8.10.0",
			serverFingerprint: fmt.Sprintf("8.10.0|generation:%d", calls),
		}, nil
	})
	c.SetCommandInfoResolver(c.NewDynamicResolver())

	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "one", "two", "three")
	if got := c.commandRoutingDecision(ctx, cmd).firstKey; got != 2 {
		t.Fatalf("first live key position=%d, want 2", got)
	}
	liveView := c.cmdMeta.view()
	c.state.onReload(&clusterState{}, &clusterState{})
	if c.cmdMeta.view() != liveView || !c.cmdMeta.view().live || calls != 1 {
		t.Fatalf("identical topology retired live metadata: view=%p want=%p live=%v calls=%d",
			c.cmdMeta.view(), liveView, c.cmdMeta.view().live, calls)
	}
	// A later topology reload invalidates any live view.
	changed := &clusterState{slots: []*clusterSlot{{start: 1}}}
	previous := &clusterState{}
	c.state.beforeReload(changed, previous)
	c.state.onReload(changed, previous)
	if c.cmdMeta.view().live {
		t.Fatal("topology reload did not retire dynamic live metadata")
	}
	if got := c.commandRoutingDecision(ctx, cmd).firstKey; got != 3 || calls != 2 {
		t.Fatalf("refetched key position=%d calls=%d, want 3/2", got, calls)
	}
}

func TestClusterTopologyPublicationRejectsInFlightMetadata(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.cmdMeta.stopAndJoin()
	started := make(chan struct{})
	release := make(chan struct{})
	c.cmdMeta = newCommandMetadataStoreForLive(nil, func(context.Context) (commandMetadataFetchResult, error) {
		close(started)
		<-release
		return commandMetadataFetchResult{
			records: map[string]*CommandInfo{
				"get": {
					Name: "get", Flags: []string{"readonly"},
					KeySpecs: []KeySpec{{Flags: []string{"RO", "access"}, BeginSearch: "index", Index: 2, FindKeys: "range", LastKey: 0, KeyStep: 1}},
				},
			},
			serverVersion: "8.10.0", serverFingerprint: "8.10.0",
		}, nil
	})

	oldState := &clusterState{generation: 1}
	newState := &clusterState{generation: 2, slots: []*clusterSlot{{start: 1, end: 1}}}
	c.state.state.Store(oldState)
	c.state.load = func(context.Context) (*clusterState, error) { return newState, nil }

	errCh := make(chan error, 1)
	go func() { errCh <- c.cmdMeta.ensureLive(context.Background()) }()
	<-started
	if _, err := c.state.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-errCh; err == nil {
		t.Fatal("metadata fetched across topology publication was accepted")
	}
	if c.cmdMeta.view().live {
		t.Fatal("old-topology live metadata was published")
	}
}

func TestClusterRoutingUsesWireFaithfulSupportedKeyTypes(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	integer := int64(42)
	cases := []struct {
		name string
		key  interface{}
		wire string
	}{
		{name: "bytes", key: []byte("{bytes}key"), wire: "{bytes}key"},
		{name: "integer pointer", key: &integer, wire: "42"},
		{name: "bool", key: true, wire: "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewStringCmd(ctx, "get", tc.key)
			decision := c.commandRoutingDecision(ctx, cmd)
			if decision.firstKey != 1 || decision.keyless {
				t.Fatalf("key was not resolved: first=%d keyless=%v", decision.firstKey, decision.keyless)
			}
			if got, want := c.cmdSlotWithDecision(cmd, decision, -1), hashtag.Slot(tc.wire); got != want {
				t.Fatalf("slot=%d, want %d for wire key %q", got, want, tc.wire)
			}
		})
	}
}

func TestClusterRoutingRejectsBinaryMarshalerKey(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	cmd := NewStringCmd(context.Background(), "get", clusterBinaryKey("{binary}key"))
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if decision.firstKey >= 0 || decision.policyErr == nil {
		t.Fatalf("BinaryMarshaler routing decision first=%d err=%v, want fail closed", decision.firstKey, decision.policyErr)
	}
}

func TestClusterRoutingUsesSharedServerCorrections(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()

	compact := c.commandRoutingDecision(ctx, NewStatusCmd(ctx, "cf.compact", "filter"))
	if compact.readOnly {
		t.Fatal("CF.COMPACT server correction still allows replica routing")
	}

	mget := c.commandRoutingDecision(ctx, NewSliceCmd(ctx, "json.mget", "{one}a", "{two}b", "$"))
	if !mget.metaOK || len(mget.meta.keySpecs) != 1 || mget.meta.keySpecs[0].lastKey != -2 {
		t.Fatalf("JSON.MGET routing metadata did not expose N keys before path: %+v", mget.meta)
	}
}

func TestClusterFanoutRejectsBinaryMarshalerArgument(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	cmd := NewCmd(context.Background(), "keys", clusterBinaryKey("*"))
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if decision.policyErr == nil {
		t.Fatal("fanout with BinaryMarshaler argument did not fail closed")
	}
}

func TestClusterFanoutResponseHandlers(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	tests := []struct {
		name  string
		cmd   Cmder
		parts []Cmder
		want  interface{}
	}{
		{
			name: "keys flatten",
			cmd:  NewStringSliceCmd(ctx, "keys", "*"),
			parts: func() []Cmder {
				a := NewStringSliceCmd(ctx, "keys", "*")
				a.SetVal([]string{"a", "b"})
				b := NewStringSliceCmd(ctx, "keys", "*")
				b.SetVal([]string{"c"})
				return []Cmder{a, b}
			}(),
			want: []string{"a", "b", "c"},
		},
		{
			name: "script exists elementwise and",
			cmd:  NewBoolSliceCmd(ctx, "script", "exists", "one", "two"),
			parts: func() []Cmder {
				a := NewBoolSliceCmd(ctx, "script", "exists", "one", "two")
				a.SetVal([]bool{true, true})
				b := NewBoolSliceCmd(ctx, "script", "exists", "one", "two")
				b.SetVal([]bool{true, false})
				return []Cmder{a, b}
			}(),
			want: []bool{true, false},
		},
		{
			name: "slowlog flatten",
			cmd:  NewSlowLogCmd(ctx, "slowlog", "get"),
			parts: func() []Cmder {
				a := NewSlowLogCmd(ctx, "slowlog", "get")
				a.SetVal([]SlowLog{{ID: 1}})
				b := NewSlowLogCmd(ctx, "slowlog", "get")
				b.SetVal([]SlowLog{{ID: 2}})
				return []Cmder{a, b}
			}(),
			want: []SlowLog{{ID: 1}, {ID: 2}},
		},
		{
			name: "waitaof elementwise min",
			cmd:  NewIntSliceCmd(ctx, "waitaof", 1, 1, 0),
			parts: func() []Cmder {
				a := NewIntSliceCmd(ctx, "waitaof", 1, 1, 0)
				a.SetVal([]int64{2, 5})
				b := NewIntSliceCmd(ctx, "waitaof", 1, 1, 0)
				b.SetVal([]int64{1, 7})
				return []Cmder{a, b}
			}(),
			want: []int64{1, 5},
		},
		{
			name: "latency reset status sum",
			cmd:  NewStatusCmd(ctx, "latency", "reset"),
			parts: func() []Cmder {
				a := NewStatusCmd(ctx, "latency", "reset")
				a.SetVal("2")
				b := NewStatusCmd(ctx, "latency", "reset")
				b.SetVal("3")
				return []Cmder{a, b}
			}(),
			want: "5",
		},
		{
			name: "randomkey skips empty shard",
			cmd:  NewStringCmd(ctx, "randomkey"),
			parts: func() []Cmder {
				a := NewStringCmd(ctx, "randomkey")
				a.SetVal("chosen")
				b := NewStringCmd(ctx, "randomkey")
				b.SetErr(Nil)
				return []Cmder{a, b}
			}(),
			want: "chosen",
		},
		{
			name: "raw integer remains integer",
			cmd:  NewCmd(ctx, "dbsize"),
			parts: func() []Cmder {
				a := NewCmd(ctx, "dbsize")
				a.SetVal(int64(1 << 53))
				b := NewCmd(ctx, "dbsize")
				b.SetVal(int64(1))
				return []Cmder{a, b}
			}(),
			want: int64(1<<53) + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := c.commandRoutingDecision(ctx, tt.cmd)
			if decision.policy == nil {
				t.Fatal("missing static fanout policy")
			}
			if err := c.aggregateResponses(tt.cmd, tt.parts, decision.policy, decision); err != nil {
				t.Fatal(err)
			}
			got, err := ExtractCommandValue(tt.cmd)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("aggregate=%#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestClusterFanoutResponseHandlerRegistryComplete(t *testing.T) {
	want := map[string]routing.ResponsePolicy{
		"keys":          routing.RespDefaultKeyless,
		"latency|reset": routing.RespAggSum,
		"randomkey":     routing.RespSpecial,
		"script|exists": routing.RespAggLogicalAnd,
		"slowlog|get":   routing.RespDefaultKeyless,
		"waitaof":       routing.RespAggMin,
	}
	if len(clusterFanoutResponseHandlers) != len(want) {
		t.Fatalf("handler count=%d, want %d", len(clusterFanoutResponseHandlers), len(want))
	}
	for name, response := range want {
		if clusterFanoutResponseHandlers[clusterFanoutResponseHandlerKey{name: name, response: response}] == nil {
			t.Errorf("missing explicit fanout response handler for %s", name)
		}
	}
}

func TestClusterFanoutHandlerHonorsEffectiveResponseOverride(t *testing.T) {
	c := newMetadataTestCluster(t, &CommandMetadataConfig{Overrides: map[string]*CommandInfo{
		"latency|reset": {
			Name: "latency|reset",
			Tips: []string{"request_policy:all_nodes", "response_policy:agg_min"},
		},
	}})
	ctx := context.Background()
	cmd := NewIntCmd(ctx, "latency", "reset")
	partOne := NewIntCmd(ctx, "latency", "reset")
	partOne.SetVal(4)
	partTwo := NewIntCmd(ctx, "latency", "reset")
	partTwo.SetVal(9)

	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policy == nil || decision.policy.Response != routing.RespAggMin {
		t.Fatalf("override policy=%#v, want agg_min", decision.policy)
	}
	if err := c.aggregateResponses(cmd, []Cmder{partOne, partTwo}, decision.policy, decision); err != nil {
		t.Fatal(err)
	}
	if got := cmd.Val(); got != 4 {
		t.Fatalf("override aggregation=%d, want min 4", got)
	}
}

func TestAggregateClusterRandomKeyFailsClosed(t *testing.T) {
	ctx := context.Background()
	emptyOne := NewStringCmd(ctx, "randomkey")
	emptyOne.SetErr(Nil)
	emptyTwo := NewStringCmd(ctx, "randomkey")
	emptyTwo.SetErr(Nil)
	if value, err := aggregateClusterRandomKey(NewStringCmd(ctx, "randomkey"), []Cmder{emptyOne, emptyTwo}); value != nil || !errors.Is(err, Nil) {
		t.Fatalf("all-empty RANDOMKEY = (%#v, %v), want (nil, redis.Nil)", value, err)
	}

	good := NewStringCmd(ctx, "randomkey")
	good.SetVal("key")
	wantErr := errors.New("shard failed")
	failed := NewStringCmd(ctx, "randomkey")
	failed.SetErr(wantErr)
	if value, err := aggregateClusterRandomKey(NewStringCmd(ctx, "randomkey"), []Cmder{good, failed}); value != nil || !errors.Is(err, wantErr) {
		t.Fatalf("partially failed RANDOMKEY = (%#v, %v), want (nil, %v)", value, err, wantErr)
	}
}

func TestClusterSpecialRequestHandlerRegistryComplete(t *testing.T) {
	for name, support := range routingSpecialPolicies {
		supported := support&routingSpecialRequestSupported != 0
		_, handled := clusterSpecialRequestHandlers[name]
		if supported != handled {
			t.Errorf("special request %s: metadata supported=%v handler registered=%v", name, supported, handled)
		}
	}
	for name := range clusterSpecialRequestHandlers {
		if routingSpecialPolicies[name]&routingSpecialRequestSupported == 0 {
			t.Errorf("handler %s is not declared supported in routing metadata", name)
		}
	}
	for name, support := range routingSpecialPolicies {
		if support&routingSpecialResponseSupported != 0 &&
			clusterFanoutResponseHandlers[clusterFanoutResponseHandlerKey{name: name, response: routing.RespSpecial}] == nil {
			t.Errorf("supported special response %s has no fanout handler", name)
		}
	}
}

func TestClusterSetCommandValueRejectsTypeMismatch(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	cmd := NewStringSliceCmd(context.Background(), "keys", "*")
	if err := c.setCommandValue(cmd, []interface{}{"key"}); err == nil {
		t.Fatal("mismatched aggregate type was silently accepted")
	}
}

func TestClusterRejectsRawAndStreamingFanoutBeforeDispatch(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	tests := []Cmder{
		NewRawCmd(ctx, "del", "{one}a", "{two}b"),
		NewRawWriteToCmd(ctx, &bytes.Buffer{}, "keys", "*"),
	}
	for _, cmd := range tests {
		decision := c.commandRoutingDecision(ctx, cmd)
		if decision.policyErr == nil {
			t.Fatalf("%T was not rejected before fanout", cmd)
		}
		if err := c.process(ctx, cmd); err == nil {
			t.Fatalf("%T unexpectedly reached routing", cmd)
		}
	}
}

func TestClusterExplicitSlotRoutingUsesWireIntegerTypes(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	slot := int64(1234)
	for _, value := range []interface{}{int(1234), slot, &slot, "1234", []byte("1234")} {
		cmd := NewStringSliceCmd(ctx, "cluster", "getkeysinslot", value, 1)
		decision := c.commandRoutingDecision(ctx, cmd)
		if got := c.cmdSlotWithDecision(cmd, decision, -1); got != 1234 {
			t.Fatalf("slot for %T=%d, want 1234", value, got)
		}
	}
}

func TestClusterMultiShardGroupsEmptyKeysDeterministically(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	node, _ := c.nodes.GetOrCreate("127.0.0.1:7401")
	installMetadataClusterState(
		c, []*clusterNode{node},
		&clusterSlot{start: 0, end: 0, nodes: []*clusterNode{node}},
	)
	calls := 0
	node.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		calls++
		if got := cmd.Args(); !reflect.DeepEqual(got, []interface{}{"mset", "", "one", "", "two"}) {
			return fmt.Errorf("args=%#v", got)
		}
		cmd.(*StatusCmd).SetVal("OK")
		return nil
	}})

	cmd := NewStatusCmd(ctx, "mset", "", "one", "", "two")
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || cmd.Val() != "OK" || clusterKeySlot("") != 0 {
		t.Fatalf("calls=%d value=%q empty-slot=%d, want 1/OK/0", calls, cmd.Val(), clusterKeySlot(""))
	}
}

func installMetadataClusterState(c *ClusterClient, masters []*clusterNode, slots ...*clusterSlot) *clusterState {
	state := &clusterState{
		nodes: c.nodes, Masters: masters, slots: slots,
		generation: 1,
		createdAt:  time.Now(),
	}
	c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
	c.state.state.Store(state)
	return state
}

func TestClusterAllShardsUsesAvailableTopology(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	node, err := c.nodes.GetOrCreate("127.0.0.1:7199")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	node.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		calls++
		cmd.(*StatusCmd).SetVal("OK")
		return nil
	}})
	state := &clusterState{
		nodes: c.nodes, Masters: []*clusterNode{node},
		generation: 1, createdAt: time.Now(),
	}
	c.state.state.Store(state)

	cmd := NewStatusCmd(ctx, "flushall")
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policy == nil || decision.policy.Request != routing.ReqAllShards {
		t.Fatalf("FLUSHALL policy=%#v, want all_shards", decision.policy)
	}
	if err := c.executeOnAllShards(ctx, cmd, decision.policy, decision); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || cmd.Val() != "OK" {
		t.Fatalf("all_shards calls/value=%d/%q, want 1/OK", calls, cmd.Val())
	}
}

func TestClusterAllNodesUsesAvailableTopology(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	node, err := c.nodes.GetOrCreate("127.0.0.1:7198")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	node.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		calls++
		cmd.(*StatusCmd).SetVal("OK")
		return nil
	}})
	state := &clusterState{
		nodes: c.nodes, Masters: []*clusterNode{node},
		generation: 1, createdAt: time.Now(),
	}
	c.state.state.Store(state)

	cmd := NewStatusCmd(ctx, "script", "flush")
	decision := c.commandRoutingDecision(ctx, cmd)
	if decision.policy == nil || decision.policy.Request != routing.ReqAllNodes {
		t.Fatalf("SCRIPT FLUSH policy=%#v, want all_nodes", decision.policy)
	}
	if err := c.executeOnAllNodes(ctx, cmd, decision.policy, decision); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || cmd.Val() != "OK" {
		t.Fatalf("all_nodes calls/value=%d/%q, want 1/OK", calls, cmd.Val())
	}
}

func TestClusterSlotsFromShardsOrdersMasterFirst(t *testing.T) {
	shards := []ClusterShard{{
		Slots: []SlotRange{{Start: 0, End: 8191}, {Start: 8192, End: 16383}},
		Nodes: []Node{
			{ID: "replica", Endpoint: "replica.local", Port: 6379, TLSPort: 6380, Role: "replica", Health: "online"},
			{ID: "master", Endpoint: "master.local", Port: 6379, TLSPort: 6380, Role: "master", Health: "online"},
		},
	}}
	slots, err := clusterSlotsFromShards(shards, "seed.local:6380", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 2 {
		t.Fatalf("slot ranges=%d, want 2", len(slots))
	}
	for _, slot := range slots {
		if len(slot.Nodes) != 2 || slot.Nodes[0].ID != "master" || slot.Nodes[1].ID != "replica" {
			t.Fatalf("node order=%#v, want master then replica", slot.Nodes)
		}
		if slot.Nodes[0].Addr != "master.local:6380" || slot.Nodes[1].Addr != "replica.local:6380" {
			t.Fatalf("TLS node addresses=%#v", slot.Nodes)
		}
	}

	opt := &ClusterOptions{}
	opt.init()
	nodes := newClusterNodes(opt)
	t.Cleanup(func() { _ = nodes.Close() })
	_, err = newClusterStateFromShards(nodes, shards, "seed.local:6379", false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterStateFromShardsPreservesEndpointHealthAndZeroSlotShards(t *testing.T) {
	shards := []ClusterShard{
		{
			Slots: []SlotRange{{Start: 0, End: 16383}},
			Nodes: []Node{
				{ID: "master-one", Endpoint: "", IP: "wrong.invalid", Port: 7001, Role: "master", Health: "online"},
				{ID: "replica-loading", Endpoint: "replica.local", Port: 7002, Role: "replica", Health: "loading"},
			},
		},
		{
			Nodes: []Node{{ID: "master-zero-slots", Endpoint: "zero.local", Port: 7003, Role: "master", Health: "online"}},
		},
	}
	opt := &ClusterOptions{}
	opt.init()
	nodes := newClusterNodes(opt)
	t.Cleanup(func() { _ = nodes.Close() })
	state, err := newClusterStateFromShards(nodes, shards, "origin.local:6379", false)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.slots[0].nodes[0].Client.opt.Addr; got != "origin.local:7001" {
		t.Fatalf("null endpoint resolved to %q, want origin.local:7001", got)
	}
	if len(state.declaredMasters()) != 2 || len(state.Masters) != 2 {
		t.Fatalf("masters declared/online=%d/%d, want zero-slot master preserved", len(state.declaredMasters()), len(state.Masters))
	}
	if len(state.declaredSlaves()) != 1 || len(state.Slaves) != 0 || len(state.slots[0].nodes) != 1 {
		t.Fatalf("loading replica declared/online/slot=%d/%d/%d, want 1/0/master-only",
			len(state.declaredSlaves()), len(state.Slaves), len(state.slots[0].nodes))
	}
}

func TestClusterStateFromShardsTreatsUnknownHealthAsOffline(t *testing.T) {
	shards := []ClusterShard{{
		Slots: []SlotRange{{Start: 0, End: 16383}},
		Nodes: []Node{
			{ID: "master", Endpoint: "master.local", Port: 7001, Role: "master", Health: "online"},
			{ID: "replica", Endpoint: "replica.local", Port: 7002, Role: "replica", Health: "future-state"},
		},
	}}
	opt := &ClusterOptions{}
	opt.init()
	nodes := newClusterNodes(opt)
	t.Cleanup(func() { _ = nodes.Close() })
	state, err := newClusterStateFromShards(nodes, shards, "origin.local:6379", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.declaredSlaves()) != 1 || len(state.Slaves) != 0 {
		t.Fatalf("unknown-health replicas declared/online=%d/%d, want 1/0",
			len(state.declaredSlaves()), len(state.Slaves))
	}
}

func TestSameClusterTopologyIgnoresHealthAndInputOrder(t *testing.T) {
	opt := &ClusterOptions{}
	opt.init()
	nodes := newClusterNodes(opt)
	t.Cleanup(func() { _ = nodes.Close() })

	forward := []ClusterSlot{
		{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: "127.0.0.1:7002"}}},
		{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: "127.0.0.1:7001"}}},
	}
	reverse := []ClusterSlot{forward[1], forward[0]}
	first, err := newClusterState(nodes, forward, "127.0.0.1:7001")
	if err != nil {
		t.Fatal(err)
	}
	second, err := newClusterState(nodes, reverse, "127.0.0.1:7002")
	if err != nil {
		t.Fatal(err)
	}
	first.health = map[*clusterNode]string{first.Masters[0]: "loading"}
	if !sameClusterTopology(first, second) {
		t.Fatal("equivalent topology changed because of health or input order")
	}
}

func TestClusterRetriesAfterUnhealthyTopologySelection(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		name := "command"
		if pipeline {
			name = "pipeline"
		}
		t.Run(name, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			stale, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}},
				Nodes: []Node{{
					ID: "stale", Endpoint: "127.0.0.1", Port: 7351, Role: "master", Health: "fail",
				}},
			}}, "127.0.0.1:7351", false)
			if err != nil {
				t.Fatal(err)
			}
			healthy, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}},
				Nodes: []Node{{
					ID: "healthy", Endpoint: "127.0.0.1", Port: 7352, Role: "master", Health: "online",
				}},
			}}, "127.0.0.1:7352", false)
			if err != nil {
				t.Fatal(err)
			}

			var calls atomic.Int32
			healthy.Masters[0].Client.AddHook(clusterMetadataNodeHook{
				process: func(_ context.Context, cmd Cmder) error {
					calls.Add(1)
					cmd.(*StringCmd).SetVal("value")
					return nil
				},
				pipeline: func(_ context.Context, cmds []Cmder) error {
					calls.Add(1)
					for _, cmd := range cmds {
						cmd.(*StringCmd).SetVal("value")
					}
					return nil
				},
			})
			c.state.state.Store(stale)
			c.state.load = func(context.Context) (*clusterState, error) { return healthy, nil }

			cmd := NewStringCmd(context.Background(), "get", "key")
			if pipeline {
				if err := c.processPipeline(context.Background(), []Cmder{cmd}); err != nil {
					t.Fatal(err)
				}
			} else if err := c.process(context.Background(), cmd); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || cmd.Val() != "value" {
				t.Fatalf("calls/value=%d/%q, want 1/value", calls.Load(), cmd.Val())
			}
		})
	}
}

func TestClusterStateFromShardsRejectsUnknownEndpoint(t *testing.T) {
	shards := []ClusterShard{{
		Slots: []SlotRange{{Start: 0, End: 16383}},
		Nodes: []Node{{ID: "master", Endpoint: "?", IP: "127.0.0.1", Port: 7000, Role: "master", Health: "online"}},
	}}
	opt := &ClusterOptions{}
	opt.init()
	nodes := newClusterNodes(opt)
	t.Cleanup(func() { _ = nodes.Close() })
	if _, err := newClusterStateFromShards(nodes, shards, "origin.local:6379", false); err == nil {
		t.Fatal("unknown endpoint was treated as a routable address")
	}
}

func TestClusterAllShardsFailsBeforeDispatchForUnhealthyMaster(t *testing.T) {
	shards := []ClusterShard{
		{
			Slots: []SlotRange{{Start: 0, End: 8191}},
			Nodes: []Node{{ID: "failed", Endpoint: "127.0.0.1", Port: 7301, Role: "master", Health: "fail"}},
		},
		{
			Slots: []SlotRange{{Start: 8192, End: 16383}},
			Nodes: []Node{{ID: "online", Endpoint: "127.0.0.1", Port: 7302, Role: "master", Health: "online"}},
		},
	}
	c := newMetadataTestCluster(t, nil)
	state, err := newClusterStateFromShards(c.nodes, shards, "127.0.0.1:7301", false)
	if err != nil {
		t.Fatal(err)
	}
	c.state.state.Store(state)
	var calls atomic.Int32
	for _, node := range state.declaredMasters() {
		node.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
			calls.Add(1)
			return nil
		}})
	}
	cmd := NewStatusCmd(context.Background(), "flushall")
	decision := c.commandRoutingDecision(context.Background(), cmd)
	if err := c.executeOnAllShards(context.Background(), cmd, decision.policy, decision); !errors.Is(err, errClusterTopologyUnhealthy) {
		t.Fatalf("all_shards error=%v, want unhealthy topology", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("unhealthy all_shards dispatched %d commands", calls.Load())
	}
}

func TestClusterLoadStatePrefersShardsAndFallsBackToSlots(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		shardsErr     error
		wantSlotsCall int
	}{
		{name: "modern topology"},
		{name: "legacy fallback", shardsErr: errors.New("ERR unknown command 'CLUSTER SHARDS'"), wantSlotsCall: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardsCalls, slotsCalls := 0, 0
			opt := &ClusterOptions{
				Addrs: []string{"127.0.0.1:1"},
				NewClient: func(options *Options) *Client {
					client := NewClient(options)
					client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
						switch cmd := cmd.(type) {
						case *ClusterShardsCmd:
							shardsCalls++
							if tt.shardsErr != nil {
								return tt.shardsErr
							}
							cmd.SetVal([]ClusterShard{{
								Slots: []SlotRange{{Start: 0, End: 16383}},
								Nodes: []Node{{
									ID: "master", Endpoint: "127.0.0.1", Port: 7000, Role: "master", Health: "online",
								}},
							}})
							return nil
						case *ClusterSlotsCmd:
							slotsCalls++
							cmd.SetVal([]ClusterSlot{{
								Start: 0, End: 16383,
								Nodes: []ClusterNode{{ID: "master", Addr: "127.0.0.1:7000"}},
							}})
							return nil
						default:
							return fmt.Errorf("unexpected topology command %T", cmd)
						}
					}})
					return client
				},
			}
			c := NewClusterClient(opt)
			t.Cleanup(func() { _ = c.Close() })
			state, err := c.loadState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if state == nil || shardsCalls != 1 || slotsCalls != tt.wantSlotsCall {
				t.Fatalf("state=%v calls(shards=%d slots=%d), want non-nil/1/%d",
					state != nil, shardsCalls, slotsCalls, tt.wantSlotsCall)
			}
		})
	}
}

func TestClusterMultiShardRedirectRetriesOnlyAffectedSubgroup(t *testing.T) {
	for _, redirect := range []string{"MOVED", "ASK"} {
		t.Run(redirect, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			ctx := context.Background()
			from, err := c.nodes.GetOrCreate("127.0.0.1:7101")
			if err != nil {
				t.Fatal(err)
			}
			to, err := c.nodes.GetOrCreate("127.0.0.1:7102")
			if err != nil {
				t.Fatal(err)
			}
			movedKey, stableKey := "{move}key", "{stable}key"
			movedSlot, stableSlot := hashtag.Slot(movedKey), hashtag.Slot(stableKey)
			installMetadataClusterState(
				c, []*clusterNode{from},
				&clusterSlot{start: min(movedSlot, stableSlot), end: max(movedSlot, stableSlot), nodes: []*clusterNode{from}},
			)

			var mu sync.Mutex
			calls := map[string]int{}
			from.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
				key, _ := routingArgText(cmd, 1)
				mu.Lock()
				calls[key]++
				mu.Unlock()
				if key == movedKey {
					return fmt.Errorf("%s %d 127.0.0.1:7102", redirect, movedSlot)
				}
				cmd.(*IntCmd).SetVal(1)
				return nil
			}})
			to.Client.AddHook(clusterMetadataNodeHook{
				process: func(_ context.Context, cmd Cmder) error {
					mu.Lock()
					calls["target"]++
					mu.Unlock()
					cmd.(*IntCmd).SetVal(1)
					return nil
				},
				pipeline: func(_ context.Context, cmds []Cmder) error {
					if len(cmds) != 2 || cmds[0].Name() != "asking" {
						return fmt.Errorf("unexpected ASK pipeline: %#v", cmds)
					}
					mu.Lock()
					calls["target"]++
					mu.Unlock()
					cmds[1].(*IntCmd).SetVal(1)
					cmds[1].SetErr(nil)
					return nil
				},
			})

			cmd := NewIntCmd(ctx, "exists", movedKey, stableKey)
			if err := c.process(ctx, cmd); err != nil {
				t.Fatal(err)
			}
			if cmd.Val() != 2 || calls[movedKey] != 1 || calls[stableKey] != 1 || calls["target"] != 1 {
				t.Fatalf("value=%d calls=%v, want each subgroup once plus one redirect", cmd.Val(), calls)
			}
		})
	}
}

func TestClusterAllShardsRetriesOnlyFailedTarget(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	one, _ := c.nodes.GetOrCreate("127.0.0.1:7201")
	two, _ := c.nodes.GetOrCreate("127.0.0.1:7202")
	installMetadataClusterState(c, []*clusterNode{one, two})

	var mu sync.Mutex
	oneCalls, twoCalls := 0, 0
	one.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		mu.Lock()
		oneCalls++
		mu.Unlock()
		cmd.(*StatusCmd).SetVal("PONG")
		return nil
	}})
	two.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		mu.Lock()
		twoCalls++
		call := twoCalls
		mu.Unlock()
		if call == 1 {
			return io.EOF
		}
		cmd.(*StatusCmd).SetVal("PONG")
		return nil
	}})

	cmd := NewStatusCmd(ctx, "ping")
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if oneCalls != 1 || twoCalls != 2 || cmd.Val() != "PONG" {
		t.Fatalf("one=%d two=%d value=%q, want 1/2/PONG", oneCalls, twoCalls, cmd.Val())
	}
}

func TestClusterAllShardsRetargetsFailedOverMaster(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	oldMaster, _ := c.nodes.GetOrCreate("127.0.0.1:7251")
	newMaster, _ := c.nodes.GetOrCreate("127.0.0.1:7252")
	installMetadataClusterState(
		c, []*clusterNode{oldMaster},
		&clusterSlot{start: 0, end: 16383, nodes: []*clusterNode{oldMaster}},
	)
	replacement := &clusterState{
		nodes: c.nodes, Masters: []*clusterNode{newMaster},
		slots:      []*clusterSlot{{start: 0, end: 16383, nodes: []*clusterNode{newMaster}}},
		generation: 2, createdAt: time.Now(),
	}
	c.state.load = func(context.Context) (*clusterState, error) { return replacement, nil }
	oldCalls, newCalls := 0, 0
	oldMaster.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
		oldCalls++
		return errors.New("READONLY You can't write against a read only replica")
	}})
	newMaster.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		newCalls++
		cmd.(*StatusCmd).SetVal("OK")
		return nil
	}})

	cmd := NewStatusCmd(ctx, "flushdb")
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if oldCalls != 1 || newCalls != 1 || cmd.Val() != "OK" {
		t.Fatalf("old=%d new=%d value=%q, want 1/1/OK", oldCalls, newCalls, cmd.Val())
	}
}

func TestClusterDirectHImportCommandUsesExistingFanout(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	node, _ := c.nodes.GetOrCreate("127.0.0.1:7301")
	installMetadataClusterState(c, []*clusterNode{node})
	calls := 0
	node.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		if _, ok := cmd.(*HImportPrepareCmd); !ok {
			return fmt.Errorf("unexpected command type %T", cmd)
		}
		calls++
		cmd.(*HImportPrepareCmd).SetVal("OK")
		return nil
	}})

	cmd := NewHImportPrepareCmd(ctx, "fieldset", "field")
	if err := c.process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || cmd.Val() != "OK" {
		t.Fatalf("calls=%d value=%q, want 1/OK", calls, cmd.Val())
	}
}

func TestClusterConcreteCommandsUseSharedRoutingResolver(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	c.opt.ShardPicker = routing.NewStaticShardPicker(0)
	ctx := context.Background()
	one, _ := c.nodes.GetOrCreate("127.0.0.1:7501")
	two, _ := c.nodes.GetOrCreate("127.0.0.1:7502")
	installMetadataClusterState(c, []*clusterNode{one, two})

	var mu sync.Mutex
	calls := 0
	hook := clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		mu.Lock()
		calls++
		mu.Unlock()
		switch cmd := cmd.(type) {
		case *IntCmd:
			cmd.SetVal(1)
		case *StringCmd:
			cmd.SetVal("sha")
		case *StatusCmd:
			cmd.SetVal("OK")
		case *BoolSliceCmd:
			cmd.SetVal([]bool{true})
		default:
			return fmt.Errorf("unexpected command type %T", cmd)
		}
		return nil
	}}
	one.Client.AddHook(hook)
	two.Client.AddHook(hook)
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{Request: routing.ReqDefault, Response: routing.RespDefaultKeyless}
	}))

	if got, err := c.DBSize(ctx).Result(); err != nil || got != 1 {
		t.Fatalf("DBSize=%d err=%v", got, err)
	}
	if got, err := c.ScriptLoad(ctx, "return 1").Result(); err != nil || got != "sha" {
		t.Fatalf("ScriptLoad=%q err=%v", got, err)
	}
	if got, err := c.ScriptFlush(ctx).Result(); err != nil || got != "OK" {
		t.Fatalf("ScriptFlush=%q err=%v", got, err)
	}
	if got, err := c.ScriptExists(ctx, "sha").Result(); err != nil || !reflect.DeepEqual(got, []bool{true}) {
		t.Fatalf("ScriptExists=%v err=%v", got, err)
	}
	if calls != 4 {
		t.Fatalf("custom ReqDefault dispatched %d node calls, want one per concrete method", calls)
	}
}

func TestClusterConcreteCommandsPreserveDisabledPolicyFanout(t *testing.T) {
	c := NewClusterClient(&ClusterOptions{
		Addrs:                  []string{"127.0.0.1:1"},
		DisableRoutingPolicies: true,
	})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	masterOne, _ := c.nodes.GetOrCreate("127.0.0.1:7551")
	masterTwo, _ := c.nodes.GetOrCreate("127.0.0.1:7552")
	replica, _ := c.nodes.GetOrCreate("127.0.0.1:7553")
	state := installMetadataClusterState(c, []*clusterNode{masterOne, masterTwo})
	state.Slaves = []*clusterNode{replica}

	var mu sync.Mutex
	calls := make(map[string]int)
	hook := clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		name := cmd.Name()
		if len(cmd.Args()) > 1 {
			name += " " + cmd.stringArg(1)
		}
		mu.Lock()
		calls[name]++
		mu.Unlock()
		switch cmd := cmd.(type) {
		case *IntCmd:
			cmd.SetVal(1)
		case *StringCmd:
			cmd.SetVal("sha")
		case *StatusCmd:
			cmd.SetVal("OK")
		case *BoolSliceCmd:
			cmd.SetVal([]bool{true})
		default:
			return fmt.Errorf("unexpected command type %T", cmd)
		}
		return nil
	}}
	masterOne.Client.AddHook(hook)
	masterTwo.Client.AddHook(hook)
	replica.Client.AddHook(hook)

	if got, err := c.DBSize(ctx).Result(); err != nil || got != 2 {
		t.Fatalf("DBSize=%d err=%v, want 2", got, err)
	}
	if _, err := c.ScriptLoad(ctx, "return 1").Result(); err != nil {
		t.Fatal(err)
	}
	if err := c.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.ScriptExists(ctx, "sha").Result(); err != nil || !reflect.DeepEqual(got, []bool{true}) {
		t.Fatalf("ScriptExists=%v err=%v", got, err)
	}
	if calls["dbsize"] != 2 || calls["script load"] != 3 ||
		calls["script flush"] != 3 || calls["script exists"] != 3 {
		t.Fatalf("disabled-policy fanout calls=%v, want DBSize masters and scripts all shards", calls)
	}
}

func TestClusterScriptExistsStaticPolicyTargetsMastersOnly(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	master, _ := c.nodes.GetOrCreate("127.0.0.1:7601")
	replica, _ := c.nodes.GetOrCreate("127.0.0.1:7602")
	state := installMetadataClusterState(
		c, []*clusterNode{master},
		&clusterSlot{start: 0, end: 16383, nodes: []*clusterNode{master, replica}},
	)
	state.Slaves = []*clusterNode{replica}
	masterCalls, replicaCalls := 0, 0
	master.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		masterCalls++
		cmd.(*BoolSliceCmd).SetVal([]bool{true})
		return nil
	}})
	replica.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
		replicaCalls++
		cmd.(*BoolSliceCmd).SetVal([]bool{true})
		return nil
	}})

	if got, err := c.ScriptExists(ctx, "sha").Result(); err != nil || !reflect.DeepEqual(got, []bool{true}) {
		t.Fatalf("ScriptExists=%v err=%v", got, err)
	}
	if masterCalls != 1 || replicaCalls != 0 {
		t.Fatalf("master calls=%d replica calls=%d, want 1/0", masterCalls, replicaCalls)
	}
}
