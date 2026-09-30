package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/routing"
	"github.com/redis/go-redis/v9/maintnotifications"
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

func TestClusterNodeCallbacksPrecedeLatencyProbes(t *testing.T) {
	probed := make(chan *himportRegistry, 1)
	opt := &ClusterOptions{RouteByLatency: true, NewClient: func(opt *Options) *Client {
		client := NewClient(opt)
		client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
			select {
			case probed <- client.himport:
			default:
			}
			return nil
		}})
		return client
	}}
	opt.init()
	nodes := newClusterNodes(opt)
	t.Cleanup(func() { _ = nodes.Close() })
	shared := newHImportRegistry()
	nodes.OnNewNode(func(client *Client) {
		select {
		case <-probed:
			t.Error("latency probe ran before node initialization completed")
		case <-time.After(50 * time.Millisecond):
		}
		client.himport = shared
	})
	if _, err := nodes.GetOrCreate("127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	select {
	case registry := <-probed:
		if registry != shared {
			t.Fatal("latency probe did not observe the shared registry")
		}
	case <-time.After(time.Second):
		t.Fatal("latency probe did not run")
	}
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
	if got, want := c.cmdSlot(cmd, -1), hashtag.Slot("actual"); got != want {
		t.Fatalf("direct slot lookup=%d, want %d", got, want)
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
	keyless := c.commandRoutingDecision(ctx, NewCmd(ctx, "module.future"))
	if keyless.policyErr != nil || !keyless.keyless || keyless.naturalSlot != -1 {
		t.Fatalf("argument-free unknown command decision=%#v, want keyless fallback", keyless)
	}
}

func TestClusterRejectsUnrenderableHintedKeys(t *testing.T) {
	ctx := context.Background()
	c := newMetadataTestCluster(t, nil)
	installMetadataClusterState(c, nil)
	for _, mode := range []string{"single", "pipeline", "transaction first", "transaction last"} {
		t.Run(mode, func(t *testing.T) {
			cmd := NewCmd(ctx, "module.future", clusterBinaryKey("{other}key"))
			cmd.SetFirstKeyPos(1)
			var err error
			if mode == "single" {
				err = c.Process(ctx, cmd)
			} else {
				pipe := c.Pipeline()
				if mode != "pipeline" {
					pipe = c.TxPipeline()
				}
				if mode == "transaction last" {
					pipe.Get(ctx, "{known}key")
				}
				_ = pipe.Process(ctx, cmd)
				_, err = pipe.Exec(ctx)
			}
			if err == nil || !strings.Contains(err.Error(), "cannot reproduce the routing key") || cmd.Err() != err {
				t.Fatalf("error=%v command error=%v, want routing error before dispatch", err, cmd.Err())
			}
		})
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

func TestClusterStreamTransactionKeyPlans(t *testing.T) {
	ctx := context.Background()
	c := newMetadataTestCluster(t, nil)
	for _, tc := range []struct {
		name  string
		cmd   Cmder
		slots int // zero means the invocation cannot prove its keys
	}{
		{"single", makeCmd("xread", "streams", "{one}a", "0"), 1},
		{"same slot", makeCmd("xread", "count", 1, []byte("STREAMS"), "{one}a", []byte("{one}b"), "0", "0"), 1},
		{"cross slot", makeCmd("xread", "streams", "{one}a", "{two}b", "0", "0"), 2},
		{"group", makeCmd("xreadgroup", "group", "g", "c", "streams", "{one}a", "{one}b", ">", ">"), 1},
		{"group cross slot", makeCmd("xreadgroup", "group", "g", "c", "streams", "{one}a", "{two}b", ">", ">"), 2},
		{"keyword names", makeCmd("xreadgroup", "count", 1, "group", "streams", "streams", "noack", "streams", "{one}a", "{one}b", ">", ">"), 1},
		{"new options", makeCmd("xreadgroup", "group", "g", "c", "maxcount", 2, "maxsize", 4096, "claim", 100, "block", 1, "streams", "{one}a", ">"), 1},
		{"no keys", makeCmd("xread", "streams"), 0},
		{"missing ID", makeCmd("xread", "streams", "{one}a", "{one}b", "0"), 0},
		{"unrenderable second key", makeCmd("xread", "streams", "{one}a", clusterBinaryKey("{two}b"), "0", "0"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmds := []Cmder{tc.cmd}
			got, err := c.slottedKeyedCommandsInRouting(ctx, cmds, c.resolvePipelineRouting(ctx, cmds))
			if tc.slots == 0 {
				if err == nil {
					t.Fatal("invalid stream layout accepted")
				}
				return
			}
			if err != nil || len(got) != tc.slots || len(got[clusterKeySlot("{one}a")]) != 1 {
				t.Fatalf("slots=%v error=%v, want %d slots including the first stream", got, err, tc.slots)
			}
			if tc.slots > 1 {
				if err := c.processTxPipeline(ctx, wrapMultiExec(ctx, cmds)); !errors.Is(err, ErrCrossSlot) {
					t.Fatalf("cross-slot transaction error=%v", err)
				}
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

func TestClusterFullDuplexPinsAndReleasesMetadata(t *testing.T) {
	for _, tc := range []struct{ blocking, disabled bool }{{}, {blocking: true}, {disabled: true}} {
		t.Run(fmt.Sprintf("blocking=%v/disabled=%v", tc.blocking, tc.disabled), func(t *testing.T) {
			c := dialClusterFDTest(t)
			defer c.Close()
			ctx := context.Background()
			key := fmt.Sprintf("cmdmeta:fd:%v:%v", tc.blocking, tc.disabled)
			if err := c.Set(ctx, key, "value", 0).Err(); err != nil {
				t.Fatal(err)
			}
			defer c.Del(ctx, key)
			c.opt.DisableRoutingPolicies = tc.disabled
			var calls atomic.Int32
			c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
				calls.Add(1)
				return &routing.CommandPolicy{Request: routing.ReqDefault, Response: routing.RespDefaultHashSlot}
			}))
			opts := &AutoPipelineOptions{FullDuplex: true}
			var ap *AutoPipeliner
			var err error
			if tc.blocking {
				ap, err = c.AutoPipelineWithOptions(opts)
			} else {
				ap, err = c.AsyncAutoPipelineWithOptions(opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			if ap.clusterFD == nil {
				t.Fatal("cluster full duplex did not engage")
			}
			preflight := ap.preflight
			ap.preflight = func(ctx context.Context, cmd Cmder) error {
				err := preflight(ctx, cmd)
				// Retire GET after admission, before the FD router selects its node.
				c.cmdMeta.current.Store(buildCommandMetadataView(nil, map[string]*CommandInfo{"get": nil}))
				return err
			}
			cmd := ap.Get(ctx, key)
			if got, err := cmd.Result(); err != nil || got != "value" {
				t.Fatalf("admitted GET = %q, %v", got, err)
			}
			wantCalls := int32(1)
			if tc.disabled {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatalf("resolver ran %d times, want %d", calls.Load(), wantCalls)
			}
			if _, retained := c.peekAutoPipelineRoutingDecision(cmd); retained {
				t.Fatal("completed full-duplex command retained its metadata")
			}
		})
	}
}

func TestClusterInitialMetadataVerifiesSiblings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		variant int32
		legacy  bool
		loading bool
		reload  bool
	}{
		{name: "homogeneous"},
		{name: "legacy", legacy: true},
		{name: "mixed versions", variant: 1},
		{name: "mixed modules", variant: 2},
		{name: "unverifiable sibling", variant: 3},
		{name: "loading sibling", variant: 2, loading: true},
		{name: "topology changes during verification", reload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var variant, hellos, commands atomic.Int32
			var reloaded atomic.Bool
			variant.Store(tc.variant)
			var c *ClusterClient
			c = NewClusterClient(&ClusterOptions{
				Addrs: []string{"127.0.0.1:7501"}, Protocol: 2, DisableIdentity: true, MaxRetries: -1,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				CommandMetadata:          &CommandMetadataConfig{Mode: CommandMetadataPreferLive},
				Dialer: func(_ context.Context, _ string, addr string) (net.Conn, error) {
					client, server := net.Pipe()
					go serveTestRESPConn(server, func(command string) string {
						phase := int32(0)
						if strings.HasSuffix(addr, ":7502") {
							phase = variant.Load()
						}
						switch command {
						case "hello":
							hellos.Add(1)
							if tc.reload && commands.Load() > 0 && reloaded.CompareAndSwap(false, true) {
								c.state.state.Store(&clusterState{generation: 99})
							}
							if tc.legacy || phase == 3 {
								return "-ERR HELLO unavailable\r\n"
							}
							version, module := "8.10.0", 1
							if phase == 1 {
								version = "8.11.0"
							}
							if phase == 2 {
								module = 2
							}
							return fmt.Sprintf("*4\r\n+version\r\n+%s\r\n+modules\r\n*1\r\n*4\r\n+name\r\n+custom\r\n+ver\r\n:%d\r\n", version, module)
						case "info":
							if phase == 3 {
								return "-NOPERM INFO denied\r\n"
							}
							return commandInfoTestBulk("# Server\r\nredis_version:5.0.14\r\n")
						case "module":
							return "*1\r\n*4\r\n+name\r\n+custom\r\n+ver\r\n:1\r\n"
						case "command":
							commands.Add(1)
							return "*0\r\n"
						default:
							return "+OK\r\n"
						}
					})
					return client, nil
				},
			})
			t.Cleanup(func() { _ = c.Close() })
			health := "online"
			if tc.loading {
				health = "loading"
			}
			state, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}}, Nodes: []Node{
					{ID: "master", Endpoint: "127.0.0.1", Port: 7501, Role: "master", Health: "online"},
					{ID: "replica", Endpoint: "127.0.0.1", Port: 7502, Role: "replica", Health: health},
				},
			}}, "127.0.0.1:7501", false)
			if err != nil {
				t.Fatal(err)
			}
			c.state.state.Store(state)
			c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
			ctx := context.Background()
			before := commands.Load()
			if tc.variant != 0 || tc.reload {
				if err := c.cmdMeta.ensureLive(ctx); err == nil || c.metadataView().live || c.cmdMeta.serverFingerprint() != "" {
					t.Fatalf("unverified initial metadata published: live=%v err=%v", c.metadataView().live, err)
				}
				variant.Store(0)
				c.state.state.Store(state)
				before = commands.Load()
				if _, err := c.state.Reload(ctx); err != nil {
					t.Fatal(err)
				}
				if !waitForCondition(t, time.Second, func() bool { return c.metadataView().live }) {
					t.Fatal("topology reload did not retry initial identity verification")
				}
			} else if err := c.cmdMeta.ensureLive(ctx); err != nil {
				t.Fatal(err)
			}
			wantCommands := int32(1)
			if tc.legacy {
				wantCommands = 2 // Refetch COMMAND with the legacy identity fallback.
			}
			if !c.metadataView().live || commands.Load()-before != wantCommands {
				t.Fatalf("live=%v COMMAND calls=%d, want one source fetch", c.metadataView().live, commands.Load()-before)
			}
			before = hellos.Load()
			if err := c.cmdMeta.refreshOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if hellos.Load()-before != 1 {
				t.Fatal("steady-state refresh repeated sibling identity checks")
			}
		})
	}
}

func TestClusterMetadataRefreshesOnNodeReconnect(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprintf("RESP%d", protocol), func(t *testing.T) {
			var phases, fetches [2]atomic.Int32
			var addrs []string
			for i := range phases {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = ln.Close() })
				addrs = append(addrs, ln.Addr().String())
				go func() {
					for {
						conn, err := ln.Accept()
						if err != nil {
							return
						}
						go serveTestRESPConn(conn, func(command string) string {
							switch command {
							case "hello":
								phase := phases[i].Load()
								version := "8.10.0"
								if phase > 0 {
									version = "8.11.0"
								}
								mapHeader := "%2\r\n"
								if protocol == 2 {
									mapHeader = "*4\r\n"
								}
								return fmt.Sprintf("%s+version\r\n+%s\r\n+modules\r\n*1\r\n%s+name\r\n+test\r\n+ver\r\n:%d\r\n", mapHeader, version, mapHeader, phase)
							case "command":
								fetches[i].Add(1)
								return "*0\r\n"
							case "ping":
								return "+PONG\r\n"
							default:
								return "+OK\r\n"
							}
						})
					}
				}()
			}
			c := NewClusterClient(&ClusterOptions{
				Addrs: addrs, Protocol: protocol, PoolSize: 1,
				DisableIdentity:          true,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				CommandMetadata:          &CommandMetadataConfig{Mode: CommandMetadataPreferLive},
				ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
					return []ClusterSlot{
						{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: addrs[0]}}},
						{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: addrs[1]}}},
					}, nil
				},
			})
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			if err := c.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			state, err := c.state.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reconnect := func(i int) {
				t.Helper()
				clusterNode, err := c.nodes.GetOrCreate(addrs[i])
				if err != nil {
					t.Fatal(err)
				}
				node := clusterNode.Client
				if err := node.connPool.(*pool.ConnPool).Filter(func(*pool.Conn) bool { return true }); err != nil {
					t.Fatal(err)
				}
				if err := node.Ping(ctx).Err(); err != nil {
					t.Fatal(err)
				}
				if node.cmdMeta != nil {
					t.Fatal("node owns a metadata store instead of notifying the parent")
				}
			}
			for i, want := range []string{"8.10.0|test:0", "8.11.0|test:1", "8.11.0|test:2"} {
				if i > 0 {
					previous := c.cmdMeta.view()
					fp := c.cmdMeta.serverFingerprint()
					before := fetches[1].Load()
					phases[0].Store(int32(i))
					reconnect(0)
					if !waitForCondition(t, 3*time.Second, func() bool { return fetches[1].Load() > before }) {
						t.Fatal("node change did not check the unchanged sibling")
					}
					// The sibling response must finish publication before checking the view.
					c.cmdMeta.refreshMu.Lock()
					retained := c.cmdMeta.view() == previous && c.cmdMeta.serverFingerprint() == fp
					c.cmdMeta.refreshMu.Unlock()
					if !retained {
						t.Fatal("mixed cluster replaced metadata before the last sibling upgraded")
					}
					phases[1].Store(int32(i))
					reconnect(1)
				}
				if !waitForCondition(t, 3*time.Second, func() bool {
					return c.cmdMeta.view().live && c.cmdMeta.serverFingerprint() == want
				}) {
					t.Fatalf("reconnect %d: identity=%q, want %q", i, c.cmdMeta.serverFingerprint(), want)
				}
				if current, _ := c.state.Get(ctx); current != state {
					t.Fatal("test unexpectedly changed the topology")
				}
			}
			_ = state.Masters[0].Client.Close()
			select {
			case <-c.cmdMeta.stop:
				t.Fatal("closing a node stopped the parent metadata store")
			default:
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

func metadataTestResult[T any](cmd interface {
	Cmder
	SetVal(T)
}, value T,
) Cmder {
	cmd.SetVal(value)
	return cmd
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
			"keys flatten", NewStringSliceCmd(ctx, "keys", "*"),
			[]Cmder{metadataTestResult(NewStringSliceCmd(ctx), []string{"a", "b"}), metadataTestResult(NewStringSliceCmd(ctx), []string{"c"})},
			[]string{"a", "b", "c"},
		},
		{
			"script exists elementwise and", NewBoolSliceCmd(ctx, "script", "exists", "one", "two"),
			[]Cmder{metadataTestResult(NewBoolSliceCmd(ctx), []bool{true, true}), metadataTestResult(NewBoolSliceCmd(ctx), []bool{true, false})},
			[]bool{true, false},
		},
		{
			"slowlog flatten", NewSlowLogCmd(ctx, "slowlog", "get"),
			[]Cmder{metadataTestResult(NewSlowLogCmd(ctx), []SlowLog{{ID: 1}}), metadataTestResult(NewSlowLogCmd(ctx), []SlowLog{{ID: 2}})},
			[]SlowLog{{ID: 1}, {ID: 2}},
		},
		{
			"waitaof elementwise min", NewIntSliceCmd(ctx, "waitaof", 1, 1, 0),
			[]Cmder{metadataTestResult(NewIntSliceCmd(ctx), []int64{2, 5}), metadataTestResult(NewIntSliceCmd(ctx), []int64{1, 7})},
			[]int64{1, 5},
		},
		{
			"latency reset status sum", NewStatusCmd(ctx, "latency", "reset"),
			[]Cmder{metadataTestResult(NewStatusCmd(ctx), "2"), metadataTestResult(NewStatusCmd(ctx), "3")},
			"5",
		},
		{
			"randomkey skips empty shard", NewStringCmd(ctx, "randomkey"),
			[]Cmder{metadataTestResult(NewStringCmd(ctx), "chosen"), NewCmdResult(nil, Nil)},
			"chosen",
		},
		{
			"raw integer remains integer", NewCmd(ctx, "dbsize"),
			[]Cmder{NewCmdResult(int64(1<<53), nil), NewCmdResult(int64(1), nil)},
			int64(1<<53) + 1,
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
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("aggregate=%#v error=%v, want %#v", got, err, tt.want)
			}
		})
	}
}

func TestClusterSlowLogGlobalOrderAndLimit(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	// IDs are node-local: the newer shard deliberately has lower IDs.
	older, newer := make([]SlowLog, 6), make([]SlowLog, 6)
	for i := range older {
		older[i] = SlowLog{ID: int64(100 - i), Time: time.Unix(int64(100-i), 0)}
		newer[i] = SlowLog{ID: int64(6 - i), Time: time.Unix(int64(200-i), 0)}
	}
	for _, tc := range []struct {
		name  string
		count interface{}
		want  int
	}{
		{"default", nil, 10},
		{"one", int64(1), 1},
		{"bytes", []byte("2"), 2},
		{"all", "-1", 12},
		{"zero", 0, 0},
		{"large", int64(1 << 40), 12},
	} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/raw=%v", tc.name, raw), func(t *testing.T) {
				args := []interface{}{"slowlog", "get"}
				if tc.count != nil {
					args = append(args, tc.count)
				}
				var cmd Cmder = NewSlowLogCmd(ctx, args...)
				var parts []Cmder
				var want interface{} = append(append([]SlowLog{}, newer...), older...)[:tc.want]
				if raw {
					cmd = NewCmd(ctx, args...)
					var all []interface{}
					for _, entries := range [][]SlowLog{older, newer} {
						var values []interface{}
						for _, entry := range entries {
							values = append(values, []interface{}{entry.ID, entry.Time.Unix(), int64(1), []interface{}{"get", "key"}, "addr", "name"})
						}
						parts = append(parts, NewCmdResult(values, nil))
						all = append(values, all...)
					}
					want = all[:tc.want]
				} else {
					parts = []Cmder{metadataTestResult(NewSlowLogCmd(ctx), older), metadataTestResult(NewSlowLogCmd(ctx), newer)}
				}
				decision := c.commandRoutingDecision(ctx, cmd)
				if err := c.aggregateResponses(cmd, parts, decision.policy, decision); err != nil {
					t.Fatal(err)
				}
				got, err := ExtractCommandValue(cmd)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("aggregate=%#v error=%v, want %#v", got, err, want)
				}
			})
		}
	}
	for _, entry := range []interface{}{[]interface{}{int64(1)}, []interface{}{int64(1), "bad timestamp"}} {
		cmd := NewCmd(ctx, "slowlog", "get")
		if _, err := aggregateClusterSlowLog(cmd, []Cmder{NewCmdResult([]interface{}{entry}, nil)}); err == nil {
			t.Fatalf("malformed slowlog entry accepted: %#v", entry)
		}
	}
}

func TestClusterJSONDebugKeylessHelp(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	for _, child := range []interface{}{"help", []byte("HELP")} {
		d := c.commandRoutingDecision(ctx, NewCmd(ctx, "json.debug", child))
		if d.policyErr != nil || !d.keyless || d.firstKey != 0 {
			t.Fatalf("JSON.DEBUG HELP: keyless=%v firstKey=%d error=%v", d.keyless, d.firstKey, d.policyErr)
		}
	}
	for _, args := range [][]interface{}{
		{"json.debug", "memory"}, {"json.debug", "unknown"}, {"json.debug", "help", "extra"},
	} {
		if d := c.commandRoutingDecision(ctx, NewCmd(ctx, args...)); d.policyErr == nil {
			t.Fatalf("%v unexpectedly accepted as keyless", args)
		}
	}
	c = newMetadataTestCluster(t, &CommandMetadataConfig{Overrides: map[string]*CommandInfo{"json.debug": nil}})
	if d := c.commandRoutingDecision(ctx, NewCmd(ctx, "json.debug", "help")); d.policyErr == nil {
		t.Fatal("HELP bypassed the metadata tombstone")
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

func TestClusterRejectsUnsafeFanoutBeforeDispatch(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	value := clusterBinaryKey("value")
	tests := []Cmder{
		NewRawCmd(ctx, "del", "{one}a", "{two}b"),
		NewRawWriteToCmd(ctx, &bytes.Buffer{}, "keys", "*"),
		NewStatusCmd(ctx, "mset", "{one}a", value, "{two}b", value),
		NewIntCmd(ctx, "msetex", 2, "{one}a", value, "{two}b", value, "ex", 60),
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

func TestClusterFanoutUsesAvailableTopology(t *testing.T) {
	for _, args := range [][]interface{}{{"flushall"}, {"script", "flush"}} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
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
			installMetadataClusterState(c, []*clusterNode{node})
			cmd := NewStatusCmd(context.Background(), args...)
			if err := c.process(context.Background(), cmd); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || cmd.Val() != "OK" {
				t.Fatalf("fanout calls/value=%d/%q, want 1/OK", calls, cmd.Val())
			}
		})
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
	c := newMetadataTestCluster(t, nil)
	state, err := newClusterStateFromShards(c.nodes, shards, "origin.local:6379", false)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.slots[0].nodes[0].Client.opt.Addr; got != "origin.local:7001" {
		t.Fatalf("null endpoint resolved to %q, want origin.local:7001", got)
	}
	if len(state.declaredMasters()) != 2 || len(state.Masters) != 2 {
		t.Fatalf("masters declared/online=%d/%d, want zero-slot master preserved", len(state.declaredMasters()), len(state.Masters))
	}
	if len(state.declaredSlaves()) != 1 || len(state.Slaves) != 0 || len(state.slots[0].nodes) != 2 {
		t.Fatalf("loading replica declared/online/slot=%d/%d/%d, want 1/0/master-and-replica",
			len(state.declaredSlaves()), len(state.Slaves), len(state.slots[0].nodes))
	}
	live := buildCommandMetadataView(nil, nil)
	live.live = true
	c.cmdMeta.current.Store(live)
	c.state.state.Store(state)
	c.state.load = func(context.Context) (*clusterState, error) {
		return newClusterStateFromShards(c.nodes, shards, "origin.local:6379", false)
	}
	for _, health := range []string{"online", "fail", "loading", "online"} {
		shards[0].Nodes[1].Health = health
		if _, err := c.state.Reload(context.Background()); err != nil {
			t.Fatal(err)
		}
		if c.cmdMeta.view() != live {
			t.Fatalf("replica health %q retired live metadata", health)
		}
	}
	shards[0].Nodes[1].Endpoint = "replacement.local"
	if _, err := c.state.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.cmdMeta.view().live {
		t.Fatal("replacing a replica endpoint did not retire live metadata")
	}
}

type clusterMetadataStatsPool struct{ pool.Pooler }

func (p clusterMetadataStatsPool) Stats() *pool.Stats {
	return &pool.Stats{TotalConns: 1, Hits: 2}
}

func TestClusterEnumeratesUnhealthyNodes(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	state, err := newClusterStateFromShards(c.nodes, []ClusterShard{
		{Slots: []SlotRange{{Start: 0, End: 8191}}, Nodes: []Node{
			{ID: "m1", Endpoint: "127.0.0.1", Port: 7501, Role: "master", Health: "online"},
			{ID: "r1", Endpoint: "127.0.0.1", Port: 7502, Role: "replica", Health: "loading"},
		}},
		{Slots: []SlotRange{{Start: 8192, End: 16383}}, Nodes: []Node{
			{ID: "m2", Endpoint: "127.0.0.1", Port: 7503, Role: "master", Health: "fail"},
			{ID: "r2", Endpoint: "127.0.0.1", Port: 7504, Role: "replica", Health: "online"},
		}},
	}, "127.0.0.1:7501", false)
	if err != nil {
		t.Fatal(err)
	}
	c.state.state.Store(state)
	c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
	for _, nodes := range [][]*clusterNode{state.declaredMasters(), state.declaredSlaves()} {
		for _, node := range nodes {
			node.Client.connPool = clusterMetadataStatsPool{node.Client.connPool}
		}
	}
	for _, tc := range []struct {
		name string
		each func(context.Context, func(context.Context, *Client) error) error
		want int32
	}{
		{"masters", c.ForEachMaster, 2},
		{"replicas", c.ForEachSlave, 2},
		{"all nodes", c.ForEachShard, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen atomic.Int32
			if err := tc.each(context.Background(), func(context.Context, *Client) error {
				seen.Add(1)
				return nil
			}); err != nil || seen.Load() != tc.want {
				t.Fatalf("callbacks=%d err=%v, want %d declared nodes", seen.Load(), err, tc.want)
			}
			wantErr := errors.New("unhealthy node callback failed")
			err := tc.each(context.Background(), func(_ context.Context, client *Client) error {
				if client.opt.Addr == "127.0.0.1:7502" || client.opt.Addr == "127.0.0.1:7503" {
					return wantErr
				}
				return nil
			})
			if !errors.Is(err, wantErr) {
				t.Fatalf("error=%v, want unhealthy node callback error", err)
			}
		})
	}
	if stats := c.PoolStats(); stats.TotalConns != 4 || stats.Hits != 8 {
		t.Fatalf("stats=%+v, want all four declared pools", stats)
	}
}

func TestClusterKeylessSelectionRespectsTopologyHealth(t *testing.T) {
	for _, health := range []string{"master", "replica", "neither"} {
		t.Run(health, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			nodes := []Node{
				{Endpoint: "master.local", Port: 6379, Role: "master", Health: "fail"},
				{Endpoint: "replica.local", Port: 6379, Role: "replica", Health: "loading"},
			}
			for i := range nodes {
				if nodes[i].Role == health {
					nodes[i].Health = "online"
				}
			}
			state, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}}, Nodes: nodes,
			}}, "master.local:6379", false)
			if err != nil {
				t.Fatal(err)
			}
			for _, node := range state.slotNodes(0) {
				node.loaded.Store(1) // Selection must use topology health, without a PING.
			}
			for name, selectNode := range map[string]func(int) (*clusterNode, error){
				"master": state.slotMasterNode, "replica": state.slotSlaveNode,
				"random": state.slotRandomNode, "closest": state.slotClosestNode,
				"tolerance": func(slot int) (*clusterNode, error) { return state.slotNodeWithinLatency(slot, time.Millisecond) },
				"picker": func(slot int) (*clusterNode, error) {
					return state.slotShardPickerSlaveNode(slot, &routing.RoundRobinPicker{})
				},
			} {
				// Exercise keyed, keyless, and unknown-slot selection.
				for _, slot := range []int{-1, 0, 16384} {
					node, err := selectNode(slot)
					if health == "neither" || name == "master" && health != "master" {
						if !errors.Is(err, errClusterTopologyUnhealthy) {
							t.Fatalf("%s slot %d: node=%v error=%v, want unhealthy topology", name, slot, node, err)
						}
					} else if err != nil || node == nil || node.Client.opt.Addr != health+".local:6379" {
						t.Fatalf("%s slot %d: node=%v error=%v, want online %s", name, slot, node, err, health)
					}
				}
			}
		})
	}
}

func TestClusterKeylessPipelineTopology(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("readOnly=%v", readOnly), func(t *testing.T) {
			var cfg *CommandMetadataConfig
			if readOnly {
				cfg = &CommandMetadataConfig{Overrides: map[string]*CommandInfo{
					"custom.read": {Name: "custom.read", Arity: 1, Flags: []string{"readonly"}},
				}}
			}
			c := newMetadataTestCluster(t, cfg)
			c.opt.MaxRedirects = 1
			c.opt.ReadOnly = readOnly
			nodeDecls := []Node{{ID: "stale", Endpoint: "127.0.0.1", Port: 7351, Role: "master", Health: "fail"}}
			if readOnly {
				nodeDecls = append(nodeDecls, Node{ID: "replica", Endpoint: "127.0.0.1", Port: 7352, Role: "replica", Health: "online"})
			}
			stale, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}}, Nodes: nodeDecls,
			}}, "127.0.0.1:7351", false)
			if err != nil {
				t.Fatal(err)
			}
			healthy, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}}, Nodes: []Node{{ID: "healthy", Endpoint: "127.0.0.1", Port: 7352, Role: "master", Health: "online"}},
			}}, "127.0.0.1:7352", false)
			if err != nil {
				t.Fatal(err)
			}
			var calls, reloads atomic.Int32
			healthy.Masters[0].Client.AddHook(clusterMetadataNodeHook{pipeline: func(_ context.Context, cmds []Cmder) error {
				calls.Add(1)
				for _, cmd := range cmds {
					cmd.(*StringCmd).SetVal("value")
				}
				return nil
			}})
			c.state.state.Store(stale)
			c.state.load = func(context.Context) (*clusterState, error) {
				reloads.Add(1)
				return healthy, nil
			}
			cmd := NewStringCmd(context.Background(), "echo", "value")
			if readOnly {
				cmd = NewStringCmd(context.Background(), "custom.read")
			}
			err = c.processPipeline(context.Background(), []Cmder{cmd})
			if readOnly && reloads.Load() != 0 {
				t.Fatal("online replica should be used without a topology reload")
			}
			if !readOnly && reloads.Load() == 0 {
				t.Fatal("stale topology was not reloaded")
			}
			if err != nil || calls.Load() != 1 {
				t.Fatalf("err=%v, executions=%d, reloads=%d; want successful execution", err, calls.Load(), reloads.Load())
			}
		})
	}
}

func TestClusterRetriesAfterUnhealthyTopologySelection(t *testing.T) {
	for _, name := range []string{"command", "pipeline", "all shards", "all nodes", "fanout retry limit"} {
		t.Run(name, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			c.opt.MaxRedirects = 1
			if strings.HasPrefix(name, "all ") || name == "fanout retry limit" {
				request := routing.ReqAllShards
				if name == "all nodes" {
					request = routing.ReqAllNodes
				}
				c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
					return &routing.CommandPolicy{Request: request, Response: routing.RespAllSucceeded}
				}))
			}
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
			if name == "fanout retry limit" {
				c.state.load = func(context.Context) (*clusterState, error) { return stale, nil }
			}

			cmd := NewStringCmd(context.Background(), "get", "key")
			if name == "fanout retry limit" {
				if err := c.process(context.Background(), cmd); !errors.Is(err, errClusterTopologyUnhealthy) || calls.Load() != 0 {
					t.Fatalf("error=%v calls=%d, want unhealthy topology before dispatch", err, calls.Load())
				}
				return
			}
			if name == "pipeline" {
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
	for _, tc := range []struct {
		name      string
		failure   error
		wantCalls int
		wantErr   error
	}{
		{"transient target error", io.EOF, 2, nil},
		{"topology error after dispatch", errClusterTopologyUnhealthy, 1, errClusterTopologyUnhealthy},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
					return tc.failure
				}
				cmd.(*StatusCmd).SetVal("PONG")
				return nil
			}})

			cmd := NewStatusCmd(ctx, "ping")
			if err := c.process(ctx, cmd); !errors.Is(err, tc.wantErr) {
				t.Fatalf("error=%v, want %v", err, tc.wantErr)
			}
			if oneCalls != 1 || twoCalls != tc.wantCalls {
				t.Fatalf("one=%d two=%d, want 1/%d", oneCalls, twoCalls, tc.wantCalls)
			}
			if tc.wantErr == nil && cmd.Val() != "PONG" {
				t.Fatalf("value=%q, want PONG", cmd.Val())
			}
		})
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

func TestClusterConcreteCommandRouting(t *testing.T) {
	for _, mode := range []string{"custom", "disabled", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			c := newMetadataTestCluster(t, nil)
			c.opt.DisableRoutingPolicies = mode == "disabled"
			c.opt.ShardPicker = routing.NewStaticShardPicker(0)
			if mode == "custom" {
				c.SetCommandInfoResolver(NewCommandInfoResolver(func(context.Context, Cmder) *routing.CommandPolicy {
					return &routing.CommandPolicy{Request: routing.ReqDefault, Response: routing.RespDefaultKeyless}
				}))
			}
			ctx := context.Background()
			one, _ := c.nodes.GetOrCreate("127.0.0.1:7501")
			two, _ := c.nodes.GetOrCreate("127.0.0.1:7502")
			replica, _ := c.nodes.GetOrCreate("127.0.0.1:7503")
			state := installMetadataClusterState(c, []*clusterNode{one, two})
			state.Slaves = []*clusterNode{replica}

			var mu sync.Mutex
			calls := make(map[string]int)
			replicaExistsCalls := 0
			for _, node := range []*clusterNode{one, two, replica} {
				node.Client.AddHook(clusterMetadataNodeHook{process: func(_ context.Context, cmd Cmder) error {
					name := cmd.Name()
					if name == "script" {
						name += " " + cmd.stringArg(1)
					}
					mu.Lock()
					calls[name]++
					if node == replica && name == "script exists" {
						replicaExistsCalls++
					}
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
				}})
			}

			wantCalls := map[string]int{"dbsize": 2, "script load": 3, "script flush": 3, "script exists": 2}
			wantReplicaExistsCalls := 0
			wantFlush := "OK"
			if mode == "custom" {
				for name := range wantCalls {
					wantCalls[name] = 1
				}
			} else if mode == "disabled" {
				wantCalls["script exists"] = 3
				wantReplicaExistsCalls = 1
				wantFlush = "" // Legacy fanout returns only an error.
			}
			if got, err := c.DBSize(ctx).Result(); err != nil || got != int64(wantCalls["dbsize"]) {
				t.Fatalf("DBSize=%d err=%v", got, err)
			}
			if got, err := c.ScriptLoad(ctx, "return 1").Result(); err != nil || got != "sha" {
				t.Fatalf("ScriptLoad=%q err=%v", got, err)
			}
			if got, err := c.ScriptFlush(ctx).Result(); err != nil || got != wantFlush {
				t.Fatalf("ScriptFlush=%q err=%v", got, err)
			}
			if got, err := c.ScriptExists(ctx, "sha").Result(); err != nil || !reflect.DeepEqual(got, []bool{true}) {
				t.Fatalf("ScriptExists=%v err=%v", got, err)
			}
			if !reflect.DeepEqual(calls, wantCalls) || replicaExistsCalls != wantReplicaExistsCalls {
				t.Fatalf("calls=%v replica SCRIPT EXISTS=%d, want %v/%d", calls, replicaExistsCalls, wantCalls, wantReplicaExistsCalls)
			}
		})
	}
}
