package redis

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/routing"
)

func TestCSCPolicyViewReuseAndAutoPipeline(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "k")
	cmd.cacheState().mark = cacheRequested
	clone := cmd.Clone()
	if clone.cacheState().mark != cacheRequested {
		t.Fatal("clone lost mark")
	}
	if err := c.WithCache(true).Process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if err := c.Process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if s.count("get") != 2 {
		t.Fatal("mark leaked to next execution")
	}
	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	for i := 0; i < 2; i++ {
		if err := ap.WithCache(true).Get(ctx, "ap").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if s.count("get") != 3 {
		t.Fatal("marked AP did not use normal cache path")
	}
	viewType := reflect.TypeOf(c.WithCache(true))
	for _, name := range []string{"Close", "Pipeline", "TxPipeline", "AutoPipeline", "AsyncAutoPipeline"} {
		if _, ok := viewType.MethodByName(name); ok {
			t.Fatal("view exposes", name)
		}
	}
}

type cscPolicyContextHook struct{ reject bool }

func (*cscPolicyContextHook) DialHook(next DialHook) DialHook { return next }
func (*cscPolicyContextHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func (h *cscPolicyContextHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(_ context.Context, cmd Cmder) error {
		if h.reject {
			return fmt.Errorf("test: command rejected by hook")
		}
		return next(context.Background(), cmd)
	}
}

func TestCSCPolicyContextReplacementAndHookRejection(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
	opt.ClientSideCacheConfig.Strict = true
	c := policyClient(t, opt)
	hook := &cscPolicyContextHook{reject: true}
	c.AddHook(hook)
	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "rejected")
	if err := c.WithCache(true).Process(ctx, cmd); err == nil {
		t.Fatal("hook did not reject")
	}
	if *cmd.cacheState() != (cscCommandState{}) {
		t.Fatal("rejected view leaked invocation state")
	}
	hook.reject = false
	if err := c.Process(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	if s.prefixes() != 0 {
		t.Fatal("reused command inherited rejected mark")
	}
	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	for i := 0; i < 2; i++ {
		if err := ap.WithCache(true).Get(ctx, "replaced-context").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if s.count("get") != 2 || s.prefixes() != 1 {
		t.Fatal("context replacement lost intent")
	}
	if err := ap.WithCache(true).Touch(ctx, "k").Err(); err == nil || s.count("touch") != 0 {
		t.Fatal("AutoPipeline did not reject an ineligible strict mark before dispatch")
	}
}

func TestCSCPolicyClusterRedirect(t *testing.T) {
	for _, kind := range []string{"MOVED", "ASK"} {
		t.Run(kind, func(t *testing.T) {
			target := newCSCPolicyServer(t, nil)
			source := newCSCPolicyServer(t, func(a []string) string {
				if strings.EqualFold(a[0], "get") {
					return "-" + kind + " 0 " + target.addr + "\r\n"
				}
				return ""
			})
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: source.addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
			c := NewClusterClient(opt)
			defer c.Close()
			if v, e := c.WithCache(true).Get(context.Background(), "k").Result(); v != "v" || e != nil {
				t.Fatalf("%q %v", v, e)
			}
			if kind == "ASK" {
				if target.prefixes() != 0 || c.CSCStats().Entries != 0 {
					t.Fatal("ASK cached/paired")
				}
				events := target.snapshot()
				for i, e := range events {
					if strings.EqualFold(e.args[0], "asking") {
						if i+1 >= len(events) || !strings.EqualFold(events[i+1].args[0], "get") {
							t.Fatal("ASKING not adjacent")
						}
					}
				}
			} else if target.prefixes() != 1 || c.CSCStats().Entries != 1 {
				t.Fatal("MOVED did not track/publish")
			}
		})
	}
}

func TestCSCPolicyClusterFanoutGuard(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(strconv.FormatBool(strict), func(t *testing.T) {
			a, b := newCSCPolicyServer(t, nil), newCSCPolicyServer(t, nil)
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a.addr}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b.addr}}}}
			})
			opt.DisableRoutingPolicies = false
			opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptOut
			opt.ClientSideCacheConfig.Strict = strict
			c := NewClusterClient(opt)
			defer c.Close()
			c.SetCommandInfoResolver(NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
				if cmd.Name() == "mget" {
					return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
				}
				return nil
			}))
			err := c.WithCache(true).MGet(context.Background(), "{bar}:a", "{foo}:b").Err()
			if strict {
				if err == nil || a.count("mget")+b.count("mget") != 0 {
					t.Fatalf("strict fanout %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if a.count("mget") != 1 || b.count("mget") != 1 || a.prefixes() != 1 || b.prefixes() != 1 || c.CSCStats().Entries != 0 {
					t.Fatalf("fanout counts %d %d prefixes %d %d", a.count("mget"), b.count("mget"), a.prefixes(), b.prefixes())
				}
			}
		})
	}
}

func TestCSCPolicyStrictClusterCapability(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
	})
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	opt.ClientSideCacheConfig.Strict = true
	c := NewClusterClient(opt)
	defer c.Close()
	if err := c.WithCache(true).SUnion(context.Background(), "{a}:1", "{a}:2").Err(); err == nil {
		t.Fatal("strict permanent bypass")
	}
	if s.count("sunion") != 0 {
		t.Fatal("unsupported command sent")
	}
	if err := c.WithCache(true).Get(context.Background(), "k").Err(); err != nil {
		t.Fatal(err)
	}
}

func TestCSCPolicyConstructorForwarding(t *testing.T) {
	cfg := &ClientSideCacheConfig{TrackingMode: TrackingModeOptOut, Strict: true, Cacheable: func(string, []string) bool { return false }}
	u := &UniversalOptions{Addrs: []string{"localhost:1"}, ClientSideCacheConfig: cfg}
	for name, opt := range map[string]*Options{"simple": u.Simple(), "cluster": u.Cluster().clientOptions(), "failoverCluster": u.Failover().clusterOptions().clientOptions()} {
		p := cachePolicy(opt.ClientSideCacheConfig)
		if p.mode != TrackingModeOptOut || !p.strict || p.cacheable == nil {
			t.Fatal(name)
		}
	}
}

func TestCSCPolicyClusterSingleSlotPlan(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
	})
	opt.DisableRoutingPolicies = false
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	c := NewClusterClient(opt)
	defer c.Close()
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
		if cmd.Name() == "mget" {
			return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
		}
		return nil
	}))
	for i := 0; i < 2; i++ {
		if err := c.WithCache(true).MGet(context.Background(), "{one}:a", "{one}:b").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if s.count("mget") != 1 || s.prefixes() != 1 {
		t.Fatalf("single-target child lost mark: mget=%d flags=%d", s.count("mget"), s.prefixes())
	}
}

func TestCSCPolicySingleSlotRedirectKeepsIntent(t *testing.T) {
	for _, kind := range []string{"MOVED", "ASK"} {
		t.Run(kind, func(t *testing.T) {
			var fetches, predicates atomic.Int32
			target := newCSCPolicyServer(t, func(a []string) string {
				if strings.EqualFold(a[0], "mget") {
					v := fetches.Add(1)
					return fmt.Sprintf("*2\r\n$1\r\n%d\r\n$1\r\n%d\r\n", v, v)
				}
				return ""
			})
			source := newCSCPolicyServer(t, func(a []string) string {
				if strings.EqualFold(a[0], "mget") {
					return "-" + kind + " 0 " + target.addr + "\r\n"
				}
				return ""
			})
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: source.addr}}}}
			})
			opt.DisableRoutingPolicies = false
			opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
			opt.ClientSideCacheConfig.Cacheable = func(name string, keys []string) bool {
				if name != "mget" || !reflect.DeepEqual(keys, []string{"{one}:a", "{one}:b"}) {
					t.Errorf("predicate input %s %v", name, keys)
				}
				return predicates.Add(1) == 1
			}
			c := NewClusterClient(opt)
			defer c.Close()
			c.SetCommandInfoResolver(NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
				if cmd.Name() == "mget" {
					return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
				}
				return nil
			}))
			ctx := context.Background()
			node, err := c.nodes.GetOrCreate(target.addr)
			if err != nil {
				t.Fatal(err)
			}
			if err := node.Client.WithCache(true).MGet(ctx, "{one}:a", "{one}:b").Err(); err != nil {
				t.Fatal(err)
			}
			got, err := c.MGet(ctx, "{one}:a", "{one}:b").Result()
			if err != nil || !reflect.DeepEqual(got, []interface{}{"2", "2"}) || predicates.Load() != 1 {
				t.Fatalf("redirect result %v %v; predicate calls %d", got, err, predicates.Load())
			}
			want, flags := "2", 2
			if kind == "ASK" {
				want, flags = "1", 1
			}
			cached, err := node.Client.WithCache(true).MGet(ctx, "{one}:a", "{one}:b").Result()
			if err != nil || !reflect.DeepEqual(cached, []interface{}{want, want}) || target.prefixes() != flags || fetches.Load() != 2 {
				t.Fatalf("destination cache %v %v; flags %d fetches %d", cached, err, target.prefixes(), fetches.Load())
			}
		})
	}
}

func TestCSCPolicyCustomFactoryStrict(t *testing.T) {
	for _, dropAll := range []bool{false, true} {
		t.Run(strconv.FormatBool(dropAll), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
			opt.ClientSideCacheConfig.Strict = true
			opt.NewClient = func(o *Options) *Client {
				if dropAll {
					o.ClientSideCacheConfig = nil
				} else {
					o.ClientSideCacheConfig = &ClientSideCacheConfig{MaxEntries: 16}
				}
				return NewClient(o)
			}
			c := NewClusterClient(opt)
			defer c.Close()
			if err := c.WithCache(true).Get(context.Background(), "k").Err(); err == nil {
				t.Fatal("factory silently lost strict or mode")
			}
			if s.count("get") != 0 {
				t.Fatal("strict sent data")
			}
		})
	}
}

func TestCSCPolicyReroutedFactoryStrict(t *testing.T) {
	for _, path := range []string{"slot-child", "single-node-plan"} {
		t.Run(path, func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
			opt.ClientSideCacheConfig.Strict = true
			opt.NewClient = func(o *Options) *Client {
				o.ClientSideCacheConfig = &ClientSideCacheConfig{MaxEntries: 16}
				return NewClient(o)
			}
			c := NewClusterClient(opt)
			defer c.Close()
			ctx := context.Background()
			node, err := c.nodes.GetOrCreate(s.addr)
			if err != nil {
				t.Fatal(err)
			}
			// The legacy router can select a different node after the outer
			// process loop's check. Validate at these actual dispatch seams too.
			view := newCacheView(func(ctx context.Context, cmd Cmder) error {
				if path == "slot-child" {
					decision := c.commandRoutingDecision(ctx, cmd)
					return c.executeMultiSlotCommand(ctx, cmd, 0, &decision)
				}
				return c.executeParallelTarget(ctx, node, -1, cmd)
			}, true)
			if err := view.MGet(ctx, "k").Err(); err == nil {
				t.Fatal("rerouted node lost strict mode validation")
			}
			if s.count("mget") != 0 {
				t.Fatal("strict sent a child read")
			}
		})
	}
}

func TestCSCPolicyMOVEDSkipsDestinationHit(t *testing.T) {
	var n atomic.Int32
	target := newCSCPolicyServer(t, func(a []string) string {
		if strings.EqualFold(a[0], "get") {
			return fmt.Sprintf("$1\r\n%d\r\n", n.Add(1))
		}
		return ""
	})
	source := newCSCPolicyServer(t, func(a []string) string {
		if strings.EqualFold(a[0], "get") {
			return "-MOVED 0 " + target.addr + "\r\n"
		}
		return ""
	})
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: source.addr}}}}
	})
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	c := NewClusterClient(opt)
	defer c.Close()
	node, err := c.nodes.GetOrCreate(target.addr)
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Client.WithCache(true).Get(context.Background(), "k").Val(); got != "1" {
		t.Fatal(got)
	}
	if got, err := c.WithCache(true).Get(context.Background(), "k").Result(); got != "2" || err != nil {
		t.Fatalf("destination hit used: %q %v", got, err)
	}
	if target.prefixes() != 2 {
		t.Fatal("MOVED did not repeat YES")
	}
}

func TestCSCPolicyASKWithoutTargetCSC(t *testing.T) {
	target := newCSCPolicyServer(t, nil)
	source := newCSCPolicyServer(t, func(a []string) string {
		if strings.EqualFold(a[0], "get") {
			return "-ASK 0 " + target.addr + "\r\n"
		}
		return ""
	})
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: source.addr}}}}
	})
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	opt.ClientSideCacheConfig.Strict = true
	opt.NewClient = func(o *Options) *Client {
		if o.Addr == target.addr {
			o.ClientSideCacheConfig = nil
		}
		return NewClient(o)
	}
	c := NewClusterClient(opt)
	defer c.Close()
	if got, err := c.WithCache(true).Get(context.Background(), "k").Result(); got != "v" || err != nil {
		t.Fatalf("runtime ASK became misuse: %q %v", got, err)
	}
	if target.prefixes() != 0 {
		t.Fatal("ASK got CACHING")
	}
}

func TestCSCPolicyStrictClusterAbsentReply(t *testing.T) {
	s := newCSCPolicyServer(t, func(a []string) string {
		if strings.EqualFold(a[0], "get") {
			return "_\r\n"
		}
		return ""
	})
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
	})
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	opt.ClientSideCacheConfig.Strict = true
	c := NewClusterClient(opt)
	defer c.Close()
	if err := c.WithCache(true).Get(context.Background(), "k").Err(); err != Nil {
		t.Fatal(err)
	}
	if c.CSCStats().Entries != 0 {
		t.Fatal("absent reply stored")
	}
}
