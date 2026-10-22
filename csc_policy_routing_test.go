package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/routing"
)

func cscPolicyCluster(t *testing.T, a, b string, mode TrackingMode, coalesce bool) *ClusterClient {
	t.Helper()
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}}}
	})
	opt.DisableRoutingPolicies = false
	opt.ClientSideCacheConfig.TrackingMode = mode
	opt.ClientSideCacheConfig.DrainInterval = time.Hour
	opt.ClientSideCacheCoalesceMisses = coalesce
	c := NewClusterClient(opt)
	c.SetCommandInfoResolver(NewCommandMetadataResolver())
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestCSCPolicyFanoutSuffixAndByteKeys(t *testing.T) {
	a := newCSCPolicyServer(t, func(args []string) string {
		if strings.EqualFold(args[0], "json.mget") {
			return "*1\r\n$1\r\nv\r\n"
		}
		return ""
	})
	b := newCSCPolicyServer(t, func(args []string) string {
		if strings.EqualFold(args[0], "json.mget") {
			return "*1\r\n$1\r\nv\r\n"
		}
		return ""
	})
	c := cscPolicyCluster(t, a.addr, b.addr, TrackingModeOptIn, false)
	var predicates atomic.Int32
	c.opt.ClientSideCacheConfig.Cacheable = func(_ string, keys []string) bool {
		predicates.Add(1)
		if !reflect.DeepEqual(keys, []string{"{bar}:a", "{foo}:b"}) {
			t.Errorf("keys include suffix: %v", keys)
		}
		return true
	}
	c.SetCommandInfoResolver(NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
		return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
	}))
	for i := 0; i < 2; i++ {
		if err := c.Do(context.Background(), "json.mget", []byte("{bar}:a"), []byte("{foo}:b"), "$").Err(); err != nil {
			t.Fatal(err)
		}
	}
	// JSON.MGET's verified dont_cache correction must survive splitting too:
	// routing understands its suffix, but Redis does not track all its keys.
	if predicates.Load() != 0 || a.count("json.mget") != 2 || b.count("json.mget") != 2 || c.CSCStats().Entries != 0 {
		t.Fatal("ineligible fragments re-evaluated intent or entered the cache")
	}
	for _, server := range []*cscPolicyServer{a, b} {
		for _, event := range server.snapshot() {
			if strings.EqualFold(event.args[0], "json.mget") && (len(event.args) != 3 || event.args[2] != "$") {
				t.Fatal("split lost suffix", event.args)
			}
		}
	}
	// Byte keys on an eligible original call do reach the predicate, and the
	// resulting intent survives cloning without converting the child arguments.
	for i := 0; i < 2; i++ {
		if err := c.Do(context.Background(), "mget", []byte("{bar}:a"), []byte("{foo}:b")).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if predicates.Load() != 2 || a.count("mget") != 1 || b.count("mget") != 1 || c.CSCStats().Entries != 2 {
		t.Fatal("byte-key fragment did not cache")
	}
}

func TestCSCPolicyAutoPipelineRoutingLifecycle(t *testing.T) {
	for _, face := range []string{"blocking", "unordered", "ordered"} {
		t.Run(face, func(t *testing.T) {
			a, b := newCSCPolicyServer(t, nil), newCSCPolicyServer(t, nil)
			c := cscPolicyCluster(t, a.addr, b.addr, TrackingModeOptIn, false)
			c.AddHook(&cscPolicyContextHook{})
			var ap *AutoPipeliner
			var err error
			switch face {
			case "blocking":
				ap, err = c.AutoPipeline()
			case "unordered":
				ap, err = c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{Unordered: true})
			default:
				ap, err = c.AsyncAutoPipeline()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()
			ctx := context.Background()
			cmd := NewSliceCmd(ctx, "mget", "{bar}:a", "{foo}:b")
			for i := 0; i < 2; i++ {
				err = ap.WithCache(true).Process(ctx, cmd)
				if (face == "ordered") != (err != nil) {
					t.Fatal(err)
				}
				if cmd.clusterRoutingDecision() != nil || *cmd.cacheState() != (cscCommandState{}) {
					t.Fatal("admission/completion leaked invocation state")
				}
			}
			want := 1
			if face == "ordered" {
				want = 0
			}
			if a.count("mget") != want || b.count("mget") != want {
				t.Fatal("wrong diversion or fragment cache behavior")
			}
			// Same-slot admission remains supported on the ordered face too.
			for i := 0; i < 2; i++ {
				if err := ap.WithCache(true).Get(ctx, "{bar}:solo").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if a.count("get") != 1 {
				t.Fatal("marked solo call lost caching")
			}
			if err := ap.Close(); err != nil {
				t.Fatal(err)
			}
			closed := NewStringCmd(ctx, "get", "closed")
			closed.setClusterRoutingDecision(c.metadataView().routingTable["get"].template)
			if err := ap.WithCache(true).Process(ctx, closed); !errors.Is(err, ErrClosed) || closed.clusterRoutingDecision() != nil || *closed.cacheState() != (cscCommandState{}) {
				t.Fatal("closed admission leaked state", err)
			}
		})
	}
}

func TestCSCPolicyAutoPipelineQueueFull(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	a := newCSCPolicyServer(t, func(args []string) string {
		if strings.EqualFold(args[0], "get") {
			close(entered)
			<-release
		}
		return ""
	})
	c := cscPolicyCluster(t, a.addr, a.addr, TrackingModeOptIn, false)
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{MaxQueuedCommands: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	first := make(chan error, 1)
	go func() { first <- ap.WithCache(true).Get(context.Background(), "{bar}:first").Err() }()
	<-entered
	cmd := NewStringCmd(context.Background(), "get", "{bar}:second")
	if err := ap.WithCache(true).Process(context.Background(), cmd); !errors.Is(err, ErrAutoPipelineQueueFull) {
		t.Fatal(err)
	}
	if cmd.clusterRoutingDecision() != nil || *cmd.cacheState() != (cscCommandState{}) {
		t.Fatal("queue-full admission leaked state")
	}
	once.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestCSCPolicyMetadataOwnership(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := clusterCSCTestOptions(func() []ClusterSlot { return nil })
	opt.DisableRoutingPolicies = false
	info := cloneCommandInfo(defaultCommandMetadataView().records["get"])
	info.KeySpecs = nil
	info.FirstKeyPos, info.LastKeyPos, info.StepCount = 2, 2, 1
	opt.CommandMetadata = &CommandMetadataConfig{Overrides: map[string]*CommandInfo{"get": info}}
	c := NewClusterClient(opt)
	defer c.Close()
	node, err := c.nodes.GetOrCreate(s.addr)
	if err != nil {
		t.Fatal(err)
	}
	if node.Client.cmdMeta != nil || node.Client.metadataSource != c.cmdMeta || node.Client.metadataView() != c.metadataView() {
		t.Fatal("node did not borrow the parent metadata view")
	}
	if first, _, _ := cscRedisKeyLayout(node.Client.metadataView().cscTable["get"], NewCmd(context.Background(), "get", "option", "actual")); first != 2 {
		t.Fatal("node CSC ignored the routing key override")
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.cmdMeta.stop:
		t.Fatal("closing a node stopped the parent metadata store")
	default:
	}
	view := buildCommandMetadataView(nil, map[string]*CommandInfo{"get": nil})
	c.cmdMeta.current.Store(view)
	other := newCSCPolicyServer(t, nil)
	next, err := c.nodes.GetOrCreate(other.addr)
	if err != nil || next.Client.metadataView() != view || next.Client.cmdMeta != nil {
		t.Fatalf("new node lost current parent metadata: %v", err)
	}
}

func TestCSCPolicyFactoryMetadataOmission(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(fmt.Sprint(strict), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
			})
			opt.DisableRoutingPolicies = false
			opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
			opt.ClientSideCacheConfig.Strict = strict
			opt.NewClient = func(opt *Options) *Client {
				copy := *opt
				copy.cscMetadataSource = nil
				return NewClient(&copy)
			}
			c := NewClusterClient(opt)
			defer c.Close()
			err := c.WithCache(true).Get(context.Background(), "k").Err()
			want := 1
			if strict {
				want = 0
			}
			if (err != nil) != strict || s.count("get") != want || s.prefixes() != 0 || c.CSCStats().Entries != 0 {
				t.Fatal("factory omission did not fail closed", err)
			}
		})
	}
}

func TestCSCPolicyFanoutPinnedView(t *testing.T) {
	a, b := newCSCPolicyServer(t, nil), newCSCPolicyServer(t, nil)
	c := cscPolicyCluster(t, a.addr, b.addr, TrackingModeOptIn, true)
	var calls atomic.Int32
	c.opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool {
		calls.Add(1)
		c.cmdMeta.current.Store(buildCommandMetadataView(nil, map[string]*CommandInfo{"mget": nil}))
		return true
	}
	if got, err := c.MGet(context.Background(), "{bar}:a", "{foo}:b").Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"v", "v"}) {
		t.Fatal(got, err)
	}
	if calls.Load() != 1 || a.prefixes() != 1 || b.prefixes() != 1 || c.CSCStats().Entries != 0 {
		t.Fatal("fragments mixed metadata generations or published a retired view")
	}
}

func TestCSCPolicyPinnedRoutingView(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	var c *ClusterClient
	var calls atomic.Int32
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
	})
	opt.DisableRoutingPolicies = false
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool {
		calls.Add(1)
		// Refresh after routing/intent capture but before node execution.
		c.cmdMeta.current.Store(buildCommandMetadataView(nil, map[string]*CommandInfo{"get": nil}))
		return true
	}
	c = NewClusterClient(opt)
	defer c.Close()
	cmd := NewStringCmd(context.Background(), "get", "k")
	if err := c.Process(context.Background(), cmd); err != nil || cmd.Val() != "v" {
		t.Fatalf("pinned read %q %v", cmd.Val(), err)
	}
	if s.prefixes() != 1 || calls.Load() != 1 || c.CSCStats().Entries != 0 {
		t.Fatal("mixed routing/CSC generations or published a retired fetch")
	}
	if *cmd.cacheState() != (cscCommandState{}) {
		t.Fatal("metadata pin leaked beyond invocation")
	}
}

func TestCSCPolicyFanoutFragments(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, coalesce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/coalesce=%t", mode, coalesce), func(t *testing.T) {
				a, b := newCSCPolicyServer(t, nil), newCSCPolicyServer(t, nil)
				var predicates atomic.Int32
				c := cscPolicyCluster(t, a.addr, b.addr, mode, coalesce)
				// Configure before any node is created; the parent snapshot is immutable.
				c.opt.ClientSideCacheConfig.Cacheable = func(name string, keys []string) bool {
					predicates.Add(1)
					if name != "mget" || !reflect.DeepEqual(keys, []string{"{bar}:a", "{foo}:b"}) {
						t.Errorf("fragment predicate input %s %v", name, keys)
					}
					return len(keys) == 2
				}
				ctx := context.Background()
				for i := 0; i < 2; i++ {
					if got, err := c.MGet(ctx, "{bar}:a", "{foo}:b").Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"v", "v"}) {
						t.Fatalf("fanout %v %v", got, err)
					}
				}
				if predicates.Load() != 2 || a.count("mget") != 1 || b.count("mget") != 1 || c.CSCStats().Entries != 2 {
					t.Fatalf("predicates %d reads %d/%d stats %+v", predicates.Load(), a.count("mget"), b.count("mget"), c.CSCStats())
				}
				if err := c.WithCache(false).MGet(ctx, "{bar}:a", "{foo}:b").Err(); err != nil {
					t.Fatal(err)
				}
				if a.count("mget") != 2 || b.count("mget") != 2 || predicates.Load() != 2 || c.CSCStats().Entries != 2 {
					t.Fatal("fanout bypass touched cache or reevaluated predicate")
				}
				flags := 0
				if mode != TrackingModePlain {
					flags = 1
				}
				if a.prefixes() != flags || b.prefixes() != flags {
					t.Fatalf("prefix count %d/%d want %d", a.prefixes(), b.prefixes(), flags)
				}
				a.assertPairs(t)
				b.assertPairs(t)
			})
		}
	}
}

func TestCSCPolicyFanoutPartialPublication(t *testing.T) {
	for _, reply := range []string{"*1\r\n_\r\n", "-WRONGTYPE changed\r\n"} {
		t.Run(strconv.Quote(reply), func(t *testing.T) {
			a := newCSCPolicyServer(t, func(args []string) string {
				if strings.EqualFold(args[0], "mget") {
					return reply
				}
				return ""
			})
			b := newCSCPolicyServer(t, nil)
			c := cscPolicyCluster(t, a.addr, b.addr, TrackingModeOptIn, false)
			c.opt.ClientSideCacheConfig.Strict = true
			for i := 0; i < 2; i++ {
				err := c.WithCache(true).MGet(context.Background(), "{bar}:a", "{foo}:b").Err()
				if strings.HasPrefix(reply, "-") != (err != nil) {
					t.Fatal(err)
				}
			}
			if a.count("mget") != 2 || b.count("mget") != 1 || c.CSCStats().Entries != 1 {
				t.Fatal("successful fragment was rolled back or replayed")
			}
		})
	}
}

func TestCSCPolicyFanoutRedirect(t *testing.T) {
	for _, kind := range []string{"MOVED", "ASK"} {
		for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
			t.Run(kind+"/"+string(mode), func(t *testing.T) {
				target := newCSCPolicyServer(t, nil)
				a := newCSCPolicyServer(t, func(args []string) string {
					if strings.EqualFold(args[0], "mget") {
						return "-" + kind + " 5061 " + target.addr + "\r\n"
					}
					return ""
				})
				b := newCSCPolicyServer(t, nil)
				c := cscPolicyCluster(t, a.addr, b.addr, mode, true)
				if got, err := c.WithCache(true).MGet(context.Background(), "{bar}:a", "{foo}:b").Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"v", "v"}) {
					t.Fatalf("redirect %v %v", got, err)
				}
				entries := uint64(2)
				if kind == "ASK" {
					entries = 1
					if target.prefixes() != 0 || target.count("asking") != 1 {
						t.Fatal("ASK paired or did not use ASKING")
					}
				}
				if a.count("mget") != 1 || b.count("mget") != 1 || target.count("mget") != 1 || uint64(c.CSCStats().Entries) != entries {
					t.Fatalf("redirect replay/publication: %d/%d/%d %+v", a.count("mget"), b.count("mget"), target.count("mget"), c.CSCStats())
				}
			})
		}
	}
}
