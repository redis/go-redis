package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/routing"
)

const clusterScanKeys = 600

// clusterScanFill writes clusterScanKeys string keys under a fresh prefix, checks
// that they span several masters, and deletes them when the test ends.
// forEachScanResolver runs f with the default policy resolver and with the
// COMMAND metadata resolver: both give SCAN its special policy.
func forEachScanResolver(t *testing.T, f func(t *testing.T, cc *ClusterClient)) {
	for _, mode := range []string{"default", "metadata"} {
		t.Run(mode, func(t *testing.T) {
			cc := dialClusterFDTest(t)
			t.Cleanup(func() { _ = cc.Close() })
			if mode == "metadata" {
				cc.SetCommandInfoResolver(NewCommandMetadataResolver())
			}
			f(t, cc)
		})
	}
}

func clusterScanFill(t *testing.T, cc *ClusterClient, name string) (prefix string, want map[string]bool) {
	t.Helper()
	ctx := context.Background()
	prefix = fmt.Sprintf("scantest:%s:%s", name, strconv.FormatInt(time.Now().UnixNano(), 36))
	want = make(map[string]bool, clusterScanKeys)
	for i := 0; i < clusterScanKeys; i++ {
		k := fmt.Sprintf("%s:%d", prefix, i)
		if err := cc.Set(ctx, k, i, time.Minute).Err(); err != nil {
			t.Fatalf("SET %s: %v", k, err)
		}
		want[k] = true
	}
	t.Cleanup(func() {
		for k := range want {
			cc.Del(ctx, k)
		}
	})
	owners := map[string]bool{}
	for k := range want {
		owners[cc.cmdNode(ctx, k)] = true
	}
	if len(owners) < 2 {
		t.Fatalf("keys on %d master(s), want several", len(owners))
	}
	return prefix, want
}

// cmdNode is the address of the master that owns key (test helper).
func (c *ClusterClient) cmdNode(ctx context.Context, key string) string {
	n, err := c.MasterForKey(ctx, key)
	if err != nil {
		return ""
	}
	return n.Options().Addr
}

func clusterScanCollect(t *testing.T, it *ScanIterator) map[string]int {
	t.Helper()
	ctx := context.Background()
	got := map[string]int{}
	for it.Next(ctx) {
		got[it.Val()]++
		if len(got) > 2*clusterScanKeys {
			t.Fatal("the scan does not end")
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return got
}

// clusterScanExpectEvery checks that got holds every key of want and no other.
// SCAN can return a key more than once while the server resizes its table, so
// duplicates are allowed.
func clusterScanExpectEvery(t *testing.T, got map[string]int, want map[string]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("scan returned %d keys, want %d", len(got), len(want))
	}
	for k := range got {
		if !want[k] {
			t.Fatalf("scan returned unknown key %q", k)
		}
	}
}

// A SCAN iterator on a ClusterClient returns every key of every master once.
// Before the fix it sent each page to a random node with the previous node's
// cursor and stopped early without an error.
func TestClusterScanIteratorCoversEveryMaster(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		prefix, want := clusterScanFill(t, cc, "next")
		clusterScanExpectEvery(t, clusterScanCollect(t, cc.Scan(context.Background(), 0, prefix+":*", 50).Iterator()), want)
	})
}

func TestClusterScanVals(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		prefix, want := clusterScanFill(t, cc, "vals")
		got := map[string]int{}
		for k := range cc.Scan(ctx, 0, prefix+":*", 50).Iterator().Vals(ctx) {
			got[k]++
		}
		clusterScanExpectEvery(t, got, want)
	})
}

func TestClusterScanTypeIterator(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		prefix, _ := clusterScanFill(t, cc, "type")
		lists := map[string]bool{}
		for i := 0; i < clusterScanKeys; i++ {
			k := fmt.Sprintf("%s-list:%d", prefix, i)
			if err := cc.RPush(ctx, k, "x").Err(); err != nil {
				t.Fatal(err)
			}
			lists[k] = true
		}
		t.Cleanup(func() {
			for k := range lists {
				cc.Del(ctx, k)
			}
		})
		clusterScanExpectEvery(t, clusterScanCollect(t, cc.ScanType(ctx, 0, prefix+"*", 50, "list").Iterator()), lists)
	})
}

// Every page runs inside the ClusterClient's process hooks.
func TestClusterScanRunsClusterHooksPerPage(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		prefix, _ := clusterScanFill(t, cc, "hook")
		var pages atomic.Int64
		cc.AddHook(scanCountHook{&pages})
		state, err := cc.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		clusterScanCollect(t, cc.Scan(ctx, 0, prefix+":*", 50).Iterator())
		if p := pages.Load(); p < int64(len(state.Masters)) {
			t.Fatalf("cluster hook saw %d SCAN pages, want at least one per master (%d)", p, len(state.Masters))
		}
	})
}

type scanCountHook struct{ n *atomic.Int64 }

func (h scanCountHook) DialHook(next DialHook) DialHook { return next }
func (h scanCountHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if cmd.Name() == "scan" {
			h.n.Add(1)
		}
		return next(ctx, cmd)
	}
}
func (h scanCountHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// Scan on a full-duplex autopipeliner over a ClusterClient covers every master.
func TestClusterScanFromFullDuplexAutoPipeliner(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		prefix, want := clusterScanFill(t, cc, "ap")
		ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
		if err != nil {
			t.Fatal(err)
		}
		defer ap.Close()
		clusterScanExpectEvery(t, clusterScanCollect(t, ap.Scan(context.Background(), 0, prefix+":*", 50).Iterator()), want)

		ap.Close()
		if err := ap.Scan(context.Background(), 0, prefix+":*", 50).Err(); err != ErrClosed {
			t.Fatalf("Scan after Close: err = %v, want ErrClosed", err)
		}
	})
}

// A MATCH pattern with a hash tag still goes to the node that owns the slot.
func TestClusterScanHashTagMatch(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		clusterScanFill(t, cc, "other")
		tag := fmt.Sprintf("{scantag-%d}", time.Now().UnixNano())
		want := map[string]bool{}
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("%s:%d", tag, i)
			if err := cc.Set(ctx, k, i, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			want[k] = true
		}
		clusterScanExpectEvery(t, clusterScanCollect(t, cc.Scan(ctx, 0, tag+":*", 10).Iterator()), want)
	})
}

// A hand-written cursor loop with a hash-tag MATCH sends every page to the slot's
// master, as v9.23.0 did: the cursor of page 1 is only valid on that node. Typed Scan
// and a raw Do("scan", ...) both. COUNT 5 makes the loop span many pages.
func TestClusterScanManualLoopHashTag(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		clusterScanFill(t, cc, "loopother")
		tag := fmt.Sprintf("{scanloop-%d}", time.Now().UnixNano())
		want := map[string]bool{}
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("%s:%d", tag, i)
			if err := cc.Set(ctx, k, i, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			want[k] = true
		}
		t.Cleanup(func() {
			for k := range want {
				cc.Del(ctx, k)
			}
		})
		pages := map[string]func(cursor uint64) ([]string, uint64, error){
			"typed": func(cursor uint64) ([]string, uint64, error) {
				return cc.Scan(ctx, cursor, tag+":*", 5).Result()
			},
			"raw": func(cursor uint64) ([]string, uint64, error) {
				cmd := NewScanCmd(ctx, nil, "scan", cursor, "match", tag+":*", "count", 5)
				_ = cc.Process(ctx, cmd)
				return cmd.Result()
			},
		}
		for name, page := range pages {
			got := map[string]int{}
			var cursor uint64
			for n := 0; ; n++ {
				keys, next, err := page(cursor)
				if err != nil {
					t.Fatalf("%s page %d: %v", name, n, err)
				}
				for _, k := range keys {
					got[k]++
				}
				if next == 0 {
					break
				}
				if n > 1000 {
					t.Fatalf("%s: the loop does not end", name)
				}
				cursor = next
			}
			if len(got) != len(want) {
				t.Fatalf("%s loop returned %d of %d keys", name, len(got), len(want))
			}
			clusterScanExpectEvery(t, got, want)
		}
	})
}

// Val and Result report the server cursor, not a value made up for the walk:
// with a large COUNT the first master ends in one page and returns cursor 0.
// The iterator on the same command still goes on to the other masters.
func TestClusterScanResultReportsServerCursor(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		prefix, want := clusterScanFill(t, cc, "cursor")
		cmd := cc.Scan(context.Background(), 0, prefix+":*", 100000)
		if _, cursor, err := cmd.Result(); err != nil || cursor != 0 {
			t.Fatalf("first page: cursor=%d err=%v, want the server cursor 0", cursor, err)
		}
		clusterScanExpectEvery(t, clusterScanCollect(t, cmd.Iterator()), want)
	})
}

// A wildcard inside the hash tag matches keys of many tags, so the walk covers
// every master.
func TestClusterScanWildcardHashTagCoversEveryMaster(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		tag := fmt.Sprintf("scanwild-%d", time.Now().UnixNano())
		want := map[string]bool{}
		owners := map[string]bool{}
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("{%s-%d}:x", tag, i)
			if err := cc.Set(ctx, k, i, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			want[k] = true
			owners[cc.cmdNode(ctx, k)] = true
		}
		t.Cleanup(func() {
			for k := range want {
				cc.Del(ctx, k)
			}
		})
		if len(owners) < 2 {
			t.Fatalf("keys on %d master(s), want several", len(owners))
		}
		clusterScanExpectEvery(t, clusterScanCollect(t, cc.Scan(ctx, 0, "{"+tag+"-*}:*", 20).Iterator()), want)
	})
}

// A loop that passes the cursor back by hand carries no walk after page 1:
// those pages go to the node the ShardPicker selects. Page 1 starts the walk
// on that node too, so a fixed picker keeps the whole loop on one master.
func TestClusterScanManualLoopUsesShardPicker(t *testing.T) {
	raw := func(ctx context.Context, cc *ClusterClient, cursor uint64, match string) ([]string, uint64, error) {
		reply, err := cc.Do(ctx, "scan", cursor, "match", match, "count", 50).Slice()
		if err != nil {
			return nil, 0, err
		}
		next, err := strconv.ParseUint(reply[0].(string), 10, 64)
		var keys []string
		for _, k := range reply[1].([]interface{}) {
			keys = append(keys, k.(string))
		}
		return keys, next, err
	}
	typed := func(ctx context.Context, cc *ClusterClient, cursor uint64, match string) ([]string, uint64, error) {
		return cc.Scan(ctx, cursor, match, 50).Result()
	}
	for _, mode := range []string{"default", "metadata"} {
		for name, scan := range map[string]func(context.Context, *ClusterClient, uint64, string) ([]string, uint64, error){"raw": raw, "typed": typed} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				cc := NewClusterClient(&ClusterOptions{
					Addrs:       clusterFDTestAddrs,
					ShardPicker: routing.NewStaticShardPicker(1),
				})
				t.Cleanup(func() { _ = cc.Close() })
				if err := cc.Ping(ctx).Err(); err != nil {
					t.Skipf("local cluster not reachable at %v: %v", clusterFDTestAddrs, err)
				}
				if mode == "metadata" {
					cc.SetCommandInfoResolver(NewCommandMetadataResolver())
				}
				prefix, all := clusterScanFill(t, cc, "raw")
				state, err := cc.state.Get(ctx)
				if err != nil {
					t.Fatal(err)
				}
				picked := state.Masters[1].Client.opt.Addr
				want := map[string]bool{}
				for k := range all {
					if cc.cmdNode(ctx, k) == picked {
						want[k] = true
					}
				}
				got := map[string]int{}
				var cursor uint64
				for page := 0; ; page++ {
					if page > 2*clusterScanKeys {
						t.Fatal("the scan does not end")
					}
					keys, next, err := scan(ctx, cc, cursor, prefix+":*")
					if err != nil {
						t.Fatal(err)
					}
					for _, k := range keys {
						got[k]++
					}
					if cursor = next; cursor == 0 {
						break
					}
				}
				clusterScanExpectEvery(t, got, want)
			})
		}
	}
}

// state.Masters holds only online masters. A walk must fail when a declared
// master is unhealthy instead of returning the other shards only.
func TestClusterScanFailsForUnhealthyMaster(t *testing.T) {
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
	c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
	var calls atomic.Int32
	for _, node := range state.declaredMasters() {
		node.Client.AddHook(clusterMetadataNodeHook{process: func(context.Context, Cmder) error {
			calls.Add(1)
			return nil
		}})
	}
	ctx := context.Background()
	cmd := NewScanCmd(ctx, c.Process, "scan", uint64(0))
	decision := c.commandRoutingDecision(ctx, cmd)
	if err := c.executeScanCommand(ctx, cmd, nil, decision); !errors.Is(err, ErrClusterTopologyUnhealthy) {
		t.Fatalf("scan error=%v, want unhealthy topology", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("unhealthy scan dispatched %d commands", calls.Load())
	}
}

// SCAN stays node-local in explicit pipelines and transactions, as before the
// SCAN policy: a transaction runs it on the node of its hash-tag slot.
func TestClusterScanInPipelineAndTransaction(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		tag := fmt.Sprintf("{scantx-%d}", time.Now().UnixNano())
		t.Cleanup(func() { cc.Del(ctx, tag+":a") })
		var scan *ScanCmd
		if _, err := cc.TxPipelined(ctx, func(pipe Pipeliner) error {
			pipe.Set(ctx, tag+":a", 1, time.Minute)
			scan = pipe.Scan(ctx, 0, tag+":*", 100)
			return nil
		}); err != nil {
			t.Fatalf("TxPipelined with SCAN: %v", err)
		}
		if keys, _ := scan.Val(); len(keys) != 1 || keys[0] != tag+":a" {
			t.Fatalf("SCAN in a transaction returned %v, want [%s:a]", keys, tag)
		}
		if _, err := cc.Pipelined(ctx, func(pipe Pipeliner) error {
			pipe.Scan(ctx, 0, tag+":*", 100)
			return nil
		}); err != nil {
			t.Fatalf("Pipelined with SCAN: %v", err)
		}
	})
}

// The walk reads the cursor and MATCH as they are sent: a []byte cursor "0"
// starts a walk over every master.
func TestClusterScanWireEncodedArgs(t *testing.T) {
	forEachScanResolver(t, func(t *testing.T, cc *ClusterClient) {
		ctx := context.Background()
		prefix, want := clusterScanFill(t, cc, "wire")
		cmd := NewScanCmd(ctx, cc.Process, "scan", []byte("0"), []byte("match"), []byte(prefix+":*"), "count", 50)
		if err := cc.Process(ctx, cmd); err != nil {
			t.Fatal(err)
		}
		clusterScanExpectEvery(t, clusterScanCollect(t, cmd.Iterator()), want)
	})
}

// With ReadOnly the ShardPicker can select a replica. A hand-written cursor loop
// then sends pages 2+ to that replica, so the walk must start there too.
func TestClusterScanManualLoopOnReplica(t *testing.T) {
	ctx := context.Background()
	cc := NewClusterClient(&ClusterOptions{
		Addrs:       clusterFDTestAddrs,
		ReadOnly:    true,
		ShardPicker: routing.NewStaticShardPicker(3), // Masters[0..2], then Slaves[0]
	})
	t.Cleanup(func() { _ = cc.Close() })
	if err := cc.Ping(ctx).Err(); err != nil {
		t.Skipf("local cluster not reachable at %v: %v", clusterFDTestAddrs, err)
	}
	cc.SetCommandInfoResolver(NewCommandMetadataResolver())
	state, err := cc.state.ReloadOrGet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Masters) != 3 || len(state.Slaves) == 0 {
		t.Skipf("want 3 masters with replicas, have %d/%d", len(state.Masters), len(state.Slaves))
	}
	replica := state.Slaves[0]
	var shardMaster string
	for _, slot := range state.slots {
		for _, n := range slot.nodes[1:] {
			if n == replica {
				shardMaster = slot.nodes[0].Client.opt.Addr
			}
		}
	}
	prefix, all := clusterScanFill(t, cc, "replica")
	if err := cc.ForEachMaster(ctx, func(ctx context.Context, m *Client) error {
		return m.Wait(ctx, 1, 2*time.Second).Err()
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for k := range all {
		if cc.cmdNode(ctx, k) == shardMaster {
			want[k] = true
		}
	}
	got := map[string]int{}
	var cursor uint64
	for page := 0; ; page++ {
		if page > 2*clusterScanKeys {
			t.Fatal("the scan does not end")
		}
		keys, next, err := cc.Scan(ctx, cursor, prefix+":*", 50).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			got[k]++
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	clusterScanExpectEvery(t, got, want)
}
