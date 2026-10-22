package redis

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/routing"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func newClusterCSCTestServer(t testing.TB, reply func(string) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Add(1)
			go func() { defer wg.Done(); serveTestRESPConn(conn, reply) }()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return ":" + port
}

func clusterCSCTestOptions(slots func() []ClusterSlot) *ClusterOptions {
	return &ClusterOptions{
		Protocol: 3, PoolSize: 2, DisableIdentity: true, DisableRoutingPolicies: true,
		ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 16, MaxStaleness: time.Minute},
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		ClusterSlots:             func(context.Context) ([]ClusterSlot, error) { return slots(), nil },
	}
}

func clusterCSCTestReply(calls *atomic.Int32) func(string) string {
	return func(command string) string {
		switch command {
		case "hello":
			return "%0\r\n"
		case "get":
			calls.Add(1)
			return "$0\r\n\r\n"
		case "exists":
			calls.Add(1)
			return ":1\r\n"
		case "mget":
			calls.Add(1)
			return "*2\r\n$0\r\n\r\n$0\r\n\r\n"
		default:
			return "+OK\r\n"
		}
	}
}

func TestClusterCSCIndependentCachesAndDirectHandles(t *testing.T) {
	var callsA, callsB atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&callsA))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&callsB))
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{
			{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a, ID: "A"}}},
			{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b, ID: "B"}}},
		}
	})
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	keys := []string{"{bar}:present", "{foo}:present"}
	if hashtag.Slot(keys[0]) >= 8192 || hashtag.Slot(keys[1]) < 8192 {
		t.Fatal("test tags changed")
	}
	for _, key := range keys {
		for i := 0; i < 2; i++ {
			if err := c.Get(ctx, key).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if callsA.Load() != 1 || callsB.Load() != 1 {
		t.Fatalf("server calls A=%d B=%d", callsA.Load(), callsB.Load())
	}
	ca, _ := c.MasterForKey(ctx, keys[0])
	cb, _ := c.MasterForKey(ctx, keys[1])
	if ca.csc == cb.csc {
		t.Fatal("nodes share cache storage")
	}
	if got := c.CSCStats(); got.Entries != 2 || got.Hits != 2 {
		t.Fatalf("stats: %+v", got)
	}
	for _, handle := range []*Client{ca, ca.WithTimeout(time.Second)} {
		if err := handle.Get(ctx, keys[0]).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if callsA.Load() != 1 {
		t.Fatal("direct handle/clone did not reuse the standalone node cache")
	}
	for i := 0; i < 2; i++ {
		if n, err := c.Exists(ctx, keys[0]).Result(); err != nil || n != 1 {
			t.Fatalf("EXISTS %d %v", n, err)
		}
	}
	if callsA.Load() != 2 {
		t.Fatal("EXISTS positive result not cached")
	}
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, keys[0]).Err(); err != nil {
		t.Fatal(err)
	}
	if callsA.Load() != 2 {
		t.Fatal("unchanged topology cleared cache")
	}
}

func TestClusterCSCEntryAgeConfiguration(t *testing.T) {
	for _, age := range []time.Duration{0, -time.Second, 5 * time.Second} {
		t.Run(age.String(), func(t *testing.T) {
			cfg := &ClientSideCacheConfig{MaxEntries: 23, MaxStaleness: age}
			opt := &ClusterOptions{ClientSideCacheConfig: cfg}
			a, b := opt.clientOptions(), opt.clientOptions()
			want := age
			if a.ClientSideCacheConfig.MaxStaleness != want || b.ClientSideCacheConfig.MaxStaleness != want {
				t.Fatal("node configuration changed the requested entry age")
			}
			if cfg.MaxStaleness != age || a.ClientSideCacheConfig == cfg || a.ClientSideCacheConfig == b.ClientSideCacheConfig {
				t.Fatal("node configuration aliases another node or the caller")
			}
			a.ClientSideCacheConfig.MaxEntries++
			if b.ClientSideCacheConfig.MaxEntries != 23 || cfg.MaxEntries != 23 {
				t.Fatal("node factory can mutate another node's cache configuration")
			}
		})
	}
	if (&ClusterOptions{}).clientOptions().ClientSideCacheConfig != nil {
		t.Fatal("default age enabled caching on a disabled cluster")
	}
	if NewLocalCache(CacheConfig{}).effectiveMaxStaleness() != 0 {
		t.Fatal("standalone default age changed")
	}
}

func TestClusterCSCTopologyReloadPreservesRoutedNodeCaches(t *testing.T) {
	var calls atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	var swapped atomic.Bool
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		first, second := a, b
		if swapped.Load() {
			first, second = second, first
		}
		return []ClusterSlot{
			{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: first}}},
			{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: second}}},
		}
	})
	opt.NewClient = func(o *Options) *Client {
		o.ClientSideCache = &clusterCSCRecordingCache{LocalCache: NewLocalCache(*o.ClientSideCacheConfig)}
		return NewClient(o)
	}
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	for _, key := range []string{"{bar}:a", "{foo}:b"} {
		if err := c.Get(ctx, key).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for _, swap := range []bool{false, true, false} {
		swapped.Store(swap)
		if _, err := c.state.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		nodes, _ := c.nodes.All()
		for _, node := range nodes {
			cache := node.Client.csc.(*clusterCSCRecordingCache)
			if cache.flushes.Load() != 0 || cache.deletes.Load() != 0 || cache.Stats().Entries != 1 {
				t.Fatal("routing reload cleared a standalone node cache")
			}
		}
	}
}

func TestClusterCSCRoutingLossClearsOnlyRemovedNode(t *testing.T) {
	ctx := context.Background()
	var callsA, callsB atomic.Int32
	var changed atomic.Bool
	a := newClusterCSCTestServer(t, func(command string) string {
		if command == "get" {
			callsA.Add(1)
			if changed.Load() {
				return "$2\r\nv2\r\n"
			}
			return "$2\r\nv1\r\n"
		}
		return clusterCSCTestReply(&callsA)(command)
	})
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&callsB))
	var removed bool
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		if removed {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: b}}}}
		}
		return []ClusterSlot{
			{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}}},
			{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}},
		}
	})
	opt.NewClient = func(o *Options) *Client {
		o.ClientSideCache = &clusterCSCRecordingCache{LocalCache: NewLocalCache(*o.ClientSideCacheConfig)}
		return NewClient(o)
	}
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	if got, err := c.Get(ctx, "{bar}:key").Result(); err != nil || got != "v1" {
		t.Fatalf("initial read: %q, %v", got, err)
	}
	if err := c.Get(ctx, "{foo}:other").Err(); err != nil {
		t.Fatal(err)
	}
	old, err := c.MasterForKey(ctx, "{bar}:key")
	if err != nil {
		t.Fatal(err)
	}
	cache := old.csc.(*clusterCSCRecordingCache)
	token, fetch := cache.Reserve("pending", []string{"pending"})
	if !fetch {
		t.Fatal("expected a new reservation")
	}
	var lateToken uint64
	cache.afterFlush = func() {
		if cache.flushes.Load() != 1 {
			return
		}
		state := c.state.state.Load().(*clusterState)
		if state.slotNodes(hashtag.Slot("{bar}:key"))[0].Client == old {
			t.Error("removed node is still routed during departure cleanup")
		}
		// A request that selected A before removal can still refill its cache
		// after cleanup. Re-entry must clear that value and any new reservation.
		if got, err := old.Get(ctx, "{bar}:key").Result(); err != nil || got != "v1" {
			t.Errorf("read during cleanup: %q, %v", got, err)
		}
		var fetch bool
		lateToken, fetch = cache.Reserve("late", []string{"late"})
		if !fetch {
			t.Error("expected a reservation after departure cleanup")
		}
	}
	removed = true
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.flushes.Load() != 1 {
		t.Fatal("routing loss did not clear the removed node before registry GC")
	}
	if cache.FulfillOwned("pending", token, 1, []byte("stale")) {
		t.Fatal("removed node accepted a fill reserved before routing loss")
	}
	if err := c.Get(ctx, "{foo}:other").Err(); err != nil || callsB.Load() != 1 {
		t.Fatalf("retained node lost its cached result: %v, calls=%d", err, callsB.Load())
	}
	// No invalidation or disconnect: a later stale slot map reuses the same
	// node client, but must not revive its value from before routing loss.
	changed.Store(true)
	removed = false
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.FulfillOwned("late", lateToken, 1, []byte("stale")) {
		t.Error("re-entering node accepted a fill reserved after routing loss")
	}
	if got, err := c.Get(ctx, "{bar}:key").Result(); err != nil || got != "v2" {
		t.Fatalf("read after re-entry: %q, %v", got, err)
	}
	if callsA.Load() != 3 || cache.flushes.Load() != 2 {
		t.Fatalf("calls=%d, flushes=%d; want 3, 2", callsA.Load(), cache.flushes.Load())
	}
}

func TestClusterCSCRoutingLossFlushesOnceAndPreservesReplicas(t *testing.T) {
	newNode := func() (*clusterNode, *clusterCSCRecordingCache) {
		cache := &clusterCSCRecordingCache{LocalCache: NewLocalCache(CacheConfig{})}
		return &clusterNode{Client: &Client{baseClient: &baseClient{csc: cache}}}, cache
	}
	a, cacheA := newNode()
	b, cacheB := newNode()
	disabled := &clusterNode{Client: &Client{baseClient: &baseClient{}}}
	states := []*clusterState{
		{slots: []*clusterSlot{
			{start: 0, end: 8191, nodes: []*clusterNode{a, b, disabled}},
			{start: 8192, end: 16383, nodes: []*clusterNode{a, b}},
		}},
		// A changes role but is still routed as a replica: no flush.
		{slots: []*clusterSlot{
			{start: 0, end: 8191, nodes: []*clusterNode{b, a}},
			{start: 8192, end: 16383, nodes: []*clusterNode{b, a}},
		}},
		{slots: []*clusterSlot{{start: 0, end: 16383, nodes: []*clusterNode{b}}}},
		{slots: []*clusterSlot{{start: 0, end: 16383, nodes: []*clusterNode{b}}}},
	}
	var next int
	holder := newClusterStateHolder(func(context.Context) (*clusterState, error) {
		state := states[next]
		next++
		return state, nil
	}, time.Minute)
	for i, want := range []int32{0, 0, 1, 1} {
		if _, err := holder.Reload(context.Background()); err != nil {
			t.Fatal(err)
		}
		if cacheA.flushes.Load() != want || cacheB.flushes.Load() != 0 {
			t.Fatalf("reload %d: flushes A=%d B=%d, want %d, 0", i, cacheA.flushes.Load(), cacheB.flushes.Load(), want)
		}
	}
}

func TestClusterCSCRoutingCleanupLastPublicationWins(t *testing.T) {
	ctx := context.Background()
	a := &clusterNode{Client: &Client{baseClient: &baseClient{}}}
	cacheB := &clusterCSCRecordingCache{LocalCache: NewLocalCache(CacheConfig{})}
	cacheC := &clusterCSCRecordingCache{LocalCache: NewLocalCache(CacheConfig{})}
	b := &clusterNode{Client: &Client{baseClient: &baseClient{csc: cacheB}}}
	c := &clusterNode{Client: &Client{baseClient: &baseClient{csc: cacheC}}}
	withNodes := func(nodes ...*clusterNode) *clusterState {
		return &clusterState{
			slots:     []*clusterSlot{{start: 0, end: 16383, nodes: nodes}},
			createdAt: time.Now(),
		}
	}
	initial, adding, competing := withNodes(a), withNodes(a, b), withNodes(a, c)
	var loads atomic.Int32
	holder := newClusterStateHolder(func(context.Context) (*clusterState, error) {
		if loads.Add(1) == 1 {
			return adding, nil
		}
		return competing, nil
	}, time.Minute)
	holder.state.Store(initial)
	cacheB.beforeFlush = func() {
		if holder.state.Load() != initial {
			t.Error("entering node was published before cache cleanup")
		}
		// Publish a competing snapshot while cleanup is in progress. Calling
		// back synchronously also detects a mutex held around custom Flush.
		if got, err := holder.ReloadOrGet(ctx); err != nil || got != competing {
			t.Errorf("competing reload: %p, %v", got, err)
		}
	}
	cacheC.beforeFlush = func() {
		want := initial // Entering during the nested reload.
		if cacheC.flushes.Load() == 2 {
			want = adding // Departing after the outer reload publishes.
		}
		if holder.state.Load() != want {
			t.Error("cache cleanup used the wrong publication order")
		}
	}
	completed := make(chan *clusterState, 1)
	go func() {
		state, err := holder.Reload(ctx)
		if err != nil {
			t.Error(err)
		}
		completed <- state
	}()
	select {
	case state := <-completed:
		if state != adding || holder.state.Load() != adding {
			t.Fatal("the last publication must win and return its own snapshot")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("custom cache callback blocked reload")
	}
	if cacheB.flushes.Load() != 1 || cacheC.flushes.Load() != 2 {
		t.Fatalf("flushes B=%d C=%d; want 1, 2 from the actual replaced snapshot", cacheB.flushes.Load(), cacheC.flushes.Load())
	}
}

// No notification reaches A when another client creates a missing key on B.
// Without an entry-age limit, rejecting absence-dependent replies must still
// let the next read reach Redis and discover the redirect.
func TestClusterCSCUnobservedMigrationDoesNotCacheAbsence(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		for _, tc := range []struct {
			command string
			before  string
			after   string
			old     interface{}
			want    interface{}
			err     error
		}{
			{"get", "$-1\r\n", "$5\r\nvalue\r\n", nil, "value", Nil},
			{"exists", ":0\r\n", ":1\r\n", int64(0), int64(1), nil},
			{"mget", "*2\r\n$-1\r\n$0\r\n\r\n", "*2\r\n$5\r\nvalue\r\n$0\r\n\r\n", []interface{}{nil, ""}, []interface{}{"value", ""}, nil},
		} {
			t.Run(fmt.Sprintf("%s/coalesce=%v", tc.command, coalesce), func(t *testing.T) {
				var migrating atomic.Bool
				var readsA, readsB atomic.Int32
				b := newClusterCSCTestServer(t, func(cmd string) string {
					if cmd == "hello" {
						return "%0\r\n"
					}
					if cmd == tc.command {
						readsB.Add(1)
						return tc.after
					}
					return "+OK\r\n"
				})
				key := "{option-b}:key"
				a := newClusterCSCTestServer(t, func(cmd string) string {
					if cmd == "hello" {
						return "%0\r\n"
					}
					if cmd == tc.command {
						readsA.Add(1)
						if migrating.Load() {
							return fmt.Sprintf("-ASK %d %s\r\n", hashtag.Slot(key), b)
						}
						return tc.before
					}
					return "+OK\r\n"
				})
				opt := clusterCSCTestOptions(func() []ClusterSlot {
					return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: a}}}}
				})
				opt.ClientSideCacheConfig.MaxStaleness = 0 // Exercise the cluster default.
				opt.ClientSideCacheCoalesceMisses = coalesce
				c := NewClusterClient(opt)
				t.Cleanup(func() { _ = c.Close() })
				ctx := context.Background()
				args := []interface{}{tc.command, key}
				if tc.command == "mget" {
					args = append(args, "{option-b}:other")
				}
				read := func(want interface{}, wantErr error) {
					t.Helper()
					cmd := NewCmd(ctx, args...)
					if err := c.Process(ctx, cmd); err != wantErr || !reflect.DeepEqual(cmd.Val(), want) {
						t.Fatalf("read: value=%#v error=%v; want %#v, %v", cmd.Val(), err, want, wantErr)
					}
				}
				read(tc.old, tc.err)
				read(tc.old, tc.err)
				if readsA.Load() != 2 || c.CSCStats().Entries != 0 {
					t.Fatal("absence-dependent reply was cached")
				}
				migrating.Store(true) // No invalidation or disconnect reaches our client.
				read(tc.want, nil)
				if readsA.Load() != 3 || readsB.Load() != 1 {
					t.Fatalf("next read did not expose ASK: A=%d B=%d", readsA.Load(), readsB.Load())
				}
				destination, _ := c.nodes.GetOrCreate(b)
				if destination.Client.CSCStats().Entries != 0 {
					t.Fatal("ASK-directed response populated destination cache")
				}
			})
		}
	}
}

func TestClusterCSCRedirectEvictsKeysAcrossConnections(t *testing.T) {
	for _, redirect := range []string{"MOVED", "ASK"} {
		t.Run(redirect, func(t *testing.T) {
			ctx := context.Background()
			cache := NewLocalCache(CacheConfig{MaxEntries: 32})
			c := &baseClient{opt: &Options{cscPresenceOnly: true}, csc: cache, cscKeyPrefix: "node:"}
			for _, entry := range []struct {
				key, dependency string
				owner           uint64
			}{
				{"get:k", "node:k", 1},
				{"strlen:k", "node:k", 2},
				{"get:other", "node:other", 1},
				{"other-node:k", "other-node:k", 1},
			} {
				token, _ := cache.Reserve(entry.key, []string{entry.dependency})
				if !cache.FulfillOwned(entry.key, token, entry.owner, []byte("value")) {
					t.Fatal("fill failed")
				}
			}
			token, _ := cache.Reserve("pending:k", []string{cscNamespacedKey("node:", "k")})
			err := fmt.Errorf("wrapped: %w", proto.ParseErrorReply([]byte("-"+redirect+" 7629 localhost:7001")))
			c.cscEvictOnRedirect([]string{"node:k"}, err)
			for _, key := range []string{"get:k", "strlen:k"} {
				if _, hit := cache.Get(ctx, key); hit {
					t.Fatalf("dependent entry survived: %s", key)
				}
			}
			if cache.FulfillOwned("pending:k", token, 2, []byte("late")) {
				t.Fatal("canceled reservation accepted late publication")
			}
			for _, key := range []string{"get:other", "other-node:k"} {
				if _, hit := cache.Get(ctx, key); !hit {
					t.Fatalf("unrelated entry evicted: %s", key)
				}
			}
		})
	}
}

func TestClusterCSCRedirectOnlyForClusterAndRedirects(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, err := range []error{nil, Nil, fmt.Errorf("transport"), proto.ParseErrorReply([]byte("-TRYAGAIN retry")), proto.ParseErrorReply([]byte("-MOVED 1 localhost:7001"))} {
			cache := NewLocalCache(CacheConfig{MaxEntries: 4})
			c := &baseClient{opt: &Options{cscPresenceOnly: cluster}, csc: cache, cscKeyPrefix: "n:"}
			token, _ := cache.Reserve("get:k", []string{cscNamespacedKey("n:", "k")})
			cache.FulfillOwned("get:k", token, 1, []byte("v"))
			c.cscEvictOnRedirect([]string{"n:k"}, err)
			_, hit := cache.Get(context.Background(), "get:k")
			moved := false
			if err != nil {
				moved, _, _ = isMovedError(err)
			}
			if hit == (cluster && moved) {
				t.Fatalf("cluster=%v err=%v hit=%v", cluster, err, hit)
			}
		}
	}
}

func TestClusterCSCFetchRedirectEvictsOtherDependentResults(t *testing.T) {
	for _, path := range []string{"foreground", "refresh", "coalesced", "abandoned"} {
		for _, redirect := range []string{"ASK", "MOVED"} {
			t.Run(path+"/"+redirect, func(t *testing.T) {
				raw := "-" + redirect + " 7629 localhost:7001\r\n"
				addr := newClusterCSCTestServer(t, func(command string) string {
					if command == "hello" {
						return "%0\r\n"
					}
					if command == "get" {
						return raw
					}
					return "+OK\r\n"
				})
				c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
					return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
				}))
				defer c.Close()
				ctx := context.Background()
				node, err := c.MasterForKey(ctx, "k")
				if err != nil {
					t.Fatal(err)
				}
				if err := node.Ping(ctx).Err(); err != nil {
					t.Fatal(err)
				}
				dependency := cscNamespacedKey(node.cscKeyPrefix, "k")
				for _, entry := range []struct{ key, dependency string }{
					{"dependent", dependency}, {"unrelated", cscNamespacedKey(node.cscKeyPrefix, "other")},
				} {
					token, _ := node.csc.Reserve(entry.key, []string{entry.dependency})
					if !node.csc.FulfillOwned(entry.key, token, 1, []byte("v")) {
						t.Fatal("fill failed")
					}
				}
				cmd := NewStringCmd(ctx, "get", "k")
				key, _ := cscRenderEntryKey(node.cscKeyPrefix, node.metadataView().cscFingerprint, cmd)
				if path == "foreground" {
					if err := node.Process(ctx, cmd); err == nil {
						t.Fatal("redirect was lost")
					}
				} else if path == "refresh" {
					if _, err := node.refreshInvalidatedBatch(ctx, []cscRefreshTarget{{cacheKey: key, redisKeys: []string{dependency}}}); err != nil {
						t.Fatal(err)
					}
				} else {
					token, _ := node.csc.Reserve(key, []string{dependency})
					req := &cscMissReq{cmd: cmd, cacheKey: key, token: token, redisKeys: []string{dependency}, done: make(chan error, 1)}
					if path == "abandoned" {
						req.cmd = nil
						req.apply.Store(cscReqAbandoned)
					}
					mc := &cscMissCoalescer{c: node.baseClient}
					mc.applyAndSettle(req, []byte(raw), 1, 0)
					if err := <-req.done; err == nil {
						t.Fatal("redirect was lost")
					}
				}
				if _, hit := node.csc.Get(ctx, "dependent"); hit {
					t.Fatal("another result for the redirected key survived")
				}
				if _, hit := node.csc.Get(ctx, "unrelated"); !hit {
					t.Fatal("unrelated result evicted")
				}
			})
		}
	}
}

func TestClusterCSCRedirectWithoutKnownKeysDoesNotFlush(t *testing.T) {
	cache := &clusterCSCRecordingCache{LocalCache: NewLocalCache(CacheConfig{MaxEntries: 4})}
	c := &baseClient{opt: &Options{cscPresenceOnly: true}, csc: cache}
	token, _ := cache.Reserve("cached", []string{"node:k"})
	cache.FulfillOwned("cached", token, 1, []byte("v"))
	for _, redirect := range []string{"MOVED", "ASK"} {
		c.cscEvictOnRedirect(nil, proto.ParseErrorReply([]byte("-"+redirect+" 1 localhost:7001")))
	}
	if cache.flushes.Load() != 0 || cache.deletes.Load() != 0 {
		t.Fatal("unknown dependencies triggered cache cleanup")
	}
	if _, hit := cache.Get(context.Background(), "cached"); !hit {
		t.Fatal("unknown dependencies discarded cached data")
	}
}

type clusterCSCRecordingCache struct {
	*LocalCache
	flushes     atomic.Int32
	deletes     atomic.Int32
	beforeFlush func()
	afterFlush  func()
}

func TestClusterCSCRefreshRedirectPreservesUnrelatedEntries(t *testing.T) {
	for _, redirect := range []string{"ASK", "MOVED"} {
		t.Run(redirect, func(t *testing.T) {
			var migrating atomic.Bool
			b := newClusterCSCTestServer(t, func(cmd string) string {
				if cmd == "hello" {
					return "%0\r\n"
				}
				if cmd == "get" {
					return "$5\r\nvalue\r\n"
				}
				return "+OK\r\n"
			})
			a := newClusterCSCTestServer(t, func(cmd string) string {
				if cmd == "hello" {
					return "%0\r\n"
				}
				if cmd == "get" {
					if migrating.Load() {
						return fmt.Sprintf("-%s %d %s\r\n", redirect, hashtag.Slot("{bar}:refresh"), b)
					}
					return "$0\r\n\r\n"
				}
				return "+OK\r\n"
			})
			c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{
					{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}}},
					{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}},
				}
			}))
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			for _, key := range []string{"{bar}:refresh", "{bar}:sibling"} {
				if err := c.Get(ctx, key).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if value := c.Get(ctx, "{foo}:destination").Val(); value != "value" {
				t.Fatal(value)
			}
			source, _ := c.MasterForKey(ctx, "{bar}:refresh")
			destination, _ := c.MasterForKey(ctx, "{foo}:destination")
			key, _ := cscRenderEntryKey(source.cscKeyPrefix, source.metadataView().cscFingerprint, NewStringCmd(ctx, "get", "{bar}:refresh"))
			dependency := cscNamespacedKey(source.cscKeyPrefix, "{bar}:refresh")
			source.csc.DeleteByRedisKey(dependency)
			migrating.Store(true)
			// Only the refresh sees the redirect. Its canceled reservation must
			// not discard the unrelated sibling or destination entry.
			count, err := source.refreshInvalidatedBatch(ctx, []cscRefreshTarget{{cacheKey: key, redisKeys: []string{dependency}}})
			if err != nil || count != 0 {
				t.Fatalf("refresh: %d %v", count, err)
			}
			if source.CSCStats().Entries != 1 {
				t.Fatal("refresh redirect evicted unrelated source entries")
			}
			// The destination's unrelated key survives both redirect types.
			wantEntries := int64(1)
			if got := int64(destination.CSCStats().Entries); got != wantEntries {
				t.Fatalf("destination entries after %s: %d, want %d", redirect, got, wantEntries)
			}
			if value, err := c.Get(ctx, "{bar}:refresh").Result(); err != nil || value != "value" {
				t.Fatalf("foreground redirect: %q %v", value, err)
			}
		})
	}
}

func (c *clusterCSCRecordingCache) Flush() int {
	c.flushes.Add(1)
	if c.beforeFlush != nil {
		c.beforeFlush()
	}
	n := c.LocalCache.Flush()
	if c.afterFlush != nil {
		c.afterFlush()
	}
	return n
}

func (c *clusterCSCRecordingCache) DeleteByRedisKey(key string) int {
	c.deletes.Add(1)
	return c.LocalCache.DeleteByRedisKey(key)
}

func TestClusterCSCReloadIntervalDoesNotDisableNodeCache(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.ClusterStateReloadInterval = -1
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || c.CSCStats().Entries != 1 {
		t.Fatal("routing reload interval disabled standalone caching")
	}
}

func TestClusterCSCUniversalAndFactoryControls(t *testing.T) {
	cfg := &ClientSideCacheConfig{MaxEntries: 23}
	u := &UniversalOptions{ClientSideCacheConfig: cfg, ClientSideCacheCoalesceMisses: true, ClientSideCacheRefreshOnInvalidate: true, ClientSideCacheInvalidationBatchWindow: time.Millisecond}
	o := u.Cluster().clientOptions()
	if o.ClientSideCacheConfig == cfg || o.ClientSideCacheConfig.MaxEntries != cfg.MaxEntries || o.ClientSideCacheConfig.MaxStaleness != 0 || cfg.MaxStaleness != 0 || !o.ClientSideCacheCoalesceMisses || !o.ClientSideCacheRefreshOnInvalidate || o.ClientSideCacheInvalidationBatchWindow != time.Millisecond {
		t.Fatal("configuration not propagated")
	}
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.NewClient = func(o *Options) *Client { o.cscPresenceOnly = false; return NewClient(o) }
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("factory discarded controls but retained active CSC")
	}
}

func TestClusterCSCFailoverConfiguration(t *testing.T) {
	cfg := &ClientSideCacheConfig{MaxEntries: 23, MaxStaleness: time.Minute}
	u := &UniversalOptions{
		MasterName: "csc-primary", Addrs: []string{":26379"},
		ClientSideCacheConfig:                  cfg,
		ClientSideCacheStrategy:                CSCStrategySharedTracking,
		ClientSideCacheRefreshOnInvalidate:     true,
		ClientSideCacheCoalesceMisses:          true,
		ClientSideCacheInvalidationBatchWindow: time.Millisecond,
	}
	f := u.Failover()
	o := f.clusterOptions().clientOptions()
	if o.ClientSideCacheConfig == nil || !reflect.DeepEqual(*o.ClientSideCacheConfig, *cfg) ||
		o.ClientSideCacheConfig == cfg || o.ClientSideCacheStrategy != u.ClientSideCacheStrategy ||
		!o.ClientSideCacheRefreshOnInvalidate || !o.ClientSideCacheCoalesceMisses ||
		o.ClientSideCacheInvalidationBatchWindow != u.ClientSideCacheInvalidationBatchWindow {
		t.Fatal("failover cluster dropped or aliased CSC configuration")
	}
	// Sentinel discovery connections and single-node failover clients do not
	// participate in Cluster CSC.
	if f.sentinelOptions(":26379").ClientSideCacheConfig != nil || f.clientOptions().ClientSideCacheConfig != nil {
		t.Fatal("Cluster CSC configuration enabled caching on a non-cluster connection")
	}
}

func TestClusterCSCUniversalFailoverCluster(t *testing.T) {
	for _, route := range []string{"latency", "random", "cluster mode"} {
		for _, tc := range []struct {
			db    int
			cache bool
		}{{0, true}, {3, true}, {0, false}, {3, false}} {
			t.Run(fmt.Sprintf("%s/db=%d/cache=%v", route, tc.db, tc.cache), func(t *testing.T) {
				db := tc.db
				var calls, selects, connects atomic.Int32
				addr := newClusterCSCTestServer(t, func(cmd string) string {
					if cmd == "command" {
						return "*0\r\n" // Valid empty metadata; no failed discovery sockets.
					}
					if cmd == "select" {
						selects.Add(1)
					}
					return clusterCSCTestReply(&calls)(cmd)
				})
				u := &UniversalOptions{
					MasterName: "csc-primary", Addrs: []string{":26379"}, DB: db,
					Protocol: 3, PoolSize: 1, DisableIdentity: true,
					OnConnect: func(_ context.Context, cn *Conn) error {
						connects.Add(1)
						wantDB := 0
						if tc.cache {
							wantDB = db
						}
						if cn.opt.DB != wantDB {
							return fmt.Errorf("connection DB=%d, want %d", cn.opt.DB, wantDB)
						}
						return nil
					},
				}
				if tc.cache {
					u.ClientSideCacheConfig = &ClientSideCacheConfig{MaxEntries: 16}
				}
				switch route {
				case "latency":
					u.RouteByLatency = true
				case "random":
					u.RouteRandomly = true
				default:
					u.IsClusterMode = true
				}
				client := NewUniversalClient(u)
				t.Cleanup(func() { _ = client.Close() })
				c, ok := client.(*ClusterClient)
				if !ok {
					t.Fatal("universal failover path did not construct a ClusterClient")
				}
				// Replace discovery, not construction, with a deterministic primary.
				// There is no replica, so every routing mode selects this primary.
				c.opt.ClusterSlots = func(context.Context) ([]ClusterSlot, error) {
					return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
				}
				ctx := context.Background()
				for i := 0; i < 2; i++ {
					if err := c.Get(ctx, "key").Err(); err != nil {
						t.Fatal(err)
					}
				}
				// SharedTracking remains DB-0-only. Other databases must execute
				// normally, without attempting a SELECT from a CSC OnConnect hook.
				wantCalls, wantHits := int32(2), uint64(0)
				if tc.cache && db == 0 {
					wantCalls, wantHits = 1, 1
				}
				if calls.Load() != wantCalls || c.CSCStats().Hits != wantHits || connects.Load() == 0 {
					t.Fatalf("calls=%d connects=%d stats=%+v", calls.Load(), connects.Load(), c.CSCStats())
				}
				node, err := c.MasterForKey(ctx, "key")
				if err != nil {
					t.Fatal(err)
				}
				wantDB := 0
				if tc.cache {
					wantDB = db
				}
				if node.opt.DB != wantDB || (tc.cache && db == 0 && node.cscKeyPrefix != cscNamespacePrefix(db, "")) {
					t.Fatal("node cache namespace does not match the selected database")
				}
				if wantHits == 0 && node.csc != nil {
					t.Fatal("disabled or nonzero-DB client attached CSC")
				}
				if db != 0 && selects.Load() == 0 {
					t.Fatal("node connection did not select the configured database")
				}
			})
		}
	}
}

func TestClusterCSCDisabledAndInitializationFallback(t *testing.T) {
	for _, mode := range []string{"disabled", "RESP2", "tracking rejected", "factory initialization rejects tracking"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			rejected := mode == "tracking rejected" || mode == "factory initialization rejects tracking"
			addr := newClusterCSCTestServer(t, func(command string) string {
				if rejected && command == "client" {
					return "-ERR tracking is unavailable\r\n"
				}
				return clusterCSCTestReply(&calls)(command)
			})
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
			})
			switch mode {
			case "disabled":
				opt.ClientSideCacheConfig = nil
			case "RESP2":
				opt.Protocol = 2
			default:
				opt.ClientSideCacheConfig.DrainInterval = time.Millisecond
				opt.ClientSideCacheRefreshOnInvalidate = true
				opt.ClientSideCacheCoalesceMisses = true
			}
			if mode == "factory initialization rejects tracking" {
				opt.NewClient = func(o *Options) *Client {
					c := NewClient(o)
					// Self-disable may tear down workers before or during Cluster's
					// registration of the returned client. No mutable handles may
					// be read by registration.
					if err := c.Ping(context.Background()).Err(); err != nil {
						t.Error(err)
					}
					return c
				}
			}
			c := NewClusterClient(opt)
			t.Cleanup(func() { _ = c.Close() })
			for i := 0; i < 2; i++ {
				if err := c.Get(context.Background(), "k").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if stats := c.CSCStats(); calls.Load() != 2 || stats.Hits != 0 || stats.Entries != 0 {
				t.Fatalf("fallback served cache: calls=%d stats=%+v", calls.Load(), stats)
			}
			if rejected {
				n, _ := c.nodes.GetOrCreate(addr)
				select {
				case <-n.Client.cscDrainHandle.done:
				case <-time.After(time.Second):
					t.Fatal("failed tracking initialization left CSC workers running")
				}
			}
		})
	}
}

func TestClusterCSCFactoryCloneWithoutControlsIsUncached(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	})
	opt.NewClient = func(o *Options) *Client {
		o.cscPresenceOnly = false
		return NewClient(o).WithTimeout(time.Second)
	}
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("factory clone discarded controls but retained active CSC")
	}
}

func TestClusterCSCFactoryCloneJoinsCanonicalWorkers(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	})
	opt.ClientSideCacheConfig.DrainInterval = time.Hour
	opt.ClientSideCacheRefreshOnInvalidate = true
	var canonical *Client
	opt.NewClient = func(o *Options) *Client {
		canonical = NewClient(o)
		t.Cleanup(func() { _ = canonical.Close() })
		return canonical.WithTimeout(time.Second)
	}
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("factory clone preserving controls failed to cache")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canonical.cscDrainHandle.done:
	default:
		t.Fatal("Cluster Close returned before the factory clone's canonical CSC workers stopped")
	}
	if c.CSCStats().Hits != 0 {
		t.Fatal("closed cluster retained node statistics")
	}
}

func TestClusterCSCFailedReloadPreservesNodeCache(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_ = c.Get(ctx, "k").Err()
	state := *c.state.state.Load().(*clusterState)
	state.createdAt = time.Now().Add(-2 * time.Minute)
	c.state.state.Store(&state)
	c.state.load = func(context.Context) (*clusterState, error) { return nil, fmt.Errorf("test unavailable topology") }
	if _, err := c.state.Reload(ctx); err == nil {
		t.Fatal("failed reload unexpectedly succeeded")
	}
	if c.state.state.Load().(*clusterState).createdAt != state.createdAt {
		t.Fatal("failed reload renewed freshness")
	}
	if err := c.Get(ctx, "k").Err(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("failed reload disabled the standalone cache")
	}
}

func TestClusterCSCNewAndRemovedNodesAndPerNodeLimits(t *testing.T) {
	var calls atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	var switched atomic.Bool
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		addr := a
		if switched.Load() {
			addr = b
		}
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	})
	opt.ClientSideCacheConfig.MaxEntries = 2
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = c.Get(ctx, fmt.Sprintf("key%d", i)).Err()
	}
	na, _ := c.nodes.GetOrCreate(a)
	if got := na.Client.CSCStats().Entries; got > 2 {
		t.Fatalf("per-node capacity exceeded: %d", got)
	}
	switched.Store(true)
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	nb, _ := c.nodes.GetOrCreate(b)
	for i := 0; i < 5; i++ {
		_ = c.Get(ctx, fmt.Sprintf("key%d", i)).Err()
	}
	if na.Client.csc == nb.Client.csc || nb.Client.CSCStats().Entries > 2 {
		t.Fatal("new node reused storage or exceeded local limit")
	}
	// Ordinary registry GC closes the removed standalone client, with no
	// separate cache retirement registry or Cluster cleanup worker.
	c.nodes.GC(c.state.state.Load().(*clusterState).generation)
	select {
	case <-na.Client.cscDrainHandle.done:
	default:
		t.Fatal("registry GC did not join the removed node's workers")
	}
	if na.Client.CSCStats().Entries != 0 || !nb.Client.cscActive.Load() {
		t.Fatal("removed node cleanup affected the wrong cache")
	}
}

func TestClusterCSCStatisticsOnlyIncludeRegisteredNodes(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		_ = c.Get(ctx, "k").Err()
	}
	old, _ := c.nodes.GetOrCreate(addr)
	// Model the registry's GC gap: removed from address lookup, but Close has
	// not joined the old workers yet. Recreating the endpoint starts a lifetime.
	c.nodes.mu.Lock()
	delete(c.nodes.nodes, addr)
	c.nodes.mu.Unlock()
	_, err := c.nodes.GetOrCreate(addr)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.CSCStats().Hits; got != 0 {
		t.Fatalf("unregistered node retained in statistics: %d", got)
	}
	_ = old.Client.Close()
	if got := c.CSCStats().Hits; got != 0 {
		t.Fatalf("closed node retained in statistics: %d", got)
	}
}

func TestClusterCSCDroppedClientRemainsCollectible(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	makeDropped := func() <-chan struct{} {
		c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
		_ = c.Get(context.Background(), "k").Err()
		collected := make(chan struct{})
		runtime.AddCleanup(c, func(done chan struct{}) { close(done) }, collected)
		// The ordinary routing GC timer retains node clients for a minute,
		// but must not retain the owning ClusterClient through its workers.
		return collected
	}
	stop := makeDropped()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		select {
		case <-stop:
			return
		default:
		}
		runtime.Gosched()
	}
	t.Fatal("CSC background work retained the owning ClusterClient")
}

func TestClusterCSCRetainedFactoryCloneOwnsItsWorkers(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	makeDropped := func() (*Client, <-chan struct{}) {
		opt := clusterCSCTestOptions(func() []ClusterSlot {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
		})
		opt.ClientSideCacheConfig.DrainInterval = time.Hour
		opt.NewClient = func(o *Options) *Client { return NewClient(o).WithTimeout(time.Second) }
		c := NewClusterClient(opt)
		if err := c.Get(context.Background(), "k").Err(); err != nil {
			_ = c.Close()
			t.Fatal(err)
		}
		node, _ := c.MasterForKey(context.Background(), "k")
		return node, node.lifecycleOwner.cscDrainHandle.done
	}
	handle, drained := makeDropped()
	defer handle.Close()
	runtime.GC()
	if err := handle.Get(context.Background(), "k").Err(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("retained handle lost its standalone cache")
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	default:
		t.Fatal("node Close did not join canonical workers")
	}
}

func TestClusterCSCNodeFullInvalidation(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	}))
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if err := c.Get(ctx, "k").Err(); err != nil {
		t.Fatal(err)
	}
	node, _ := c.MasterForKey(ctx, "k")
	lookupInvalidateHandler(node.pushProcessor).fullFlush(node.csc)
	if node.CSCStats().Entries != 0 {
		t.Fatal("full invalidation did not clear node cache")
	}
}

func TestClusterCSCRedirectsPreserveUnrelatedNodeCaches(t *testing.T) {
	for _, path := range []string{"command", "pipeline", "transaction", "full-duplex"} {
		for _, redirect := range []string{"ASK", "MOVED"} {
			t.Run(path+"/"+redirect, func(t *testing.T) {
				var moved atomic.Bool
				var multiA, multiB bool
				var muA, muB sync.Mutex
				b := newClusterCSCTestServer(t, func(command string) string {
					muB.Lock()
					defer muB.Unlock()
					switch command {
					case "hello":
						return "%0\r\n"
					case "multi":
						multiB = true
						return "+OK\r\n"
					case "exec":
						multiB = false
						return "*1\r\n+OK\r\n"
					case "set":
						if multiB {
							return "+QUEUED\r\n"
						}
						return "+OK\r\n"
					case "get":
						return "$1\r\nv\r\n"
					default:
						return "+OK\r\n"
					}
				})
				a := newClusterCSCTestServer(t, func(command string) string {
					muA.Lock()
					defer muA.Unlock()
					switch command {
					case "hello":
						return "%0\r\n"
					case "multi":
						multiA = true
						return "+OK\r\n"
					case "exec":
						multiA = false
						return "-EXECABORT Transaction discarded because of previous errors.\r\n"
					case "set":
						moved.Store(true)
						return fmt.Sprintf("-%s %d %s\r\n", redirect, hashtag.Slot("{bar}:key"), b)
					case "get":
						if moved.Load() {
							return fmt.Sprintf("-%s %d %s\r\n", redirect, hashtag.Slot("{bar}:key"), b)
						}
						if multiA {
							return "+QUEUED\r\n"
						}
						return "$0\r\n\r\n"
					default:
						return "+OK\r\n"
					}
				})
				opt := clusterCSCTestOptions(func() []ClusterSlot {
					return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}}}
				})
				opt.NewClient = func(o *Options) *Client {
					o.ClientSideCache = &clusterCSCRecordingCache{LocalCache: NewLocalCache(*o.ClientSideCacheConfig)}
					return NewClient(o)
				}
				c := NewClusterClient(opt)
				t.Cleanup(func() { _ = c.Close() })
				ctx := context.Background()
				key := "{bar}:key"
				// Uncached paths leave existing caches to server invalidations.
				// Unrelated keys must survive every redirect path.
				for _, cached := range []string{"{bar}:cached", "{foo}:cached"} {
					if err := c.Get(ctx, cached).Err(); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				switch path {
				case "command":
					err = c.Set(ctx, key, "v", 0).Err()
				case "pipeline":
					_, err = c.Pipelined(ctx, func(p Pipeliner) error { p.Set(ctx, key, "v", 0); return nil })
				case "transaction":
					_, err = c.TxPipelined(ctx, func(p Pipeliner) error { p.Set(ctx, key, "v", 0); return nil })
				case "full-duplex":
					ap, e := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
					if e != nil {
						t.Fatal(e)
					}
					err = ap.Set(ctx, key, "v", 0).Err()
					_ = ap.Close()
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, addr := range []string{a, b} {
					node, err := c.nodes.GetOrCreate(addr)
					if err != nil {
						t.Fatal(err)
					}
					cache := node.Client.csc.(*clusterCSCRecordingCache)
					if cache.flushes.Load() != 0 || cache.deletes.Load() != 0 {
						t.Fatalf("redirect invoked cache cleanup on %s: flushes=%d deletes=%d", addr, cache.flushes.Load(), cache.deletes.Load())
					}
				}
				before := c.CSCStats().Hits
				for _, cached := range []struct{ key, value string }{{"{bar}:cached", ""}, {"{foo}:cached", "v"}} {
					if value, err := c.Get(ctx, cached.key).Result(); err != nil || value != cached.value {
						t.Fatalf("redirect discarded unrelated cache entry %q: %q %v", cached.key, value, err)
					}
				}
				if stats := c.CSCStats(); stats.Hits != before+2 || stats.Entries != 2 {
					t.Fatalf("redirect changed unrelated node caches: %+v", stats)
				}
				if got, err := c.Get(ctx, key).Result(); err != nil || got != "v" {
					t.Fatalf("read did not follow redirect: %q %v", got, err)
				}
			})
		}
	}
}

func TestClusterCSCCachingIsNodeLocalAcrossRoles(t *testing.T) {
	var calls atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{
			{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}, {Addr: b}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}},
		}
	}))
	t.Cleanup(func() { _ = c.Close() })
	_, err := c.state.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	node, _ := c.nodes.GetOrCreate(b)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		cmd := NewStringCmd(ctx, "get", "{bar}:replica")
		if err := node.Client.Process(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || node.Client.CSCStats().Entries != 1 {
		t.Fatal("replica node did not use its standalone cache")
	}
	if err := c.Get(context.Background(), "{foo}:primary").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.state.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	if err := c.Get(context.Background(), "{foo}:primary").Err(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != before {
		t.Fatal("unchanged mixed-role map retired a primary's cache")
	}
	// Explicit bypass must survive another dispatch to an otherwise eligible
	// primary, as happens when a full-duplex retry re-enters Cluster routing.
	ctx = cscBypass(ctx)
	for i := 0; i < 2; i++ {
		cmd := NewStringCmd(ctx, "get", "{foo}:primary")
		if err := node.Client.Process(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != before+2 {
		t.Fatal("primary dispatch replaced explicit cache bypass")
	}
}

func BenchmarkClusterCSCRoutedHit(b *testing.B) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(b, func(command string) string {
		if command == "get" {
			calls.Add(1)
			return "$1\r\nv\r\n"
		}
		return clusterCSCTestReply(&calls)(command)
	})
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
	b.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_ = c.Get(ctx, "k").Err()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Get(ctx, "k").Err()
	}
}

func BenchmarkClientCSCHit(b *testing.B) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(b, func(command string) string {
		if command == "get" {
			return "$1\r\nv\r\n"
		}
		return clusterCSCTestReply(&calls)(command)
	})
	c := NewClient(&Options{
		Addr: addr, Protocol: 3, DisableIdentity: true,
		ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 16, MaxStaleness: time.Minute},
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	b.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_ = c.Get(ctx, "k").Err()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Get(ctx, "k").Err()
	}
}

func BenchmarkClusterCSCMiss(b *testing.B) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(b, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	}))
	b.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_ = c.Get(ctx, "warmup").Err()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Get(ctx, strconv.Itoa(i)).Err()
	}
}

func BenchmarkClusterCSCConcurrentHits(b *testing.B) {
	var calls atomic.Int32
	reply := func(command string) string {
		if command == "get" {
			return "$1\r\nv\r\n"
		}
		return clusterCSCTestReply(&calls)(command)
	}
	a, other := newClusterCSCTestServer(b, reply), newClusterCSCTestServer(b, reply)
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: other}}}}
	}))
	b.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	keys := []string{"{bar}:hit", "{foo}:hit"}
	for _, key := range keys {
		_ = c.Get(ctx, key).Err()
	}
	var worker atomic.Uint32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := keys[worker.Add(1)%2]
		for pb.Next() {
			_ = c.Get(ctx, key).Err()
		}
	})
}

func BenchmarkClusterCSCTopologyReload(b *testing.B) {
	var calls atomic.Int32
	a, other := newClusterCSCTestServer(b, clusterCSCTestReply(&calls)), newClusterCSCTestServer(b, clusterCSCTestReply(&calls))
	var swapped bool
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		first, second := a, other
		if swapped {
			first, second = second, first
		}
		return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: first}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: second}}}}
	}))
	b.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	for _, key := range []string{"{bar}:churn", "{foo}:churn"} {
		_ = c.Get(ctx, key).Err()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swapped = !swapped
		_, err := c.state.Reload(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = c.Get(ctx, "{bar}:churn").Err()
		_ = c.Get(ctx, "{foo}:churn").Err()
	}
}

func TestClusterCSCFixtureRequirement(t *testing.T) {
	if os.Getenv("GOREDIS_CSC_GATE_CHILD") == "1" {
		newClusterCSCLive(t)
		t.Fatal("fixture unexpectedly started without redis-server")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		ci       string
		required string
	}{
		{name: "local"},
		{name: "generic CI", ci: "true"},
		{name: "dedicated live job", ci: "true", required: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(binary, "-test.run=^TestClusterCSCFixtureRequirement$", "-test.v")
			cmd.Env = append(os.Environ(), "PATH=", "GOREDIS_CSC_GATE_CHILD=1",
				"CI="+tc.ci, "RE_CLUSTER=true", "GOREDIS_CSC_LIVE="+tc.required)
			out, err := cmd.CombinedOutput()
			if tc.required == "" {
				if err != nil || !bytes.Contains(out, []byte("--- SKIP: TestClusterCSCFixtureRequirement")) {
					t.Fatalf("expected skip without redis-server: %v\n%s", err, out)
				}
			} else if err == nil || !bytes.Contains(out, []byte("live CSC tests require redis-server")) {
				t.Fatalf("expected failure without required redis-server: %v\n%s", err, out)
			}
		})
	}
}

// Run against a private six-process OSS cluster, never the developer's database.
// CI runs these fixtures inside the integration test stack's Redis image.
func newClusterCSCLive(t *testing.T) (*ClusterClient, []*Client) {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("GOREDIS_CSC_LIVE") != "" {
			t.Fatal("live CSC tests require redis-server on PATH")
		}
		t.Skip("live CSC requires redis-server on PATH")
	}
	base := 18030
	for ; base < 19000; base += 6 {
		var listeners []net.Listener
		for _, offset := range []int{0, 1, 2, 3, 4, 5, 10000, 10001, 10002, 10003, 10004, 10005} {
			ln, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+offset))
			if e != nil {
				break
			}
			listeners = append(listeners, ln)
		}
		for _, ln := range listeners {
			_ = ln.Close()
		}
		if len(listeners) == 12 {
			break
		}
	}
	if base >= 19000 {
		t.Fatal("no free private Cluster ports")
	}
	dir := t.TempDir()
	busFlags := clusterCSCLiveBusFlags(t, binary, dir, base)
	var nodes []*Client
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		port := base + i
		var output bytes.Buffer
		t.Cleanup(func() {
			if t.Failed() {
				log, _ := os.ReadFile(filepath.Join(dir, fmt.Sprintf("node-%d.log", port)))
				if len(log) == 0 {
					log = output.Bytes()
				}
				if len(log) > 4096 {
					log = log[len(log)-4096:]
				}
				t.Logf("private Redis %d log:\n%s", port, log)
			}
		})
		args := []string{
			"--port", strconv.Itoa(port), "--bind", "127.0.0.1", "--protected-mode", "no",
			"--save", "", "--appendonly", "no", "--dbfilename", fmt.Sprintf("node-%d.rdb", port),
			"--repl-diskless-sync-delay", "0",
			"--cluster-enabled", "yes", "--cluster-config-file", fmt.Sprintf("node-%d.conf", port),
			"--cluster-announce-ip", "127.0.0.1", "--cluster-node-timeout", "1000", "--dir", dir, "--logfile", fmt.Sprintf("node-%d.log", port),
		}
		cmd := exec.Command(binary, append(args, busFlags...)...)
		// Redis opens startup logs before applying --dir on some versions.
		// Keep every generated file in this test's directory, even then.
		cmd.Dir = dir
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
		n := NewClient(&Options{
			Addr: fmt.Sprintf("127.0.0.1:%d", port), Protocol: 3, DisableIdentity: true,
			DialTimeout: 100 * time.Millisecond, MaxRetries: -1, MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		nodes = append(nodes, n)
		t.Cleanup(func() { _ = n.Close() })
		clusterCSCLiveOwnedReady(t, n, cmd.Process.Pid)
	}
	for _, n := range nodes[1:] {
		if err := n.ClusterMeet(ctx, "127.0.0.1", strconv.Itoa(base)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	clusterCSCLiveEventually(t, func() bool {
		for _, n := range nodes {
			s, err := n.ClusterNodes(ctx).Result()
			if err != nil || len(strings.Split(strings.TrimSpace(s), "\n")) != 6 {
				return false
			}
		}
		return true
	})
	for i := 0; i < 3; i++ {
		start, end := i*5461, (i+1)*5461-1
		if i == 2 {
			end = 16383
		}
		if err := nodes[i].ClusterAddSlotsRange(ctx, start, end).Err(); err != nil {
			t.Fatal(err)
		}
		id, err := nodes[i].ClusterMyID(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := nodes[i+3].ClusterReplicate(ctx, id).Err(); err != nil {
			t.Fatal(err)
		}
	}
	clusterCSCLiveEventually(t, func() bool {
		for _, n := range nodes {
			s, e := n.ClusterInfo(ctx).Result()
			if e != nil || !strings.Contains(s, "cluster_state:ok") {
				return false
			}
		}
		return true
	})
	info, _ := nodes[0].Info(ctx, "server").Result()
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			t.Log(strings.TrimSpace(line))
		}
	}
	c := NewClusterClient(&ClusterOptions{
		Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true,
		PoolSize: 4, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 128, DrainInterval: time.Millisecond},
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	t.Cleanup(func() { _ = c.Close() })
	return c, nodes
}

// Probe with a short-lived standalone process: new servers require an explicit
// cluster-bus security setting before Cluster startup, while older versions
// reject that setting. Do not guess support from a development version number.
func clusterCSCLiveBusFlags(t *testing.T, binary, dir string, port int) []string {
	t.Helper()
	var output bytes.Buffer
	cmd := exec.Command(binary, "--port", strconv.Itoa(port), "--bind", "127.0.0.1", "--protected-mode", "no",
		"--save", "", "--appendonly", "no", "--dir", dir)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var once sync.Once
	stop := func() { once.Do(func() { _ = cmd.Process.Kill(); <-done }) }
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Logf("private Redis startup probe:\n%s", output.String())
		}
	})
	c := NewClient(&Options{
		Addr: fmt.Sprintf("127.0.0.1:%d", port), Protocol: 3, DisableIdentity: true, MaxRetries: -1,
		DialTimeout: 100 * time.Millisecond, MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	clusterCSCLiveOwnedReady(t, c, cmd.Process.Pid)
	cfg, err := c.ConfigGet(ctx, "cluster-bus-port-protected-mode").Result()
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	stop()
	if _, supported := cfg["cluster-bus-port-protected-mode"]; supported {
		// Every bus peer is bound to loopback and belongs to this fixture.
		return []string{"--cluster-bus-port-protected-mode", "no"}
	}
	return nil
}

func clusterCSCLiveOwnedReady(t *testing.T, c *Client, pid int) {
	t.Helper()
	// Port probing cannot reserve ports across process startup. Before issuing
	// any Cluster mutation, reject a different service that won that race.
	clusterCSCLiveEventually(t, func() bool {
		info, err := c.Info(context.Background(), "server").Result()
		if err != nil {
			return false
		}
		for _, line := range strings.Split(info, "\n") {
			if strings.HasPrefix(line, "process_id:") {
				if strings.TrimSpace(line) != "process_id:"+strconv.Itoa(pid) {
					t.Fatalf("private Redis port belongs to another process: %s, expected PID %d", strings.TrimSpace(line), pid)
				}
				return true
			}
		}
		t.Fatal("private Redis did not report its process identity")
		return false
	})
}

func clusterCSCLiveEventually(t testing.TB, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("live Cluster condition did not complete")
}

func TestClusterCSCLiveReplicaInvalidation(t *testing.T) {
	writer, _ := newClusterCSCLive(t)
	ctx := context.Background()
	reader := NewClusterClient(&ClusterOptions{
		Addrs: writer.opt.Addrs, Protocol: 3, ReadOnly: true, DisableIdentity: true,
		ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 32, DrainInterval: time.Millisecond},
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	t.Cleanup(func() { _ = reader.Close() })
	key := "{csc-replica}:key"
	// Do not accidentally exercise ReadOnly's primary fallback while the new
	// replica is still completing its initial synchronization. CLUSTER SHARDS
	// includes replicas while their health is still loading.
	clusterCSCLiveEventually(t, func() bool {
		state, err := reader.state.Reload(ctx)
		if err != nil {
			return false
		}
		entry := state.slotEntry(hashtag.Slot(key))
		return entry != nil && len(entry.nodes) > 1 && state.nodeOnline(entry.nodes[1])
	})
	for _, value := range []string{"before", "after"} {
		if err := writer.Set(ctx, key, value, 0).Err(); err != nil {
			t.Fatal(err)
		}
		clusterCSCLiveEventually(t, func() bool {
			got, err := reader.Get(ctx, key).Result()
			return err == nil && got == value
		})
		before := reader.CSCStats().Hits
		if got, err := reader.Get(ctx, key).Result(); err != nil || got != value {
			t.Fatalf("replica cached read: %q %v", got, err)
		}
		if reader.CSCStats().Hits != before+1 {
			t.Fatal("replica-routed read did not hit its node cache")
		}
	}
	state, err := reader.state.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, primary := range state.Masters {
		if primary.Client.CSCStats().Entries != 0 {
			t.Fatal("replica-routed read populated a primary cache")
		}
	}
}

func TestClusterCSCLiveAtomicSlotMigration(t *testing.T) {
	reader, nodes := newClusterCSCLive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := nodes[0].Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	var major, minor int
	for _, line := range strings.Split(info, "\n") {
		if version, ok := strings.CutPrefix(line, "redis_version:"); ok {
			if _, err := fmt.Sscanf(version, "%d.%d", &major, &minor); err != nil {
				t.Fatal(err)
			}
		}
	}
	// This private fixture's version can differ from the Ginkgo suite's server.
	if major == 0 {
		t.Fatal("private Redis did not report its version")
	}
	if major < 8 || (major == 8 && minor < 4) {
		t.Skip("atomic slot migration requires Redis 8.4+")
	}
	key := "{csc-atomic-migration}:key"
	if err := reader.Set(ctx, key, "before", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if got, err := reader.Get(ctx, key).Result(); err != nil || got != "before" {
			t.Fatalf("warm cache: %q %v", got, err)
		}
	}
	if reader.CSCStats().Hits == 0 {
		t.Fatal("migration must start with a verified cache hit")
	}
	source, err := reader.MasterForKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var destination *Client
	for _, node := range nodes[:3] {
		if node.opt.Addr != source.opt.Addr {
			destination = node
			break
		}
	}
	if destination == nil {
		t.Fatal("no importing node")
	}
	// Start probing before IMPORT to write as soon as the importer accepts it,
	// not after STATUS/completed or a topology poll has hidden the handoff window.
	written := make(chan time.Time, 1)
	writeErr := make(chan error, 1)
	writerDone := make(chan struct{})
	defer func() { cancel(); <-writerDone }()
	go func() {
		defer close(writerDone)
		for ctx.Err() == nil {
			err := destination.Set(ctx, key, "after", 0).Err()
			if err == nil {
				written <- time.Now()
				return
			}
			if moved, ask, _ := isMovedError(err); !moved && !ask && !IsTryAgainError(err) {
				writeErr <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	slot := hashtag.Slot(key)
	task, err := destination.Do(ctx, "CLUSTER", "MIGRATION", "IMPORT", slot, slot).Text()
	if err != nil {
		t.Fatal(err)
	}
	var acknowledged time.Time
	select {
	case acknowledged = <-written:
	case err := <-writeErr:
		t.Fatalf("importer write: %v", err)
	case <-ctx.Done():
		t.Fatal("importer never accepted the write")
	}
	staleReads := 0
	var lastStale time.Duration
	for {
		if ctx.Err() != nil {
			t.Fatalf("cache did not converge after atomic migration: stale reads=%d", staleReads)
		}
		value, err := reader.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("read during atomic migration: %v", err)
		}
		if value == "after" {
			t.Logf("stale reads=%d, last stale=%s, first fresh=%s", staleReads, lastStale, time.Since(acknowledged))
			break
		}
		if value != "before" {
			t.Fatalf("unexpected value during atomic migration: %q", value)
		}
		staleReads++
		lastStale = time.Since(acknowledged)
		time.Sleep(time.Millisecond)
	}
	// Measure rather than require a positive window or a machine-specific timing
	// bound: server versions may close the window, and CI scheduling adds latency.
	status, err := destination.Do(ctx, "CLUSTER", "MIGRATION", "STATUS", "ID", task).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("atomic migration %s status: %v", task, status)
}

func TestClusterCSCLiveMissingKeyMigration(t *testing.T) {
	c, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	key := "{csc-cluster-absence}:key"
	for i := 0; i < 2; i++ {
		if err := c.Get(ctx, key).Err(); err != Nil {
			t.Fatal(err)
		}
	}
	if stats := c.CSCStats(); stats.Hits != 0 || stats.Entries != 0 {
		t.Fatalf("missing reads populated the cache: %+v", stats)
	}
	source, err := c.MasterForKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var a, b *Client
	for _, n := range nodes[:3] {
		if n.opt.Addr == source.opt.Addr {
			a = n
		} else if b == nil {
			b = n
		}
	}
	if a == nil || b == nil {
		t.Fatal("migration nodes not found")
	}
	aid, err := a.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	bid, err := b.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	slot := hashtag.Slot(key)
	if err := b.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
		t.Fatal(err)
	}
	if err := a.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
		t.Fatal(err)
	}
	conn := b.Conn()
	defer conn.Close()
	if err := conn.Do(ctx, "asking").Err(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Set(ctx, key, "created-on-importer", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := source.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, key).Result()
	if err != nil || got != "created-on-importer" {
		t.Fatalf("cached absence hid ASK: got=%q err=%v", got, err)
	}
}

func TestClusterCSCLiveChangedValueFailoverFailback(t *testing.T) {
	c, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	key := "{csc-cluster-failback}:key"
	if err := c.Set(ctx, key, "before", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := c.Get(ctx, key).Val(); got != "before" {
		t.Fatal(got)
	}
	if got := c.Get(ctx, key).Val(); got != "before" {
		t.Fatal(got)
	}
	if c.CSCStats().Hits == 0 {
		t.Fatal("failover test did not warm the primary cache")
	}
	source, err := c.MasterForKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var a, b *Client
	for i, n := range nodes[:3] {
		if n.opt.Addr == source.opt.Addr {
			a, b = n, nodes[i+3]
			break
		}
	}
	if a == nil {
		t.Fatal("original primary not found")
	}
	if err := b.ReadOnly(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	clusterCSCLiveEventually(t, func() bool { return b.Get(ctx, key).Val() == "before" })
	reloadOwner := func(expected *Client) {
		t.Helper()
		clusterCSCLiveEventually(t, func() bool {
			if _, err := c.state.Reload(ctx); err != nil {
				return false
			}
			n, err := c.MasterForKey(ctx, key)
			return err == nil && n.opt.Addr == expected.opt.Addr
		})
	}
	if err := b.ClusterFailover(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	reloadOwner(b)
	if err := c.Set(ctx, key, "changed-on-b", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := c.Get(ctx, key).Val(); got != "changed-on-b" {
		t.Fatal(got)
	}
	if err := a.ReadOnly(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	clusterCSCLiveEventually(t, func() bool { return a.Get(ctx, key).Val() == "changed-on-b" })
	// Data synchronization can precede gossip convergence; wait for the
	// cluster to recognize A as B's replica before requesting failback.
	aid, err := a.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	bid, err := b.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	clusterCSCLiveEventually(t, func() bool {
		for _, observer := range nodes {
			view, err := observer.ClusterNodes(ctx).Result()
			if err != nil {
				return false
			}
			found := false
			for _, line := range strings.Split(view, "\n") {
				fields := strings.Fields(line)
				if len(fields) > 3 && fields[0] == aid {
					found = strings.Contains(fields[2], "slave") && fields[3] == bid
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
	if err := a.ClusterFailover(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	reloadOwner(a)
	for i := 0; i < 3; i++ {
		if got, err := c.Get(ctx, key).Result(); err != nil || got != "changed-on-b" {
			t.Fatalf("failback reused an old value: %q %v", got, err)
		}
	}
	if err := a.Set(ctx, key, "after-failback", 0).Err(); err != nil {
		t.Fatal(err)
	}
	clusterCSCLiveEventually(t, func() bool { return c.Get(ctx, key).Val() == "after-failback" })
}

func TestClusterCSCLive(t *testing.T) {
	c, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	t.Run("hits external invalidation aggregate replies and uncached pipelines", func(t *testing.T) {
		keys := []string{"{bar}:csc-live", "{foo}:csc-live", "{baz}:csc-live"}
		for i, key := range keys {
			value := fmt.Sprintf("v%d", i)
			if err := c.Set(ctx, key, value, 0).Err(); err != nil {
				t.Fatal(err)
			}
			for j := 0; j < 2; j++ {
				if got, err := c.Get(ctx, key).Result(); err != nil || got != value {
					t.Fatalf("GET %q %v", got, err)
				}
			}
		}
		if c.CSCStats().Hits < 3 {
			t.Fatal("routed cache hits not observed")
		}
		writer := NewClusterClient(&ClusterOptions{Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}})
		defer writer.Close()
		if err := writer.Set(ctx, keys[0], "external", 0).Err(); err != nil {
			t.Fatal(err)
		}
		clusterCSCLiveEventually(t, func() bool { return c.Get(ctx, keys[0]).Val() == "external" })
		values, err := c.MGet(ctx, keys[0], "{bar}:other").Result()
		if err != nil || len(values) != 2 || values[0] != "external" {
			t.Fatalf("MGET %v %v", values, err)
		}
		before := c.CSCStats().Hits
		if _, err := c.Pipelined(ctx, func(p Pipeliner) error { p.Get(ctx, keys[0]); p.Get(ctx, keys[1]); return nil }); err != nil {
			t.Fatal(err)
		}
		if c.CSCStats().Hits != before {
			t.Fatal("ordinary pipeline used CSC")
		}
		if _, err := c.TxPipelined(ctx, func(p Pipeliner) error { p.Get(ctx, keys[0]); return nil }); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("borrowed Conn and Watch retain node caching", func(t *testing.T) {
		key := "{bar}:sticky"
		_ = c.Set(ctx, key, "v", 0).Err()
		_ = c.Get(ctx, key).Err()
		node, err := c.MasterForKey(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		conn := node.Conn()
		if err := conn.Ping(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if err := c.Watch(ctx, func(tx *Tx) error { return tx.Get(ctx, key).Err() }, key); err != nil {
			t.Fatal(err)
		}
		before := c.CSCStats().Hits
		_ = c.Get(ctx, key).Err()
		if !node.cscActive.Load() {
			t.Fatal("sticky wrapper Close disabled the parent cache")
		}
		clusterCSCLiveEventually(t, func() bool { _ = c.Get(ctx, key).Err(); return c.CSCStats().Hits > before })
	})
	t.Run("coalesced misses refresh and concurrent hits", func(t *testing.T) {
		r := NewClusterClient(&ClusterOptions{
			Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true, PoolSize: 4,
			ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 128, DrainInterval: time.Millisecond},
			ClientSideCacheRefreshOnInvalidate: true, ClientSideCacheCoalesceMisses: true,
			ClientSideCacheInvalidationBatchWindow: time.Millisecond,
			MaintNotificationsConfig:               &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		defer r.Close()
		key := "{foo}:refresh"
		if err := c.Set(ctx, key, "initial", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got := r.Get(ctx, key).Val(); got != "initial" {
			t.Fatal(got)
		}
		if err := c.Set(ctx, key, "updated", 0).Err(); err != nil {
			t.Fatal(err)
		}
		clusterCSCLiveEventually(t, func() bool { return r.CSCRefreshStats().Refreshed > 0 })
		if got := r.Get(ctx, key).Val(); got != "updated" {
			t.Fatal(got)
		}
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if got, err := r.Get(ctx, key).Result(); err != nil || got != "updated" {
					t.Errorf("concurrent GET %q %v", got, err)
				}
			}()
		}
		wg.Wait()
	})
	t.Run("disconnect evicts only the serving connection", func(t *testing.T) {
		// Keep the drainer out of this ownership test: no invalidations are
		// needed, and borrowing all idle connections must be deterministic.
		r := NewClusterClient(&ClusterOptions{
			Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true, PoolSize: 4,
			ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 128, DrainInterval: time.Hour},
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		defer r.Close()
		k1, k2 := "{bar}:conn1", "{bar}:conn2"
		_ = c.Set(ctx, k1, "one", 0).Err()
		_ = c.Set(ctx, k2, "two", 0).Err()
		if err := r.Get(ctx, k1).Err(); err != nil {
			t.Fatal(err)
		}
		node, _ := r.MasterForKey(ctx, k1)
		cache := node.csc.(*LocalCache)
		owner := func(key string) uint64 {
			entryKey, _ := cscRenderEntryKey(node.cscKeyPrefix, node.metadataView().cscFingerprint, NewStringCmd(ctx, "get", key))
			s := cache.shardFor(entryKey)
			s.mu.RLock()
			defer s.mu.RUnlock()
			if entry := s.entries[entryKey]; entry != nil {
				return entry.ownerConnID
			}
			return 0
		}
		ownerID := owner(k1)
		if ownerID == 0 {
			t.Fatal("first entry has no tracking owner")
		}
		var held *pool.Conn
		var borrowed []*pool.Conn
		defer func() {
			for _, cn := range borrowed {
				node.releaseConn(ctx, cn, nil)
			}
		}()
		for i := 0; i < 4; i++ {
			cn, err := node.getConn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if cn.GetID() == ownerID {
				held = cn
				break
			}
			borrowed = append(borrowed, cn)
		}
		if held == nil {
			t.Fatal("tracking owner was not present in its pool")
		}
		if err := r.Get(ctx, k2).Err(); err != nil {
			node.releaseConn(ctx, held, nil)
			t.Fatal(err)
		}
		if id := owner(k2); id == 0 || id == ownerID {
			node.releaseConn(ctx, held, nil)
			t.Fatal("second entry must use a different tracking connection")
		}
		node.connPool.Remove(ctx, held, fmt.Errorf("test connection loss"))
		if owner(k1) != 0 {
			t.Fatal("lost connection's entry was not evicted")
		}
		before := r.CSCStats()
		if got := r.Get(ctx, k2).Val(); got != "two" {
			t.Fatal(got)
		}
		if r.CSCStats().Hits != before.Hits+1 {
			t.Fatal("unrelated connection's entry was flushed")
		}
		if got := r.Get(ctx, k1).Val(); got != "one" {
			t.Fatal(got)
		}
		if r.CSCStats().Misses != before.Misses+1 || owner(k1) == 0 {
			t.Fatal("evicted entry was not fetched over a new tracked connection")
		}
	})
	t.Run("fanout caches actual subcommands", func(t *testing.T) {
		r := NewClusterClient(&ClusterOptions{
			Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true,
			ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 128},
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		defer r.Close()
		r.SetCommandInfoResolver(NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
			if cmd.Name() == "mget" {
				return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
			}
			return nil
		}))
		keys := []string{"{bar}:fanout", "{foo}:fanout", "{baz}:fanout"}
		for _, key := range keys {
			if err := c.Set(ctx, key, key, 0).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for attempt := 0; attempt < 2; attempt++ {
			values, err := r.MGet(ctx, keys...).Result()
			if err != nil || len(values) != len(keys) {
				t.Fatalf("fanout MGET: %v %v", values, err)
			}
			for i, value := range values {
				if value != keys[i] {
					t.Fatalf("fanout reply order: %v", values)
				}
			}
		}
		if stats := r.CSCStats(); stats.Hits != 3 || stats.Entries != 3 {
			t.Fatalf("want three node-local subcommand hits/entries, got %+v", stats)
		}
	})
	t.Run("ordered AutoPipeline solo uses CSC without overtaking writes", func(t *testing.T) {
		ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{MaxBatchSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		key := "{foo}:solo-order"
		set := ap.Set(ctx, key, "ordered", 0)
		get := ap.Get(ctx, key)
		if err := set.Err(); err != nil {
			t.Fatal(err)
		}
		if value, err := get.Result(); err != nil || value != "ordered" {
			t.Fatalf("queued GET overtook SET: %q %v", value, err)
		}
		before := c.CSCStats().Hits
		clusterCSCLiveEventually(t, func() bool {
			return ap.Get(ctx, key).Val() == "ordered" && c.CSCStats().Hits > before
		})
	})
	t.Run("another client creates missing key via ASK without reader entry age", func(t *testing.T) {
		newCachingClient := func() *ClusterClient {
			client := NewClusterClient(&ClusterOptions{
				Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, PoolSize: 1,
				DisableIdentity: true, DisableRoutingPolicies: true,
				ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 16},
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
			})
			t.Cleanup(func() { _ = client.Close() })
			return client
		}
		reader, writer := newCachingClient(), newCachingClient()
		key := "{csc-external-ask}:absent"
		existing := "{csc-external-ask}:present"
		slot := hashtag.Slot(key)
		source, err := reader.MasterForKey(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		var a, b *Client
		for _, node := range nodes[:3] {
			if node.opt.Addr == source.opt.Addr {
				a = node
			} else if b == nil {
				b = node
			}
		}
		if a == nil || b == nil {
			t.Fatal("test owners not found")
		}
		aid, err := a.ClusterMyID(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		bid, err := b.ClusterMyID(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Set(ctx, existing, "kept-on-a", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if source.csc.(*LocalCache).effectiveMaxStaleness() != 0 {
			t.Fatal("test must not rely on an entry-age limit")
		}
		for _, client := range []*ClusterClient{reader, writer} {
			if err := client.Get(ctx, key).Err(); err != Nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 2; i++ {
			got, err := reader.MGet(ctx, existing, key).Result()
			if err != nil || !reflect.DeepEqual(got, []interface{}{"kept-on-a", nil}) {
				t.Fatalf("initial mixed MGET: %v %v", got, err)
			}
		}
		if reader.CSCStats().Hits != 0 || reader.CSCStats().Entries != 0 {
			t.Fatal("mixed MGET must not cache even though one key exists")
		}
		t.Cleanup(func() {
			conn := b.Conn()
			_ = conn.Do(ctx, "asking").Err()
			_ = conn.Del(ctx, key, existing).Err()
			_ = conn.Close()
			_ = a.Do(ctx, "cluster", "setslot", slot, "stable").Err()
			_ = b.Do(ctx, "cluster", "setslot", slot, "stable").Err()
			_ = a.Del(ctx, existing).Err()
		})
		if err := b.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
			t.Fatal(err)
		}
		if err := a.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
			t.Fatal(err)
		}
		if err := writer.Set(ctx, key, "created-on-b", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got, err := writer.Get(ctx, key).Result(); err != nil || got != "created-on-b" {
			t.Fatalf("writer did not follow ASK: %q %v", got, err)
		}
		// Read A's connection after the write; pending push frames would be
		// consumed here. No invalidation for creating this key on B is sent to A.
		if err := source.Ping(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		before := reader.CSCStats().Hits
		if source.CSCStats().Entries != 0 {
			t.Fatal("missing GET populated the source cache")
		}
		// The existing key has NOT migrated. Same-slot membership does not
		// make the earlier mixed reply safe: Redis must now return TRYAGAIN,
		// not the cached [kept-on-a, nil]. Check before GET observes any ASK.
		if _, err := reader.MGet(ctx, existing, key).Result(); err == nil || !strings.HasPrefix(err.Error(), "TRYAGAIN") {
			t.Fatalf("split-slot MGET did not reach Redis: %v", err)
		}
		if got, err := reader.Get(ctx, key).Result(); err != nil || got != "created-on-b" {
			t.Fatalf("uncached absence did not expose ASK: %q %v", got, err)
		}
		if reader.CSCStats().Hits != before {
			t.Fatal("reader served a cached absence")
		}
		_, destPort, _ := net.SplitHostPort(b.opt.Addr)
		if err := a.Migrate(ctx, "127.0.0.1", destPort, existing, 0, 3*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
		got, err := reader.MGet(ctx, existing, key).Result()
		if err != nil || !reflect.DeepEqual(got, []interface{}{"kept-on-a", "created-on-b"}) {
			t.Fatalf("MGET after migrating both keys: %v %v", got, err)
		}
	})
	t.Run("ASK missing key and MOVED migration", func(t *testing.T) {
		// This slot must not also contain keys from earlier subtests: assigning
		// its new owner is valid only after every source key has migrated.
		key := "{csc-migration}:absent"
		slot := hashtag.Slot(key)
		source, err := c.MasterForKey(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		var a, b *Client
		for _, n := range nodes[:3] {
			if n.opt.Addr == source.opt.Addr {
				a = n
			} else if b == nil {
				b = n
			}
		}
		if a == nil || b == nil {
			t.Fatal("test owners not found")
		}
		aid, _ := a.ClusterMyID(ctx).Result()
		bid, _ := b.ClusterMyID(ctx).Result()
		// Read the missing key before IMPORTING/MIGRATING starts, without caching it.
		if err := c.Get(ctx, key).Err(); err != Nil {
			t.Fatal(err)
		}
		if err := b.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
			t.Fatal(err)
		}
		if err := a.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.Set(ctx, key, "imported", 0).Err(); err != nil {
			t.Fatal(err)
		} // ASK is observed on a write.
		if got, err := c.Get(ctx, key).Result(); err != nil || got != "imported" {
			t.Fatalf("GET did not follow ASK after creation: %q %v", got, err)
		}
		// Make permanent ownership visible. Reads must discover MOVED on a miss.
		for _, n := range []*Client{b, a} {
			if err := n.Do(ctx, "cluster", "setslot", slot, "node", bid).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for _, n := range nodes[:3] {
			if n != a && n != b {
				_ = n.Do(ctx, "cluster", "setslot", slot, "node", bid).Err()
			}
		}
		if _, err := c.state.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		if got := c.Get(ctx, key).Val(); got != "imported" {
			t.Fatal(got)
		}
		// Existing tracked-key migration sends invalidation to the original owner.
		existing := "{csc-migration}:existing"
		_ = c.Set(ctx, existing, "move", 0).Err()
		_ = c.Get(ctx, existing).Err()
		refreshClient := NewClusterClient(&ClusterOptions{
			Addrs: []string{b.opt.Addr}, Protocol: 3, DisableIdentity: true,
			ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 64, DrainInterval: time.Millisecond},
			ClientSideCacheRefreshOnInvalidate: true,
			MaintNotificationsConfig:           &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		defer refreshClient.Close()
		if got := refreshClient.Get(ctx, existing).Val(); got != "move" {
			t.Fatal(got)
		}
		refreshSource, _ := refreshClient.MasterForKey(ctx, existing)
		refreshCacheKey, _ := cscRenderEntryKey(refreshSource.cscKeyPrefix, refreshSource.metadataView().cscFingerprint, NewStringCmd(ctx, "get", existing))
		if err := a.Do(ctx, "cluster", "setslot", slot, "importing", bid).Err(); err != nil {
			t.Fatal(err)
		}
		if err := b.Do(ctx, "cluster", "setslot", slot, "migrating", aid).Err(); err != nil {
			t.Fatal(err)
		}
		_, port, _ := net.SplitHostPort(a.opt.Addr)
		if err := b.Migrate(ctx, "127.0.0.1", port, existing, 0, 3*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
		// No foreground call observes the redirect for refreshClient: its tracked
		// source gets invalidated, and node-local refresh sees ASK first.
		clusterCSCLiveEventually(t, func() bool {
			_, hit := refreshSource.csc.Get(ctx, refreshCacheKey)
			return !hit
		})
		importConn := a.Conn()
		if err := importConn.Process(ctx, NewCmd(ctx, "asking")); err != nil {
			t.Fatal(err)
		}
		if err := importConn.Set(ctx, existing, "moved-updated", 0).Err(); err != nil {
			t.Fatal(err)
		}
		_ = importConn.Close()
		clusterCSCLiveEventually(t, func() bool { return c.Get(ctx, existing).Val() == "moved-updated" })
		if got := c.Get(ctx, existing).Val(); got != "moved-updated" {
			t.Fatal(got)
		}
		// Move all remaining keys in this slot before completing failback.
		migrating, err := b.ClusterGetKeysInSlot(ctx, slot, 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range migrating {
			if err := b.Migrate(ctx, "127.0.0.1", port, k, 0, 3*time.Second).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for _, n := range []*Client{a, b} {
			if err := n.Do(ctx, "cluster", "setslot", slot, "node", aid).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for _, n := range nodes[:3] {
			if n != a && n != b {
				_ = n.Do(ctx, "cluster", "setslot", slot, "node", aid).Err()
			}
		}
		if _, err := c.state.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		if got := c.Get(ctx, existing).Val(); got != "moved-updated" {
			t.Fatal(got)
		}
	})
	t.Run("primary promotion invalidates previous ownership", func(t *testing.T) {
		key := "{bar}:promote"
		if err := c.Set(ctx, key, "before", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got := c.Get(ctx, key).Val(); got != "before" {
			t.Fatal(got)
		}
		old, err := c.MasterForKey(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		var replica *Client
		for _, n := range nodes[3:] {
			info, e := n.Info(ctx, "replication").Result()
			if e == nil && strings.Contains(info, "master_port:"+strings.TrimPrefix(old.opt.Addr, "127.0.0.1:")) {
				replica = n
				break
			}
		}
		if replica == nil {
			t.Fatal("replica not found")
		}
		if err := replica.ReadOnly(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		clusterCSCLiveEventually(t, func() bool { return replica.Get(ctx, key).Val() == "before" })
		if err := replica.ClusterFailover(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		clusterCSCLiveEventually(t, func() bool {
			_, e := c.state.Reload(ctx)
			if e != nil {
				return false
			}
			n, e := c.MasterForKey(ctx, key)
			return e == nil && n.opt.Addr == replica.opt.Addr
		})
		if got := c.Get(ctx, key).Val(); got != "before" {
			t.Fatal(got)
		}
	})
}

// CSC tracking modes: OPTIN/OPTOUT policy and integration.

func TestCSCPolicyClusterConstructorSnapshot(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: s.addr}}}}
	})
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptIn
	opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool { return true }
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	if opt.FailingTimeoutSeconds != 15 || c.Options().FailingTimeoutSeconds != 15 {
		t.Errorf("caller and client defaults differ: caller=%d client=%d", opt.FailingTimeoutSeconds, c.Options().FailingTimeoutSeconds)
	}

	// Mutations before the first node is created must not change its policy.
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptOut
	opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool {
		t.Error("node used the mutated predicate")
		return false
	}
	for i := 0; i < 2; i++ {
		if got, err := c.Get(context.Background(), "k").Result(); err != nil || got != "v" {
			t.Fatal(got, err)
		}
	}
	if s.count("get") != 1 || s.prefixes() != 1 {
		t.Fatalf("node did not retain OPTIN caching: %v", s.snapshot())
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

func TestCSCPolicyClusterIneligibleFanout(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(strconv.FormatBool(strict), func(t *testing.T) {
			a, b := newCSCPolicyServer(t, nil), newCSCPolicyServer(t, nil)
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a.addr}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b.addr}}}}
			})
			opt.DisableRoutingPolicies = false
			info := cloneCommandInfo(defaultCommandMetadataView().records["mget"])
			info.Tips = append(info.Tips, "dont_cache")
			opt.CommandMetadata = &CommandMetadataConfig{Overrides: map[string]*CommandInfo{"mget": info}}
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
			view := newCacheView(c, func(ctx context.Context, cmd Cmder) error {
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

type cscHelperCommands interface {
	DBSize(context.Context) *IntCmd
	ScriptLoad(context.Context, string) *StringCmd
	ScriptExists(context.Context, ...string) *BoolSliceCmd
	ScriptFlush(context.Context) *StatusCmd
	HImportPrepare(context.Context, string, ...string) *StatusCmd
	HImportDiscard(context.Context, string) *IntCmd
	HImportDiscardAll(context.Context) *IntCmd
}

func TestCSCPolicyClusterHelpers(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, mark := range []bool{false, true} {
			for _, strict := range []bool{false, true} {
				t.Run(fmt.Sprintf("routingDisabled=%t/cache=%t/strict=%t", disabled, mark, strict), func(t *testing.T) {
					server := func(size int, exists bool) *cscPolicyServer {
						return newCSCPolicyServer(t, func(args []string) string {
							switch strings.ToLower(args[0]) {
							case "dbsize":
								return fmt.Sprintf(":%d\r\n", size)
							case "script":
								switch strings.ToLower(args[1]) {
								case "load":
									return "$3\r\nsha\r\n"
								case "exists":
									if exists {
										return "*1\r\n:1\r\n"
									}
									return "*1\r\n:0\r\n"
								}
							case "himport":
								if strings.ToLower(args[1]) != "prepare" {
									return ":1\r\n"
								}
							}
							return ""
						})
					}
					a, b := server(2, true), server(3, false)
					opt := clusterCSCTestOptions(func() []ClusterSlot {
						return []ClusterSlot{
							{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a.addr}}},
							{Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b.addr}}},
						}
					})
					opt.DisableRoutingPolicies = disabled
					opt.ClientSideCacheConfig.Strict = strict
					c := NewClusterClient(opt)
					defer c.Close()
					ap, err := c.AsyncAutoPipeline()
					if err != nil {
						t.Fatal(err)
					}
					defer ap.Close()
					nativeCounts := make(map[string][2]int)
					for surface, v := range []cscHelperCommands{c, WithCache(c, mark), WithCache(ap, mark)} {
						ctx := context.Background()
						for _, call := range []struct {
							name string
							run  func() Cmder
							want interface{}
						}{
							{"dbsize", func() Cmder { return v.DBSize(ctx) }, int64(5)},
							{"script load", func() Cmder { return v.ScriptLoad(ctx, "return 1") }, "sha"},
							{"script exists", func() Cmder { return v.ScriptExists(ctx, "sha") }, []bool{false}},
							{"script flush", func() Cmder { return v.ScriptFlush(ctx) }, nil},
							{"himport prepare", func() Cmder { return v.HImportPrepare(ctx, "fields", "f") }, "OK"},
							{"himport discard", func() Cmder { return v.HImportDiscard(ctx, "fields") }, int64(1)},
							{"himport discardall", func() Cmder { return v.HImportDiscardAll(ctx) }, int64(0)},
						} {
							count := func(s *cscPolicyServer) int {
								n := 0
								for _, e := range s.snapshot() {
									if strings.HasPrefix(strings.ToLower(strings.Join(e.args, " ")), call.name) {
										n++
									}
								}
								return n
							}
							beforeA, beforeB := count(a), count(b)
							cmd := call.run()
							if surface != 0 && mark && strict {
								if cmd.Err() == nil || count(a) != beforeA || count(b) != beforeB {
									t.Fatalf("%s strict dispatched: %v", call.name, cmd.Err())
								}
								continue
							}
							if cmd.Err() != nil {
								t.Fatalf("%s: %v", call.name, cmd.Err())
							}
							counts := [2]int{count(a) - beforeA, count(b) - beforeB}
							if surface == 0 {
								if counts[0] == 0 || counts[1] == 0 {
									t.Fatal("native helper missed a shard", call.name)
								}
								nativeCounts[call.name] = counts
							} else if counts != nativeCounts[call.name] {
								t.Fatalf("%s shard calls %v, native %v", call.name, counts, nativeCounts[call.name])
							}
							var got interface{}
							switch cmd := cmd.(type) {
							case *IntCmd:
								got = cmd.Val()
							case *StringCmd:
								got = cmd.Val()
							case *BoolSliceCmd:
								got = cmd.Val()
							case *StatusCmd:
								got = cmd.Val()
							}
							if call.want != nil && !reflect.DeepEqual(got, call.want) {
								t.Fatalf("%s = %v, want %v", call.name, got, call.want)
							}
						}
					}
				})
			}
		}
	}
}

// Exercise three physical masters, not merely three slots on one server. Live
// metadata, refresh and disconnects must retain fragment-local publication.
func TestCSCPolicyFanoutLive(t *testing.T) {
	writer, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	keys := make([]string, 3)
	for i := 0; i < 3; i++ {
		for n := 0; ; n++ {
			key := fmt.Sprintf("{policy-fanout-%d-%d}:key", i, n)
			if slot := hashtag.Slot(key); slot >= i*5461 && (i == 2 || slot < (i+1)*5461) {
				keys[i] = key
				break
			}
		}
	}
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, coalesce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/coalesce=%t", mode, coalesce), func(t *testing.T) {
				for _, key := range keys {
					if err := writer.Set(ctx, key, "before", 0).Err(); err != nil {
						t.Fatal(err)
					}
				}
				var predicates atomic.Int32
				r := NewClusterClient(&ClusterOptions{
					Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true, PoolSize: 4,
					CommandMetadata: &CommandMetadataConfig{Mode: CommandMetadataPreferLive},
					ClientSideCacheConfig: &ClientSideCacheConfig{
						MaxEntries: 128, TrackingMode: mode, DrainInterval: time.Millisecond,
						Cacheable: func(name string, got []string) bool {
							predicates.Add(1)
							if name != "mget" || !reflect.DeepEqual(got, keys) {
								t.Errorf("predicate saw fragment: %s %v", name, got)
							}
							return true
						},
					},
					ClientSideCacheCoalesceMisses: coalesce, ClientSideCacheRefreshOnInvalidate: true,
					ClientSideCacheInvalidationBatchWindow: time.Millisecond,
					MaintNotificationsConfig:               &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				})
				defer r.Close()
				r.SetCommandInfoResolver(r.NewDynamicResolver())
				if err := r.MGet(ctx, keys...).Err(); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.metadataView().live })
				for i := 0; i < 2; i++ {
					got, err := r.MGet(ctx, keys...).Result()
					if err != nil || !reflect.DeepEqual(got, []interface{}{"before", "before", "before"}) {
						t.Fatal(got, err)
					}
				}
				if r.CSCStats().Entries != 3 || r.CSCStats().Hits < 3 || predicates.Load() != 3 {
					t.Fatalf("fanout did not cache independently: %+v predicate=%d", r.CSCStats(), predicates.Load())
				}
				before := r.CSCRefreshStats().Refreshed
				if err := writer.Set(ctx, keys[1], "after", 0).Err(); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.CSCRefreshStats().Refreshed > before })
				if predicates.Load() != 3 {
					t.Fatal("refresh re-evaluated the application predicate")
				}
				var wg sync.WaitGroup
				for i := 0; i < 16; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						got, err := r.MGet(ctx, keys...).Result()
						if err != nil || !reflect.DeepEqual(got, []interface{}{"before", "after", "before"}) {
							t.Errorf("parallel fanout: %v %v", got, err)
						}
					}()
				}
				wg.Wait()
				if predicates.Load() != 19 {
					t.Fatal("predicate count changed during fragment execution", predicates.Load())
				}
				owner, err := r.MasterForKey(ctx, keys[0])
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.connPool.(*pool.ConnPool).Filter(func(*pool.Conn) bool { return true }); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.CSCStats().Entries == 2 })
				if got, err := r.WithCache(true).MGet(ctx, keys...).Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"before", "after", "before"}) {
					t.Fatal(got, err)
				}
				if r.CSCStats().Entries != 3 || predicates.Load() != 19 {
					t.Fatal("disconnected fragment failed to reacquire tracking")
				}
			})
		}
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode)+"/fanout-MOVED", func(t *testing.T) {
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: nodes[0].opt.Addr}}}}
			})
			opt.DisableRoutingPolicies = false
			opt.ClientSideCacheConfig.TrackingMode = mode
			opt.ClientSideCacheConfig.Strict = true
			r := NewClusterClient(opt)
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandMetadataResolver())
			if got, err := r.WithCache(true).MGet(ctx, keys...).Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"before", "after", "before"}) {
				t.Fatal(got, err)
			}
			if r.CSCStats().Entries != 3 {
				t.Fatal("redirected fragments did not publish", r.CSCStats())
			}
		})
		t.Run(string(mode)+"/fanout-ASK", func(t *testing.T) {
			// Leave the source empty while the key exists on the importer.
			// Other slots remain ordinary cacheable fragments on their owners.
			source, target := nodes[0], nodes[1]
			key, slot := keys[0], hashtag.Slot(keys[0])
			if err := writer.Del(ctx, key).Err(); err != nil {
				t.Fatal(err)
			}
			aid, err := source.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			bid, err := target.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			if err := target.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
				t.Fatal(err)
			}
			defer target.Do(ctx, "cluster", "setslot", slot, "stable")
			if err := source.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
				t.Fatal(err)
			}
			defer source.Do(ctx, "cluster", "setslot", slot, "stable")
			cn := target.Conn()
			defer cn.Close()
			if err := cn.Do(ctx, "asking").Err(); err != nil {
				t.Fatal(err)
			}
			if err := cn.Set(ctx, key, "imported", 0).Err(); err != nil {
				t.Fatal(err)
			}
			// Delete on the importer while it still accepts ASKING, then restore
			// the original source value after both nodes leave migration state.
			defer func() {
				_ = cn.Do(ctx, "asking").Err()
				_ = cn.Del(ctx, key).Err()
				_ = target.Do(ctx, "cluster", "setslot", slot, "stable").Err()
				_ = source.Do(ctx, "cluster", "setslot", slot, "stable").Err()
				_ = writer.Set(ctx, key, "before", 0).Err()
			}()
			r := NewClusterClient(&ClusterOptions{
				Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true,
				ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 128, TrackingMode: mode, Strict: true},
				ClientSideCacheCoalesceMisses: true,
				MaintNotificationsConfig:      &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
			})
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandMetadataResolver())
			for i := 0; i < 2; i++ {
				if got, err := r.WithCache(true).MGet(ctx, keys...).Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"imported", "after", "before"}) {
					t.Fatal(got, err)
				}
				if r.CSCStats().Entries != 2 {
					t.Fatal("ASK fragment published or siblings were lost", r.CSCStats())
				}
			}
		})
	}
}

// Uses the existing owned six-process fixture, never a developer database.
func TestCSCPolicyLive(t *testing.T) {
	writer, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, coalesce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/coalesce=%t", mode, coalesce), func(t *testing.T) {
				r := NewClusterClient(&ClusterOptions{
					Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true, PoolSize: 4,
					ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 128, TrackingMode: mode, DrainInterval: time.Millisecond, Cacheable: func(string, []string) bool { return true }},
					ClientSideCacheCoalesceMisses: coalesce, ClientSideCacheRefreshOnInvalidate: true, ClientSideCacheInvalidationBatchWindow: time.Millisecond,
					MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				})
				defer r.Close()
				key := fmt.Sprintf("{policy-%s-%t}:key", mode, coalesce)
				if err := writer.Set(ctx, key, "before", 0).Err(); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if got, err := r.Get(ctx, key).Result(); got != "before" || err != nil {
						t.Fatalf("warm %q %v", got, err)
					}
				}
				if r.CSCStats().Hits < 1 {
					t.Fatal("no cache hit")
				}
				if err := writer.Set(ctx, key, "after", 0).Err(); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.CSCRefreshStats().Refreshed > 0 })
				if got, err := r.Get(ctx, key).Result(); got != "after" || err != nil {
					t.Fatalf("refresh %q %v", got, err)
				}
				var wg sync.WaitGroup
				for i := 0; i < 16; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if got, err := r.WithCache(true).Get(ctx, key).Result(); got != "after" || err != nil {
							t.Errorf("parallel %q %v", got, err)
						}
					}()
				}
				wg.Wait()
			})
		}
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode)+"/MOVED", func(t *testing.T) {
			key := "{policy-moved}:key"
			if err := writer.Set(ctx, key, "moved", 0).Err(); err != nil {
				t.Fatal(err)
			}
			owner, err := writer.MasterForKey(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			var wrong *Client
			for _, n := range nodes[:3] {
				if n.opt.Addr != owner.opt.Addr {
					wrong = n
					break
				}
			}
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: wrong.opt.Addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = mode
			opt.DisableRoutingPolicies = false
			var predicates atomic.Int32
			opt.ClientSideCacheConfig.Cacheable = func(_ string, _ []string) bool {
				predicates.Add(1)
				return true
			}
			r := NewClusterClient(opt)
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandInfoResolver(cscPolicyLiveMGetPolicy))
			if got, err := r.WithCache(true).Get(ctx, key).Result(); got != "moved" || err != nil {
				t.Fatalf("MOVED %q %v", got, err)
			}
			if r.CSCStats().Entries != 1 {
				t.Fatal("MOVED did not publish tracked result")
			}
			other := key + ":other"
			if err := writer.Set(ctx, other, "other", 0).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := r.MGet(ctx, key, other).Result()
			if err != nil || len(got) != 2 || got[0] != "moved" || got[1] != "other" || predicates.Load() != 1 {
				t.Fatalf("one-slot MOVED %v %v; predicate calls %d", got, err, predicates.Load())
			}
		})
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode)+"/ASK", func(t *testing.T) {
			key := fmt.Sprintf("{policy-ask-%s}:key", mode)
			owner, err := writer.MasterForKey(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			var source, target *Client
			for _, n := range nodes[:3] {
				if n.opt.Addr == owner.opt.Addr {
					source = n
				} else if target == nil {
					target = n
				}
			}
			aid, err := source.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			bid, err := target.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			slot := hashtag.Slot(key)
			if err := target.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
				t.Fatal(err)
			}
			defer target.Do(ctx, "cluster", "setslot", slot, "stable")
			if err := source.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
				t.Fatal(err)
			}
			defer source.Do(ctx, "cluster", "setslot", slot, "stable")
			cn := target.Conn()
			defer cn.Close()
			if err := cn.Do(ctx, "asking").Err(); err != nil {
				t.Fatal(err)
			}
			if err := cn.Set(ctx, key, "imported", 0).Err(); err != nil {
				t.Fatal(err)
			}
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: source.opt.Addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = mode
			opt.ClientSideCacheConfig.Strict = true
			opt.DisableRoutingPolicies = false
			r := NewClusterClient(opt)
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandInfoResolver(cscPolicyLiveMGetPolicy))
			if got, err := r.WithCache(true).Get(ctx, key).Result(); got != "imported" || err != nil {
				t.Fatalf("ASK %q %v", got, err)
			}
			if r.CSCStats().Entries != 0 {
				t.Fatal("ASK published")
			}
			got, err := r.WithCache(true).MGet(ctx, key, key).Result()
			if err != nil || len(got) != 2 || got[0] != "imported" || got[1] != "imported" || r.CSCStats().Entries != 0 {
				t.Fatalf("one-slot ASK %v %v", got, err)
			}
		})
	}
}

func cscPolicyLiveMGetPolicy(_ context.Context, cmd Cmder) *routing.CommandPolicy {
	if cmd.Name() == "mget" {
		return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
	}
	return nil
}

// Validate mode selection and ordered marked reads against actual Redis tracking,
// using the same owned Cluster fixture as the other CSC live tests.
func TestCSCPolicyLiveReviewFixes(t *testing.T) {
	writer, _ := newClusterCSCLive(t)
	ctx := context.Background()
	key := "{review-fixes}:key"
	owner, err := writer.MasterForKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		for _, path := range []string{"Conn", "Watch"} {
			t.Run(string(mode)+"/"+path, func(t *testing.T) {
				if err := writer.Set(ctx, key, "value", 0).Err(); err != nil {
					t.Fatal(err)
				}
				opt := cscPolicyOptions(owner.Options().Addr, mode)
				opt.PoolSize = 1
				c := policyClient(t, opt)
				if path == "Conn" {
					cn := c.Conn()
					defer cn.Close()
					if err := cn.Ping(ctx).Err(); err != nil {
						t.Fatal(err)
					}
					if err := cn.Close(); err != nil {
						t.Fatal(err)
					}
				} else if err := c.Watch(ctx, func(tx *Tx) error { return tx.Ping(ctx).Err() }); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if got, err := c.WithCache(true).Get(ctx, key).Result(); got != "value" || err != nil {
						t.Fatal(got, err)
					}
				}
				if c.CSCStats().Entries != 1 || c.CSCStats().Hits != 1 {
					t.Fatal("borrowed connection selected wrong tracking mode", c.CSCStats())
				}
			})
		}
	}
	for _, fd := range []bool{false, true} {
		for _, mark := range []bool{false, true} {
			t.Run(fmt.Sprintf("ordered/fd=%t/mark=%t", fd, mark), func(t *testing.T) {
				if err := writer.Set(ctx, key, "old", 0).Err(); err != nil {
					t.Fatal(err)
				}
				gate := &cscOrderedWriteGate{entered: make(chan struct{}), release: make(chan struct{})}
				opt := cscPolicyOptions(owner.Options().Addr, TrackingModePlain)
				opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
					cn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					return &cscOrderedConn{cn, gate}, nil
				}
				c := policyClient(t, opt)
				if err := c.Get(ctx, key).Err(); err != nil {
					t.Fatal(err)
				}
				ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: fd})
				if err != nil {
					t.Fatal(err)
				}
				defer ap.Close()
				defer gate.unblock()
				first := ap.Set(ctx, key, "new", 0)
				select {
				case <-gate.entered:
				case <-time.After(3 * time.Second):
					t.Fatal("SET was not submitted")
				}
				done := make(chan *StringCmd, 1)
				go func() { done <- ap.WithCache(mark).Get(ctx, key) }()
				select {
				case cmd := <-done:
					t.Fatalf("marked read overtook blocked SET: %v", cmd)
				case <-time.After(50 * time.Millisecond):
				}
				gate.unblock()
				if err := first.Err(); err != nil {
					t.Fatal(err)
				}
				select {
				case cmd := <-done:
					if got, err := cmd.Result(); got != "new" || err != nil {
						t.Fatal(got, err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("marked read did not complete")
				}
			})
		}
	}
}

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
