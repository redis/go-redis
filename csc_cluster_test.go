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
			return "$-1\r\n"
		case "exists":
			calls.Add(1)
			return ":0\r\n"
		case "mget":
			calls.Add(1)
			return "*2\r\n$-1\r\n$0\r\n\r\n"
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
	keys := []string{"{bar}:missing", "{foo}:missing"}
	if hashtag.Slot(keys[0]) >= 8192 || hashtag.Slot(keys[1]) < 8192 {
		t.Fatal("test tags changed")
	}
	for _, key := range keys {
		for i := 0; i < 2; i++ {
			if err := c.Get(ctx, key).Err(); err != Nil {
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
		if err := handle.Get(ctx, keys[0]).Err(); err != Nil {
			t.Fatal(err)
		}
	}
	if callsA.Load() != 3 {
		t.Fatal("direct handle/clone used a cluster cache without routing token")
	}
	for i := 0; i < 2; i++ {
		if n, err := c.Exists(ctx, keys[0]).Result(); err != nil || n != 0 {
			t.Fatalf("EXISTS %d %v", n, err)
		}
	}
	if callsA.Load() != 4 {
		t.Fatal("EXISTS zero not cached")
	}
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, keys[0]).Err(); err != Nil {
		t.Fatal(err)
	}
	if callsA.Load() != 4 {
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
			if want <= 0 {
				want = time.Second
			}
			if a.ClientSideCacheConfig.MaxStaleness != want || b.ClientSideCacheConfig.MaxStaleness != want {
				t.Fatal("node configuration did not apply bounded entry age")
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

// There is no notification on A when another client creates an absent key on
// importing B. Prove that the hit hides ASK, then advance entry age deterministically
// and verify that the next miss discovers ASK and fetches B's actual reply.
func TestClusterCSCUnobservedMigrationExpiresAbsence(t *testing.T) {
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
				migrating.Store(true) // No invalidation or disconnect reaches our client.
				read(tc.old, tc.err)
				if readsA.Load() != 1 || readsB.Load() != 0 {
					t.Fatal("expected the negative hit to hide the redirect before expiry")
				}
				node, _ := c.MasterForKey(ctx, key)
				cache := node.csc.(*LocalCache)
				entryKey, _ := buildCacheKeyNS(NewCmd(ctx, args...), node.cscKeyPrefix)
				shard := cache.shardFor(entryKey)
				shard.mu.Lock()
				entry := shard.entries[entryKey]
				if entry != nil {
					entry.validAt = time.Now().Add(-2 * time.Second)
				}
				shard.mu.Unlock()
				if entry == nil {
					t.Fatal("missing cached absence")
				}
				read(tc.want, nil)
				if readsA.Load() != 2 || readsB.Load() != 1 {
					t.Fatalf("expiry did not expose ASK: A=%d B=%d", readsA.Load(), readsB.Load())
				}
				destination, _ := c.nodes.GetOrCreate(b)
				if destination.Client.CSCStats().Entries != 0 {
					t.Fatal("ASK-directed response populated destination cache")
				}
			})
		}
	}
}

type clusterCSCBlockingCache struct {
	*LocalCache
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	flushes atomic.Int32
	deletes atomic.Int32
}

func TestClusterCSCRefreshFirstRedirectObserver(t *testing.T) {
	for _, redirect := range []string{"ASK", "MOVED"} {
		t.Run(redirect, func(t *testing.T) {
			var migrating atomic.Bool
			b := newClusterCSCTestServer(t, func(cmd string) string {
				if cmd == "hello" {
					return "%0\r\n"
				}
				if cmd == "get" {
					return "+value\r\n"
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
					return "$-1\r\n"
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
				if err := c.Get(ctx, key).Err(); err != Nil {
					t.Fatal(err)
				}
			}
			if value := c.Get(ctx, "{foo}:destination").Val(); value != "value" {
				t.Fatal(value)
			}
			source, _ := c.MasterForKey(ctx, "{bar}:refresh")
			destination, _ := c.MasterForKey(ctx, "{foo}:destination")
			key, _ := buildCacheKeyNS(NewStringCmd(ctx, "get", "{bar}:refresh"), source.cscKeyPrefix)
			dependency := cscNamespacedKey(source.cscKeyPrefix, "{bar}:refresh")
			source.csc.DeleteByRedisKey(dependency)
			migrating.Store(true)
			// Only the refresh sees the redirect. It must evict dependencies
			// without discarding the unrelated sibling or destination entry.
			count, err := source.refreshInvalidatedBatch(ctx, []cscRefreshTarget{{cacheKey: key, redisKeys: []string{dependency}}})
			if err != nil || count != 0 {
				t.Fatalf("refresh: %d %v", count, err)
			}
			waitClusterCSCReady(t, source.opt.clusterCSC)
			if source.CSCStats().Entries != 1 {
				t.Fatal("refresh redirect evicted unrelated source entries")
			}
			waitClusterCSCReady(t, destination.opt.clusterCSC)
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

func (c *clusterCSCBlockingCache) Flush() int {
	c.flushes.Add(1)
	c.blockCleanup()
	return c.LocalCache.Flush()
}

func (c *clusterCSCBlockingCache) DeleteByRedisKey(key string) int {
	c.deletes.Add(1)
	c.blockCleanup()
	return c.LocalCache.DeleteByRedisKey(key)
}

func (c *clusterCSCBlockingCache) blockCleanup() {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
}

func waitClusterCSCReady(t testing.TB, n *clusterCSCNode) {
	t.Helper()
	clusterCSCLiveEventually(t, n.usable)
}

func TestClusterCSCRedirectKeyExtraction(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  Cmder
		info *CommandInfo
		want []string
	}{
		{"get", makeCmd("get", []byte("key")), nil, []string{"key"}},
		{"mget", makeCmd("mget", "a", "b"), nil, []string{"a", "b"}},
		{"exists", makeCmd("exists", "a", "b"), nil, []string{"a", "b"}},
		{"json", makeCmd("json.mget", "a", "b", "$.value"), nil, []string{"a", "b"}},
		{"set value is not a key", makeCmd("set", "a", "unrelated", "px", 1000), nil, []string{"a"}},
		{"mset values are not keys", makeCmd("mset", "a", "value1", "b", "value2"), nil, []string{"a", "b"}},
		{"del", makeCmd("del", "a", "b"), nil, []string{"a", "b"}},
		{"eval arguments are not keys", makeCmd("eval", "return 1", 2, "a", "b", "unrelated"), nil, []string{"a", "b"}},
		{"invalid eval count", makeCmd("eval", "return 1", -1, "a"), nil, nil},
		{"metadata", makeCmd("hset", "a", "field", "unrelated"), &CommandInfo{FirstKeyPos: 1, LastKeyPos: 1, StepCount: 1}, []string{"a"}},
		{"metadata stride", makeCmd("module.mset", "a", "value1", "b", "value2"), &CommandInfo{FirstKeyPos: 1, LastKeyPos: -1, StepCount: 2}, []string{"a", "b"}},
		{"unknown layout", makeCmd("module.dynamic", "a", "value"), nil, nil},
		{"movable layout", makeCmd("module.dynamic", "a", "b"), &CommandInfo{FirstKeyPos: 1, LastKeyPos: -1, StepCount: 1, Flags: []string{"movablekeys"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := clusterCSCRedirectKeys(tc.cmd, tc.info)
			if ok != (tc.want != nil) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("keys=%q known=%v, want %q", got, ok, tc.want)
			}
		})
	}
	key := []byte("key")
	keys, _ := clusterCSCRedirectKeys(makeCmd("get", key), nil)
	copy(key, "new")
	if keys[0] != "key" {
		t.Fatal("queued cleanup retained mutable caller bytes")
	}
}

func TestClusterCSCRedirectEvictsDependenciesAcrossNodes(t *testing.T) {
	for _, redirect := range []string{"ASK", "MOVED"} {
		t.Run(redirect, func(t *testing.T) {
			var calls atomic.Int32
			addrs := []string{
				newClusterCSCTestServer(t, clusterCSCTestReply(&calls)),
				newClusterCSCTestServer(t, clusterCSCTestReply(&calls)),
				newClusterCSCTestServer(t, clusterCSCTestReply(&calls)),
			}
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{
					{Start: 0, End: 5460, Nodes: []ClusterNode{{Addr: addrs[0]}}},
					{Start: 5461, End: 10922, Nodes: []ClusterNode{{Addr: addrs[1]}}},
					{Start: 10923, End: 16383, Nodes: []ClusterNode{{Addr: addrs[2]}}},
				}
			})
			opt.NewClient = func(o *Options) *Client {
				// Factories can use different fixed identities per node. Apply
				// each node's dependency namespace, not the observer's prefix.
				o.Username = "node:" + o.Addr
				o.ClientSideCache = &clusterCSCBlockingCache{LocalCache: NewLocalCache(*o.ClientSideCacheConfig)}
				return NewClient(o)
			}
			c := NewClusterClient(opt)
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			if _, err := c.state.Reload(ctx); err != nil {
				t.Fatal(err)
			}
			commands := []Cmder{
				makeCmd("get", "{key}:moved"),
				makeCmd("mget", "{key}:moved", "{key}:sibling"),
				makeCmd("get", "{key}:sibling"),
				makeCmd("get", "unrelated"),
			}
			type reservation struct {
				cache *clusterCSCBlockingCache
				key   string
				token uint64
			}
			var reservations []reservation
			var nodes []*clusterNode
			for _, addr := range addrs {
				node, _ := c.nodes.GetOrCreate(addr)
				nodes = append(nodes, node)
				cache := node.Client.csc.(*clusterCSCBlockingCache)
				for _, cmd := range commands {
					key, _ := buildCacheKeyNS(cmd, node.Client.cscKeyPrefix)
					lo, hi, _ := cscKeySpan(cmd)
					token, _ := cache.Reserve(key, cscMissKeys(cmd, node.Client.cscKeyPrefix, lo, hi))
					if !cache.FulfillOwned(key, token, 777, []byte("$-1\r\n")) {
						t.Fatal("could not seed cache")
					}
				}
				pending, _ := buildCacheKeyNS(makeCmd("exists", "{key}:moved"), node.Client.cscKeyPrefix)
				token, _ := cache.Reserve(pending, []string{cscNamespacedKey(node.Client.cscKeyPrefix, "{key}:moved")})
				reservations = append(reservations, reservation{cache, pending, token})
			}
			// SET's value deliberately matches another cached Redis key.
			nodes[0].Client.observeCSCRedirect(fmt.Errorf("%s %d %s", redirect, hashtag.Slot("{key}:moved"), addrs[1]), makeCmd("set", "{key}:moved", "unrelated"))
			for _, node := range nodes {
				waitClusterCSCReady(t, node.Client.opt.clusterCSC)
				cache := node.Client.csc.(*clusterCSCBlockingCache)
				if cache.flushes.Load() != 0 {
					t.Fatal("key redirect flushed unrelated entries")
				}
				for i, cmd := range commands {
					key, _ := buildCacheKeyNS(cmd, node.Client.cscKeyPrefix)
					_, hit := cache.Get(ctx, key)
					if hit != (i >= 2) {
						t.Fatalf("node %s command %v: hit=%v", node.Client.opt.Addr, cmd.Args(), hit)
					}
				}
			}
			for _, pending := range reservations {
				if pending.cache.FulfillOwned(pending.key, pending.token, 777, []byte(":0\r\n")) {
					t.Fatal("evicted reservation accepted a late reply")
				}
			}
		})
	}
}

func TestClusterCSCRedirectQueuesKeysDuringCleanup(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown-keys=%v", unknown), func(t *testing.T) {
			var calls atomic.Int32
			addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
			cache := &clusterCSCBlockingCache{LocalCache: NewLocalCache(CacheConfig{MaxEntries: 16, MaxStaleness: time.Minute}), entered: make(chan struct{}), release: make(chan struct{})}
			opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
			opt.NewClient = func(o *Options) *Client { o.ClientSideCache = cache; return NewClient(o) }
			c := NewClusterClient(opt)
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			for _, key := range []string{"first", "second", "unrelated"} {
				if err := c.Get(ctx, key).Err(); err != Nil {
					t.Fatal(err)
				}
			}
			node, _ := c.nodes.GetOrCreate(addr)
			var once sync.Once
			unblock := func() { once.Do(func() { close(cache.release) }) }
			defer unblock()
			cache.armed.Store(true)
			node.Client.observeCSCRedirect(fmt.Errorf("ASK 1 %s", addr), makeCmd("get", "first"))
			select {
			case <-cache.entered:
			case <-time.After(time.Second):
				t.Fatal("first key cleanup did not start")
			}
			command := makeCmd("get", "second")
			if unknown {
				command = makeCmd("module.dynamic", "second", "arguments")
			}
			node.Client.observeCSCRedirect(fmt.Errorf("ASK 2 %s", addr), command)
			// Repeat the second key while the first callback is blocked: it must be
			// retained, but coalesced into one pending delete, not silently discarded.
			node.Client.observeCSCRedirect(fmt.Errorf("ASK 2 %s", addr), makeCmd("get", "second"))
			unblock()
			waitClusterCSCReady(t, node.Client.opt.clusterCSC)
			wantFlushes, wantDeletes := int32(0), int32(2)
			if unknown {
				wantFlushes, wantDeletes = 1, 1
			}
			if cache.flushes.Load() != wantFlushes || cache.deletes.Load() != wantDeletes {
				t.Fatalf("flushes=%d deletes=%d", cache.flushes.Load(), cache.deletes.Load())
			}
			for _, key := range []string{"first", "second", "unrelated"} {
				cacheKey, _ := buildCacheKeyNS(makeCmd("get", key), node.Client.cscKeyPrefix)
				_, hit := cache.Get(ctx, cacheKey)
				if hit != (key == "unrelated" && !unknown) {
					t.Fatalf("key %s: hit=%v", key, hit)
				}
			}
		})
	}
}

func TestClusterCSCASKClearsNegativeRepliesAndReservations(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	cache := &clusterCSCBlockingCache{LocalCache: NewLocalCache(CacheConfig{MaxEntries: 16, MaxStaleness: time.Second}), entered: make(chan struct{}), release: make(chan struct{})}
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.NewClient = func(o *Options) *Client { o.ClientSideCache = cache; return NewClient(o) }
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if err := c.Get(ctx, "k").Err(); err != Nil {
		t.Fatal(err)
	}
	node, _ := c.nodes.GetOrCreate(addr)
	control := node.Client.opt.clusterCSC
	key, _ := buildCacheKeyNS(makeCmd("get", "late"), node.Client.cscKeyPrefix)
	dependencies := []string{cscNamespacedKey(node.Client.cscKeyPrefix, "late")}
	token, _ := cache.Reserve(key, dependencies)
	var once sync.Once
	unblock := func() { once.Do(func() { close(cache.release) }) }
	defer unblock()
	cache.armed.Store(true)
	control.observe(fmt.Errorf("ASK 7629 %s", addr), []string{"k", "late"})
	select {
	case <-cache.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup not scheduled")
	}
	if control.usable() {
		t.Fatal("cache usable during clear")
	}
	if err := c.Get(ctx, "k").Err(); err != Nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("read did not bypass pending cleanup")
	}
	target := cscRefreshTarget{cacheKey: key, redisKeys: dependencies}
	if n, err := node.Client.refreshInvalidatedBatch(ctx, []cscRefreshTarget{target}); err != nil || n != 0 {
		t.Fatalf("refresh during cleanup: %d %v", n, err)
	}
	unblock()
	waitClusterCSCReady(t, control)
	// The existing standalone reservation token, not an ownership scope,
	// prevents a pre-clear fetch from fulfilling a deleted reservation.
	if cache.FulfillOwned(key, token, 1, []byte("+stale\r\n")) {
		t.Fatal("pre-clear reservation published after key eviction")
	}
	for i := 0; i < 2; i++ {
		if err := c.Get(ctx, "k").Err(); err != Nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 3 {
		t.Fatal("ASK cleanup did not restore miss/fill/hit")
	}
}

func TestClusterCSCTopologyChangesRetireBothOwners(t *testing.T) {
	var ca, cb atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&ca))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&cb))
	var owner atomic.Int32
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		primary, replica := a, b
		if owner.Load() == 1 {
			primary, replica = b, a
		}
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: primary}, {Addr: replica}}}}
	})
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if err := c.Get(ctx, "key").Err(); err != Nil {
		t.Fatal(err)
	}
	na, _ := c.nodes.GetOrCreate(a)
	nb, _ := c.nodes.GetOrCreate(b)
	// The gaining node may retain a value from an earlier ownership period.
	key, _ := buildCacheKeyNS(makeCmd("get", "key"), nb.Client.cscKeyPrefix)
	token, _ := nb.Client.csc.Reserve(key, []string{cscNamespacedKey(nb.Client.cscKeyPrefix, "key")})
	nb.Client.csc.FulfillOwned(key, token, 0, []byte("+old-owner\r\n"))
	owner.Store(1)
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	waitClusterCSCReady(t, nb.Client.opt.clusterCSC)
	if na.Client.opt.clusterCSC.usable() {
		t.Fatal("former primary remained cache eligible")
	}
	if err := c.Get(ctx, "key").Err(); err != Nil {
		t.Fatal(err)
	}
	owner.Store(0)
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	waitClusterCSCReady(t, na.Client.opt.clusterCSC)
	if err := c.Get(ctx, "key").Err(); err != Nil {
		t.Fatal(err)
	}
	if ca.Load() != 2 || cb.Load() != 1 {
		t.Fatalf("failback reused old values: A=%d B=%d", ca.Load(), cb.Load())
	}
}

func TestClusterCSCExpiredTopologyBypassesAndFailedReloadDoesNotRenew(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.ClusterStateReloadInterval = -1
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != Nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 || c.CSCStats().Entries != 0 {
		t.Fatal("expired topology used CSC")
	}
}

func TestClusterCSCUniversalAndFactoryControls(t *testing.T) {
	cfg := &ClientSideCacheConfig{MaxEntries: 23}
	u := &UniversalOptions{ClientSideCacheConfig: cfg, ClientSideCacheCoalesceMisses: true, ClientSideCacheRefreshOnInvalidate: true, ClientSideCacheInvalidationBatchWindow: time.Millisecond}
	o := u.Cluster().clientOptions()
	if o.ClientSideCacheConfig == cfg || o.ClientSideCacheConfig.MaxEntries != cfg.MaxEntries || o.ClientSideCacheConfig.MaxStaleness != time.Second || cfg.MaxStaleness != 0 || !o.ClientSideCacheCoalesceMisses || !o.ClientSideCacheRefreshOnInvalidate || o.ClientSideCacheInvalidationBatchWindow != time.Millisecond {
		t.Fatal("configuration not propagated")
	}
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.NewClient = func(o *Options) *Client { o.clusterCSC = nil; return NewClient(o) }
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != Nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("factory discarded controls but retained active CSC")
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
				if err := c.Get(context.Background(), "k").Err(); err != Nil {
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
		o.clusterCSC = nil
		return NewClient(o).WithTimeout(time.Second)
	}
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	for i := 0; i < 2; i++ {
		if err := c.Get(context.Background(), "k").Err(); err != Nil {
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
		if err := c.Get(context.Background(), "k").Err(); err != Nil {
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
	if c.CSCStats().Hits != 1 {
		t.Fatal("factory clone's final statistics were omitted or double counted")
	}
}

func TestClusterCSCFailedReloadDoesNotRenewTrust(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_ = c.Get(ctx, "k").Err()
	state := *c.state.state.Load().(*clusterState)
	state.createdAt = time.Now().Add(-2 * time.Minute)
	c.state.state.Store(&state)
	c.csc.freshness.Store(&state.createdAt)
	c.state.load = func(context.Context) (*clusterState, error) { return nil, fmt.Errorf("test unavailable topology") }
	if _, err := c.state.Reload(ctx); err == nil {
		t.Fatal("failed reload unexpectedly succeeded")
	}
	if *c.csc.freshness.Load() != state.createdAt {
		t.Fatal("failed reload renewed freshness")
	}
	if err := c.Get(ctx, "k").Err(); err != Nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("failed reload permitted old cache hit")
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
	if na.Client.opt.clusterCSC.usable() {
		t.Fatal("removed owner remained cache eligible")
	}
	nb, _ := c.nodes.GetOrCreate(b)
	waitClusterCSCReady(t, nb.Client.opt.clusterCSC)
	for i := 0; i < 5; i++ {
		_ = c.Get(ctx, fmt.Sprintf("key%d", i)).Err()
	}
	if na.Client.csc == nb.Client.csc || nb.Client.CSCStats().Entries > 2 {
		t.Fatal("new node reused storage or exceeded local limit")
	}
}

func TestClusterCSCReloadOrdering(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if _, err := c.state.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	original := c.state.load
	entered, release := make(chan struct{}), make(chan struct{})
	var loads atomic.Int32
	c.state.load = func(ctx context.Context) (*clusterState, error) {
		if loads.Add(1) == 1 {
			close(entered)
			<-release
		}
		return original(ctx)
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = c.state.Reload(ctx) }()
	<-entered
	newer, err := c.state.Reload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	if c.state.state.Load() != newer {
		t.Fatal("delayed old fetch overwrote newer routing")
	}
}

func TestClusterCSCDiscardedReloadCollectsOrphanNodes(t *testing.T) {
	var calls atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	started, release, firstDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var loads atomic.Int32
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		addr := b
		if loads.Add(1) == 1 {
			close(started)
			<-release
			addr = a
		}
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	})
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	go func() { _, err := c.state.Reload(ctx); firstDone <- err }()
	<-started
	if _, err := c.state.Reload(ctx); err != nil {
		close(release)
		t.Fatal(err)
	}
	_ = c.Get(ctx, "k").Err()
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	orphan, err := c.nodes.GetOrCreate(a)
	if err != nil {
		t.Fatal(err)
	}
	// A discarded candidate cannot use its own GC threshold: its discovery
	// generation is newer than that of the accepted owner's cached snapshot.
	clusterCSCLiveEventually(t, func() bool {
		state := c.state.state.Load().(*clusterState)
		return loads.Load() >= 3 && state.generation > orphan.Generation()
	})
	state := c.state.state.Load().(*clusterState)
	c.nodes.GC(state.generation)
	c.nodes.mu.RLock()
	_, retained := c.nodes.nodes[a]
	c.nodes.mu.RUnlock()
	if retained || !orphan.Client.opt.clusterCSC.closed.Load() {
		t.Fatal("discarded topology retained an orphan node/CSC lifetime")
	}
	select {
	case <-orphan.Client.cscDrainHandle.done:
	case <-time.After(time.Second):
		t.Fatal("orphan CSC worker survived registry collection")
	}
	before := calls.Load()
	if err := c.Get(ctx, "k").Err(); err != Nil || calls.Load() != before {
		t.Fatal("orphan collection closed/flushed the accepted owner's cache")
	}
}

func TestClusterCSCClosedReloadDoesNotStartRecovery(t *testing.T) {
	var loads atomic.Int32
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		loads.Add(1)
		// An empty successful callback does not touch the closed node registry,
		// so it can keep producing candidates even after Close.
		return nil
	}))
	original, err := c.state.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.state.Reload(context.Background()); err != nil || got != original {
		t.Fatal("closed coordinator published a new routing snapshot")
	}
	if c.state.reloading.Load() != 0 || c.state.reloadPending.Load() != 0 || loads.Load() != 2 {
		t.Fatal("closed coordinator scheduled another recovery reload")
	}
}

func TestClusterCSCMOVEDRetiresEveryDestinationDuringCleanup(t *testing.T) {
	var calls atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	d := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	cache := &clusterCSCBlockingCache{LocalCache: NewLocalCache(CacheConfig{MaxEntries: 16, MaxStaleness: time.Minute}), entered: make(chan struct{}), release: make(chan struct{})}
	opt := clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{
			{Start: 0, End: 5460, Nodes: []ClusterNode{{Addr: a}}}, {Start: 5461, End: 10922, Nodes: []ClusterNode{{Addr: b}}}, {Start: 10923, End: 16383, Nodes: []ClusterNode{{Addr: d}}},
		}
	})
	opt.NewClient = func(o *Options) *Client {
		if o.Addr == a {
			o.ClientSideCache = cache
		}
		return NewClient(o)
	}
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	_, err := c.state.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	na, _ := c.nodes.GetOrCreate(a)
	nd, _ := c.nodes.GetOrCreate(d)
	var once sync.Once
	defer once.Do(func() { close(cache.release) })
	cache.armed.Store(true)
	na.Client.observeCSCRedirect(fmt.Errorf("MOVED 1 %s", b), makeCmd("get", "first"))
	select {
	case <-cache.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	na.Client.observeCSCRedirect(fmt.Errorf("MOVED 2 %s", d), makeCmd("get", "second"))
	if !nd.Client.opt.clusterCSC.dirty.Load() {
		t.Fatal("second destination was not retired")
	}
	once.Do(func() { close(cache.release) })
	_, err = c.state.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitClusterCSCReady(t, nd.Client.opt.clusterCSC)
}

func TestClusterCSCStatisticsRetainOverlappingNodeLifetimes(t *testing.T) {
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
	if got := c.CSCStats().Hits; got != 1 {
		t.Fatalf("closing lifetime lost: %d", got)
	}
	_ = old.Client.Close()
	if got := c.CSCStats().Hits; got != 1 {
		t.Fatalf("retired lifetime lost/doubled: %d", got)
	}
}

func TestClusterCSCConcurrentCloseJoinsCleanup(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	cache := &clusterCSCBlockingCache{LocalCache: NewLocalCache(CacheConfig{MaxEntries: 16, MaxStaleness: time.Minute}), entered: make(chan struct{}), release: make(chan struct{})}
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.NewClient = func(o *Options) *Client { o.ClientSideCache = cache; return NewClient(o) }
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	_ = c.Get(context.Background(), "k").Err()
	var once sync.Once
	defer once.Do(func() { close(cache.release) })
	cache.armed.Store(true)
	n, _ := c.nodes.GetOrCreate(addr)
	n.Client.observeCSCRedirect(fmt.Errorf("ASK 7629 %s", addr), makeCmd("get", "k"))
	select {
	case <-cache.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	first, second := make(chan struct{}), make(chan struct{})
	go func() { _ = c.Close(); close(first) }()
	clusterCSCLiveEventually(t, func() bool { return c.csc.closed.Load() })
	go func() { _ = c.Close(); close(second) }()
	// Neither closer can finish until the deterministic cleanup barrier opens.
	select {
	case <-first:
		t.Fatal("first Close skipped cleanup")
	case <-second:
		t.Fatal("second Close skipped cleanup")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(cache.release) })
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first Close hung")
	}
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second Close hung")
	}
}

func TestClusterCSCDroppedClientRemainsCollectible(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	makeDropped := func() <-chan struct{} {
		c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
		_ = c.Get(context.Background(), "k").Err()
		return c.csc.stop
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

func TestClusterCSCDroppedClientStopsFactoryCloneWorkers(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	makeDropped := func() (*Client, <-chan struct{}, <-chan struct{}) {
		opt := clusterCSCTestOptions(func() []ClusterSlot {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
		})
		opt.ClientSideCacheConfig.DrainInterval = time.Hour
		opt.ClientSideCacheRefreshOnInvalidate = true
		var canonical *Client
		opt.NewClient = func(o *Options) *Client {
			canonical = NewClient(o)
			return canonical.WithTimeout(time.Second)
		}
		c := NewClusterClient(opt)
		if err := c.Get(context.Background(), "k").Err(); err != Nil {
			_ = c.Close()
			t.Fatal(err)
		}
		node, _ := c.nodes.GetOrCreate(addr)
		return node.Client, c.csc.stop, canonical.cscDrainHandle.done
	}
	handle, stop, drained := makeDropped()
	defer handle.Close()
	defer runtime.KeepAlive(handle)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		select {
		case <-stop:
			select {
			case <-drained:
				return
			case <-time.After(time.Second):
				t.Fatal("dropped Cluster failed to stop the retained factory clone's canonical CSC workers")
			}
		default:
		}
		runtime.Gosched()
	}
	t.Fatal("factory clone retained the owning ClusterClient")
}

func TestClusterCSCConcurrentCloseWaitsForNodeTeardown(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} }))
	t.Cleanup(func() { _ = c.Close() })
	_ = c.Get(context.Background(), "k").Err()
	node, _ := c.nodes.GetOrCreate(addr)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	node.Client.onClose.register("test-close-barrier", func() error { close(entered); <-release; return nil })
	first, second := make(chan struct{}), make(chan struct{})
	go func() { _ = c.Close(); close(first) }()
	<-entered
	go func() { _ = c.Close(); close(second) }()
	select {
	case <-second:
		t.Fatal("concurrent Close returned before node teardown")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first Close hung")
	}
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second Close hung")
	}
}

func TestClusterCSCMOVEDCleanupDoesNotWaitForReload(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	cache := &clusterCSCBlockingCache{LocalCache: NewLocalCache(CacheConfig{MaxEntries: 16, MaxStaleness: time.Second}), entered: make(chan struct{}), release: make(chan struct{})}
	opt := clusterCSCTestOptions(func() []ClusterSlot { return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}} })
	opt.NewClient = func(o *Options) *Client { o.ClientSideCache = cache; return NewClient(o) }
	c := NewClusterClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	_ = c.Get(ctx, "k").Err()
	original := c.state.load
	loadStarted, allowLoad := make(chan struct{}, 1), make(chan struct{})
	c.state.load = func(ctx context.Context) (*clusterState, error) {
		select {
		case loadStarted <- struct{}{}:
		default:
		}
		<-allowLoad
		return original(ctx)
	}
	defer close(allowLoad)
	var once sync.Once
	unblock := func() { once.Do(func() { close(cache.release) }) }
	defer unblock()
	node, _ := c.nodes.GetOrCreate(addr)
	control := node.Client.opt.clusterCSC
	cache.armed.Store(true)
	control.observe(fmt.Errorf("MOVED 7629 %s", addr), []string{"k"})
	select {
	case <-cache.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup not started")
	}
	select {
	case <-loadStarted:
	case <-time.After(time.Second):
		t.Fatal("reload not started")
	}
	if control.usable() {
		t.Fatal("cache usable during cleanup")
	}
	unblock()
	waitClusterCSCReady(t, control)
	// The unchanged routing map is still trusted. A cleared cache may resume
	// before reload completes: its next miss reaches Redis, not the old entry.
	for i := 0; i < 2; i++ {
		_ = c.Get(ctx, "k").Err()
	}
	if calls.Load() != 2 {
		t.Fatalf("cleanup did not restore miss/fill/hit: %d", calls.Load())
	}
	// Standalone full invalidation remains synchronous and clears the cache.
	lookupInvalidateHandler(node.Client.pushProcessor).fullFlush(cache)
	if cache.Stats().Entries != 0 {
		t.Fatal("full invalidation did not clear node cache")
	}
}

func TestClusterCSCRedirectObserversOnUncachedPaths(t *testing.T) {
	for _, path := range []string{"pipeline", "transaction", "full-duplex"} {
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
						return "$-1\r\n"
					default:
						return "+OK\r\n"
					}
				})
				opt := clusterCSCTestOptions(func() []ClusterSlot {
					return []ClusterSlot{{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}}}
				})
				c := NewClusterClient(opt)
				t.Cleanup(func() { _ = c.Close() })
				ctx := context.Background()
				key := "{bar}:key"
				if err := c.Get(ctx, key).Err(); err != Nil {
					t.Fatal(err)
				}
				node, _ := c.MasterForKey(ctx, key)
				var err error
				switch path {
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
				waitClusterCSCReady(t, node.opt.clusterCSC)
				if got, err := c.Get(ctx, key).Result(); err != nil || got != "v" {
					t.Fatalf("old negative cache hid redirect: %q %v", got, err)
				}
			})
		}
	}
}

func TestClusterCSCTokenValidatesSelectedSlotRole(t *testing.T) {
	var calls atomic.Int32
	a := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	b := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{
			{Start: 0, End: 8191, Nodes: []ClusterNode{{Addr: a}, {Addr: b}}}, {Start: 8192, End: 16383, Nodes: []ClusterNode{{Addr: b}}},
		}
	}))
	t.Cleanup(func() { _ = c.Close() })
	state, err := c.state.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	node, _ := c.nodes.GetOrCreate(b)
	ctx := context.WithValue(context.Background(), clusterCSCRouteKey{}, state)
	for i := 0; i < 2; i++ {
		cmd := NewStringCmd(ctx, "get", "{bar}:replica")
		if err := node.Client.Process(c.cscNodeContext(ctx, node, cmd), cmd); err != Nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 || node.Client.CSCStats().Entries != 0 {
		t.Fatal("replica-selected slot cached on a mixed-role endpoint")
	}
	if err := c.Get(context.Background(), "{foo}:primary").Err(); err != Nil {
		t.Fatal(err)
	}
	if _, err := c.state.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	if err := c.Get(context.Background(), "{foo}:primary").Err(); err != Nil {
		t.Fatal(err)
	}
	if calls.Load() != before {
		t.Fatal("unchanged mixed-role map retired a primary's cache")
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

func BenchmarkClusterCSCTopologyCleanup(b *testing.B) {
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
		state, err := c.state.Reload(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for _, node := range state.Masters {
			for !node.Client.opt.clusterCSC.usable() {
				runtime.Gosched()
			}
		}
		_ = c.Get(ctx, "{bar}:churn").Err()
		_ = c.Get(ctx, "{foo}:churn").Err()
	}
}

// Run against a private six-process OSS cluster, never the developer's database.
// CI images without redis-server can still run all deterministic CSC tests.
func newClusterCSCLive(t *testing.T) (*ClusterClient, []*Client) {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
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
		if node.opt.clusterCSC.closed.Load() || !node.opt.clusterCSC.usable() {
			t.Fatalf("sticky wrapper Close retired parent node: nodeClosed=%v cacheUsable=%v active=%v stats=%+v", node.opt.clusterCSC.closed.Load(), node.opt.clusterCSC.usable(), node.cscActive.Load(), c.CSCStats())
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
			entryKey, _ := buildCacheKeyNS(NewStringCmd(ctx, "get", key), node.cscKeyPrefix)
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
	t.Run("replica reads preserve routing without caching", func(t *testing.T) {
		r := NewClusterClient(&ClusterOptions{
			Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, ReadOnly: true, DisableIdentity: true,
			ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 128},
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		})
		defer r.Close()
		key := "{baz}:replica-cache-bypass"
		if err := c.Set(ctx, key, "replicated", 0).Err(); err != nil {
			t.Fatal(err)
		}
		// CLUSTER SLOTS can omit a replica until its initial synchronization
		// completes. ReadOnly correctly falls back to the primary in that case.
		clusterCSCLiveEventually(t, func() bool {
			state, err := r.state.Reload(ctx)
			if err != nil {
				return false
			}
			entry := state.slotEntry(hashtag.Slot(key))
			return entry != nil && len(entry.nodes) > 1
		})
		clusterCSCLiveEventually(t, func() bool { return r.Get(ctx, key).Val() == "replicated" })
		for i := 0; i < 3; i++ {
			if value, err := r.Get(ctx, key).Result(); err != nil || value != "replicated" {
				t.Fatalf("replica GET: %q %v", value, err)
			}
		}
		if stats := r.CSCStats(); stats.Hits != 0 || stats.Entries != 0 {
			t.Fatalf("replica-selected reads used CSC: %+v", stats)
		}
	})
	t.Run("ASK observed by another client does not evict tracked absence", func(t *testing.T) {
		newCachingClient := func() *ClusterClient {
			client := NewClusterClient(&ClusterOptions{
				Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, PoolSize: 1,
				DisableIdentity: true, DisableRoutingPolicies: true,
				ClientSideCacheConfig:    &ClientSideCacheConfig{MaxEntries: 16, MaxStaleness: time.Hour},
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
			})
			t.Cleanup(func() { _ = client.Close() })
			return client
		}
		reader, writer := newCachingClient(), newCachingClient()
		key := "{csc-external-ask}:absent"
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
		for _, client := range []*ClusterClient{reader, writer} {
			if err := client.Get(ctx, key).Err(); err != Nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() {
			conn := b.Conn()
			_ = conn.Do(ctx, "asking").Err()
			_ = conn.Del(ctx, key).Err()
			_ = conn.Close()
			_ = a.Do(ctx, "cluster", "setslot", slot, "stable").Err()
			_ = b.Do(ctx, "cluster", "setslot", slot, "stable").Err()
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
			t.Fatalf("writer's ASK did not evict its own absence: %q %v", got, err)
		}
		// Read A's connection after the write; pending push frames would be
		// consumed here. No invalidation for creating this key on B is sent to A.
		if err := source.Ping(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		before := reader.CSCStats().Hits
		if err := reader.Get(ctx, key).Err(); err != Nil || reader.CSCStats().Hits != before+1 {
			t.Fatalf("expected the separate reader's cached absence: err=%v stats=%+v", err, reader.CSCStats())
		}
		// Expire deterministically: do not rely on a sleep or the one-second
		// default while demonstrating the missing cross-client notification.
		cache := source.csc.(*LocalCache)
		cacheKey, _ := buildCacheKeyNS(makeCmd("get", key), source.cscKeyPrefix)
		shard := cache.shardFor(cacheKey)
		shard.mu.Lock()
		entry := shard.entries[cacheKey]
		if entry != nil {
			entry.validAt = time.Now().Add(-2 * time.Hour)
		}
		shard.mu.Unlock()
		if entry == nil {
			t.Fatal("tracked absence unexpectedly evicted")
		}
		if got, err := reader.Get(ctx, key).Result(); err != nil || got != "created-on-b" {
			t.Fatalf("expiry did not expose ASK: %q %v", got, err)
		}
	})
	t.Run("ASK cached absence and MOVED migration", func(t *testing.T) {
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
		// The source caches a missing key before IMPORTING/MIGRATING starts.
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
			t.Fatalf("cached absence survived ASK: %q %v", got, err)
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
		dest, _ := c.nodes.GetOrCreate(b.opt.Addr)
		waitClusterCSCReady(t, dest.Client.opt.clusterCSC)
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
		refreshCacheKey, _ := buildCacheKeyNS(NewStringCmd(ctx, "get", existing), refreshSource.cscKeyPrefix)
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
		waitClusterCSCReady(t, source.opt.clusterCSC)
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
		if old.opt.clusterCSC.usable() {
			t.Fatal("former primary remained cache eligible after promotion")
		}
		if got := c.Get(ctx, key).Val(); got != "before" {
			t.Fatal(got)
		}
	})
}
