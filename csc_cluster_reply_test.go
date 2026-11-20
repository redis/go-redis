package redis

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

type clusterCSCReplyCase struct {
	name  string
	args  []interface{}
	raw   string
	cache bool
}

func clusterCSCReplyCases() []clusterCSCReplyCase {
	return []clusterCSCReplyCase{
		{"GET missing RESP2", []interface{}{"get", "k"}, "$-1\r\n", false},
		{"GET missing RESP3", []interface{}{"get", "k"}, "_\r\n", false},
		{"GET empty string", []interface{}{"GET", "k"}, "$0\r\n\r\n", true},
		{"GET zero string", []interface{}{"get", "k"}, "$1\r\n0\r\n", true},
		{"GET null-looking string", []interface{}{"get", "k"}, "$3\r\n$-1\r\n", true},
		{"GET attributed null", []interface{}{"get", "k"}, "|1\r\n+meta\r\n+v\r\n_\r\n", false},
		{"GET attributed value", []interface{}{"get", "k"}, "|1\r\n+meta\r\n+v\r\n$1\r\nv\r\n", true},
		{"MGET all present", []interface{}{"mget", "k", "other"}, "*2\r\n$1\r\nv\r\n$0\r\n\r\n", true},
		{"MGET mixed", []interface{}{"mget", "k", "other"}, "*2\r\n$1\r\nv\r\n$-1\r\n", false},
		{"MGET missing first", []interface{}{"mget", "k", "other"}, "*2\r\n_\r\n$1\r\nv\r\n", false},
		{"MGET all missing", []interface{}{"mget", "k", "other"}, "*2\r\n_\r\n_\r\n", false},
		{"MGET attributed element", []interface{}{"mget", "k", "other"}, "*2\r\n$1\r\nv\r\n|1\r\n+m\r\n+v\r\n_\r\n", false},
		{"EXISTS missing", []interface{}{"exists", "k"}, ":0\r\n", false},
		{"EXISTS partial", []interface{}{"exists", "k", "other"}, ":1\r\n", false},
		{"EXISTS all", []interface{}{"exists", "k", "other"}, ":2\r\n", true},
		{"EXISTS duplicates", []interface{}{"exists", "k", "k"}, ":2\r\n", true},
		{"HGET empty field", []interface{}{"hget", "k", "field"}, "$0\r\n\r\n", true},
		{"HGET absent", []interface{}{"hget", "k", "field"}, "_\r\n", false},
		{"HMGET mixed fields", []interface{}{"hmget", "k", "a", "b"}, "*2\r\n_\r\n$0\r\n\r\n", true},
		{"HMGET all absent", []interface{}{"hmget", "k", "a", "b"}, "*2\r\n_\r\n_\r\n", false},
		{"STRLEN zero", []interface{}{"strlen", "k"}, ":0\r\n", false},
		{"STRLEN present", []interface{}{"strlen", "k"}, ":3\r\n", true},
		{"GETRANGE empty", []interface{}{"getrange", "k", 0, 1}, "$0\r\n\r\n", false},
		{"GETRANGE present", []interface{}{"getrange", "k", 0, 1}, "$1\r\nv\r\n", true},
		{"HGETALL empty", []interface{}{"hgetall", "k"}, "%0\r\n", false},
		{"HGETALL present", []interface{}{"hgetall", "k"}, "%1\r\n+a\r\n+b\r\n", true},
		{"LPOS absent", []interface{}{"lpos", "k", "v"}, "_\r\n", false},
		{"LPOS first element", []interface{}{"lpos", "k", "v"}, ":0\r\n", true},
		{"LPOS count absent", []interface{}{"lpos", "k", "v", "count", 2}, "*0\r\n", false},
		{"LPOS count present", []interface{}{"lpos", "k", "v", "count", 2}, "*1\r\n:0\r\n", true},
		{"LRANGE absent", []interface{}{"lrange", "k", 0, -1}, "*0\r\n", false},
		{"LRANGE present", []interface{}{"lrange", "k", 0, -1}, "*1\r\n$0\r\n\r\n", true},
		{"SMEMBERS absent", []interface{}{"smembers", "k"}, "~0\r\n", false},
		{"SMEMBERS present", []interface{}{"smembers", "k"}, "~1\r\n+v\r\n", true},
		{"SMISMEMBER absent", []interface{}{"smismember", "k", "a", "b"}, "*2\r\n:0\r\n:0\r\n", false},
		{"SMISMEMBER present", []interface{}{"smismember", "k", "a", "b"}, "*2\r\n:0\r\n:1\r\n", true},
		{"SUNION ambiguous", []interface{}{"sunion", "k", "other"}, "~1\r\n+v\r\n", false},
		{"SDIFF ambiguous", []interface{}{"sdiff", "k", "other"}, "~1\r\n+v\r\n", false},
		{"SINTER empty", []interface{}{"sinter", "k", "other"}, "~0\r\n", false},
		{"SINTER common member", []interface{}{"sinter", "k", "other"}, "~1\r\n+v\r\n", true},
		{"SINTERCARD empty", []interface{}{"sintercard", 2, "k", "other"}, ":0\r\n", false},
		{"SINTERCARD common member", []interface{}{"sintercard", 2, "k", "other"}, ":1\r\n", true},
		{"ZRANK zero", []interface{}{"zrank", "k", "v"}, ":0\r\n", true},
		{"ZSCORE zero", []interface{}{"zscore", "k", "v"}, ",0\r\n", true},
		{"ZMSCORE absent", []interface{}{"zmscore", "k", "a", "b"}, "*2\r\n_\r\n_\r\n", false},
		{"ZMSCORE mixed", []interface{}{"zmscore", "k", "a", "b"}, "*2\r\n_\r\n,0\r\n", true},
		{"TYPE absent", []interface{}{"type", "k"}, "+none\r\n", false},
		{"TYPE present", []interface{}{"type", "k"}, "+string\r\n", true},
		{"JSON.GET absent", []interface{}{"json.get", "k", "$"}, "_\r\n", false},
		{"JSON.GET stored null", []interface{}{"json.get", "k", "$"}, "$6\r\n[null]\r\n", true},
		{"JSON.MGET mixed", []interface{}{"json.mget", "k", "other", "$"}, "*2\r\n$2\r\n[]\r\n_\r\n", false},
		{"JSON.MGET present", []interface{}{"json.mget", "k", "other", "$"}, "*2\r\n$2\r\n[]\r\n$4\r\nnull\r\n", true},
		{"JSON.ARRINDEX ambiguous", []interface{}{"json.arrindex", "k", "$", "v"}, "*1\r\n:-1\r\n", false},
		{"BITPOS ambiguous", []interface{}{"bitpos", "k", 0}, ":0\r\n", false},
		{"BITFIELD_RO ambiguous", []interface{}{"bitfield_ro", "k", "get", "u8", 0}, "*1\r\n:0\r\n", false},
	}
}

func TestClusterCSCReplyPolicies(t *testing.T) {
	c := &baseClient{cscKeyPrefix: cscNamespacePrefix(0, "")}
	for _, tc := range clusterCSCReplyCases() {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := cscRenderEntryKey(c.cscKeyPrefix, c.metadataView().cscFingerprint, NewCmd(context.Background(), tc.args...))
			if !ok {
				t.Fatal("cache key encoding failed")
			}
			if got := c.clusterCSCReplyCacheable(key, []byte(tc.raw), c.metadataView()); got != tc.cache {
				t.Fatalf("cacheable=%v, want %v", got, tc.cache)
			}
		})
	}
	// Unsupported commands fail closed, including newly added standalone ones.
	for _, name := range []string{"sdiff", "sunion", "zdiff", "zunion", "lcs", "bitpos", "bitfield_ro", "json.arrlen", "unknown"} {
		if clusterCSCCommandPolicy(name) != clusterCSCReplyBypass {
			t.Fatalf("ambiguous command %s was enabled", name)
		}
	}
}

func TestClusterCSCReplyUsesCapturedMetadata(t *testing.T) {
	view := defaultCommandMetadataView()
	c := &baseClient{cscKeyPrefix: cscNamespacePrefix(0, "")}
	cmd := makeCmd("get", "k")
	key, _ := cscRenderEntryKey(c.cscKeyPrefix, view.cscFingerprint, cmd)
	// Publication checks that the current view still agrees separately. Parsing
	// must use the view that encoded the key, even if metadata has since changed.
	c.staticCmdMeta = &commandMetadataView{cscFingerprint: "changed"}
	raw := []byte("$1\r\nv\r\n")
	if !c.clusterCSCReplyCacheable(key, raw, view) {
		t.Fatal("present reply rejected under its captured metadata")
	}
	if c.clusterCSCReplyCacheable(key, raw, c.metadataView()) {
		t.Fatal("cache key accepted under a different fingerprint")
	}
	legacy, _ := buildCacheKeyNS(cmd, c.cscKeyPrefix)
	if c.clusterCSCReplyCacheable(legacy, raw, view) {
		t.Fatal("cache key without a metadata fingerprint accepted")
	}
}

func TestClusterCSCReplyPolicyProcess(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		for _, custom := range []bool{false, true} {
			// Background features intentionally support only the built-in cache.
			if custom && coalesce {
				continue
			}
			for _, tc := range clusterCSCReplyCases() {
				t.Run(fmt.Sprintf("%s/coalesce=%v/custom=%v", tc.name, coalesce, custom), func(t *testing.T) {
					var calls atomic.Int32
					command := strings.ToLower(tc.args[0].(string))
					addr := newClusterCSCTestServer(t, func(cmd string) string {
						if cmd == "hello" {
							return "%0\r\n"
						}
						if cmd == command {
							calls.Add(1)
							return tc.raw
						}
						return "+OK\r\n"
					})
					opt := clusterCSCTestOptions(func() []ClusterSlot {
						return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
					})
					opt.ClientSideCacheConfig.MaxStaleness = 0
					opt.ClientSideCacheCoalesceMisses = coalesce
					if custom {
						opt.NewClient = func(o *Options) *Client {
							o.ClientSideCache = &clusterCSCRecordingCache{LocalCache: NewLocalCache(*o.ClientSideCacheConfig)}
							return NewClient(o)
						}
					}
					c := NewClusterClient(opt)
					t.Cleanup(func() { _ = c.Close() })
					want, wantErr := proto.NewReader(strings.NewReader(tc.raw)).ReadReply()
					for i := 0; i < 2; i++ {
						cmd := NewCmd(context.Background(), tc.args...)
						if err := c.Process(context.Background(), cmd); err != wantErr || !reflect.DeepEqual(cmd.Val(), want) {
							t.Fatalf("reply=%#v, %v; want %#v, %v", cmd.Val(), err, want, wantErr)
						}
					}
					wantCalls, wantEntries := int32(2), 0
					// Presence is an additional cluster requirement; metadata
					// may still rule out caching (e.g. JSON.MGET and TS.INFO).
					if tc.cache && command != "json.mget" && command != "ts.info" {
						wantCalls, wantEntries = 1, 1
					}
					if calls.Load() != wantCalls || c.CSCStats().Entries != wantEntries {
						t.Fatalf("calls=%d stats=%+v; want calls=%d entries=%d", calls.Load(), c.CSCStats(), wantCalls, wantEntries)
					}
				})
			}
		}
	}
}

func TestClusterCSCRefreshRejectsAbsence(t *testing.T) {
	for _, tc := range clusterCSCReplyCases() {
		if tc.cache || clusterCSCCommandPolicy(strings.ToLower(tc.args[0].(string))) == clusterCSCReplyBypass {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			addr := newClusterCSCTestServer(t, func(cmd string) string {
				if cmd == "hello" {
					return "%0\r\n"
				}
				if cmd == strings.ToLower(tc.args[0].(string)) {
					calls.Add(1)
					return tc.raw
				}
				return "+OK\r\n"
			})
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
			})
			opt.ClientSideCacheConfig.MaxStaleness = 0
			c := NewClusterClient(opt)
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			node, err := c.MasterForKey(ctx, "k")
			if err != nil {
				t.Fatal(err)
			}
			if err := node.Ping(ctx).Err(); err != nil { // Establish tracked coverage.
				t.Fatal(err)
			}
			cmd := NewCmd(ctx, tc.args...)
			key, _ := cscRenderEntryKey(node.cscKeyPrefix, node.metadataView().cscFingerprint, cmd)
			meta, _ := cscLookupMeta(node.metadataView(), cmd)
			first, step, count := cscRedisKeyLayout(meta, cmd)
			keys := cscMissKeys(cmd, node.cscKeyPrefix, first, step, count)
			if n, err := node.refreshInvalidatedBatch(ctx, []cscRefreshTarget{{cacheKey: key, redisKeys: keys}}); err != nil || n != 0 {
				t.Fatalf("refresh published %d entries: %v", n, err)
			}
			if calls.Load() != 1 || node.CSCStats().Entries != 0 {
				t.Fatalf("refresh calls=%d stats=%+v", calls.Load(), node.CSCStats())
			}
			// Cancellation must remove the placeholder, not leave future readers
			// parked until StaleTimeout. An old token cannot resurrect the reply.
			if token, fetch := node.csc.Reserve(key, keys); !fetch || token == 0 {
				t.Fatal("refresh left a pending reservation")
			} else {
				node.csc.Cancel(key, token)
			}
		})
	}
}

func TestClusterCSCAbandonedMissUsesOriginalCommand(t *testing.T) {
	var calls atomic.Int32
	addr := newClusterCSCTestServer(t, clusterCSCTestReply(&calls))
	c := NewClusterClient(clusterCSCTestOptions(func() []ClusterSlot {
		return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}
	}))
	defer c.Close()
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	node, _ := c.MasterForKey(ctx, "k")
	// nil cmd deliberately proves the abandoned path never reads it. The
	// caller could already have changed EXISTS k other to EXISTS k.
	key, _ := cscRenderEntryKey(node.cscKeyPrefix, node.metadataView().cscFingerprint, makeCmd("exists", "k", "other"))
	token, _ := node.csc.Reserve(key, []string{cscNamespacedKey(node.cscKeyPrefix, "k"), cscNamespacedKey(node.cscKeyPrefix, "other")})
	req := &cscMissReq{cacheKey: key, token: token, view: node.metadataView(), done: make(chan error, 1)}
	req.apply.Store(cscReqAbandoned)
	cn, err := node.getConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mc := &cscMissCoalescer{c: node.baseClient}
	mc.applyAndSettle(req, []byte(":1\r\n"), cn.GetID(), node.cscConnInitGen(cn.GetID()))
	node.releaseConn(ctx, cn, nil)
	if err := <-req.done; err != nil {
		t.Fatal(err)
	}
	if node.CSCStats().Entries != 0 || mc.abandonedApplies.Load() != 1 {
		t.Fatal("abandoned partial EXISTS reply was cached")
	}
}

func BenchmarkClusterCSCReplyFilter(b *testing.B) {
	c := &baseClient{cscKeyPrefix: cscNamespacePrefix(0, "")}
	for _, size := range []int{16, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			key, _ := cscRenderEntryKey(c.cscKeyPrefix, c.metadataView().cscFingerprint, makeCmd("get", "k"))
			raw := append([]byte(fmt.Sprintf("$%d\r\n", size)), bytes.Repeat([]byte("v"), size)...)
			raw = append(raw, '\r', '\n')
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !c.clusterCSCReplyCacheable(key, raw, c.metadataView()) {
					b.Fatal("present reply rejected")
				}
			}
		})
	}
}
