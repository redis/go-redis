package redis

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// helper to create a Cmd with the given args.
func makeCmd(args ...interface{}) Cmder {
	return NewCmd(context.Background(), args...)
}

// --- isCacheable -----------------------------------------------------------

func TestIsCacheable_AllowedCommands(t *testing.T) {
	allowed := []string{
		"GET", "MGET", "HGET", "HMGET", "HGETALL",
		"HKEYS", "HVALS", "HLEN", "HEXISTS", "HSTRLEN",
		"LINDEX", "LLEN", "LPOS", "LRANGE",
		"SCARD", "SISMEMBER", "SMEMBERS", "SMISMEMBER",
		"SDIFF", "SINTER", "SINTERCARD", "SUNION",
		"ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZMSCORE",
		"ZRANGE", "ZRANGEBYLEX", "ZRANGEBYSCORE",
		"ZRANK", "ZREVRANGE", "ZREVRANGEBYLEX",
		"ZREVRANGEBYSCORE", "ZREVRANK", "ZSCORE",
		"ZDIFF", "ZINTER", "ZUNION",
		"STRLEN", "GETBIT", "GETRANGE", "SUBSTR",
		"BITCOUNT", "BITFIELD_RO", "BITPOS",
		"EXISTS", "TYPE", "SORT_RO", "LCS",
		"GEODIST", "GEOHASH", "GEOPOS", "GEOSEARCH",
		"GEORADIUSBYMEMBER_RO", "GEORADIUS_RO",
		"XLEN", "XRANGE", "XREVRANGE",
		"JSON.GET", "JSON.MGET", "JSON.ARRINDEX", "JSON.ARRLEN",
		"JSON.OBJKEYS", "JSON.OBJLEN", "JSON.RESP",
		"JSON.STRLEN", "JSON.TYPE",
		"TS.GET", "TS.INFO", "TS.RANGE", "TS.REVRANGE",
	}
	for _, name := range allowed {
		// Use lower-case name as first arg (matching how go-redis sends commands)
		cmd := makeCmd(name, "mykey")
		if !isCacheable(cmd) {
			t.Errorf("expected %q to be cacheable", name)
		}
	}
}

func TestIsCacheable_CaseInsensitive(t *testing.T) {
	for _, name := range []string{"get", "Get", "GET", "gEt"} {
		cmd := makeCmd(name, "k")
		if !isCacheable(cmd) {
			t.Errorf("expected %q to be cacheable (case-insensitive)", name)
		}
	}
}

func TestIsCacheable_WriteCommandsRejected(t *testing.T) {
	writes := []string{"SET", "DEL", "HSET", "LPUSH", "SADD", "ZADD", "EXPIRE", "FLUSHDB"}
	for _, name := range writes {
		cmd := makeCmd(name, "k")
		if isCacheable(cmd) {
			t.Errorf("expected %q to NOT be cacheable", name)
		}
	}
}

func TestIsCacheable_XReadRejected(t *testing.T) {
	// XREAD supports BLOCK and state-relative $/+ IDs, so it must not be cached.
	cmd := makeCmd("XREAD", "COUNT", "5", "STREAMS", "s", "0")
	if isCacheable(cmd) {
		t.Error("expected XREAD to NOT be cacheable")
	}
}

// TestExtractRedisKeys_WireFaithfulTypesOnly: the invalidation index must hold
// keys exactly as proto.Writer sends them, or the server's invalidation pushes
// never match and stale entries are served forever. Types whose fmt.Sprint
// rendering diverges from the wire form (pointers, bools, durations, floats...)
// must make extraction fail (want nil) so the command is served uncached.
func TestExtractRedisKeys_WireFaithfulTypesOnly(t *testing.T) {
	key := "real-key"
	cases := []struct {
		name string
		cmd  Cmder
		want []string
	}{
		{"string key", makeCmd("get", "k"), []string{"k"}},
		{"[]byte key", makeCmd("get", []byte("k")), []string{"k"}},
		{"int key", makeCmd("get", 123), []string{"123"}},
		{"uint64 key", makeCmd("get", uint64(7)), []string{"7"}},
		{"pointer key", makeCmd("get", &key), nil},
		{"bool key", makeCmd("get", true), nil},
		{"float key", makeCmd("get", 1.5), nil},
		// Multi-key commands: one divergent key poisons the whole extraction.
		{"mget with pointer key", makeCmd("mget", "a", &key, "b"), nil},
		{"mget with string keys", makeCmd("mget", "a", "b"), []string{"a", "b"}},
	}
	for _, tc := range cases {
		got := extractRedisKeys(tc.cmd)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				break
			}
		}
	}
}

func TestIsCacheable_XPendingRejected(t *testing.T) {
	// XPENDING's extended form returns wall-clock-relative idle times and its
	// IDLE filter is time-dependent, so it must not be cached.
	for _, cmd := range []Cmder{
		makeCmd("XPENDING", "s", "grp"),
		makeCmd("XPENDING", "s", "grp", "IDLE", "9000", "-", "+", "10"),
	} {
		if isCacheable(cmd) {
			t.Errorf("expected %v to NOT be cacheable", cmd.Args())
		}
	}
}

func TestIsCacheable_KeylessCommandRejected(t *testing.T) {
	// PING has no keys; even if someone added it to the allow-list it
	// should be rejected because cmdFirstKeyPos returns 0.
	cmd := makeCmd("ping")
	if isCacheable(cmd) {
		t.Error("expected keyless command PING to NOT be cacheable")
	}
}

func TestIsCacheable_RawWriteToRejected(t *testing.T) {
	cmd := NewRawWriteToCmd(context.Background(), &bytes.Buffer{}, "get", "k")
	if isCacheable(cmd) {
		t.Fatal("RawWriteToCmd must bypass CSC to preserve direct streaming")
	}
}

func TestIsSelectCmd(t *testing.T) {
	for _, cmd := range []Cmder{
		makeCmd("select", 1),
		makeCmd("SELECT", 1),
		makeCmd([]byte("select"), 1),
	} {
		if !isSelectCmd(cmd) {
			t.Errorf("expected %v to match SELECT", cmd.Args())
		}
	}
	for _, cmd := range []Cmder{
		makeCmd("get", "select"),
		makeCmd("swapdb", 0, 1),
	} {
		if isSelectCmd(cmd) {
			t.Errorf("expected %v not to match SELECT", cmd.Args())
		}
	}
}

func TestCSCStateCommandMatchers(t *testing.T) {
	for _, cmd := range []Cmder{
		makeCmd("auth", "password"),
		makeCmd("AUTH", "user", "password"),
		makeCmd([]byte("auth"), "password"),
	} {
		if !isAuthCmd(cmd) {
			t.Errorf("expected %v to match AUTH", cmd.Args())
		}
	}
	if isAuthCmd(makeCmd("get", "auth")) {
		t.Fatal("GET auth must not match AUTH")
	}

	for _, cmd := range []Cmder{
		makeCmd("hello", 2),
		makeCmd("HELLO", 3),
		makeCmd([]byte("hello"), []byte("2")),
	} {
		if !isProtocolChangingHelloCmd(cmd) {
			t.Errorf("expected %v to match state-changing HELLO", cmd.Args())
		}
	}
	for _, cmd := range []Cmder{
		makeCmd("hello"),
		makeCmd("get", "hello"),
	} {
		if isProtocolChangingHelloCmd(cmd) {
			t.Errorf("expected %v not to match state-changing HELLO", cmd.Args())
		}
	}

	for _, cmd := range []Cmder{
		makeCmd("reset"),
		makeCmd("RESET"),
		makeCmd([]byte("reset")),
	} {
		if !isResetCmd(cmd) {
			t.Errorf("expected %v to match RESET", cmd.Args())
		}
	}
	if isResetCmd(makeCmd("config", "resetstat")) {
		t.Fatal("CONFIG RESETSTAT must not match RESET")
	}
}

func TestIsCacheable_EmptyArgs(t *testing.T) {
	cmd := makeCmd()
	if isCacheable(cmd) {
		t.Error("expected empty command to NOT be cacheable")
	}
}

// --- buildCacheKey ---------------------------------------------------------

func TestBuildCacheKey_SimpleGet(t *testing.T) {
	cmd := makeCmd("GET", "foo")
	key, ok := buildCacheKey(cmd)
	if !ok || key == "" {
		t.Fatal("expected non-empty cache key")
	}
	// Same command must produce identical keys.
	if key2, _ := buildCacheKey(makeCmd("GET", "foo")); key != key2 {
		t.Errorf("identical commands produced different keys: %q vs %q", key, key2)
	}
}

func TestBuildCacheKey_DifferentArgsDiffer(t *testing.T) {
	k1, _ := buildCacheKey(makeCmd("GET", "foo"))
	k2, _ := buildCacheKey(makeCmd("GET", "bar"))
	if k1 == k2 {
		t.Error("different keys must produce different cache keys")
	}
}

func TestBuildCacheKey_CollisionSafety(t *testing.T) {
	// "a|b" as one arg vs "a" and "b" as two args must differ.
	k1, _ := buildCacheKey(makeCmd("GET", "a|b"))
	k2, _ := buildCacheKey(makeCmd("GET", "a", "b"))
	if k1 == k2 {
		t.Error("length-prefixing should prevent separator collision")
	}
}

func TestBuildCacheKey_BinaryData(t *testing.T) {
	cmd := makeCmd("GET", []byte{0x00, 0x01, 0xff})
	key, ok := buildCacheKey(cmd)
	if !ok || key == "" {
		t.Fatal("expected non-empty cache key for binary argument")
	}
}

func TestBuildCacheKey_MultiKey(t *testing.T) {
	k1, _ := buildCacheKey(makeCmd("MGET", "a", "b"))
	k2, _ := buildCacheKey(makeCmd("MGET", "a", "b", "c"))
	if k1 == k2 {
		t.Error("different arg counts must produce different cache keys")
	}
}

func TestBuildCacheKey_EmptyArgs(t *testing.T) {
	cmd := makeCmd()
	if key, ok := buildCacheKey(cmd); ok || key != "" {
		t.Errorf("expected empty cache key for no-args command, got %q (ok=%v)", key, ok)
	}
}

// --- extractRedisKeys ------------------------------------------------------

func TestExtractRedisKeys_SingleKey(t *testing.T) {
	cmd := makeCmd("GET", "mykey")
	keys := extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "mykey" {
		t.Errorf("expected [mykey], got %v", keys)
	}
}

func TestExtractRedisKeys_SingleKeyWithExtraArgs(t *testing.T) {
	// LRANGE has one key followed by start/stop — only the key should be extracted.
	cmd := makeCmd("LRANGE", "mylist", "0", "10")
	keys := extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "mylist" {
		t.Errorf("LRANGE: expected [mylist], got %v", keys)
	}

	// HGET has one key followed by a field name.
	cmd = makeCmd("HGET", "myhash", "field1")
	keys = extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "myhash" {
		t.Errorf("HGET: expected [myhash], got %v", keys)
	}

	// ZCOUNT has one key followed by min/max.
	cmd = makeCmd("ZCOUNT", "myset", "-inf", "+inf")
	keys = extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "myset" {
		t.Errorf("ZCOUNT: expected [myset], got %v", keys)
	}

	// GETRANGE has one key followed by start/end offsets.
	cmd = makeCmd("GETRANGE", "mystr", "0", "5")
	keys = extractRedisKeys(cmd)
	if len(keys) != 1 || keys[0] != "mystr" {
		t.Errorf("GETRANGE: expected [mystr], got %v", keys)
	}
}

func TestExtractRedisKeys_MultiKey(t *testing.T) {
	cmd := makeCmd("MGET", "a", "b", "c")
	keys := extractRedisKeys(cmd)
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}
	want := []string{"a", "b", "c"}
	for i, k := range keys {
		if k != want[i] {
			t.Errorf("key[%d] = %q, want %q", i, k, want[i])
		}
	}
}

func TestExtractRedisKeys_MultiKeyExists(t *testing.T) {
	cmd := makeCmd("EXISTS", "k1", "k2", "k3")
	keys := extractRedisKeys(cmd)
	if len(keys) != 3 {
		t.Fatalf("EXISTS: expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestExtractRedisKeys_NumKeysPattern(t *testing.T) {
	// ZDIFF numkeys key [key ...]
	cmd := makeCmd("ZDIFF", 2, "zs1", "zs2")
	cmd.(*Cmd).SetFirstKeyPos(2)
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "zs1" || keys[1] != "zs2" {
		t.Errorf("ZDIFF: expected [zs1 zs2], got %v", keys)
	}

	// SINTERCARD numkeys key [key ...] LIMIT limit
	cmd = makeCmd("SINTERCARD", 2, "s1", "s2", "LIMIT", 10)
	keys = extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "s1" || keys[1] != "s2" {
		t.Errorf("SINTERCARD: expected [s1 s2], got %v", keys)
	}
}

func TestExtractRedisKeys_LCS(t *testing.T) {
	cmd := makeCmd("LCS", "key1", "key2")
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "key1" || keys[1] != "key2" {
		t.Errorf("LCS: expected [key1 key2], got %v", keys)
	}
}

func TestExtractRedisKeys_JSONMGet(t *testing.T) {
	// JSON.MGET key [key ...] path
	cmd := makeCmd("JSON.MGET", "j1", "j2", "$.name")
	keys := extractRedisKeys(cmd)
	if len(keys) != 2 || keys[0] != "j1" || keys[1] != "j2" {
		t.Errorf("JSON.MGET: expected [j1 j2], got %v", keys)
	}
}

func TestExtractRedisKeys_KeylessCommand(t *testing.T) {
	cmd := makeCmd("ping")
	keys := extractRedisKeys(cmd)
	if keys != nil {
		t.Errorf("expected nil for keyless command, got %v", keys)
	}
}

func TestIsCacheable_SortRO_ByGetExcluded(t *testing.T) {
	// Plain SORT_RO reads only the sorted key: cacheable.
	if cmd := makeCmd("sort_ro", "mylist", "LIMIT", "0", "10", "ALPHA"); !isCacheable(cmd) {
		t.Error("plain SORT_RO should be cacheable")
	}
	// BY/GET forms read pattern-derived keys the reverse index cannot cover:
	// their invalidations would be dropped, serving stale results forever.
	if cmd := makeCmd("sort_ro", "mylist", "BY", "weight_*"); isCacheable(cmd) {
		t.Error("SORT_RO ... BY must not be cacheable")
	}
	if cmd := makeCmd("sort_ro", "mylist", "get", "obj_*"); isCacheable(cmd) {
		t.Error("SORT_RO ... GET must not be cacheable (case-insensitive)")
	}
	if cmd := makeCmd("sort_ro", "mylist", "LIMIT", "0", "10", "By", "weight_*", "ALPHA"); isCacheable(cmd) {
		t.Error("SORT_RO with BY among other options must not be cacheable")
	}
	by := "BY"
	if cmd := makeCmd("sort_ro", "mylist", &by, "weight_*"); isCacheable(cmd) {
		t.Error("SORT_RO with pointer-encoded BY must not be cacheable")
	}
}

type nonComparableCache struct {
	Cache
	marker []byte
}

type typedNilCache struct{ Cache }

type operationDurationRecorder struct {
	OTelRecorder
	calls    atomic.Int32
	attempts atomic.Int32
}

func (r *operationDurationRecorder) RecordOperationDuration(
	_ context.Context,
	_ time.Duration,
	_ Cmder,
	attempts int,
	_ error,
	_ ConnInfo,
	_ int,
) {
	r.calls.Add(1)
	r.attempts.Store(int32(attempts))
}

func testCSCNamespacedKey(db int, key string) string {
	return cscNamespacedKey(cscNamespacePrefix(db, ""), key)
}

type unusedStreamingProvider struct{}

func (unusedStreamingProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	panic("Subscribe must not be called without a connection")
}

func TestAttachCSC_EnabledForExplicitCache(t *testing.T) {
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: NewLocalCache(CacheConfig{MaxEntries: 16}),
	})
	defer client.Close()
	if client.csc == nil {
		t.Fatal("CSC must be enabled for an owner-aware cache")
	}
}

func TestAttachCSC_DisablesForTypedNilCache(t *testing.T) {
	var cache *typedNilCache
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: cache,
	})
	defer client.Close()

	if client.csc != nil || client.cscTrackingRequested() {
		t.Fatal("a typed-nil cache must leave CSC disabled")
	}
}

func TestAttachCSC_DisabledForCredentialProviders(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Options)
	}{
		{
			name: "streaming",
			configure: func(opt *Options) {
				opt.StreamingCredentialsProvider = unusedStreamingProvider{}
			},
		},
		{
			name: "context",
			configure: func(opt *Options) {
				opt.CredentialsProviderContext = func(context.Context) (string, string, error) {
					panic("provider must not be called without a connection")
				}
			},
		},
		{
			name: "legacy",
			configure: func(opt *Options) {
				opt.CredentialsProvider = func() (string, string) {
					panic("provider must not be called without a connection")
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opt := &Options{
				Addr:                  "127.0.0.1:0", // never dialed
				Protocol:              3,
				ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
			}
			tc.configure(opt)
			client := NewClient(opt)
			t.Cleanup(func() { _ = client.Close() })

			if client.csc != nil || client.cscActive != nil {
				t.Fatal("CSC must stay detached when credentials can vary by identity")
			}
			if client.cscTrackingRequested() {
				t.Fatal("dynamic credentials must not enable CLIENT TRACKING for CSC")
			}
		})
	}
}

func TestAttachCSC_AllowsFixedCredentials(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0", // never dialed
		Protocol:              3,
		Username:              "fixed-user",
		Password:              "fixed-password",
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	if client.csc == nil || client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("fixed Username/Password must remain compatible with CSC")
	}
}

func TestSharedCacheSeparatesFixedCredentialIdentities(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	clientA := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		Username:        "privileged",
		Password:        "secret-a",
		ClientSideCache: cache,
	})
	clientB := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		Username:        "restricted",
		Password:        "secret-b",
		ClientSideCache: cache,
	})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})

	if clientA.cscKeyPrefix == clientB.cscKeyPrefix {
		t.Fatal("different fixed ACL identities must not share a cache namespace")
	}
	if strings.Contains(clientA.cscKeyPrefix, "secret-a") ||
		strings.Contains(clientB.cscKeyPrefix, "secret-b") {
		t.Fatal("cache namespaces must not retain plaintext passwords")
	}

	redisKeyA := cscNamespacedKey(clientA.cscKeyPrefix, "secret")
	redisKeyB := cscNamespacedKey(clientB.cscKeyPrefix, "secret")
	cacheKeyA := cscNamespacedKey(clientA.cscKeyPrefix, "get-secret")
	cacheKeyB := cscNamespacedKey(clientB.cscKeyPrefix, "get-secret")
	if !cache.set(cacheKeyA, []string{redisKeyA}, []byte("a")) ||
		!cache.set(cacheKeyB, []string{redisKeyB}, []byte("b")) {
		t.Fatal("failed to seed identity-scoped cache entries")
	}

	handlerA := lookupInvalidateHandler(clientA.pushProcessor)
	if handlerA == nil {
		t.Fatal("client A invalidate handler is missing")
	}
	if err := handlerA.HandlePushNotification(
		context.Background(),
		push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, []interface{}{"secret"}},
	); err != nil {
		t.Fatalf("handle identity-scoped invalidation: %v", err)
	}
	if _, ok := cache.Get(context.Background(), cacheKeyA); ok {
		t.Fatal("client A invalidation did not delete its identity-scoped entry")
	}
	if value, ok := cache.Get(context.Background(), cacheKeyB); !ok || string(value) != "b" {
		t.Fatal("client A invalidation crossed into client B's identity namespace")
	}
}

// TestInvalBatchWindowRebuildOnChange pins #3965: a running batcher's window is
// fixed at creation, so when a second client binds to the same shared handler
// with a different window, setInvalBatchWindow must drop the running batcher so
// the next invalidation starts a fresh one with the new (e.g. stricter) window.
func TestInvalBatchWindowRebuildOnChange(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })

	h.setInvalBatchWindow(time.Second)
	b1 := h.ensureBatcher()
	if b1 == nil || b1.window != time.Second {
		t.Fatalf("first batcher window = %v, want 1s", b1)
	}

	// Stricter window from a second binding: the running batcher must be dropped.
	h.setInvalBatchWindow(10 * time.Millisecond)
	h.mu.Lock()
	running := h.batcher
	h.mu.Unlock()
	if running != nil {
		t.Fatal("batcher not dropped on window change — the stale window would be kept")
	}
	b2 := h.ensureBatcher()
	if b2 == nil || b2.window != 10*time.Millisecond {
		t.Fatalf("rebuilt batcher window = %v, want 10ms", b2)
	}
	if b2 == b1 {
		t.Fatal("expected a fresh batcher after the window change")
	}

	// Same window again must not churn the batcher.
	h.setInvalBatchWindow(10 * time.Millisecond)
	h.mu.Lock()
	same := h.batcher
	h.mu.Unlock()
	if same != b2 {
		t.Fatal("setInvalBatchWindow with an unchanged window must not rebuild the batcher")
	}

	// LOOSER window from a later binding must NOT apply: the effective window is
	// the strictest across attached clients, or the 10ms client's staleness bound
	// would be silently violated by a 1s attach.
	h.setInvalBatchWindow(time.Second)
	h.mu.Lock()
	kept, keptWindow := h.batcher, h.invalBatchWindow
	h.mu.Unlock()
	if kept != b2 || keptWindow != 10*time.Millisecond {
		t.Fatalf("a looser window applied over a stricter one: batcher rebuilt=%v window=%v, want kept 10ms", kept != b2, keptWindow)
	}

	// Explicit 0 (batching off, inline deletes) is strictest of all and must win.
	h.setInvalBatchWindow(0)
	h.mu.Lock()
	zeroWindow, zeroBatcher := h.invalBatchWindow, h.batcher
	h.mu.Unlock()
	if zeroWindow != 0 || zeroBatcher != nil {
		t.Fatalf("explicit 0 window must win (inline) and drop the batcher: window=%v batcher=%v", zeroWindow, zeroBatcher)
	}
	// ...and a later nonzero window must not loosen past it.
	h.setInvalBatchWindow(time.Second)
	h.mu.Lock()
	afterZero := h.invalBatchWindow
	h.mu.Unlock()
	if afterZero != 0 {
		t.Fatalf("nonzero window applied over an explicit 0: window=%v, want 0 (inline stays strictest)", afterZero)
	}
}

// TestInvalBatchStopAppliesQueuedDeletes pins #3965 (cursor High): stopping the
// batcher — as a window-change rebuild does — must apply the deletes still
// buffered in its channel, not just the in-progress batch, or the cache serves
// pre-invalidation values until TTL/MaxStaleness.
func TestInvalBatchStopAppliesQueuedDeletes(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour) // never fires on its own in this test

	nsKey := cscNamespacedKey("p:", "sq")
	cacheKey := cscNamespacedKey("p:", "get:sq")
	if !cache.set(cacheKey, []string{nsKey}, []byte("stale")) {
		t.Fatal("seed")
	}
	// Queue the delete on the batcher (1h window: buffered, not applied).
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, []interface{}{"sq"}}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if _, ok := cache.Get(ctx, cacheKey); !ok {
		t.Fatal("delete applied before the window elapsed — batching not in effect, test proves nothing")
	}

	// Tighten the window: the rebuild stops the 1h batcher, whose stop path must
	// drain + apply the queued delete.
	h.setInvalBatchWindow(10 * time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := cache.Get(ctx, cacheKey); !ok {
			break // delete applied
		}
		if time.Now().After(deadline) {
			t.Fatal("queued delete lost by the batcher stop/rebuild — entry still served")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestInvalBatchEnqueueAfterStopAppliesInline pins #3965 (cursor): a handler
// goroutine can hold a batcher pointer across a concurrent stop (window-change
// rebuild mid-enqueue-loop). enqueue on a stopped batcher must apply the delete
// inline — a key parked in a channel nothing drains would serve
// pre-invalidation values until TTL/MaxStaleness.
func TestInvalBatchEnqueueAfterStopAppliesInline(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour)
	b := h.ensureBatcher()
	if b == nil {
		t.Fatal("no batcher")
	}

	nsKey := cscNamespacedKey("p:", "as")
	cacheKey := cscNamespacedKey("p:", "get:as")
	if !cache.set(cacheKey, []string{nsKey}, []byte("stale")) {
		t.Fatal("seed")
	}

	b.stop()
	b.enqueue(nsKey) // stopped: must delete inline, not park in b.ch
	if _, ok := cache.Get(ctx, cacheKey); ok {
		t.Fatal("enqueue on a stopped batcher parked the delete — entry still served")
	}
}

// TestInvalBatchDroppedOnFlush pins #3965: a full cache Flush (FLUSHDB/FLUSHALL)
// must discard the batcher's queued per-key deletes, or a delete queued before
// the flush fires afterward and evicts an entry repopulated post-flush.
func TestInvalBatchDroppedOnFlush(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(80 * time.Millisecond) // batched; long enough not to fire before the flush

	nsKey := cscNamespacedKey("p:", "x")
	cacheKey := cscNamespacedKey("p:", "get:x")
	if !cache.set(cacheKey, []string{nsKey}, []byte("v1")) {
		t.Fatal("seed")
	}

	// Batched invalidation for x: queued, not yet applied (window not elapsed).
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, []interface{}{"x"}}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	// Full flush: clears the cache AND must drop the queued delete for x.
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, nil}); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Repopulate x after the flush, then wait past the window: the dropped delete
	// must NOT fire and evict the fresh entry.
	if !cache.set(cacheKey, []string{nsKey}, []byte("v2")) {
		t.Fatal("repopulate")
	}
	time.Sleep(200 * time.Millisecond)
	if v, ok := cache.Get(ctx, cacheKey); !ok || string(v) != "v2" {
		t.Fatalf("repopulated entry evicted by a stale batched delete after flush: got %q ok=%v, want v2", v, ok)
	}
}

func TestAttachCSC_HandlerConflictDoesNotEnableTracking(t *testing.T) {
	proc := push.NewProcessor()
	if err := proc.RegisterHandler(invalidatePushName, &recordingHandler{}, true); err != nil {
		t.Fatalf("register foreign invalidate handler: %v", err)
	}

	client := NewClient(&Options{
		Addr:                      "127.0.0.1:0",
		Protocol:                  3,
		PushNotificationProcessor: proc,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	if client.csc != nil || client.cscActive != nil {
		t.Fatal("a handler conflict must leave CSC fully detached")
	}
	if client.cscTrackingRequested() {
		t.Fatal("a configured cache whose attachment failed must not enable tracking")
	}
	if err := client.cscCommandError(NewCmd(context.Background(), "select", 1)); err != nil {
		t.Fatalf("a configured cache whose attachment failed rejected SELECT: %v", err)
	}
}

// TestFulfillCached_FailsClosedOnZeroConnID: with an active eviction hook a real
// serving conn id is an invariant; a zero id would leave the entry unattributed
// and never evicted on close, so fulfillCached must fail closed (not cache it).
func TestFulfillCached_FailsClosedOnZeroConnID(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	if c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v")}) {
		t.Fatal("fulfillCached must fail closed when an eviction hook is active and connID==0")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("unattributed entry must not be cached")
	}
}

func TestProcessCached_HitHonorsCanceledContext(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	c := &baseClient{
		opt:          &Options{Protocol: 3},
		csc:          cache,
		cscKeyPrefix: cscNamespacePrefix(0, ""),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := NewStringCmd(ctx, "get", "k")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed")
	}
	cacheKey := testCSCNamespacedKey(0, rawKey)
	if !cache.set(cacheKey, []string{testCSCNamespacedKey(0, "k")}, []byte("$1\r\nv\r\n")) {
		t.Fatal("failed to seed cache")
	}
	cancel()

	if err := c.processCached(ctx, cmd, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached hit with canceled context: got %v, want context.Canceled", err)
	}
}

func TestProcessCached_NilHitIsTerminal(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	c := &baseClient{
		opt:          &Options{Protocol: 3},
		csc:          cache,
		cscKeyPrefix: cscNamespacePrefix(0, ""),
	}

	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "missing")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed")
	}
	cacheKey := testCSCNamespacedKey(0, rawKey)
	if !cache.set(cacheKey, []string{testCSCNamespacedKey(0, "missing")}, []byte("$-1\r\n")) {
		t.Fatal("failed to seed negative cache entry")
	}

	if err := c.processCached(ctx, cmd, nil); err != Nil {
		t.Fatalf("negative cache hit: got %v, want redis.Nil", err)
	}
	if cache.Len() != 1 {
		t.Fatal("a valid redis.Nil cache hit must not be deleted")
	}
}

func TestProcessCached_RecordsCacheHitDuration(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: cache,
	})
	t.Cleanup(func() {
		SetOTelRecorder(nil)
		_ = client.Close()
	})

	cmd := NewStringCmd(context.Background(), "get", "key")
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		t.Fatal("buildCacheKey failed")
	}
	cacheKey := cscNamespacedKey(client.cscKeyPrefix, rawKey)
	if !cache.set(cacheKey, []string{cscNamespacedKey(client.cscKeyPrefix, "key")},
		[]byte("$5\r\nvalue\r\n")) {
		t.Fatal("failed to seed cache")
	}

	recorder := &operationDurationRecorder{}
	SetOTelRecorder(recorder)
	if got, err := client.Get(context.Background(), "key").Result(); err != nil || got != "value" {
		t.Fatalf("cached GET: value=%q err=%v", got, err)
	}
	if got := recorder.calls.Load(); got != 1 {
		t.Fatalf("operation duration calls: got %d, want 1", got)
	}
	if got := recorder.attempts.Load(); got != 0 {
		t.Fatalf("cache hit attempts: got %d, want 0", got)
	}
}

// TestCSCActive_ClonesStopServingWhenDrainerStops: a WithTimeout clone shares the
// owner's cscActive flag; stopping the owner's drainer (Close, or the GC cleanup)
// flips it, so the clone stops serving hits nothing is invalidating.
func TestCSCActive_ClonesStopServingWhenDrainerStops(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0",
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	defer client.Close()
	if client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("precondition: cscActive must be set true when CSC is enabled")
	}

	clone := client.WithTimeout(time.Second)
	if clone.cscActive != client.cscActive {
		t.Fatal("WithTimeout clone must share the owner's cscActive flag")
	}

	client.baseClient.stopBackgroundDrainer()
	if clone.cscActive.Load() {
		t.Fatal("clone must observe cscActive=false once the owner's drainer stops")
	}
}

func TestStopBackgroundDrainerEvictsSharedCacheCoverage(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0",
		Protocol:        3,
		ClientSideCache: cache,
	})
	t.Cleanup(func() { _ = client.Close() })

	hook := client.cscHook()
	if hook == nil {
		t.Fatal("precondition: shared-cache client must install its coverage hook")
	}
	const connID = uint64(44)
	hook.bumpInitGen(connID)
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, connID, []byte("v")) {
		t.Fatal("failed to seed shared cache entry")
	}

	client.stopBackgroundDrainer()
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("stopping one client's drainer must evict that pool's shared-cache entries")
	}
}

// TestCSCActive_CloneKeepsOwnerAlive: a surviving WithTimeout clone retains the
// canonical wrapper whose GC cleanup owns the shared drainer.
func TestCSCActive_CloneKeepsOwnerAlive(t *testing.T) {
	clone, active := func() (*Client, *atomic.Bool) {
		owner := NewClient(&Options{
			Addr:                  "127.0.0.1:0",
			Protocol:              3,
			ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		})
		cl := owner.WithTimeout(time.Second)
		// owner falls out of lexical scope here, but cl must keep it reachable
		// because its cleanup owns the drainer cl relies on.
		return cl, owner.cscActive
	}()

	if active == nil {
		t.Fatal("precondition: cscActive must be set")
	}
	for range 20 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if !active.Load() {
		t.Fatal("a reachable clone must keep its CSC drainer owner alive")
	}
	if clone.cscLifecycleOwner == nil {
		t.Fatal("CSC clone must retain its canonical lifecycle owner")
	}
	if err := clone.Close(); err != nil {
		t.Fatalf("close clone: %v", err)
	}
	if active.Load() {
		t.Fatal("closing a CSC clone must stop its canonical owner's drainer")
	}
}

// TestReadBufferSize_ClampedForRESP3: a read buffer too small to hold a push
// header is clamped for RESP3 so client-reserved Pub/Sub frames are never
// consumed before their name is known.
func TestReadBufferSize_ClampedForRESP3(t *testing.T) {
	opt := &Options{Addr: "x:1", Protocol: 3, ReadBufferSize: 16}
	opt.init()
	if opt.ReadBufferSize != proto.MinRESP3ReadBufferSize {
		t.Fatalf("RESP3 ReadBufferSize should clamp to %d, got %d",
			proto.MinRESP3ReadBufferSize, opt.ReadBufferSize)
	}
}

// TestReadBufferSize_NotClampedForRESP2: RESP2 has no push frames, so a small
// buffer is left as configured.
func TestReadBufferSize_NotClampedForRESP2(t *testing.T) {
	opt := &Options{Addr: "x:1", Protocol: 2, ReadBufferSize: 16}
	opt.init()
	if opt.ReadBufferSize != 16 {
		t.Fatalf("RESP2 ReadBufferSize should not be clamped, got %d", opt.ReadBufferSize)
	}
}

// TestProcessCached_CachesServerNilReply exercises the complete miss/fill/hit
// path. The second GET must be answered locally even though the cached command
// still returns redis.Nil to its caller.
func TestProcessCached_CachesServerNilReply(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var getCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveNegativeCacheTestConn(netConn, &getCalls)
		}
	}()

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCache: cache,
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if err := client.Get(context.Background(), "missing").Err(); err != Nil {
			t.Fatalf("GET %d: got %v, want redis.Nil", i+1, err)
		}
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("server received %d GETs, want 1 (second lookup should hit CSC)", got)
	}
	if cache.Len() != 1 {
		t.Fatalf("negative lookup was not retained in CSC, Len=%d", cache.Len())
	}
}

func serveNegativeCacheTestConn(netConn net.Conn, getCalls *atomic.Int32) {
	serveTestRESPConn(netConn, func(command string) string {
		switch command {
		case "hello":
			return "%0\r\n"
		case "get":
			getCalls.Add(1)
			return "$-1\r\n"
		default:
			return "+OK\r\n"
		}
	})
}

func serveTestRESPConn(netConn net.Conn, replyFor func(command string) string) {
	defer netConn.Close()

	scanner := bufio.NewScanner(netConn)
	for scanner.Scan() {
		header := scanner.Text()
		if !strings.HasPrefix(header, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
		if err != nil || n <= 0 {
			return
		}

		command := ""
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
				return
			}
			if i == 0 {
				command = strings.ToLower(scanner.Text())
			}
		}

		if _, err := netConn.Write([]byte(replyFor(command))); err != nil {
			return
		}
	}
}

// TestIsClientTrackingCmd pins the guard's matcher: any CLIENT TRACKING
// subcommand matches, other CLIENT subcommands (incl. TRACKINGINFO) do not.
func TestIsClientTrackingCmd(t *testing.T) {
	tracking := "tracking"
	matching := []Cmder{
		makeCmd("client", "tracking", "on"),
		makeCmd("client", "tracking", "off"),
		makeCmd("CLIENT", "TRACKING", "on", "bcast"),
		makeCmd("Client", "Tracking"),
		makeCmd([]byte("client"), []byte("tracking"), "off"), // raw []byte args
		makeCmd("client", &tracking, "off"),                  // proto.Writer dereferences *string
	}
	for _, cmd := range matching {
		if !isClientTrackingCmd(cmd) {
			t.Errorf("expected %v to match CLIENT TRACKING", cmd.Args())
		}
	}
	nonMatching := []Cmder{
		makeCmd("client", "trackinginfo"),
		makeCmd("client", "info"),
		makeCmd("client", "kill", "id", "1"),
		makeCmd("get", "tracking"),
		makeCmd("client"),
	}
	for _, cmd := range nonMatching {
		if isClientTrackingCmd(cmd) {
			t.Errorf("expected %v NOT to match CLIENT TRACKING", cmd.Args())
		}
	}
}

// TestClientTrackingRejectedWithCSC: on a client with the built-in cache
// configured, CLIENT TRACKING must be rejected before it reaches a connection —
// it would flip an arbitrary pool conn's tracking state and leave it filling
// the cache with entries the server never invalidates. The guard fires without
// dialing, so no server is needed.
func TestClientTrackingRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.ClientTrackingOff(ctx).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}
	if err := c.ClientTrackingOn(ctx, nil).Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
	// The raw escape hatch is caught too: the guard matches leading args.
	// (Non-tracking CLIENT subcommands are covered by TestIsClientTrackingCmd's
	// non-matching cases — probing one here would dial for seconds.)
	if err := c.Do(ctx, "client", "tracking", "off").Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("raw Do(client tracking off) must be rejected with CSC enabled, got %v", err)
	}
	tracking := "tracking"
	if err := c.Do(ctx, "client", &tracking, "off").Err(); !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("pointer-encoded CLIENT TRACKING must be rejected with CSC enabled, got %v", err)
	}
}

// TestClientTrackingRejectedWithCSC_Pipeline: pipelines bypass process(), so
// generalProcessPipeline mirrors the guard — a CLIENT TRACKING frame inside a
// Pipeline or TxPipeline must be rejected on a CSC client too.
func TestClientTrackingRejectedWithCSC_Pipeline(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOff(ctx)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("Pipelined ClientTrackingOff must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOn(ctx, nil)
		return nil
	})
	if !errors.Is(err, errClientTrackingWithCSC) {
		t.Fatalf("TxPipelined ClientTrackingOn must be rejected with CSC enabled, got %v", err)
	}
}

func TestCSCDisablesWhenHELLO3FallsBackToRESP2(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var getCalls atomic.Int32
	var trackingCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "-ERR unknown command 'hello'\r\n"
				case "get":
					getCalls.Add(1)
					return "$-1\r\n"
				case "client":
					trackingCalls.Add(1)
					return "+OK\r\n"
				default:
					return "+OK\r\n"
				}
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if err := client.Get(context.Background(), "missing").Err(); err != Nil {
			t.Fatalf("GET %d: got %v, want redis.Nil", i+1, err)
		}
	}
	if client.cscActive == nil || client.cscActive.Load() {
		t.Fatal("CSC must be disabled after HELLO 3 falls back to RESP2")
	}
	if got := trackingCalls.Load(); got != 0 {
		t.Fatalf("server received %d CLIENT TRACKING commands after RESP2 fallback, want 0", got)
	}
	if got := getCalls.Load(); got != 2 {
		t.Fatalf("server received %d GETs, want 2 (RESP2 fallback must bypass CSC)", got)
	}
}

func TestCSCDisablesWhenClientTrackingIsRejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var acceptCalls atomic.Int32
	var getCalls atomic.Int32
	var trackingCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			acceptCalls.Add(1)
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "%0\r\n"
				case "client":
					trackingCalls.Add(1)
					return "-ERR client tracking is disabled\r\n"
				case "get":
					getCalls.Add(1)
					return "$1\r\nv\r\n"
				default:
					return "+OK\r\n"
				}
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	for i := 0; i < 2; i++ {
		if got, err := client.Get(context.Background(), "key").Result(); err != nil || got != "v" {
			t.Fatalf("GET %d: got value %q, error %v; want value %q", i+1, got, err, "v")
		}
	}
	if client.cscActive == nil || client.cscActive.Load() {
		t.Fatal("CSC must be disabled after CLIENT TRACKING is rejected")
	}
	if got := trackingCalls.Load(); got != 1 {
		t.Fatalf("server received %d CLIENT TRACKING commands, want 1", got)
	}
	if got := getCalls.Load(); got != 2 {
		t.Fatalf("server received %d GETs, want 2 (tracking rejection must bypass CSC)", got)
	}
	if got := acceptCalls.Load(); got != 1 {
		t.Fatalf("server accepted %d connections, want 1 (tracking rejection must not discard a usable connection)", got)
	}
}

func TestCSCDoesNotEnableTrackingOnPubSubConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var trackingCalls atomic.Int32
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "%0\r\n"
				case "client":
					trackingCalls.Add(1)
				}
				return "+OK\r\n"
			})
		}
	}()

	client := NewClient(&Options{
		Addr:            ln.Addr().String(),
		Protocol:        3,
		MaxRetries:      -1,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = client.Close() })

	pubsub := client.pubSub()
	cn, err := pubsub.newConn(context.Background(), ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("create Pub/Sub connection: %v", err)
	}
	t.Cleanup(func() {
		client.pubSubPool.UntrackConn(cn)
		_ = cn.Close()
	})
	if !cn.IsPubSub() {
		t.Fatal("test connection is not marked as Pub/Sub")
	}
	if got := trackingCalls.Load(); got != 0 {
		t.Fatalf("Pub/Sub initialization sent %d CLIENT commands, want 0", got)
	}
}

func TestSelectRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: the guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Do(ctx, "select", 1).Err(); !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("raw SELECT must be rejected with CSC enabled, got %v", err)
	}

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "select", 1)
		return nil
	})
	if !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("pipelined SELECT must be rejected with CSC enabled, got %v", err)
	}
}

func TestConnectionStateCommandsRejectedWithCSC(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                  "localhost:1", // never dialed: every guard fires first
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c.Close() })

	tests := []struct {
		name string
		args []interface{}
		want error
	}{
		{"AUTH", []interface{}{"auth", "password"}, errAuthWithCSC},
		{"HELLO 2", []interface{}{"hello", 2}, errHelloWithCSC},
		{"RESET", []interface{}{"reset"}, errResetWithCSC},
		{"SUBSCRIBE", []interface{}{"subscribe", "channel"}, errSubscribeWithCSC},
		{"PSUBSCRIBE", []interface{}{"psubscribe", "channel:*"}, errSubscribeWithCSC},
		{"SSUBSCRIBE", []interface{}{"ssubscribe", "channel"}, errSubscribeWithCSC},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.Do(ctx, tc.args...).Err(); !errors.Is(err, tc.want) {
				t.Fatalf("%v must be rejected with CSC enabled, got %v", tc.args, err)
			}
		})
	}

	_, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "hello", 2)
		return nil
	})
	if !errors.Is(err, errHelloWithCSC) {
		t.Fatalf("pipelined HELLO 2 must be rejected with CSC enabled, got %v", err)
	}

	_, err = c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Do(ctx, "subscribe", "channel")
		return nil
	})
	if !errors.Is(err, errSubscribeWithCSC) {
		t.Fatalf("pipelined SUBSCRIBE must be rejected with CSC enabled, got %v", err)
	}

	// Bare HELLO only reports connection properties and does not change
	// protocol, authentication, or tracking state.
	if err := c.cscCommandError(makeCmd("hello")); err != nil {
		t.Fatalf("bare HELLO must remain allowed, got %v", err)
	}
}

// TestOnConnectUsesCSCStateGuard verifies that initConn's exemption ends after
// the library's own CLIENT TRACKING command. OnConnect is user code and must
// not be able to mutate a tracked pool connection.
func TestOnConnectUsesCSCStateGuard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(netConn net.Conn) {
				defer netConn.Close()
				scanner := bufio.NewScanner(netConn)
				command := 0
				for scanner.Scan() {
					if !strings.HasPrefix(scanner.Text(), "*") {
						continue
					}
					command++
					if command == 1 {
						// HELLO 3 returns an empty RESP3 map.
						_, _ = netConn.Write([]byte("%0\r\n"))
					} else {
						_, _ = netConn.Write([]byte("+OK\r\n"))
					}
				}
			}(netConn)
		}
	}()

	c := NewClient(&Options{
		Addr:                  ln.Addr().String(),
		Protocol:              3,
		MaxRetries:            -1,
		DisableIdentity:       true,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		OnConnect: func(ctx context.Context, cn *Conn) error {
			return cn.Select(ctx, 1).Err()
		},
	})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Ping(context.Background()).Err(); !errors.Is(err, errSelectWithCSC) {
		t.Fatalf("OnConnect SELECT must be rejected after init's exemption ends, got %v", err)
	}
}

// TestClientTrackingAllowedWithoutCSC: without the built-in cache the guard
// predicate is off entirely (asserted directly — a live dial would prove
// nothing more and costs seconds against an unreachable address).
func TestClientTrackingAllowedWithoutCSC(t *testing.T) {
	c := NewClient(&Options{Addr: "localhost:1", Protocol: 3})
	t.Cleanup(func() { _ = c.Close() })

	if err := c.baseClient.cscCommandError(
		NewCmd(context.Background(), "client", "tracking", "on"),
	); err != nil {
		t.Fatalf("a client without CSC rejected CLIENT TRACKING: %v", err)
	}
}

type recordingCache struct {
	Cache
	owner        Cache
	fulfillCalls int
}

func (c *recordingCache) FulfillOwned(
	cacheKey string,
	token, ownerConnID uint64,
	value []byte,
) bool {
	c.fulfillCalls++
	return c.owner.FulfillOwned(cacheKey, token, ownerConnID, value)
}

func (c *recordingCache) EvictByConn(connID uint64) int {
	return c.owner.EvictByConn(connID)
}

func TestLocalCache_FulfillOwned_EvictByConn(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	var owner Cache = cache

	// Two entries owned by conn 1, one by conn 2.
	for _, kv := range []struct {
		key    string
		connID uint64
	}{{"get:a", 1}, {"get:b", 1}, {"get:c", 2}} {
		tok, sf := cache.Reserve(kv.key, []string{kv.key})
		if !sf {
			t.Fatalf("Reserve(%s) should fetch", kv.key)
		}
		if !owner.FulfillOwned(kv.key, tok, kv.connID, []byte("v")) {
			t.Fatalf("FulfillOwned(%s) failed", kv.key)
		}
	}

	if n := owner.EvictByConn(1); n != 2 {
		t.Fatalf("EvictByConn(1) removed %d, want 2", n)
	}
	if _, ok := cache.Get(context.Background(), "get:a"); ok {
		t.Fatal("conn-1 entry a should be evicted")
	}
	if _, ok := cache.Get(context.Background(), "get:b"); ok {
		t.Fatal("conn-1 entry b should be evicted")
	}
	if _, ok := cache.Get(context.Background(), "get:c"); !ok {
		t.Fatal("conn-2 entry c must survive")
	}
	// Idempotent: evicting again removes nothing.
	if n := owner.EvictByConn(1); n != 0 {
		t.Fatalf("second EvictByConn(1) removed %d, want 0", n)
	}
}

func TestLocalCache_OwnerIndexCleanedOnInvalidation(t *testing.T) {
	// The owning-conn index must be cleaned when an entry is removed for other
	// reasons (invalidation, LRU) so a later EvictByConn can't touch a re-used
	// cache key and the index cannot leak.
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := Cache(cache)
	lc := cache

	tok, _ := cache.Reserve("get:k", []string{"rk"})
	if !owner.FulfillOwned("get:k", tok, 7, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	// Invalidate via the redis key; the owner index for conn 7 must be gone.
	if n := cache.DeleteByRedisKey("rk"); n != 1 {
		t.Fatalf("DeleteByRedisKey removed %d, want 1", n)
	}
	shard := lc.shardFor("get:k")
	shard.mu.RLock()
	_, present := shard.byConnID[7]
	shard.mu.RUnlock()
	if present {
		t.Fatal("byConnID must be cleaned when the entry is invalidated")
	}
	if n := owner.EvictByConn(7); n != 0 {
		t.Fatalf("EvictByConn(7) after invalidation removed %d, want 0", n)
	}
}

// TestCSCEvictOnRemoveHook: the pool OnRemove hook must evict exactly the
// removed connection's owned entries and leave others intact.
func TestCSCEvictOnRemoveHook(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := Cache(cache)

	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()
	id := cn.GetID()

	tok, _ := cache.Reserve("get:owned", []string{"owned"})
	if !owner.FulfillOwned("get:owned", tok, id, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	tok2, _ := cache.Reserve("get:other", []string{"other"})
	if !owner.FulfillOwned("get:other", tok2, id+1000, []byte("v")) {
		t.Fatal("FulfillOwned (other conn) failed")
	}

	hook := &cscEvictOnRemoveHook{evictor: owner}
	hook.OnRemove(context.Background(), cn, nil)

	if _, ok := cache.Get(context.Background(), "get:owned"); ok {
		t.Fatal("removed conn's entry must be evicted by OnRemove")
	}
	if _, ok := cache.Get(context.Background(), "get:other"); !ok {
		t.Fatal("another conn's entry must survive OnRemove")
	}
}

// TestCSCConnCloseHook_EvictsOnAnyClose: the per-conn onCscClose hook installed
// by initConn must evict a conn's owned entries when the conn is closed for ANY
// reason — including ConnMaxLifetime / idle-timeout retirement via
// ConnPool.CloseConn, which bypasses the OnRemove pool hook. Without it the
// server drops the conn's tracking table on close while its cached entries
// linger uninvalidated (Window-2 staleness on normal connection retirement).
func TestCSCConnCloseHook_EvictsOnAnyClose(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner := Cache(cache)
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache}

	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	id := cn.GetID()

	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !owner.FulfillOwned("get:k", tok, id, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}
	if cache.Len() != 1 {
		t.Fatalf("setup: want 1 entry, got %d", cache.Len())
	}

	// Install the hook exactly as initConn does, then close the conn directly:
	// no pool OnRemove fires, modelling the CloseConn/ConnMaxLifetime path.
	c.cscInstallConnCloseHook(cn)
	if err := cn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("conn close must evict the conn's owned entries")
	}
}

// TestCSCEvictOwnedEntries_UsesSharedHookWhenCscNil: the handoff/reinit eviction
// path must reach the parent's shared cache through the carried eviction hook
// even when the client's own csc is nil (the Client.Conn / Tx shape), and must
// NOT record the recently-removed ring (the same conn keeps serving on the fresh
// socket, so post-handoff fulfills are legitimate).
func TestCSCEvictOwnedEntries_UsesSharedHookWhenCscNil(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	derived := &baseClient{opt: &Options{Protocol: 3}, csc: nil, cscPoolHook: hook}

	const connID = uint64(9)
	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", tok, connID, []byte("v")) {
		t.Fatal("FulfillOwned failed")
	}

	derived.cscEvictOwnedEntries(connID)
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("handoff eviction must evict via the shared hook when csc is nil")
	}
	// The conn keeps serving: fetches capturing the post-eviction generation
	// must remain valid (the eviction bumps, it does not tombstone).
	if got := hook.initGenOf(connID); got == 0 {
		t.Fatal("handoff must leave a live generation for the replacement socket")
	}
}

func TestCSCHookInvalidateAllCoverage(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	owner := Cache(cache)
	hook := &cscEvictOnRemoveHook{
		evictor: owner,
		initGen: make(map[uint64]uint64),
	}

	const (
		conn1 = uint64(11)
		conn2 = uint64(22)
	)
	hook.bumpInitGen(conn1)
	hook.bumpInitGen(conn2)
	oldGen := hook.initGenOf(conn1)

	for _, entry := range []struct {
		key    string
		connID uint64
	}{
		{"get:one", conn1},
		{"get:two", conn2},
	} {
		token, _ := cache.Reserve(entry.key, []string{entry.key})
		if !owner.FulfillOwned(entry.key, token, entry.connID, []byte("v")) {
			t.Fatalf("FulfillOwned(%q) failed", entry.key)
		}
	}

	lateToken, _ := cache.Reserve("get:late", []string{"late"})
	hook.invalidateAllCoverage()

	if cache.Len() != 1 {
		t.Fatalf("valid entries from stopped coverage must be evicted; got len=%d", cache.Len())
	}
	if hook.fulfillOwnedIfCovered("get:late", lateToken, conn1, oldGen, []byte("stale")) {
		t.Fatal("an in-flight fetch from revoked coverage must not publish")
	}
	cache.Cancel("get:late", lateToken)
}

func TestFulfillCached_RejectsInactiveCSC(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	const connID = uint64(33)
	hook.bumpInitGen(connID)

	active := &atomic.Bool{}
	active.Store(false)
	c := &baseClient{
		opt:         &Options{Protocol: 3},
		csc:         cache,
		cscPoolHook: hook,
		cscActive:   active,
	}
	token, _ := cache.Reserve("get:k", []string{"k"})
	if c.fulfillCached("get:k", token, &cscFetchCapture{
		raw:     []byte("$1\r\nv\r\n"),
		connID:  connID,
		initGen: hook.initGenOf(connID),
	}) {
		t.Fatal("a fetch must not publish after its CSC drainer stops")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("inactive CSC left a cached entry behind")
	}
}

// TestNewTx_CarriesSharedEvictionHook: a Tx must carry the parent's shared
// eviction hook (so close/reinit hooks installed on a Watch-initialized
// connection evict from the parent cache) but must not serve cached reads itself.
func TestNewTx_CarriesSharedEvictionHook(t *testing.T) {
	client := NewClient(&Options{
		Addr:                  "127.0.0.1:0", // never dialed in this unit test
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	defer client.Close()
	if client.cscPoolHook == nil {
		t.Fatal("precondition: parent must have a shared eviction hook")
	}

	tx := client.newTx()
	defer func() { _ = tx.Close(context.Background()) }()
	if tx.cscPoolHook != client.cscPoolHook {
		t.Fatal("newTx must carry the parent's shared eviction hook")
	}
	if tx.csc != nil {
		t.Fatal("Tx must not serve cached reads (csc must stay nil)")
	}
}

// TestCSCConnCloseHook_NoOrphanWhenCloseRacesFulfill: the close-hook path
// (cscOnConnClose, used by ConnPool.CloseConn / ConnMaxLifetime retirement) must
// record the recently-removed ring BEFORE evicting, so a fulfill that lands after
// the conn closed (reply released → conn closed → fulfillCached) does not leave an
// orphaned entry with no invalidation coverage.
func TestCSCConnCloseHook_NoOrphanWhenCloseRacesFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(7)
	// The conn served (first init bumped its generation, captured at reply
	// time), then closes before the entry exists (fulfill has not run yet).
	hook.bumpInitGen(connID)
	gen := hook.initGenOf(connID)
	c.cscOnConnClose(connID)

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	// Fulfill attributes to the just-closed conn; the generation guard must
	// drop it (the close deleted the conn's entry, so it reads 0 != gen).
	c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen})
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry owned by a conn closed before fulfill must not survive")
	}
}

// TestFulfillCached_RaceWithConnRemoval: if the owning connection is removed
// around the fulfill (its OnRemove eviction ran before the entry existed),
// fulfillCached must drop the orphaned entry rather than leave it resident with
// no invalidation coverage (the Window-2 TOCTOU).
func TestFulfillCached_RaceWithConnRemoval(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	// Model "conn 42 served, then was just removed" (no need for a real conn
	// whose GetID == 42): first init bumped its generation, the fetch captured
	// it, and the OnRemove eviction ran before the entry existed.
	hook.bumpInitGen(connID)
	gen := hook.initGenOf(connID)
	hook.markRemoved(connID)

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	if c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen}) {
		t.Fatal("coverage loss must reject fulfillment before publication")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry owned by a just-removed conn must not remain resident")
	}
}

// TestFulfillCached_CoverageLossSkipsPublication verifies the generation guard
// runs before FulfillOwned changes the placeholder to Valid. This matters to
// concurrent Get waiters: publishing and deleting immediately afterward still
// leaves a window in which a waiter can return an uncovered value.
func TestFulfillCached_CoverageLossSkipsPublication(t *testing.T) {
	base := NewLocalCache(CacheConfig{MaxEntries: 64})
	cache := &recordingCache{
		Cache: base,
		owner: base,
	}
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	hook.bumpInitGen(connID)
	gen := hook.initGenOf(connID)

	token, shouldFetch := cache.Reserve("get:k", []string{"k"})
	if !shouldFetch {
		t.Fatal("Reserve should fetch")
	}
	local := base
	shard := local.shardFor("get:k")
	shard.mu.RLock()
	waitCh := shard.entries["get:k"].waitCh
	shard.mu.RUnlock()

	// Removal wins before fulfillment. The cache method must never be called:
	// fulfillCached cancels the placeholder, waking waiters to observe a miss.
	hook.markRemoved(connID)
	if c.fulfillCached("get:k", token, &cscFetchCapture{
		raw:     []byte("v"),
		connID:  connID,
		initGen: gen,
	}) {
		t.Fatal("a reply without invalidation coverage must not be published")
	}
	if cache.fulfillCalls != 0 {
		t.Fatalf("FulfillOwned was called %d times after coverage loss; want 0", cache.fulfillCalls)
	}
	select {
	case <-waitCh:
	default:
		t.Fatal("coverage loss must cancel the placeholder and wake waiters")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("a waiter must observe a miss, never a transient uncovered value")
	}
}

// TestInitGenLifecycle pins the map bounding that the coverage guard relies
// on: first init bumps to >=1, removal (markRemoved) and failed init
// (forgetConn) delete the entry — so a served conn's captured generation
// always mismatches after its conn goes away, and the map stays bounded to
// live conns.
func TestInitGenLifecycle(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{evictor: cache}

	const connID = uint64(5)
	hook.bumpInitGen(connID)
	if got := hook.initGenOf(connID); got != 1 {
		t.Fatalf("first init must bump the generation to 1, got %d", got)
	}
	hook.markRemoved(connID)
	if got := hook.initGenOf(connID); got != 0 {
		t.Fatalf("markRemoved must delete the generation entry, got %d", got)
	}

	// Failed init: forgetConn drops the entry the failed init's bump created.
	hook.bumpInitGen(connID)
	hook.forgetConn(connID)
	if got := hook.initGenOf(connID); got != 0 {
		t.Fatalf("forgetConn must delete the generation entry, got %d", got)
	}
}

// TestFulfillCached_RaceWithHandoffReinit: a maintenance handoff replaces a
// conn's socket (and its server-side tracking) while the conn id keeps serving.
// If the re-init eviction runs between a fetch's reply (read on the OLD socket)
// and its fulfill, the published entry has no invalidation coverage — the
// init-generation check must drop it. The removed-ring cannot cover this: the
// conn was never removed.
func TestFulfillCached_RaceWithHandoffReinit(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}

	// Reply read on the pre-handoff socket: generation captured while the conn
	// was still held (what _process does).
	gen := c.cscConnInitGen(connID)

	// The handoff worker re-inits the conn after it was released but before
	// fulfillCached runs: bumps the generation, then evicts (a no-op here — the
	// entry does not exist yet; only the placeholder does).
	c.cscEvictOwnedEntries(connID)

	c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen})
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("entry fetched on a socket replaced before fulfill must not remain resident")
	}
}

func TestFulfillCached_HandoffBumpsCoverageBeforeSocketSwap(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	oldServer, oldClient := net.Pipe()
	defer oldServer.Close()
	defer oldClient.Close()
	newServer, newClient := net.Pipe()
	defer newServer.Close()
	defer newClient.Close()
	cn := pool.NewConn(oldClient)
	connID := cn.GetID()

	// Model the first successful tracked initialization and capture a reply
	// from that socket.
	hook.bumpInitGen(connID)
	c.cscInstallConnReinitHook(cn)
	token, _ := cache.Reserve("get:k", []string{"k"})
	oldGen := c.cscConnInitGen(connID)

	cn.SetInitConnFunc(func(context.Context, *pool.Conn) error {
		cn.GetStateMachine().Transition(pool.StateIdle)
		return nil
	})
	if err := cn.SetNetConnAndInitConn(context.Background(), newClient); err != nil {
		t.Fatalf("replace socket: %v", err)
	}
	if got := c.cscConnInitGen(connID); got == oldGen {
		t.Fatal("socket replacement did not change CSC coverage generation")
	}

	if c.fulfillCached("get:k", token, &cscFetchCapture{
		raw:     []byte("v"),
		connID:  connID,
		initGen: oldGen,
	}) {
		t.Fatal("an old-socket reply must be rejected after handoff")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("old-socket reply became visible after handoff")
	}
}

// TestFulfillCached_PostHandoffFetchIsCached: after a handoff, fetches served by
// the conn's NEW socket capture the post-bump generation and must be cached
// normally — the generation check only drops entries from the replaced socket.
func TestFulfillCached_PostHandoffFetchIsCached(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	hook := &cscEvictOnRemoveHook{evictor: cache}
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache, cscPoolHook: hook}

	const connID = uint64(42)
	// Handoff completed; the conn keeps serving on its new socket.
	c.cscEvictOwnedEntries(connID)

	tok, sf := cache.Reserve("get:k", []string{"k"})
	if !sf {
		t.Fatal("Reserve should fetch")
	}
	gen := c.cscConnInitGen(connID) // captured at reply time, post-bump

	if !c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: connID, initGen: gen}) {
		t.Fatal("post-handoff fetch on the new socket should be cached")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); !ok {
		t.Fatal("post-handoff entry must remain resident")
	}
}

// TestFulfillCached_NoHookUsesUnownedFulfill: without an evict-on-remove hook,
// fulfillCached publishes with ownerConnID zero and still caches the value.
func TestFulfillCached_NoHookUsesUnownedFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache} // cscPoolHook nil

	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: 7}) {
		t.Fatal("fulfillCached should store an unowned value when no hook is present")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); !ok {
		t.Fatal("value should be cached")
	}
}

// TestCSCStrategyValidation_ClampsUnknown: an out-of-range strategy must not
// thread the per-strategy gates into "tracking on, nothing draining".
func TestCSCStrategyValidation_ClampsUnknown(t *testing.T) {
	opt := &Options{ClientSideCacheStrategy: CSCStrategy(99)}
	opt.init()
	if opt.ClientSideCacheStrategy != CSCStrategySharedTracking {
		t.Fatalf("unknown strategy must clamp to SharedTracking, got %d", opt.ClientSideCacheStrategy)
	}
}

func TestCSCNamespaceUsesACLUsername(t *testing.T) {
	newClient := func(username, password string) *Client {
		return NewClient(&Options{
			Addr:                  "127.0.0.1:0",
			Protocol:              3,
			Username:              username,
			Password:              password,
			ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		})
	}

	oldPassword := newClient("alice", "old-secret")
	newPassword := newClient("alice", "new-secret")
	otherUser := newClient("bob", "new-secret")
	t.Cleanup(func() {
		_ = oldPassword.Close()
		_ = newPassword.Close()
		_ = otherUser.Close()
	})

	if oldPassword.cscKeyPrefix != newPassword.cscKeyPrefix {
		t.Fatal("password rotation changed the cache namespace for the same ACL user")
	}
	if oldPassword.cscKeyPrefix == otherUser.cscKeyPrefix {
		t.Fatal("different ACL users must have different cache namespaces")
	}
}

// TestBaseClientClone_CarriesCSCPointers: clone() must carry the shared cache and
// the eviction-hook handle a clone reads for attribution, but not the owner-only
// lifecycle fields.
func TestBaseClientClone_CarriesCSCPointers(t *testing.T) {
	c := &baseClient{
		opt: &Options{},
		csc: NewLocalCache(CacheConfig{MaxEntries: 16}),
	}
	// cscPoolHook IS carried: a clone reads it to attribute fetches to the
	// shared eviction hook. The owner-only fields are NOT carried, so a derived
	// client's Close can't stop the owner's drainer or flush its cache.
	hook := &cscEvictOnRemoveHook{}
	active := &atomic.Bool{}
	active.Store(true)
	c.cscPoolHook = hook
	c.cscActive = active
	c.cscKeyPrefix = cscNamespacePrefix(0, "user")
	c.cscOwnsCache = true
	c.cscDrainHandle = &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}

	cl := c.clone()
	if cl.csc == nil {
		t.Fatal("clone dropped csc")
	}
	if cl.cscPoolHook != hook {
		t.Fatal("clone must copy cscPoolHook (needed for attribution)")
	}
	if cl.cscActive != active {
		t.Fatal("clone must share the successful CSC attachment signal")
	}
	if cl.cscKeyPrefix != c.cscKeyPrefix {
		t.Fatal("clone must retain the shared cache namespace")
	}
	if cl.cscOwnsCache {
		t.Fatal("clone must not copy cscOwnsCache (owner-only)")
	}
	if cl.cscDrainHandle != nil {
		t.Fatal("clone must not copy cscDrainHandle (owner-only)")
	}
}

// TestRegisterInvalidateHandler_IdempotentForSameCache: a derived client
// sharing the parent's push processor must be able to re-attach the same
// cache (Client.Conn), while a different cache must still be refused.
func TestRegisterInvalidateHandler_IdempotentForSameCache(t *testing.T) {
	proc := push.NewProcessor()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	keyPrefix := cscNamespacePrefix(0, "")

	if err := registerInvalidateHandler(proc, cache, keyPrefix); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := registerInvalidateHandler(proc, cache, keyPrefix); err != nil {
		t.Fatalf("re-registration of the same cache+db must succeed, got: %v", err)
	}
	if err := registerInvalidateHandler(proc, NewLocalCache(CacheConfig{MaxEntries: 16}), keyPrefix); err == nil {
		t.Fatal("registering a different cache on the same processor must fail")
	}
}

func TestRegisterInvalidateHandler_ConcurrentSameCache(t *testing.T) {
	proc := push.NewProcessor()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	keyPrefix := cscNamespacePrefix(0, "")

	const clients = 32
	start := make(chan struct{})
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	wg.Add(clients)
	for range clients {
		go func() {
			defer wg.Done()
			<-start
			errs <- registerInvalidateHandler(proc, cache, keyPrefix)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("compatible concurrent registration failed: %v", err)
		}
	}
	if got := boundCache(lookupInvalidateHandler(proc)); got != cache {
		t.Fatal("concurrent registration did not retain the shared cache binding")
	}
}

func TestRegisterInvalidateHandler_NonComparableCacheDoesNotPanic(t *testing.T) {
	proc := push.NewProcessor()
	cache := nonComparableCache{
		Cache:  NewLocalCache(CacheConfig{MaxEntries: 16}),
		marker: []byte("non-comparable"),
	}
	keyPrefix := cscNamespacePrefix(0, "")

	if err := registerInvalidateHandler(proc, cache, keyPrefix); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := registerInvalidateHandler(proc, cache, keyPrefix); !errors.Is(err, errInvalidateHandlerBound) {
		t.Fatalf("second registration: got %v, want errInvalidateHandlerBound", err)
	}
}

// plainPooler is a Pooler with no DrainIdleConns capability.
type plainPooler struct{ pool.Pooler }

// drainOnlyPooler can sweep idle conns but cannot register the lifecycle hook
// that serializes cache publication with connection removal/reinitialization.
type drainOnlyPooler struct{ plainPooler }

func (*drainOnlyPooler) DrainIdleConns(
	context.Context,
	*pool.DrainState,
	func(*pool.Conn) error,
) {
}

// TestAttachCSC_StrategyGates: attachCSC refuses poolers without idle-conn
// draining (a sticky pool serving hits would be unboundedly stale — nothing
// applies invalidations).
func TestAttachCSC_StrategyGates(t *testing.T) {
	ctx := context.Background()

	sticky := &baseClient{
		opt:           &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategySharedTracking},
		pushProcessor: push.NewProcessor(),
		connPool:      &plainPooler{},
	}
	sticky.attachCSC(ctx, NewLocalCache(CacheConfig{MaxEntries: 16}))
	if sticky.csc != nil {
		t.Fatal("SharedTracking without a drainable pooler must stay uncached")
	}
	if sticky.cscDrainHandle != nil {
		t.Fatal("no drainer may be started when attachCSC refused the pooler")
	}

	hookless := &baseClient{
		opt:           &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategySharedTracking},
		pushProcessor: push.NewProcessor(),
		connPool:      &drainOnlyPooler{},
	}
	hookless.attachCSC(ctx, NewLocalCache(CacheConfig{MaxEntries: 16}))
	if hookless.csc != nil {
		t.Fatal("SharedTracking without lifecycle hooks must stay uncached")
	}
	if hookless.cscDrainHandle != nil {
		t.Fatal("no drainer may be started when lifecycle-hook registration is unavailable")
	}
}

// TestCSCTrackingRequested: tracking requires a successful, still-active CSC
// attachment. Merely configuring a cache is insufficient because handler
// registration or another attachment gate can fail.
func TestCSCTrackingRequested(t *testing.T) {
	cfg := &ClientSideCacheConfig{MaxEntries: 16}
	active := &atomic.Bool{}
	active.Store(true)
	stopped := &atomic.Bool{}
	cases := []struct {
		name string
		c    *baseClient
		want bool
	}{
		{"successful attachment", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}, cscActive: active}, true},
		{"derived client: csc nil, attachment shared", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}, cscActive: active}, true},
		{"configured but attachment failed", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}}, false},
		{"attachment stopped", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}, cscActive: stopped}, false},
		{"resp2", &baseClient{opt: &Options{Protocol: 2, ClientSideCacheConfig: cfg}, cscActive: active}, false},
		{"non-zero db", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg, DB: 1}, cscActive: active}, false},
	}
	for _, tc := range cases {
		if got := tc.c.cscTrackingRequested(); got != tc.want {
			t.Errorf("%s: cscTrackingRequested() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestCSCTrackingSignalSharedWithStickyClients(t *testing.T) {
	parent := NewClient(&Options{
		Addr:                  "127.0.0.1:0",
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = parent.Close() })

	conn := parent.Conn()
	t.Cleanup(func() { _ = conn.Close() })
	if conn.cscActive != parent.cscActive || !conn.cscTrackingRequested() {
		t.Fatal("Client.Conn must share the parent's active attachment signal")
	}

	tx := parent.newTx()
	t.Cleanup(func() { _ = tx.baseClient.Close() })
	if tx.cscActive != parent.cscActive || !tx.cscTrackingRequested() {
		t.Fatal("Tx must share the parent's active attachment signal")
	}
}

type borrowedConnPool struct {
	pool.Pooler
	cn *pool.Conn
}

func (p *borrowedConnPool) Get(context.Context) (*pool.Conn, error) {
	return p.cn, nil
}

func (*borrowedConnPool) Put(context.Context, *pool.Conn) {}

func TestStickyClaimRevokesParentCacheCoverage(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
	cn := pool.NewConn(client)

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	hook.bumpInitGen(cn.GetID())
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, cn.GetID(), []byte("v")) {
		t.Fatal("failed to seed parent-owned cache entry")
	}

	base := &baseClient{
		connPool:    &borrowedConnPool{cn: cn},
		cscPoolHook: hook,
	}
	sticky := base.newStickyConnPool()
	claimed, err := sticky.Get(context.Background())
	if err != nil {
		t.Fatalf("sticky Get: %v", err)
	}
	if claimed != cn {
		t.Fatal("sticky pool returned an unexpected connection")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("sticky claim left parent-cache entries without drainer coverage")
	}
	if got := hook.initGenOf(cn.GetID()); got != 2 {
		t.Fatalf("sticky claim generation: got %d, want 2", got)
	}
	sticky.Put(context.Background(), claimed)
	if err := sticky.Close(); err != nil {
		t.Fatalf("sticky Close: %v", err)
	}
}

// recordingHookPool is a poolHookSupport that counts RemovePoolHook calls.
type recordingHookPool struct {
	pool.Pooler
	removed int
}

func (p *recordingHookPool) AddPoolHook(pool.PoolHook)    {}
func (p *recordingHookPool) RemovePoolHook(pool.PoolHook) { p.removed++ }
func (p *recordingHookPool) SupportsPoolHooks() bool      { return true }
func (p *recordingHookPool) DrainIdleConns(
	context.Context, *pool.DrainState, func(*pool.Conn) error,
) {
}

// TestClone_SharesHookButOnlyOwnerDeregisters: a clone copies cscPoolHook (so it
// can attribute fetches to the shared eviction hook) but must not deregister it
// on Close; only the owner (the client holding the drain handle) does.
func TestClone_SharesHookButOnlyOwnerDeregisters(t *testing.T) {
	rp := &recordingHookPool{}
	owner := &baseClient{
		opt:         &Options{},
		connPool:    rp,
		cscPoolHook: &cscEvictOnRemoveHook{},
	}
	owner.startBackgroundDrainer()
	if owner.cscDrainHandle == nil {
		t.Fatal("owner did not start its drainer")
	}

	cl := owner.clone()
	if cl.cscPoolHook != owner.cscPoolHook {
		t.Fatal("clone should share the eviction hook for attribution")
	}
	if cl.cscDrainHandle != nil {
		t.Fatal("clone must not own the drain handle")
	}

	// Clone Close: no drain handle -> early return, must not touch the hook.
	cl.stopBackgroundDrainer()
	if rp.removed != 0 {
		t.Fatalf("clone must not deregister the shared hook, got %d removals", rp.removed)
	}

	// Owner Close: deregisters the hook exactly once.
	owner.stopBackgroundDrainer()
	if rp.removed != 1 {
		t.Fatalf("owner must deregister the hook once, got %d", rp.removed)
	}
}

// fakeTimeout is a net.Error reporting a timeout.
type fakeTimeout struct{}

func (fakeTimeout) Error() string   { return "i/o timeout" }
func (fakeTimeout) Timeout() bool   { return true }
func (fakeTimeout) Temporary() bool { return true }

// TestDrainErrorClassificationContract pins the drain-path classification for
// errors surfaced after reply consumption starts. At that point a timeout means
// the reader may be desynchronized and the connection must be removed.
func TestDrainErrorClassificationContract(t *testing.T) {
	const addr = "localhost:6379"

	timeoutErr := &net.OpError{Op: "read", Net: "tcp", Err: fakeTimeout{}}
	if !isBadConn(timeoutErr, false, addr) {
		t.Error("net i/o timeout must be fatal on the drain path (conn removed)")
	}
	if !isBadConn(io.EOF, false, addr) {
		t.Error("io.EOF must be fatal (conn removed)")
	}
	if !isBadConn(context.DeadlineExceeded, false, addr) {
		t.Error("context.DeadlineExceeded must be fatal")
	}
}

// invalidateFrame builds a RESP3 `>` push frame: ["invalidate", [key]].
func invalidateFrame(key string) []byte {
	return []byte(fmt.Sprintf(">2\r\n$10\r\ninvalidate\r\n*1\r\n$%d\r\n%s\r\n", len(key), key))
}

type recordingHandler struct {
	mu sync.Mutex
	n  int
}

func (h *recordingHandler) HandlePushNotification(_ context.Context, _ push.NotificationHandlerContext, _ []interface{}) error {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

type closingHandler struct {
	closeReturned chan error
}

func (h *closingHandler) HandlePushNotification(
	_ context.Context, handlerCtx push.NotificationHandlerContext, _ []interface{},
) error {
	closer, ok := handlerCtx.Client.(interface{ Close() error })
	if !ok {
		return errors.New("handler client does not implement Close")
	}
	h.closeReturned <- closer.Close()
	return nil
}

func newIdleTCPConnPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server = <-accepted:
	case <-time.After(time.Second):
		_ = client.Close()
		t.Fatal("accept timed out")
	}
	return server, client
}

// newReaderBufferedPushConn returns a conn with frame buffered in proto.Reader
// but no socket-visible data — the coalesced-push case (an invalidate left in
// cn.rd after a prior reply).
func newReaderBufferedPushConn(t *testing.T, frame []byte) (*pool.Conn, func()) {
	t.Helper()
	server, client := net.Pipe()
	cn := pool.NewConn(client)
	go func() { _, _ = server.Write(frame) }()
	// PeekReplyType fills the bufio buffer without consuming the frame.
	if err := cn.WithReader(context.Background(), time.Second, func(rd *proto.Reader) error {
		_, err := rd.PeekReplyType()
		return err
	}); err != nil {
		_ = server.Close()
		_ = client.Close()
		t.Fatalf("priming reader buffer: %v", err)
	}
	return cn, func() { _ = server.Close(); _ = client.Close() }
}

type releaseRecordingPool struct {
	pool.Pooler
	puts    int
	removes int
}

func (p *releaseRecordingPool) Put(context.Context, *pool.Conn) {
	p.puts++
}

func (p *releaseRecordingPool) Remove(context.Context, *pool.Conn, error) {
	p.removes++
}

func TestReleaseConnRemovesConnectionAfterPartialPushRead(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	partial := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfo")
	if _, err := server.Write(partial); err != nil {
		t.Fatalf("write partial push: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("partial push never became readable")
	}

	cp := &releaseRecordingPool{}
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		connPool:      cp,
		pushProcessor: push.NewProcessor(),
	}
	c.releaseConn(context.Background(), cn, nil)

	// The invariant is no PARTIALLY-CONSUMED conn is ever re-pooled. On a loaded
	// runner the drain's short probe deadline can expire before consuming any
	// byte — a benign timeout: nothing was read, the frame is intact in the
	// socket, and re-pooling is safe (the next drain consumes it whole). Only a
	// Put after bytes moved into the reader is the desync bug. An empty reader
	// buffer alone does NOT prove zero consumption (a partial parse can eat
	// every available byte and still leave the buffer empty), so before
	// skipping, prove the stream is intact: complete the frame and require the
	// whole push to parse. A parse failure means a desynced conn was re-pooled
	// — exactly the regression this test pins.
	if cp.puts == 1 && cp.removes == 0 && !cn.HasBufferedData() {
		if _, err := server.Write([]byte("o\r\n")); err != nil {
			t.Fatalf("completing push frame: %v", err)
		}
		if err := cn.WithReader(context.Background(), 2*time.Second, func(rd *proto.Reader) error {
			_, err := rd.ReadReply()
			return err
		}); err != nil {
			t.Fatalf("re-pooled conn is desynced: completed push failed to parse: %v", err)
		}
		t.Skip("probe timed out before consuming anything; conn re-pooled intact (whole-frame parse verified) — mid-frame path not exercised this run")
	}
	if cp.removes != 1 || cp.puts != 0 {
		t.Fatalf("partial push read must remove, not re-pool, the connection: removes=%d puts=%d",
			cp.removes, cp.puts)
	}
}

// TestCSCMissReadDrainsSocketPendingPushBeforeReply pins Finding A: a
// reply-expected CSC reader must block past a push that is still ON THE SOCKET
// (not yet buffered) ahead of the command reply. The old Buffered drain stopped
// the instant the reader buffer emptied, so a second invalidation arriving after
// the first was consumed would be read by ReadRawReply as the command's reply and
// cached under the wrong key. The blocking drain (drainPushFrames(..., true))
// keeps skipping pushes until a non-push frame is next.
func TestCSCMissReadDrainsSocketPendingPushBeforeReply(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		pushProcessor: push.NewProcessor(),
	}

	pushX := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nx\r\n")
	pushY := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\ny\r\n")
	reply := []byte("$5\r\nhello\r\n")

	type res struct {
		raw []byte
		err error
	}
	done := make(chan res, 1)
	go func() {
		var raw []byte
		err := cn.WithReader(context.Background(), 5*time.Second, func(rd *proto.Reader) error {
			if e := c.drainPushFrames(context.Background(), cn, rd, true); e != nil {
				return e
			}
			var e error
			raw, e = rd.ReadRawReply()
			return e
		})
		done <- res{raw, err}
	}()

	// First push, then a pause long enough for the reader to consume it and EMPTY
	// its buffer while blocked on the next frame — the exact window the Buffered
	// drain used to return in. Then a SECOND push ahead of the real reply.
	if _, err := server.Write(pushX); err != nil {
		t.Fatalf("write first push: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := server.Write(pushY); err != nil {
		t.Fatalf("write second push: %v", err)
	}
	if _, err := server.Write(reply); err != nil {
		t.Fatalf("write reply: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("reader failed: %v", r.err)
		}
		if string(r.raw) != string(reply) {
			t.Fatalf("blocking drain must skip both pushes and return the reply; got %q want %q",
				r.raw, reply)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reader did not complete")
	}
}

// bufferedNetConn models a wrapper such as tls.Conn: bytes may already be
// buffered inside the wrapper while NetConn's raw socket is empty.
type bufferedNetConn struct {
	net.Conn
	buffered *bytes.Reader
}

func (c *bufferedNetConn) Read(p []byte) (int, error) {
	if c.buffered.Len() > 0 {
		return c.buffered.Read(p)
	}
	return c.Conn.Read(p)
}

func (c *bufferedNetConn) NetConn() net.Conn {
	return c.Conn
}

func TestDrainPushNotifications_ConsumesWrappedBufferedPush(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("platform has no non-consuming raw-socket probe")
	}

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(&bufferedNetConn{
		Conn:     client,
		buffered: bytes.NewReader(invalidateFrame("foo")),
	})
	cn.MarkCscReadPending()

	processorSucceeded, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drain wrapped push: %v", err)
	}
	if !processorSucceeded {
		t.Fatal("successful wrapped-buffer processing must reset consecutive failure damping")
	}
	if rec.count() != 1 {
		t.Fatal("push hidden in the wrapper buffer was not consumed")
	}
	if cn.TakeCscReadPending() {
		t.Fatal("wrapped-reader drain request was not consumed")
	}
}

func TestDrainPushNotifications_EmptyWrappedProbeStaysShort(t *testing.T) {
	oldHardReadCap := cscDrainHardReadCap
	cscDrainHardReadCap = 200 * time.Millisecond
	defer func() { cscDrainHardReadCap = oldHardReadCap }()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: push.NewProcessor()}
	cn := pool.NewConn(&bufferedNetConn{
		Conn:     client,
		buffered: bytes.NewReader(nil),
	})
	cn.MarkCscReadPending()

	start := time.Now()
	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("empty wrapped probe: %v", err)
	}
	if processed {
		t.Fatal("empty wrapped probe must not report processor success")
	}
	if elapsed := time.Since(start); elapsed >= 50*time.Millisecond {
		t.Fatalf("empty wrapped probe held the connection for %v", elapsed)
	}
}

// TestDrainPushNotifications_ConsumesReaderBufferedPush is a regression guard: a
// push buffered in proto.Reader (no socket data) must still drain — the gate
// checks HasBufferedData(), not only MaybeHasData().
func TestDrainPushNotifications_ConsumesReaderBufferedPush(t *testing.T) {
	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	if !cn.HasBufferedData() {
		t.Fatal("precondition: frame was not buffered in the reader")
	}

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if !processed {
		t.Fatal("drain with buffered data must report processed=true")
	}
	if rec.count() == 0 {
		t.Fatal("reader-buffered invalidate was not consumed/dispatched (gate skipped it)")
	}
}

func TestDrainPushNotifications_AllowsFragmentedFrame(t *testing.T) {
	oldHardReadCap := cscDrainHardReadCap
	cscDrainHardReadCap = 200 * time.Millisecond
	defer func() { cscDrainHardReadCap = oldHardReadCap }()

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(client)

	frame := invalidateFrame("fragmented")
	split := len(frame) - 4
	if _, err := server.Write(frame[:split]); err != nil {
		t.Fatalf("write frame prefix: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("frame prefix never became socket-readable")
	}
	go func() {
		time.Sleep(5 * time.Millisecond)
		_, _ = server.Write(frame[split:])
	}()

	start := time.Now()
	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drain fragmented frame: %v", err)
	}
	if !processed || rec.count() != 1 {
		t.Fatalf("fragmented frame was not processed: processed=%v calls=%d",
			processed, rec.count())
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("drain waited after the fragmented frame completed: %v", elapsed)
	}
}

// erroringProcessor returns err from ProcessPendingNotifications, delegating
// other methods to the embedded Processor. Wrapping the built-in processor
// this way still classifies as CUSTOM in drainPushNotifications (the type
// assertion is exact) — which is the point: a wrapper gives no
// no-bytes-consumed guarantee either.
type erroringProcessor struct {
	*push.Processor
	err error
}

func (p erroringProcessor) ProcessPendingNotifications(_ context.Context, _ push.NotificationHandlerContext, _ *proto.Reader) error {
	return p.err
}

// TestDrainPushNotifications_CustomProcessorErrorIsFatal: a custom processor
// (any non-*push.Processor, including a wrapper around the built-in one) gives
// no guarantee that no bytes were consumed before its error, so the reader may
// be mid-frame — the error must be connection-fatal (non-nil), exactly like
// the built-in processor's mid-frame errors, so the drainer removes the conn
// instead of re-pooling a possibly desynced reader. (This inverts the earlier
// not-fatal behavior, which re-pooled the conn on the same evidence.)
func TestDrainPushNotifications_CustomProcessorErrorIsFatal(t *testing.T) {
	proc := erroringProcessor{push.NewProcessor(), errors.New("semantic boom")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	if _, err := c.drainPushNotifications(cn); err == nil {
		t.Fatal("custom-processor drain error must be fatal so the conn is removed, got nil")
	}
}

// TestDrainPushNotifications_CustomProcessorSkipsKernelOnlyData pins the spec gate
// (push.md): a custom processor gets work only when real RESP bytes are BUFFERED.
// Here a full frame sits on the socket (CheckForData readable) but nothing is
// buffered yet — the kernel-only case that over TLS can be a control record with no
// RESP bytes. The custom processor must be skipped (not invoked, not fatal); the
// frame is left for a later drain with buffered bytes or the MaxStaleness backstop.
func TestDrainPushNotifications_CustomProcessorSkipsKernelOnlyData(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("platform has no non-consuming raw-socket probe")
	}

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	// A wrapper around the built-in processor is classified CUSTOM; erroringProcessor
	// returns its error WITHOUT reading, so if the gate wrongly invokes it the drain
	// returns a (fatal) non-nil error.
	proc := erroringProcessor{push.NewProcessor(), errors.New("custom must be skipped on kernel-only data")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(client)

	if _, err := server.Write(invalidateFrame("foo")); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("frame never became socket-readable")
	}
	if cn.HasBufferedData() {
		t.Fatal("precondition: nothing should be buffered in the reader yet")
	}

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("custom processor was invoked on kernel-only data (must be skipped): %v", err)
	}
	if processed {
		t.Fatal("custom processor must not report success on kernel-only data")
	}
}

// TestDrainPushNotifications_CustomProcessorSkippedOnProbedBytes pins that the
// custom-processor gate keys on bytes ALREADY buffered, not on a byte the probe just
// peeked from an opaque transport. An opaque wrapper hides real invalidate bytes
// from the socket check and requests a drain (MarkCscReadPending); the probe can
// peek a byte into the buffer, but a custom processor must still be skipped this
// pass — handing its blocking loop probed (not pre-buffered) data risks a fatal
// spurious retire (spec: push.md). Skipping is bounded (periodic fallback +
// MaxStaleness). A built-in processor is unaffected.
func TestDrainPushNotifications_CustomProcessorSkippedOnProbedBytes(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	proc := erroringProcessor{push.NewProcessor(), errors.New("custom must not run on probed (not pre-buffered) bytes")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(&bufferedNetConn{
		Conn:     client,
		buffered: bytes.NewReader(invalidateFrame("foo")),
	})
	cn.MarkCscReadPending()

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("custom processor was run on probed (not pre-buffered) bytes and retired the conn: %v", err)
	}
	if processed {
		t.Fatal("custom processor must not report success on probed (not pre-buffered) bytes")
	}
}

// TestDrainPushNotifications_RelaxedBudgetToleratesFragmentedFrame pins that the
// background drain uses pushDrainBudget (push.md): while maintenance relaxation is
// active, a push frame fragmented past the small hard cap must still complete rather
// than be treated as a fatal mid-frame desync that evicts this conn's CSC coverage.
func TestDrainPushNotifications_RelaxedBudgetToleratesFragmentedFrame(t *testing.T) {
	oldHardReadCap := cscDrainHardReadCap
	cscDrainHardReadCap = 10 * time.Millisecond // tail arrives after this, within relaxed
	defer func() { cscDrainHardReadCap = oldHardReadCap }()

	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
	cn := pool.NewConn(client)
	// Activate relaxation so pushDrainBudget raises the cap well above 10ms.
	cn.SetRelaxedTimeout(500*time.Millisecond, 500*time.Millisecond)

	frame := invalidateFrame("fragmented")
	split := len(frame) - 4
	if _, err := server.Write(frame[:split]); err != nil {
		t.Fatalf("write frame prefix: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("frame prefix never became socket-readable")
	}
	go func() {
		time.Sleep(60 * time.Millisecond) // past the 10ms hard cap, within the 500ms budget
		_, _ = server.Write(frame[split:])
	}()

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("relaxed drain of fragmented frame errored (budget not applied?): %v", err)
	}
	if !processed || rec.count() != 1 {
		t.Fatalf("fragmented frame not processed under relaxed budget: processed=%v calls=%d",
			processed, rec.count())
	}
}

// TestBackgroundDrainerLifecycle verifies start stores a handle on the client,
// double-start is a no-op, and stop joins the goroutine. The handle is
// intentionally RETAINED (not cleared) after stop: cscPoolHook is read on the
// command hot path, so niling the CSC fields under a concurrent Close would race;
// repeat stops are made idempotent by teardownOnce instead.
func TestBackgroundDrainerLifecycle(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in lifecycle test") },
		PoolSize: 1,
	})
	defer cp.Close()
	c := &baseClient{opt: &Options{Protocol: 3}, connPool: cp}

	c.startBackgroundDrainer()
	h := c.cscDrainHandle
	if h == nil {
		t.Fatal("startBackgroundDrainer did not store a drain handle")
	}

	// Double-start must not replace the handle.
	c.startBackgroundDrainer()
	if c.cscDrainHandle != h {
		t.Fatal("double start replaced the drain handle")
	}

	c.stopBackgroundDrainer()
	// The handle is retained (see doc above), but the goroutine must have exited.
	if c.cscDrainHandle != h {
		t.Fatal("stopBackgroundDrainer must retain the drain handle")
	}
	select {
	case <-h.done:
	default:
		t.Fatal("stopBackgroundDrainer returned before the drainer goroutine exited")
	}

	// Stop again: idempotent, no panic, no double-close of the stop channel.
	c.stopBackgroundDrainer()
}

func TestHandlerContextCloseDuringCSCDrainIsDeferred(t *testing.T) {
	closeReturned := make(chan error, 1)
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", &closingHandler{closeReturned: closeReturned}, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	h := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	resourcesClosed := make(chan struct{})
	onClose := &onCloseHooks{}
	onClose.register("test", func() error {
		close(resourcesClosed)
		return nil
	})
	c := &baseClient{
		cscDrainHandle: h,
		opt:            &Options{Protocol: 3},
		pushProcessor:  proc,
		onClose:        onClose,
	}
	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("key"))
	defer cleanup()

	drainReturned := make(chan error, 1)
	go func() {
		_, err := c.drainPushNotifications(cn)
		drainReturned <- err
	}()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("handler Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler Close joined its own drain and deadlocked")
	}
	select {
	case <-h.stop:
	case <-time.After(time.Second):
		t.Fatal("Close did not signal the drainer to stop")
	}
	select {
	case err := <-drainReturned:
		if err != nil {
			t.Fatalf("drainPushNotifications: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not return after the handler")
	}
	select {
	case <-resourcesClosed:
		t.Fatal("resources closed before the active drain returned")
	default:
	}
	ordinaryCloseStarted := make(chan struct{})
	ordinaryCloseReturned := make(chan error, 1)
	go func() {
		close(ordinaryCloseStarted)
		ordinaryCloseReturned <- c.Close()
	}()
	<-ordinaryCloseStarted
	select {
	case err := <-ordinaryCloseReturned:
		t.Fatalf("ordinary Close returned before teardown completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// The real drainer closes done immediately after the pass returns.
	close(h.done)
	select {
	case err := <-ordinaryCloseReturned:
		if err != nil {
			t.Fatalf("ordinary Close during deferred teardown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary Close did not return after teardown completed")
	}
	select {
	case <-resourcesClosed:
	case <-time.After(time.Second):
		t.Fatal("deferred Close did not finish after the drain returned")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
}

// TestCscDrainIntervalClampsMinimum: sub-millisecond DrainInterval values are
// clamped to cscMinDrainInterval (unreliable timers would silently loosen the
// staleness bound); values at or above the floor pass through.
func TestCscDrainIntervalClampsMinimum(t *testing.T) {
	sub := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: 100 * time.Microsecond},
	}}
	if got := sub.cscDrainInterval(); got != cscMinDrainInterval {
		t.Fatalf("sub-ms DrainInterval must clamp to %v, got %v", cscMinDrainInterval, got)
	}

	above := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: 10 * time.Millisecond},
	}}
	if got := above.cscDrainInterval(); got != 10*time.Millisecond {
		t.Fatalf("above-floor DrainInterval must pass through, got %v", got)
	}

	unset := &baseClient{opt: &Options{Protocol: 3}}
	if got := unset.cscDrainInterval(); got != cscDrainSkipWindow {
		t.Fatalf("unset DrainInterval must default to %v, got %v", cscDrainSkipWindow, got)
	}
}

// TestInvalidateHandlerDecodesPayloads: the SharedTracking invalidate handler
// must evict for both string and []byte key names in the push payload.
func TestInvalidateHandlerDecodesPayloads(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.set("get:foo", []string{testCSCNamespacedKey(0, "foo")}, []byte("1"))
	cache.set("get:quux", []string{testCSCNamespacedKey(0, "quux")}, []byte("2"))
	h := &invalidateHandler{cache: cache, keyPrefix: cscNamespacePrefix(0, "")}

	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", []interface{}{"foo", []byte("quux")}})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("both entries should be invalidated, Len=%d", n)
	}
}

// TestInvalidateHandlerNilPayloadFlushes: a nil <keys> payload (emitted by the
// server on FLUSHDB/FLUSHALL) must flush the entire cache.
func TestInvalidateHandlerNilPayloadFlushes(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.set("get:foo", []string{testCSCNamespacedKey(0, "foo")}, []byte("1"))
	cache.set("get:quux", []string{testCSCNamespacedKey(0, "quux")}, []byte("2"))
	h := &invalidateHandler{cache: cache, keyPrefix: cscNamespacePrefix(0, "")}

	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", nil})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("nil payload must flush the whole cache, Len=%d", n)
	}
}

// boundCache reads the handler's current cache binding under its lock.
func boundCache(h *invalidateHandler) Cache {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.cache
}

// TestInvalidateHandlerReleasedOnClose: closing a client that OWNS its cache
// (ClientSideCacheConfig) must release the invalidate handler's BINDING. An
// application-supplied processor outlives the client; a handler left bound to
// the dead cache would make a successor client's registration fail and
// silently disable its CSC. The handler itself stays registered — and
// protected — so application code cannot unregister invalidation out from
// under a live client.
func TestInvalidateHandlerReleasedOnClose(t *testing.T) {
	p := NewPushNotificationProcessor()

	c1 := NewClient(&Options{
		Addr:                      "localhost:1", // never dialed
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	if c1.baseClient.csc == nil {
		t.Fatal("first client should have CSC attached")
	}
	ih, ok := p.GetHandler(invalidatePushName).(*invalidateHandler)
	if !ok {
		t.Fatal("invalidate handler should be registered while the client lives")
	}
	// The handler is protected: user-level unregistration must FAIL, so a live
	// client's invalidation can't be silently removed by application code.
	if err := p.UnregisterHandler(invalidatePushName); err == nil {
		t.Fatal("UnregisterHandler must fail for the protected invalidate handler")
	}

	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Still registered, but the binding is released.
	if p.GetHandler(invalidatePushName) == nil {
		t.Fatal("handler must stay registered (protected) after Close; only its binding is released")
	}
	if boundCache(ih) != nil {
		t.Fatal("owned-cache binding must be released on Close")
	}

	// A successor client reusing the processor rebinds the handler.
	c2 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c2.Close() })
	if c2.baseClient.csc == nil {
		t.Fatal("successor client must be able to attach CSC after the first Close")
	}
	if boundCache(ih) != c2.baseClient.csc {
		t.Fatal("successor client must rebind the released handler to its own cache")
	}
}

// TestInvalidateHandlerRetainedForSharedCache: with an explicitly supplied
// (shared) cache the client does not own it — other clients on the same
// processor may still rely on the handler, so Close must leave it registered.
func TestInvalidateHandlerRetainedForSharedCache(t *testing.T) {
	p := NewPushNotificationProcessor()
	shared := NewLocalCache(CacheConfig{MaxEntries: 16})

	c1 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCache:           shared,
	})
	c2 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCache:           shared,
	})
	if c1.baseClient.csc == nil || c2.baseClient.csc == nil {
		t.Fatal("both clients should share CSC on the same cache+processor")
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ih, ok := p.GetHandler(invalidatePushName).(*invalidateHandler)
	if !ok {
		t.Fatal("shared-cache handler must survive one client's Close: the second client still needs invalidations")
	}
	if boundCache(ih) != shared {
		t.Fatal("shared-cache binding must not be released by a non-owning client's Close")
	}
	if err := c2.Close(); err != nil {
		t.Fatalf("close second client: %v", err)
	}
	if boundCache(ih) != nil {
		t.Fatal("shared-cache binding must be released after the last client closes")
	}
}

// newDampingClient builds a baseClient whose drainer ticks every millisecond,
// draining the given conns round-robin (one per pass) through an
// always-erroring custom processor.
func newDampingClient(cns ...*pool.Conn) *baseClient {
	return &baseClient{
		opt: &Options{
			Protocol:              3,
			ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: time.Millisecond},
		},
		connPool:      &drainablePooler{cns: cns},
		pushProcessor: erroringProcessor{push.NewProcessor(), errors.New("always fails")},
	}
}

// waitDrainerSelfStop asserts the drainer stops itself (damping) and that CSC
// serving is disabled.
func waitDrainerSelfStop(t *testing.T, c *baseClient) {
	t.Helper()
	h := c.cscDrainHandle
	if h == nil {
		t.Fatal("drainer did not start")
	}
	select {
	case <-h.done:
		// Drainer self-stopped after the damping threshold.
	case <-time.After(5 * time.Second):
		t.Fatal("drainer did not self-stop on persistent custom-processor errors")
	}
	if c.cscActive.Load() {
		t.Fatal("cscActive must be false after the damping threshold: stale hits must not be served")
	}
}

// TestBackgroundDrainerDisablesCSCOnPersistentCustomErrors: a custom processor
// that fails every drain would otherwise remove (and force a redial of) a conn
// per tick forever. After cscDrainCustomErrCap consecutive failures the drainer
// must disable CSC serving (cscActive=false) and stop, instead of churning.
func TestBackgroundDrainerDisablesCSCOnPersistentCustomErrors(t *testing.T) {
	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	c := newDampingClient(cn)
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	if err := registerInvalidateHandler(c.pushProcessor, cache, cscNamespacePrefix(0, "")); err != nil {
		t.Fatalf("register invalidate handler: %v", err)
	}
	ih := lookupInvalidateHandler(c.pushProcessor)
	if ih == nil {
		t.Fatal("invalidate handler was not registered")
	}
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	const ownerConnID = uint64(55)
	hook.bumpInitGen(ownerConnID)
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, ownerConnID, []byte("v")) {
		t.Fatal("failed to seed owned entry")
	}
	c.csc = cache
	c.cscPoolHook = hook
	dp := c.connPool.(*drainablePooler)
	dp.AddPoolHook(hook)
	c.startBackgroundDrainer()
	t.Cleanup(c.stopBackgroundDrainer)
	waitDrainerSelfStop(t, c)
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("damping must evict entries whose invalidation coverage just stopped")
	}
	if boundCache(ih) != nil {
		t.Fatal("damping must release the invalidate-handler binding")
	}
	if got := dp.removedHooks(); got != 1 {
		t.Fatalf("damping must remove the inactive pool hook once, got %d removals", got)
	}
}

// TestBackgroundDrainerDampingSurvivesCleanConns: in a real pool, each fatal
// drain removes its conn and the freshly dialed replacement has nothing
// buffered — its drain is a no-op. Such clean drains must NOT reset the
// damping counter, or a persistently failing processor would churn conns
// forever without ever tripping the cap.
func TestBackgroundDrainerDampingSurvivesCleanConns(t *testing.T) {
	switch runtime.GOOS {
	case "linux", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris", "illumos":
	default:
		t.Skip("platform has no non-consuming socket probe for an empty connection")
	}

	pushy, cleanup1 := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup1()
	// A conn with nothing buffered and nothing on the socket: its drain is a
	// clean no-op. Use TCP so the Unix non-consuming socket probe can prove
	// emptiness; opaque wrappers deliberately request a bounded read instead.
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()
	clean := pool.NewConn(client)

	// Alternate failing and clean conns per drain pass.
	c := newDampingClient(pushy, clean)
	c.startBackgroundDrainer()
	t.Cleanup(c.stopBackgroundDrainer)
	waitDrainerSelfStop(t, c)
}

// drainablePooler is a non-*pool.ConnPool Pooler implementing idleConnDrainer.
// With cns set, each DrainIdleConns pass hands the callback one conn,
// round-robin; without, passes are just counted.
type drainablePooler struct {
	pool.Pooler
	cns []*pool.Conn

	mu          sync.Mutex
	called      int
	removedHook int
}

func (d *drainablePooler) DrainIdleConns(_ context.Context, _ *pool.DrainState, fn func(cn *pool.Conn) error) {
	d.mu.Lock()
	n := d.called
	d.called++
	d.mu.Unlock()
	if len(d.cns) > 0 {
		_ = fn(d.cns[n%len(d.cns)])
	}
}

func (d *drainablePooler) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.called
}

func (d *drainablePooler) AddPoolHook(pool.PoolHook) {}

func (d *drainablePooler) RemovePoolHook(pool.PoolHook) {
	d.mu.Lock()
	d.removedHook++
	d.mu.Unlock()
}

func (d *drainablePooler) SupportsPoolHooks() bool {
	return true
}

func (d *drainablePooler) removedHooks() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.removedHook
}

// pubsubMessageFrame builds a RESP3 `>` push frame: ["message", ch, payload].
func pubsubMessageFrame(ch, payload string) []byte {
	return []byte(fmt.Sprintf(">3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(ch), ch, len(payload), payload))
}

// TestDrainPushNotifications_LeavesPubSubFrames: the drain loop must not
// consume pub/sub-reserved push frames — they belong to the pub/sub system
// (same guard as the built-in processor, cf. PR #3842).
func TestDrainPushNotifications_LeavesPubSubFrames(t *testing.T) {
	proc := push.NewProcessor()
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, pubsubMessageFrame("ch", "hello"))
	defer cleanup()

	if _, err := c.drainPushNotifications(cn); err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if !cn.HasBufferedData() {
		t.Fatal("pub/sub message frame was consumed by the drain loop; it must stay buffered")
	}
}

// TestDrainPushNotifications_IncompleteFrameTimeouts distinguishes a
// non-consuming name peek from a reply read that has already consumed bytes.
func TestDrainPushNotifications_IncompleteFrameTimeouts(t *testing.T) {
	tests := map[string]struct {
		partial   []byte
		wantFatal bool
	}{
		"name": {
			partial: []byte(">2\r\n$10\r\ninval"),
		},
		"payload": {
			partial:   []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfo"),
			wantFatal: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			proc := push.NewProcessor()
			c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}
			cn, cleanup := newReaderBufferedPushConn(t, test.partial)
			defer cleanup()

			_, err := c.drainPushNotifications(cn)
			if test.wantFatal && err == nil {
				t.Fatal("timeout after reply consumption must be fatal")
			}
			if !test.wantFatal && err != nil {
				t.Fatalf("non-consuming peek timeout must end the batch: %v", err)
			}
		})
	}
}

// TestBackgroundDrainerUsesOptionalInterface: any Pooler implementing
// idleConnDrainer gets background draining, not only *pool.ConnPool.
func TestBackgroundDrainerUsesOptionalInterface(t *testing.T) {
	dp := &drainablePooler{}
	c := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: time.Millisecond},
	}, connPool: dp}

	c.startBackgroundDrainer()
	defer c.stopBackgroundDrainer()

	deadline := time.After(2 * time.Second)
	for dp.calls() == 0 {
		select {
		case <-deadline:
			t.Fatal("drainer never called the pooler's DrainIdleConns")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestBackgroundDrainerCleanupOnGC verifies the runtime.AddCleanup safety net:
// a client that starts a drainer and is then dropped WITHOUT Close must have its
// drainer goroutine stopped once the *Client wrapper is garbage-collected.
func TestBackgroundDrainerCleanupOnGC(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in cleanup test") },
		PoolSize: 1,
	})
	defer cp.Close()

	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	hook := &cscEvictOnRemoveHook{
		evictor: cache,
		initGen: make(map[uint64]uint64),
	}
	const ownerConnID = uint64(66)
	hook.bumpInitGen(ownerConnID)
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.FulfillOwned("get:k", token, ownerConnID, []byte("v")) {
		t.Fatal("failed to seed owned entry")
	}

	// Build a *Client with a running drainer, register the cleanup, and return
	// ONLY its done channel so the *Client becomes unreachable when this returns.
	done := func() <-chan struct{} {
		c := &Client{baseClient: &baseClient{
			opt:         &Options{Protocol: 3},
			connPool:    cp,
			csc:         cache,
			cscPoolHook: hook,
		}}
		c.baseClient.startBackgroundDrainer()
		h := c.baseClient.cscDrainHandle
		if h == nil {
			t.Fatal("drainer did not start")
		}
		cscRegisterCleanups(c)
		return h.done
	}()

	deadline := time.After(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-done:
			if _, ok := cache.Get(context.Background(), "get:k"); ok {
				t.Fatal("GC cleanup stopped the drainer without evicting its coverage")
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
		select {
		case <-deadline:
			t.Fatal("drainer goroutine did not stop after the client was GC'd")
		default:
		}
	}
}

// TestInvalidationStatsSplitByDedup proves the invalidation/deletion split that
// #3 introduced: Invalidations counts every key named in incoming pushes (at the
// handler choke point, BEFORE dedup), while Deletions counts the keys the window
// batcher actually applied (AFTER dedup). Their gap is the dedup — the signal the
// split exists to expose, and the invariant every prior stats-counting round
// violated.
func TestInvalidationStatsSplitByDedup(t *testing.T) {
	ctx := context.Background()
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour) // buffer everything; flush deterministically via stop-drain

	// One push naming 'hot' 200 times plus 'cold' once: 201 incoming keys.
	keys := make([]interface{}, 0, 201)
	for i := 0; i < 200; i++ {
		keys = append(keys, "hot")
	}
	keys = append(keys, "cold")
	if err := h.HandlePushNotification(ctx, push.NotificationHandlerContext{},
		[]interface{}{invalidatePushName, keys}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}

	// Invalidations are counted at the handler, before dedup: all 201.
	if inv := cache.InvalidationStats(); inv != 201 {
		t.Fatalf("InvalidationStats = %d, want 201 (every incoming key, pre-dedup)", inv)
	}

	// Deletions are applied post-dedup by the window batcher. The 1h window never
	// fires on its own — force the flush via the stop-drain.
	h.mu.RLock()
	b := h.batcher
	h.mu.RUnlock()
	if b == nil {
		t.Fatal("expected a windowed batcher for a nonzero window")
	}
	b.stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if del, _ := cache.DeletionStats(); del >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	del, _ := cache.DeletionStats()
	if del != 2 {
		t.Fatalf("deletions = %d, want 2 (dedup collapses 200 'hot' + 1 'cold')", del)
	}
	// The gap between incoming and applied IS the dedup.
	if got := cache.InvalidationStats() - del; got != 199 {
		t.Fatalf("Invalidations - Deletions = %d, want 199 (the dedup gap)", got)
	}
}

// TestBatcherRepointedToSurvivorOnRefreshClose pins the cursor finding: when the
// active refresh owner closes, clearRefreshQueue must repoint the running batcher
// at the surviving sibling BEFORE stopping it, so the stop-drain feeds evicted-hot
// keys to the live survivor's refresher rather than the closed owner's (whose
// drainer is gone). Asserting the pointer is enough: it is the whole fix.
func TestBatcherRepointedToSurvivorOnRefreshClose(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	t.Cleanup(func() { h.release() })
	h.setInvalBatchWindow(time.Hour)

	qA := &cscRefreshQueue{}
	qB := &cscRefreshQueue{}
	h.setRefreshQueue(qA)
	h.setRefreshQueue(qB) // B is the active owner

	b := h.ensureBatcher()
	if b == nil {
		t.Fatal("expected a windowed batcher")
	}
	if got := b.refresh.Load(); got != qB {
		t.Fatalf("batcher refresh = %p, want active owner qB %p", got, qB)
	}

	// Active owner B closes: survivor A is restored AND the batcher is repointed at
	// A before its stop-drain. clearRefreshQueue now detaches+signals only and
	// returns the batcher; join it (its run() was started by ensureBatcher) so the
	// stop-drain finishes before the test ends and no goroutine touches the shared
	// LocalCache concurrently with the next test under -race.
	if bb := h.clearRefreshQueue(qB); bb != nil {
		bb.join()
	}
	if got := b.refresh.Load(); got != qA {
		t.Fatalf("after owner close: batcher refresh = %p, want survivor qA %p (stop-drain would feed the dead owner)", got, qA)
	}
}

// TestPushDrainBudgetHonorsRelaxation pins the drain budget used by every push
// drain (pushDrainWithin): the hard cap normally, RAISED to the connection's
// relaxed timeout while maintenance relaxation is active. Without the raise a
// push frame fragmented past the cap — likeliest mid-failover, exactly when
// relaxation is on — times out mid-frame, and the pre-command path then retires
// the healthy connection (and fails the command outright with MaxRetries=0).
func TestPushDrainBudgetHonorsRelaxation(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()

	const hardCap = 50 * time.Millisecond
	if got := pushDrainBudget(cn, hardCap); got != hardCap {
		t.Fatalf("no relaxation: budget = %v, want the hard cap %v", got, hardCap)
	}
	cn.SetRelaxedTimeout(10*time.Second, 10*time.Second)
	if got := pushDrainBudget(cn, hardCap); got != 10*time.Second {
		t.Fatalf("relaxed: budget = %v, want the relaxed 10s", got)
	}
	cn.ClearRelaxedTimeout()
	if got := pushDrainBudget(cn, hardCap); got != hardCap {
		t.Fatalf("cleared: budget = %v, want the hard cap %v", got, hardCap)
	}
	// A relaxed window SMALLER than the cap must not lower the budget: the raise
	// is one-directional.
	cn.SetRelaxedTimeout(time.Millisecond, time.Millisecond)
	if got := pushDrainBudget(cn, hardCap); got != hardCap {
		t.Fatalf("small relaxed window: budget = %v, want the hard cap %v", got, hardCap)
	}
}

// TestCSCMissCoalescerEnqueueHonorsCallerCancel pins that while a coalesced miss is
// merely WAITING TO ENQUEUE (queue full / worker stalled, pre-I/O), a caller that
// cancels its ctx aborts even when ContextTimeoutEnabled is false — the enqueue
// select watches the ORIGINAL ctx, not the Background wctx that (correctly) gates the
// reply wait. Without the fix the enqueue select watches Background, so a cancelled
// caller blocks on a full queue indefinitely.
func TestCSCMissCoalescerEnqueueHonorsCallerCancel(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	mc := &cscMissCoalescer{
		c:    &baseClient{opt: &Options{}, csc: cache}, // ContextTimeoutEnabled defaults false
		ch:   make(chan *cscMissReq, 1),
		stop: make(chan struct{}),
	}
	mc.ch <- &cscMissReq{} // fill ch to capacity so the enqueue send blocks

	token, fetch := cache.Reserve("ck", []string{"rk"})
	if token == 0 || !fetch {
		t.Fatalf("Reserve = (%d, %v); want a fresh reservation", token, fetch)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // caller already cancelled

	errCh := make(chan error, 1)
	go func() {
		_, err := mc.fetch(ctx, makeCmd("get", "rk"), "ck", token)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("fetch returned %v, want context.Canceled (enqueue must honor caller "+
				"cancel even with ContextTimeoutEnabled=false)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch blocked on a full queue despite a cancelled caller ctx; the enqueue " +
			"select must watch the caller ctx, not the ContextTimeoutEnabled-gated wctx")
	}
}

type rejectingLimiter struct {
	allow  atomic.Int64
	report atomic.Int64
	err    error
}

func (l *rejectingLimiter) Allow() error         { l.allow.Add(1); return l.err }
func (l *rejectingLimiter) ReportResult(_ error) { l.report.Add(1) }

// TestGetConnLimitedSurfacesRejection pins F-A: getConnLimited reports a
// Limiter.Allow rejection distinctly (limited=true) with the error RAW — not
// reported (ReportResult is for dial results only) and not tagged for the
// coalescer's re-run. A rejection tagged errCSCRetryUncached/cscSessionError would
// send processCached back through processWithRetry -> getConn -> Allow a SECOND
// time, letting a stateful limiter admit the very miss it just denied.
func TestGetConnLimitedSurfacesRejection(t *testing.T) {
	sentinel := errors.New("breaker open")
	lim := &rejectingLimiter{err: sentinel}
	c := &baseClient{opt: &Options{Limiter: lim}} // Allow rejects before any dial; connPool untouched

	cn, limited, err := c.getConnLimited(context.Background())
	if cn != nil || !limited || err != sentinel {
		t.Fatalf("getConnLimited = (%v, limited=%v, %v); want (nil, true, sentinel)", cn, limited, err)
	}
	if got := lim.allow.Load(); got != 1 {
		t.Fatalf("Allow called %d times, want 1", got)
	}
	if got := lim.report.Load(); got != 0 {
		t.Fatalf("ReportResult called %d times for an admission denial, want 0", got)
	}
	// Must not be a re-run-triggering error (that path would re-Allow).
	var se cscSessionError
	if err == errCSCRetryUncached || errors.As(err, &se) {
		t.Fatal("a limiter rejection is tagged for the coalescer re-run; it would call Allow twice")
	}
}

// --- CSC teardown lifecycle (F1/F2/F3) -------------------------------------

// newCSCTeardownClient builds a client with BOTH refresh-on-invalidate and
// reader-miss coalescing enabled and a fast drain tick. No live redis is needed:
// the drainer, refresher, and coalescer goroutines all park at startup (no idle
// conns to drain, no misses to fetch), which is exactly the teardown surface these
// tests exercise. It asserts all three background workers are running.
func newCSCTeardownClient(t *testing.T) (*Client, *cscMissCoalescer, *cscRevalidateHandle, *cscDrainHandle) {
	t.Helper()
	client := NewClient(&Options{
		Addr:                               "127.0.0.1:0",
		Protocol:                           3,
		ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 16, DrainInterval: time.Millisecond},
		ClientSideCacheRefreshOnInvalidate: true,
		ClientSideCacheCoalesceMisses:      true,
	})
	mc := client.cscMissCoalescer.Load()
	rh := client.cscRefreshHandle
	dh := client.cscDrainHandle
	if mc == nil || rh == nil || dh == nil {
		client.Close()
		t.Fatalf("precondition: coalescer(%v) refresher(%v) drainer(%v) must all be running", mc, rh, dh)
	}
	return client, mc, rh, dh
}

// TestCSCSelfDisableStopsRefresherAndCoalescer pins F1: when CSC turns ITSELF off
// (here via disableCSCServing; also custom-processor damping), the drainer exits
// its loop and its defer must tear the refresher and coalescer down the same way
// Close does — otherwise those goroutines run for the client's life (the refresher
// parked on its window, a coalescer session able to hold a pool connection). Before
// the fix the defer only unbound the queue and released the handler, so the
// refresher goroutine never joined and this test times out on rh.done.
func TestCSCSelfDisableStopsRefresherAndCoalescer(t *testing.T) {
	client, mc, rh, dh := newCSCTeardownClient(t)
	defer client.Close()

	// Self-disable: the drainer observes cscActive=false on its next (~1ms) tick,
	// exits, and (with the fix) stops the refresher + coalescer.
	client.disableCSCServing(context.Background(), "test self-disable")

	select {
	case <-rh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresher goroutine leaked: self-disable did not join it")
	}
	select {
	case <-mc.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("coalescer leaked: self-disable did not signal its sessions to stop")
	}
	select {
	case <-dh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainer did not exit after self-disable")
	}
}

// TestCSCTeardownReleasesCoalescerBeforeRefresher pins F3's ordering: teardown must
// stop the coalescer (releasing its held pool connection) BEFORE it signals and
// joins the refresher, whose stop-drain flush needs a main-pool connection. It also
// pins that this happens WHILE serving is still active (the refresher's flush would
// otherwise be a no-op). We block the coalescer's join with an extra wg counter and
// assert the refresher has not been signalled while that join is parked. With the
// wrong order (refresher first) rh.stop is closed before the coalescer join, so the
// mid-teardown assertion fires.
func TestCSCTeardownReleasesCoalescerBeforeRefresher(t *testing.T) {
	client, mc, rh, _ := newCSCTeardownClient(t)

	// Park stopCSCMissCoalescer's wg.Wait() so we can observe the intermediate
	// teardown state. Add precedes the concurrent Wait and the counter is already
	// >=1 (a session is running), so this is a legal WaitGroup use. Release via a
	// once-guarded cleanup so an early t.Fatal cannot leave the join (and the
	// deferred Close) deadlocked.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCoalescer := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCoalescer)
	mc.wg.Add(1)
	go func() { defer mc.wg.Done(); <-release }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.stopCSCRefresherAndCoalescer()
	}()

	// Step 1 signalled the coalescer to stop (stopWorkers closed mc.stop) ...
	select {
	case <-mc.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("coalescer was never signalled to stop")
	}
	// ... but the refresher must NOT be signalled yet: its flush needs the pool
	// connection the coalescer is still (artificially) holding.
	select {
	case <-rh.stop:
		t.Fatal("refresher was signalled before the coalescer released its connection (F3 ordering regression)")
	case <-time.After(150 * time.Millisecond):
	}
	// Serving must still be active so the refresher's final flush is not a no-op.
	if client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("cscActive was cleared before the refresher's final flush ran")
	}

	// Let the coalescer join complete; the refresher teardown then proceeds.
	releaseCoalescer()
	select {
	case <-rh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresher did not stop after the coalescer released")
	}
	<-done
	if client.cscActive.Load() {
		t.Fatal("cscActive must be false after teardown completes")
	}
	client.Close()
}

// TestCSCHandlerCloseKeepsServingUntilRefresherFlush pins F2: a handler-initiated
// close (a custom push handler calling handlerCtx.Client.Close()) must NOT
// preemptively deactivate serving. The async canonical close drives the teardown,
// so the refresher's final flush runs with cscActive still true and re-fetches the
// in-window keys. Before the fix cscHandlerClient.Close stored cscActive=false
// synchronously, so this assertion fails immediately after the handler Close.
func TestCSCHandlerCloseKeepsServingUntilRefresherFlush(t *testing.T) {
	client, mc, _, dh := newCSCTeardownClient(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCoalescer := func() { releaseOnce.Do(func() { close(release) }) }
	// Order matters: Close is deferred FIRST so it runs LAST; releaseCoalescer is
	// deferred second so it runs BEFORE Close on unwind. An early t.Fatal would
	// otherwise deadlock — the parked coalescer join holds the drainer teardownOnce
	// that the deferred Close would then block on.
	defer client.Close()
	defer releaseCoalescer()

	// Park the coalescer join so the async close cannot reach the deactivate step
	// (which follows the coalescer stop and the refresher flush in the canonical
	// order). This makes the "still serving" observation deterministic.
	mc.wg.Add(1)
	go func() { defer mc.wg.Done(); <-release }()

	// Handler-initiated close (runs on the drainer goroutine in production; here we
	// invoke the same entry point directly). Returns immediately; teardown is async.
	hc := cscHandlerClient{baseClient: client.baseClient}
	if err := hc.Close(); err != nil {
		t.Fatalf("handler close: %v", err)
	}

	// The async close entered teardown (coalescer signalled) ...
	select {
	case <-mc.stop:
	case <-time.After(2 * time.Second):
		t.Fatal("handler close did not start the canonical teardown")
	}
	// ... and serving is STILL active: the fix removed the preemptive deactivate.
	if client.cscActive == nil || !client.cscActive.Load() {
		t.Fatal("handler close deactivated CSC before the refresher's final flush (F2 regression)")
	}

	// Unblock; the teardown completes and deactivates.
	releaseCoalescer()
	select {
	case <-dh.done:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown did not complete after the coalescer join was released")
	}
	if client.cscActive.Load() {
		t.Fatal("CSC still serving after teardown completed")
	}
}

// TestPushDrainWithinShortBoundaryUnderRelaxation pins #3989: the speculative drain's
// FIRST-byte wait stays bounded by the short cap even while maintenance relaxation is
// active. Otherwise a TLS control record (socket-readable, zero RESP bytes) would block
// the drain for the full relaxed timeout, stalling a ready command or the idle drainer.
func TestPushDrainWithinShortBoundaryUnderRelaxation(t *testing.T) {
	oldCap := cscDrainHardReadCap
	cscDrainHardReadCap = 20 * time.Millisecond
	defer func() { cscDrainHardReadCap = oldCap }()

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second) // relaxation active: budget >> cap
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: push.NewProcessor()}

	// No data on the pipe: with no RESP frame begun, the drain must return within the
	// short cap, not wait out the 2s relaxed budget.
	start := time.Now()
	if err := c.pushDrainWithin(context.Background(), cn, cscDrainHardReadCap); err != nil {
		t.Fatalf("pushDrainWithin: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("speculative drain waited %v with no frame; must be bounded by the short cap "+
			"(%v), not the relaxed timeout (#3989)", elapsed, cscDrainHardReadCap)
	}
}

// TestInvalidateHandlerFullFlushPairsBatcherWithSnapshotCache pins #3989 fCCqu: a
// full-cache flush must drop the batcher only when it still belongs to the cache being
// flushed. If a release+rebind (A->B) races between the caller's snapshot and the
// flush, dropping B's batcher while flushing A would skip B's queued deletes and B
// would serve stale.
func TestInvalidateHandlerFullFlushPairsBatcherWithSnapshotCache(t *testing.T) {
	cacheA := NewLocalCache(CacheConfig{MaxEntries: 16})
	cacheB := NewLocalCache(CacheConfig{MaxEntries: 16})
	batcherB := newTestBatcher(cacheB, 64, time.Hour)

	// Handler now bound to cache B (post-rebind); the full-flush was snapshotted for A.
	h := &invalidateHandler{cache: cacheB, batcher: batcherB}

	cacheA.set("get:x", []string{"x"}, []byte("v"))
	if cacheA.Len() == 0 {
		t.Fatal("precondition: cacheA should hold an entry")
	}
	epochBefore := batcherB.epoch.Load()

	// Snapshot was A, but h.cache is B: flush A, and DO NOT drop B's batcher.
	h.fullFlush(cacheA)

	if got := batcherB.epoch.Load(); got != epochBefore {
		t.Fatalf("fullFlush dropped the rebound cache B's batcher (epoch %d -> %d) while flushing A (#3989)",
			epochBefore, got)
	}
	if cacheA.Len() != 0 {
		t.Fatal("fullFlush did not flush the snapshot cache A")
	}

	// Control: an unchanged binding (h.cache == cache) DOES drop the batcher.
	epochB2 := batcherB.epoch.Load()
	h.fullFlush(cacheB)
	if batcherB.epoch.Load() == epochB2 {
		t.Fatal("fullFlush must drop the batcher when the binding is unchanged")
	}
}

// TestInvalidateHandlerFullFlushNonComparableCacheNoPanic pins the sameCache guard in
// fullFlush: comparing h.cache == cache directly panics when the cache's dynamic type is
// non-comparable (a struct with a slice field), crashing the push goroutine on every
// FLUSHDB/FLUSHALL. sameCache compares safely (#3989).
func TestInvalidateHandlerFullFlushNonComparableCacheNoPanic(t *testing.T) {
	inner := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache := nonComparableCache{Cache: inner, marker: []byte("m")}
	batcher := newTestBatcher(cache, 64, time.Hour)
	h := &invalidateHandler{cache: cache, batcher: batcher}

	inner.set("get:x", []string{"x"}, []byte("v"))
	if inner.Len() == 0 {
		t.Fatal("precondition: cache should hold an entry")
	}

	// A bare == would panic here; sameCache must guard it. The batcher is not dropped for
	// a non-comparable type (sameCache returns false), but the snapshot is still flushed.
	h.fullFlush(cache)

	if inner.Len() != 0 {
		t.Fatal("fullFlush did not flush the non-comparable cache")
	}
}
