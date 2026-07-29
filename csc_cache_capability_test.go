package redis

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// nonOwnerCache satisfies Cache (by embedding the interface) but deliberately
// does NOT implement ConnOwnedCache — it exposes none of FulfillOwned/EvictByConn.
type nonOwnerCache struct{ Cache }

func testCSCNamespacedKey(db int, key string) string {
	return cscNamespacedKey(cscNamespacePrefix(db, ""), key)
}

type unusedStreamingProvider struct{}

func (unusedStreamingProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	panic("Subscribe must not be called without a connection")
}

// TestAttachCSC_DisablesForNonOwnerCache: SharedTracking requires per-connection
// eviction, so an explicit cache lacking ConnOwnedCache disables CSC (rather than
// serve entries it can't evict on connection close).
func TestAttachCSC_DisablesForNonOwnerCache(t *testing.T) {
	client := NewClient(&Options{
		Addr:            "127.0.0.1:0", // never dialed
		Protocol:        3,
		ClientSideCache: nonOwnerCache{NewLocalCache(CacheConfig{MaxEntries: 16})},
	})
	defer client.Close()

	if _, ok := interface{}(nonOwnerCache{}).(ConnOwnedCache); ok {
		t.Fatal("test setup: nonOwnerCache must not implement ConnOwnedCache")
	}
	if client.csc != nil {
		t.Fatal("CSC must be disabled for a cache without ConnOwnedCache")
	}
	if client.cscTrackingRequested() {
		t.Fatal("tracking must not be requested for a non-owner cache")
	}
}

// TestAttachCSC_EnabledForOwnerCache: an owner-aware explicit cache enables CSC.
func TestAttachCSC_EnabledForOwnerCache(t *testing.T) {
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
	if !cache.Set(cacheKeyA, []string{redisKeyA}, []byte("a")) ||
		!cache.Set(cacheKeyB, []string{redisKeyB}, []byte("b")) {
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
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
	if !cache.Set(cacheKey, []string{testCSCNamespacedKey(0, "k")}, []byte("$1\r\nv\r\n")) {
		t.Fatal("failed to seed cache")
	}
	cancel()

	if err := c.processCached(ctx, cmd); !errors.Is(err, context.Canceled) {
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
	if !cache.Set(cacheKey, []string{testCSCNamespacedKey(0, "missing")}, []byte("$-1\r\n")) {
		t.Fatal("failed to seed negative cache entry")
	}

	if err := c.processCached(ctx, cmd); err != Nil {
		t.Fatalf("negative cache hit: got %v, want redis.Nil", err)
	}
	if cache.Len() != 1 {
		t.Fatal("a valid redis.Nil cache hit must not be deleted")
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
	if !cache.(ConnOwnedCache).FulfillOwned("get:k", token, connID, []byte("v")) {
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
