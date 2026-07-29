package redis

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

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
		evictor: cache.(ConnOwnedCache),
		initGen: make(map[uint64]uint64),
	}
	hook.bumpInitGen(cn.GetID())
	token, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.(ConnOwnedCache).FulfillOwned("get:k", token, cn.GetID(), []byte("v")) {
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

// TestClone_SharesHookButOnlyOwnerDeregisters: a clone copies cscPoolHook (so it
// can attribute fetches to the shared eviction hook) but must not deregister it
// on Close; only the owner (the client holding the drain handle) does.
func TestClone_SharesHookButOnlyOwnerDeregisters(t *testing.T) {
	rp := &recordingHookPool{}
	owner := &baseClient{
		opt:            &Options{},
		connPool:       rp,
		cscPoolHook:    &cscEvictOnRemoveHook{},
		cscDrainHandle: &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})},
	}
	close(owner.cscDrainHandle.done) // so the owner's join doesn't block

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
