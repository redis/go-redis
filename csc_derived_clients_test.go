package redis

import (
	"context"
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
	c.cscPoolHook = hook
	c.cscOwnsCache = true
	c.cscDrainHandle = &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}

	cl := c.clone()
	if cl.csc == nil {
		t.Fatal("clone dropped csc")
	}
	if cl.cscPoolHook != hook {
		t.Fatal("clone must copy cscPoolHook (needed for attribution)")
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

	if err := registerInvalidateHandler(proc, cache, 0); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := registerInvalidateHandler(proc, cache, 0); err != nil {
		t.Fatalf("re-registration of the same cache+db must succeed, got: %v", err)
	}
	if err := registerInvalidateHandler(proc, NewLocalCache(CacheConfig{MaxEntries: 16}), 0); err == nil {
		t.Fatal("registering a different cache on the same processor must fail")
	}
}

// plainPooler is a Pooler with no DrainIdleConns capability.
type plainPooler struct{ pool.Pooler }

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
}

// TestCSCTrackingRequested: tracking is derived from options so conns dialed
// by derived clients (sticky Conn, Tx) are never initialized untracked and
// then reused by the parent.
func TestCSCTrackingRequested(t *testing.T) {
	cfg := &ClientSideCacheConfig{MaxEntries: 16}
	cases := []struct {
		name string
		c    *baseClient
		want bool
	}{
		{"shared tracking with config", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}}, true},
		{"tx-style client: options set, csc nil", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg}}, true},
		{"resp2", &baseClient{opt: &Options{Protocol: 2, ClientSideCacheConfig: cfg}}, false},
		{"non-zero db", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg, DB: 1}}, false},
		{"no csc configured", &baseClient{opt: &Options{Protocol: 3}}, false},
	}
	for _, tc := range cases {
		if got := tc.c.cscTrackingRequested(); got != tc.want {
			t.Errorf("%s: cscTrackingRequested() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// recordingHookPool is a poolHookRegistrar that counts RemovePoolHook calls.
type recordingHookPool struct {
	pool.Pooler
	removed int
}

func (p *recordingHookPool) AddPoolHook(pool.PoolHook)    {}
func (p *recordingHookPool) RemovePoolHook(pool.PoolHook) { p.removed++ }

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
