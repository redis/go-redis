package redis

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// nonOwnerCache satisfies Cache (by embedding the interface) but deliberately
// does NOT implement ConnOwnedCache — it exposes none of FulfillOwned/EvictByConn.
type nonOwnerCache struct{ Cache }

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
	if c.fulfillCached("get:k", tok, 0, []byte("v")) {
		t.Fatal("fulfillCached must fail closed when an eviction hook is active and connID==0")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); ok {
		t.Fatal("unattributed entry must not be cached")
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

// TestCSCActive_GCOfOwnerStopsClonesServing: dropping the owner *Client without
// Close must (via the GC cleanup) stop a surviving WithTimeout clone from serving
// cached hits, since the drainer that fed the shared cache is gone.
func TestCSCActive_GCOfOwnerStopsClonesServing(t *testing.T) {
	clone, active := func() (*Client, *atomic.Bool) {
		owner := NewClient(&Options{
			Addr:                  "127.0.0.1:0",
			Protocol:              3,
			ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
		})
		cl := owner.WithTimeout(time.Second)
		// owner falls out of scope here (not Closed) and becomes collectible;
		// cscActive is a standalone pointer, so returning it keeps no *Client ref.
		return cl, owner.cscActive
	}()
	defer clone.Close()

	if active == nil {
		t.Fatal("precondition: cscActive must be set")
	}
	deadline := time.Now().Add(5 * time.Second)
	for active.Load() && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if active.Load() {
		t.Fatal("dropping the owner without Close must flip cscActive=false via the GC cleanup")
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
