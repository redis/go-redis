package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

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

// TestBaseClientClone_CarriesCSCPointers: clone() must carry every CSC
// lifecycle pointer — dropping cscBcastReady disables the Broadcast
// sidecar-down bypass on WithTimeout clones; dropping cscPerConnState
// detaches them from the per-connection dispatch table.
func TestBaseClientClone_CarriesCSCPointers(t *testing.T) {
	var ready atomic.Bool
	owner := &baseClient{opt: &Options{}}
	state := newPerConnState(CacheConfig{MaxEntries: 16}, 0, owner)
	c := &baseClient{
		opt:             &Options{},
		csc:             NewLocalCache(CacheConfig{MaxEntries: 16}),
		cscBcastReady:   &ready,
		cscPerConnState: state,
	}
	// Owner-only fields must NOT be carried: a derived client's Close must not
	// stop the owner's drainer/sidecar or flush its cache.
	c.cscOwnsCache = true
	c.cscDrainHandle = &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	c.cscSidecar = &broadcastSidecar{}

	cl := c.clone()
	if cl.cscBcastReady != &ready {
		t.Fatal("clone dropped cscBcastReady")
	}
	if cl.cscPerConnState != state {
		t.Fatal("clone dropped cscPerConnState")
	}
	if cl.csc == nil {
		t.Fatal("clone dropped csc")
	}
	if cl.cscOwnsCache {
		t.Fatal("clone must not copy cscOwnsCache (owner-only)")
	}
	if cl.cscDrainHandle != nil {
		t.Fatal("clone must not copy cscDrainHandle (owner-only)")
	}
	if cl.cscSidecar != nil {
		t.Fatal("clone must not copy cscSidecar (owner-only)")
	}
}

// TestPerConn_DerivedCloseDoesNotWipeParent: closing a derived client that
// shares the per-conn state must not flush the owner's cache table; only the
// owner's Close tears it down.
func TestPerConn_DerivedCloseDoesNotWipeParent(t *testing.T) {
	ownerOpt := &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategyPerConnection}
	owner := &baseClient{opt: ownerOpt}
	state := newPerConnState(CacheConfig{MaxEntries: 16}, 0, owner)
	owner.cscPerConnState = state
	state.putCache(1, NewLocalCache(CacheConfig{MaxEntries: 16}))

	derived := &baseClient{opt: ownerOpt}
	owner.cscPerConnPropagateTo(derived)
	if derived.cscPerConnState != state {
		t.Fatal("propagation did not share the per-conn state")
	}

	derived.cscPerConnOnClose()
	if got := state.len(); got != 1 {
		t.Fatalf("derived close wiped the owner's per-conn caches: len=%d want 1", got)
	}

	owner.cscPerConnOnClose()
	if got := state.len(); got != 0 {
		t.Fatalf("owner close must tear down the caches: len=%d want 0", got)
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

// TestAttachCSC_StrategyGates: PerConnection keeps the shared cache nil, and
// SharedTracking refuses poolers without idle-conn draining (a sticky pool
// serving hits would be unboundedly stale — nothing applies invalidations).
func TestAttachCSC_StrategyGates(t *testing.T) {
	ctx := context.Background()

	perConn := &baseClient{
		opt:           &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategyPerConnection},
		pushProcessor: push.NewProcessor(),
	}
	perConn.attachCSC(ctx, NewLocalCache(CacheConfig{MaxEntries: 16}))
	if perConn.csc != nil {
		t.Fatal("attachCSC must keep the shared cache nil under CSCStrategyPerConnection")
	}

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
		{"broadcast pool conns do not track", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg, ClientSideCacheStrategy: CSCStrategyBroadcast}}, false},
		{"non-zero db", &baseClient{opt: &Options{Protocol: 3, ClientSideCacheConfig: cfg, DB: 1}}, false},
		{"no csc configured", &baseClient{opt: &Options{Protocol: 3}}, false},
	}
	for _, tc := range cases {
		if got := tc.c.cscTrackingRequested(); got != tc.want {
			t.Errorf("%s: cscTrackingRequested() = %v, want %v", tc.name, got, tc.want)
		}
	}

	perConn := &baseClient{opt: &Options{Protocol: 3, ClientSideCacheStrategy: CSCStrategyPerConnection}}
	perConn.cscPerConnState = newPerConnState(CacheConfig{MaxEntries: 16}, 0, perConn)
	if !perConn.cscTrackingRequested() {
		t.Error("per-connection state must enable tracking")
	}
}

// TestDerivedClients_ShareCSCUnderBroadcast: Client.Conn() and WithTimeout
// clones must share the parent's cache AND its sidecar-readiness gate.
func TestDerivedClients_ShareCSCUnderBroadcast(t *testing.T) {
	srv := startFakeSidecarServer(t, true)
	client := NewClient(&Options{
		Addr:                    srv.addr,
		Protocol:                3,
		ClientSideCacheConfig:   &ClientSideCacheConfig{MaxEntries: 16},
		ClientSideCacheStrategy: CSCStrategyBroadcast,
	})
	defer client.Close()

	if client.baseClient.csc == nil {
		t.Fatal("parent CSC not attached")
	}
	if client.baseClient.cscBcastReady == nil {
		t.Fatal("parent readiness gate not set")
	}

	conn := client.Conn()
	defer conn.Close()
	if conn.baseClient.csc != client.baseClient.csc {
		t.Fatal("derived Conn must share the parent's cache under Broadcast")
	}
	if conn.baseClient.cscBcastReady != client.baseClient.cscBcastReady {
		t.Fatal("derived Conn must share the parent's readiness gate")
	}

	wt := client.WithTimeout(time.Second)
	if wt.baseClient.csc != client.baseClient.csc {
		t.Fatal("WithTimeout clone must share the parent's cache")
	}
	if wt.baseClient.cscBcastReady != client.baseClient.cscBcastReady {
		t.Fatal("WithTimeout clone must share the parent's readiness gate")
	}
}

// TestBroadcastSidecar_StartRetriesInBackground: a transient dial failure at
// construction must not disable CSC forever — the read loop owns the first
// connect with the same backoff as a mid-life reconnect.
func TestBroadcastSidecar_StartRetriesInBackground(t *testing.T) {
	srv := startFakeSidecarServer(t, true)
	opt := fakeSidecarOptions(srv.addr)
	realDialer := opt.Dialer
	var remainingFails atomic.Int32
	remainingFails.Store(2)
	opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if remainingFails.Add(-1) >= 0 {
			return nil, errors.New("transient dial failure")
		}
		return realDialer(ctx, network, addr)
	}

	s := newBroadcastSidecar(opt, NewLocalCache(CacheConfig{MaxEntries: 16}), 0)
	s.backoffInitial = 10 * time.Millisecond
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start must not fail on a transient dial error: %v", err)
	}
	defer s.Shutdown()

	if s.ready.Load() {
		t.Fatal("sidecar must not report ready before the background connect succeeds")
	}
	deadline := time.After(3 * time.Second)
	for !s.ready.Load() {
		select {
		case <-deadline:
			t.Fatal("sidecar never became ready via background retry")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
