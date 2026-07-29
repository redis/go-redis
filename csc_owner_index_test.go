package redis

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

type recordingConnOwnedCache struct {
	Cache
	owner        ConnOwnedCache
	fulfillCalls int
}

func (c *recordingConnOwnedCache) FulfillOwned(
	cacheKey string,
	token, ownerConnID uint64,
	value []byte,
) bool {
	c.fulfillCalls++
	return c.owner.FulfillOwned(cacheKey, token, ownerConnID, value)
}

func (c *recordingConnOwnedCache) EvictByConn(connID uint64) int {
	return c.owner.EvictByConn(connID)
}

func TestLocalCache_FulfillOwned_EvictByConn(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	owner, ok := cache.(ConnOwnedCache)
	if !ok {
		t.Fatal("localCache must implement ConnOwnedCache")
	}

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
	owner := cache.(ConnOwnedCache)
	lc := cache.(*localCache)

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
	owner := cache.(ConnOwnedCache)

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
	owner := cache.(ConnOwnedCache)
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
	derived := &baseClient{opt: &Options{Protocol: 3}, csc: nil, cscPoolHook: hook}

	const connID = uint64(9)
	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !cache.(ConnOwnedCache).FulfillOwned("get:k", tok, connID, []byte("v")) {
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
	owner := cache.(ConnOwnedCache)
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
		evictor: cache.(ConnOwnedCache),
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
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
	cache := &recordingConnOwnedCache{
		Cache: base,
		owner: base.(ConnOwnedCache),
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
	local := base.(*localCache)
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}

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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
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
	hook := &cscEvictOnRemoveHook{evictor: cache.(ConnOwnedCache)}
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

// TestFulfillCached_NoHookUsesPlainFulfill: without an evict-on-remove hook
// (e.g. a pooler without hooks), fulfillCached must fall back to plain Fulfill
// and still cache the value.
func TestFulfillCached_NoHookUsesPlainFulfill(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 64})
	c := &baseClient{opt: &Options{Protocol: 3}, csc: cache} // cscPoolHook nil

	tok, _ := cache.Reserve("get:k", []string{"k"})
	if !c.fulfillCached("get:k", tok, &cscFetchCapture{raw: []byte("v"), connID: 7}) {
		t.Fatal("fulfillCached should store via plain Fulfill when no hook")
	}
	if _, ok := cache.Get(context.Background(), "get:k"); !ok {
		t.Fatal("value should be cached")
	}
}
