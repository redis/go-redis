package redis

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// cscRegisterCleanups arranges for a client dropped without Close to still stop
// its background CSC drainer goroutine. The cleanup captures only the handle
// (never *Client, which the goroutine doesn't reference) so the wrapper can be
// collected, and only signals stop (a GC cleanup must not block); the signal is
// idempotent, so a later explicit Close is safe.
func cscRegisterCleanups(c *Client) {
	h := c.baseClient.cscDrainHandle
	if h == nil {
		return
	}
	// Capture cscActive (a standalone *atomic.Bool, not *Client) so the cleanup
	// also stops clones from serving once the drainer is gone.
	active := c.baseClient.cscActive
	// For an owned cache, also release the handler binding on GC (see
	// invalidateHandler): capture the handler and cache, never *Client.
	// releaseIfBoundTo is a tiny leaf-mutex op, safe in a cleanup.
	var ih *invalidateHandler
	var ownedCache Cache
	if c.baseClient.cscOwnsCache {
		ih = lookupInvalidateHandler(c.baseClient.pushProcessor)
		ownedCache = c.baseClient.csc
	}
	runtime.AddCleanup(c, func(h *cscDrainHandle) {
		h.signalStop()
		if active != nil {
			active.Store(false)
		}
		if ih != nil && ownedCache != nil {
			ih.releaseIfBoundTo(ownedCache)
		}
	}, h)
}

// commandHits / commandMisses count cache outcomes once per processCached call,
// regardless of how many internal cache.Get probes it issues.
var (
	commandHits   atomic.Uint64
	commandMisses atomic.Uint64

	// commandCacheRejects counts failed cache fills (Fulfill returned false):
	// capacity rejections, but also reservations lost to a racing invalidation
	// or a stale-fetch takeover. A high rate with a low hit rate CAN mean
	// MaxMemoryBytes is too small for the reply sizes (each shard admits at
	// most its 1/16 share) — but on write-heavy keys race losses dominate, so
	// it is not a pure capacity signal.
	commandCacheRejects atomic.Uint64
)

// CacheAdmissionRejects returns the cumulative count of failed cache fills
// since process start: capacity rejections plus fills lost to a racing
// invalidation or reservation takeover. On write-heavy keys the race losses
// dominate, so a high value does not by itself mean the cache is undersized.
// Process-wide, like CommandStats.
func CacheAdmissionRejects() uint64 {
	return commandCacheRejects.Load()
}

// CommandStats returns the cumulative count of CSC-served-command hits and
// misses since process start.
func CommandStats() (hits, misses uint64) {
	return commandHits.Load(), commandMisses.Load()
}

// ClientSideCacheConfig configures the built-in client-side cache. Pass a
// non-nil value to Options.ClientSideCacheConfig to enable caching on a RESP3
// client.
type ClientSideCacheConfig = CacheConfig

const (
	invalidatePushName = "invalidate"
	// cscNamespaceSep separates the DB-number prefix from the key. NUL is a
	// legal byte in Redis keys, but CSC is restricted to DB 0 (see attachCSC),
	// so a collision requires a key starting with "0\x00" — out of scope.
	cscNamespaceSep = "\x00"
)

// dbNamespacedKey prefixes key with its database number so entries and
// invalidation indexes do not collide across SELECTed databases.
func dbNamespacedKey(db int, key string) string {
	return strconv.Itoa(db) + cscNamespaceSep + key
}

// invalidateHandler propagates RESP3 "invalidate" push notifications into the
// shared client-side cache. The db field scopes incoming key names so a shared
// cache is not cross-evicted by clients pointing at a different DB.
//
// The binding (cache, db) is mutable under mu: the owning client's teardown
// RELEASES it (cache=nil) instead of unregistering the handler, so the handler
// can stay registered protected — application code holding the processor
// cannot silently unregister invalidation out from under a live client — while
// a successor client on the same processor can still rebind it (see
// registerInvalidateHandler).
type invalidateHandler struct {
	mu    sync.RWMutex
	cache Cache
	db    int
}

// HandlePushNotification decodes ["invalidate", <keys>] notifications. A nil
// <keys> payload is emitted on FLUSHDB/FLUSHALL and triggers a full cache flush.
func (h *invalidateHandler) HandlePushNotification(
	_ context.Context, _ push.NotificationHandlerContext, notification []interface{},
) error {
	h.mu.RLock()
	cache, db := h.cache, h.db
	h.mu.RUnlock()
	if cache == nil || len(notification) < 2 {
		return nil
	}

	switch payload := notification[1].(type) {
	case nil:
		cache.Flush()
	case []interface{}:
		for _, k := range payload {
			var name string
			switch v := k.(type) {
			case string:
				name = v
			case []byte:
				name = string(v)
			default:
				continue
			}
			cache.DeleteByRedisKey(dbNamespacedKey(db, name))
		}
	}
	return nil
}

// releaseIfBoundTo unbinds the handler from cache (making it rebindable by a
// successor client) if that is still its current binding. Owner-teardown only;
// no-op when the binding has already changed.
func (h *invalidateHandler) releaseIfBoundTo(cache Cache) {
	h.mu.Lock()
	if h.cache == cache {
		h.cache = nil
	}
	h.mu.Unlock()
}

// errInvalidateHandlerBound: piggybacking on a handler bound to a live
// different cache would leave the new cache uninvalidated.
var errInvalidateHandlerBound = errors.New(`csc: a different "invalidate" push handler is already registered`)

// bindTo binds the handler to (cache, db). Success when that is already the
// binding (a derived Client.Conn sharing the parent's processor and cache) or
// when the handler was released by a previous owner's teardown (rebind);
// errInvalidateHandlerBound otherwise.
func (h *invalidateHandler) bindTo(cache Cache, db int) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case h.cache == cache && h.db == db:
		return nil
	case h.cache == nil:
		h.cache, h.db = cache, db
		return nil
	default:
		return errInvalidateHandlerBound
	}
}

// lookupInvalidateHandler returns the processor's CSC invalidate handler, nil
// when absent or foreign.
func lookupInvalidateHandler(p push.NotificationProcessor) *invalidateHandler {
	if p == nil {
		return nil
	}
	h, _ := p.GetHandler(invalidatePushName).(*invalidateHandler)
	return h
}

func registerInvalidateHandler(p push.NotificationProcessor, cache Cache, db int) error {
	if p == nil || cache == nil {
		return nil
	}
	if existing := p.GetHandler(invalidatePushName); existing != nil {
		h, ok := existing.(*invalidateHandler)
		if !ok {
			return errInvalidateHandlerBound
		}
		return h.bindTo(cache, db)
	}
	// VoidProcessor (RESP2) returns an error here; the caller treats it as
	// "CSC not available" rather than fatal. Registered PROTECTED: application
	// code holding the processor must not be able to unregister invalidation
	// under a live client (that would serve unbounded-stale hits with no
	// signal); owner teardown releases the BINDING instead of the handler.
	return p.RegisterHandler(invalidatePushName, &invalidateHandler{cache: cache, db: db}, true)
}

// attachCSC dispatches to the invalidation strategy in
// Options.ClientSideCacheStrategy. Safe with a nil cache; on failure c.csc stays
// nil and commands fall back to normal round-trips. Adding a strategy: a new
// CSCStrategy constant plus cases in Options.init, here, and (if it doesn't track
// on pool conns) cscStrategyTracksPoolConns.
func (c *baseClient) attachCSC(ctx context.Context, cache Cache) {
	if cache == nil || c.opt.Protocol != 3 {
		return
	}
	switch c.opt.ClientSideCacheStrategy {
	case CSCStrategySharedTracking:
		c.attachSharedTrackingCSC(ctx, cache)
	default:
		// Options.init clamps unknown strategies to SharedTracking; delegate anyway.
		c.attachSharedTrackingCSC(ctx, cache)
	}
}

// attachSharedTrackingCSC wires SharedTracking: one shared cache, per-conn CLIENT
// TRACKING, a background drainer, and the owning-conn eviction hook. DB-0 only:
// tracking is bound to the conn's DB and a runtime SELECT does not re-key it.
func (c *baseClient) attachSharedTrackingCSC(ctx context.Context, cache Cache) {
	if c.opt.DB != 0 {
		internal.Logger.Printf(ctx,
			"csc: client-side caching is restricted to DB 0; disabling CSC for client configured with DB=%d. "+
				"Use one client per DB if you need caching against non-zero databases.", c.opt.DB)
		return
	}
	// A pooler without idle-conn draining (e.g. Client.Conn's StickyConnPool)
	// can't apply buffered invalidations, so stay uncached.
	if _, ok := c.connPool.(idleConnDrainer); !ok {
		return
	}
	// SharedTracking needs per-conn attribution to evict a connection's entries on
	// close; disable CSC for a cache that lacks it rather than serve un-evictable
	// entries.
	if _, ok := cache.(ConnOwnedCache); !ok {
		internal.Logger.Printf(ctx,
			"csc: disabling client-side caching: ClientSideCache must implement redis.ConnOwnedCache "+
				"(FulfillOwned/EvictByConn); use redis.NewLocalCache or implement it.")
		return
	}
	if err := registerInvalidateHandler(c.pushProcessor, cache, c.opt.DB); err != nil {
		internal.Logger.Printf(ctx, "csc: failed to register invalidate handler: %v", err)
		return
	}
	c.csc = cache
	c.startBackgroundDrainer()
	c.registerConnEvictHook(cache)
}

// cscStrategyTracksPoolConns reports whether the strategy issues CLIENT TRACKING
// ON on pool conns. SharedTracking does; a future sidecar strategy would not.
func (c *baseClient) cscStrategyTracksPoolConns() bool {
	switch c.opt.ClientSideCacheStrategy {
	case CSCStrategySharedTracking:
		return true
	default:
		return true
	}
}

// cscHook returns the shared evict-on-remove hook, nil when CSC is off or the
// pool doesn't support hooks.
func (c *baseClient) cscHook() *cscEvictOnRemoveHook {
	h, _ := c.cscPoolHook.(*cscEvictOnRemoveHook)
	return h
}

// cscInstallConnCloseHook evicts cn's owned entries on any close — including the
// ConnMaxLifetime/idle retirement path (CloseConn) that bypasses the OnRemove
// hook — so entries don't outlive the server tracking dropped at close. Uses the
// onCscClose slot so it doesn't clobber streaming-credentials cleanup.
func (c *baseClient) cscInstallConnCloseHook(cn *pool.Conn) {
	cn.SetOnCscClose(func() error {
		c.cscOnConnClose(cn.GetID())
		return nil
	})
}

// cscOnConnClose evicts a closing conn's entries: via the shared hook (which
// records the removed-ring, closing the close-before-fulfill race), else scoped
// EvictByConn on the owning cache. CSC only attaches for ConnOwnedCache caches,
// so no full-flush fallback is needed.
func (c *baseClient) cscOnConnClose(connID uint64) {
	if h := c.cscHook(); h != nil {
		h.markRemoved(connID)
		return
	}
	if owner, ok := c.csc.(ConnOwnedCache); ok {
		owner.EvictByConn(connID)
	}
}

// poolHookRegistrar is the *pool.ConnPool subset used to (de)register the
// evict-on-remove hook. Without it, close-time eviction still runs via
// cscOnConnClose (but without the OnRemove path's removed-ring).
type poolHookRegistrar interface {
	AddPoolHook(hook pool.PoolHook)
	RemovePoolHook(hook pool.PoolHook)
}

// cscEvictOnRemoveHook evicts a connection's owned entries when the pool removes
// it (the server stops delivering their invalidations — Window 2), and tracks
// per-conn init generations so fulfillCached can catch a value whose owning
// conn was removed or re-initialized mid-fetch (see coverageLostSince).
type cscEvictOnRemoveHook struct {
	evictor ConnOwnedCache

	mu sync.Mutex
	// initGen counts a live conn's socket (re)initializations: bumped by
	// cscEvictOwnedEntries before its eviction (first init included, so every
	// serving conn has gen >= 1), deleted on removal/close. fulfillCached
	// compares it with the generation captured at reply time.
	initGen map[uint64]uint64
}

func (h *cscEvictOnRemoveHook) OnGet(_ context.Context, _ *pool.Conn, _ bool) (bool, error) {
	return true, nil
}

func (h *cscEvictOnRemoveHook) OnPut(_ context.Context, _ *pool.Conn) (shouldPool, shouldRemove bool, err error) {
	return true, false, nil
}

func (h *cscEvictOnRemoveHook) OnRemove(_ context.Context, cn *pool.Conn, _ error) {
	if cn == nil {
		return
	}
	h.markRemoved(cn.GetID())
}

// markRemoved forgets connID's generation, then evicts. Forgetting before
// evicting lets a racing fulfillCached see the change (a served conn's captured
// generation is >= 1, an absent entry reads 0) and drop an entry created after
// the eviction — closing the close-before-fulfill race.
func (h *cscEvictOnRemoveHook) markRemoved(connID uint64) {
	h.forgetConn(connID)
	h.evictor.EvictByConn(connID)
}

// bumpInitGen records that connID's socket (and with it, the server-side
// tracking table) was replaced. Called before the re-init eviction so a racing
// fulfillCached that captured the pre-bump generation cannot publish an
// uncovered entry unnoticed.
func (h *cscEvictOnRemoveHook) bumpInitGen(connID uint64) {
	h.mu.Lock()
	if h.initGen == nil {
		h.initGen = make(map[uint64]uint64)
	}
	h.initGen[connID]++
	h.mu.Unlock()
}

// initGenOf returns connID's current init generation (0 if never bumped).
func (h *cscEvictOnRemoveHook) initGenOf(connID uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.initGen[connID]
}

// forgetConn drops connID's init-generation entry: the conn was removed/closed,
// or its init failed before ever serving (the pubsub path would otherwise leak
// the entry — no OnRemove hook, close hook not yet installed).
func (h *cscEvictOnRemoveHook) forgetConn(connID uint64) {
	h.mu.Lock()
	delete(h.initGen, connID)
	h.mu.Unlock()
}

// coverageLostSince reports whether connID's invalidation coverage changed
// after gen was captured at reply time: a removal deletes the entry (reads 0)
// and a handoff/reauth re-init bumps it — either way an entry fetched on the
// old socket receives no invalidations and must not stay cached. Relies on the
// invariant that every serving conn's captured gen is >= 1 (first init bumps;
// Conn/Tx/clone all carry the hook, so derived-client inits bump too).
func (h *cscEvictOnRemoveHook) coverageLostSince(connID, gen uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.initGen[connID] != gen
}

// registerConnEvictHook wires the OnRemove eviction hook when the cache supports
// owning-conn attribution and the pool supports hooks. No-op otherwise; close-time
// eviction still runs via cscOnConnClose.
func (c *baseClient) registerConnEvictHook(cache Cache) {
	owner, ok := cache.(ConnOwnedCache)
	if !ok {
		return
	}
	reg, ok := c.connPool.(poolHookRegistrar)
	if !ok {
		return
	}
	h := &cscEvictOnRemoveHook{evictor: owner, initGen: make(map[uint64]uint64)}
	reg.AddPoolHook(h)
	c.cscPoolHook = h
}

// cscEvictOwnedEntries evicts connID's entries on (re)init/handoff, where the
// socket (and its server tracking) is replaced but the conn id keeps serving. It
// prefers the shared hook (so Conn/Tx, which carry it but have a nil csc, still
// evict from the parent cache). Scoped only — no removed-ring (the conn keeps
// serving, and the ring never ages out); the fulfill-vs-re-init race is closed
// by the init-generation bump instead. No custom-cache flush (this also runs on
// first init).
func (c *baseClient) cscEvictOwnedEntries(connID uint64) {
	if h := c.cscHook(); h != nil {
		h.bumpInitGen(connID)
		h.evictor.EvictByConn(connID)
		return
	}
	if c.csc == nil {
		return
	}
	if owner, ok := c.csc.(ConnOwnedCache); ok {
		owner.EvictByConn(connID)
	}
}

// cscFetchCapture receives, from the successful attempt's reply read — while
// the serving connection is still held — everything the CSC fetch path needs to
// attribute the cached entry: the raw RESP reply, the conn id, and the conn's
// CSC init generation. The generation must be captured before the conn is
// released: a handoff queued at Put can re-init the socket (bumping the
// generation) before fulfillCached runs (see coverageLostSince).
type cscFetchCapture struct {
	raw     []byte
	connID  uint64
	initGen uint64
}

// cscConnInitGen returns connID's CSC init generation, captured by _process at
// reply time (while the conn is still held) and compared by fulfillCached via
// coverageLostSince. Zero without an active evict-on-remove hook.
func (c *baseClient) cscConnInitGen(connID uint64) uint64 {
	if h := c.cscHook(); h != nil {
		return h.initGenOf(connID)
	}
	return 0
}

// cscForgetConn drops connID's init-generation entry after a FAILED init (the
// conn never serves; see forgetConn for why the pubsub path would leak it).
func (c *baseClient) cscForgetConn(connID uint64) {
	if h := c.cscHook(); h != nil {
		h.forgetConn(connID)
	}
}

// errClientTrackingWithCSC rejects CLIENT TRACKING on clients with built-in CSC
// (see the guards in baseClient.process and generalProcessPipeline). The raw
// escape hatches — Do(ctx, "client", "tracking", ...) with string or []byte
// args, and pipelines — are also caught: the guard matches on the command's
// leading args, not the typed method.
var errClientTrackingWithCSC = errors.New(
	"redis: CLIENT TRACKING is not allowed when client-side caching is enabled")

// cscRejectsClientTracking reports whether this client must reject user-issued
// CLIENT TRACKING commands: CSC is (or per options, should be) tracking pool
// conns, and this is not initConn's exempt internal conn.
func (c *baseClient) cscRejectsClientTracking() bool {
	return (c.csc != nil || c.cscTrackingRequested()) && !c.allowClientTracking
}

// cscDrainHandle holds the drainer goroutine's lifecycle channels: stop signals
// shutdown; done is closed on exit so stopBackgroundDrainer can join.
type cscDrainHandle struct {
	stop         chan struct{}
	done         chan struct{}
	stopOnce     sync.Once
	teardownOnce sync.Once
}

// signalStop closes stop at most once (so Close and the AddCleanup safety net
// can't double-close) and does not join — a GC cleanup must not block.
func (h *cscDrainHandle) signalStop() {
	h.stopOnce.Do(func() { close(h.stop) })
}

// cscMinDrainInterval floors a user-supplied DrainInterval: sub-millisecond
// timers are unreliable (https://github.com/golang/go/issues/53824).
const cscMinDrainInterval = time.Millisecond

// cscDrainInterval returns DrainInterval clamped to cscMinDrainInterval, or the
// default (cscDrainSkipWindow) when unset.
func (c *baseClient) cscDrainInterval() time.Duration {
	if cfg := c.opt.ClientSideCacheConfig; cfg != nil && cfg.DrainInterval > 0 {
		if cfg.DrainInterval < cscMinDrainInterval {
			return cscMinDrainInterval
		}
		return cfg.DrainInterval
	}
	return cscDrainSkipWindow
}

// idleConnDrainer is the pooler capability the drainer needs (*pool.ConnPool has
// it). attachSharedTrackingCSC leaves a pooler without it uncached, rather than
// serve entries nothing would invalidate.
type idleConnDrainer interface {
	DrainIdleConns(ctx context.Context, st *pool.DrainState, fn func(cn *pool.Conn) error)
}

// startBackgroundDrainer launches the per-client invalidation drainer: each tick
// runs one pool.DrainIdleConns pass, draining idle conns' buffered push frames.
// No-op for poolers that don't implement idleConnDrainer.
func (c *baseClient) startBackgroundDrainer() {
	cp, ok := c.connPool.(idleConnDrainer)
	if !ok {
		return
	}
	if c.cscDrainHandle != nil {
		return // already running (startBackgroundDrainer runs once, in NewClient)
	}
	h := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	c.cscDrainHandle = h
	active := &atomic.Bool{}
	active.Store(true)
	c.cscActive = active
	interval := c.cscDrainInterval()
	// Custom-processor drain errors are connection-fatal (drainPushNotifications),
	// so a PERSISTENTLY failing custom processor would turn every tick into a
	// conn removal + redial — a sustained dial storm. Damping: after
	// cscDrainCustomErrCap consecutive fatal custom-processor drains, disable
	// CSC serving and stop the drainer (with one log line) instead of churning.
	// Built-in processor errors are real conn desyncs and are never damped.
	_, builtinProc := c.pushProcessor.(*push.Processor)
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// st persists round/visited across ticks; single-goroutine, no lock.
		var st pool.DrainState
		consecFatal := 0
		drain := func(cn *pool.Conn) error {
			processed, err := c.drainPushNotifications(cn)
			switch {
			case err != nil:
				consecFatal++
			case processed:
				// Only a drain that actually consumed data proves the
				// processor works. Conns with nothing buffered — including
				// the fresh redial replacing each removed conn — must not
				// reset the counter, or the damping never trips.
				consecFatal = 0
			}
			return err
		}
		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
				// ctx bounds the whole pass; the drain read has its own hard deadline.
				cycleCtx, cancel := context.WithTimeout(context.Background(), interval/2)
				cp.DrainIdleConns(cycleCtx, &st, drain)
				cancel()
				if !builtinProc && consecFatal >= cscDrainCustomErrCap {
					internal.Logger.Printf(context.Background(),
						"csc: disabling client-side caching: the custom push notification processor failed %d consecutive drains "+
							"(each failure removes a connection because the reader may be mid-frame); "+
							"caching cannot be kept fresh safely with this processor", consecFatal)
					active.Store(false)
					return
				}
			}
		}
	}()
}

// stopBackgroundDrainer joins the drainer goroutine, deregisters the evict hook,
// and flushes an owned cache. Owner-only: clones have no handle and return early.
// The fields are never cleared here — fulfillCached reads cscPoolHook on the hot
// path, so niling under a concurrent Close would race; teardownOnce makes repeat
// Close idempotent instead.
func (c *baseClient) stopBackgroundDrainer() {
	h := c.cscDrainHandle
	if h == nil {
		return
	}
	h.teardownOnce.Do(func() {
		// Stop serving cache hits on any clone before the drainer is gone.
		if c.cscActive != nil {
			c.cscActive.Store(false)
		}
		// Owner only. The hook (when present) is always registered alongside the
		// drainer, so removing it here — before the pool is closed — can't strand it.
		if c.cscPoolHook != nil {
			if reg, ok := c.connPool.(poolHookRegistrar); ok {
				reg.RemovePoolHook(c.cscPoolHook)
			}
		}
		h.signalStop()
		<-h.done
		// Owned cache: release the handler binding so a successor client on
		// the same processor can rebind (see invalidateHandler). A shared
		// explicit cache (cscOwnsCache false) may still serve other clients,
		// so its binding is left in place.
		if c.cscOwnsCache {
			if ih := lookupInvalidateHandler(c.pushProcessor); ih != nil {
				ih.releaseIfBoundTo(c.csc)
			}
		}
		if c.cscOwnsCache && c.csc != nil {
			c.csc.Flush()
		}
	})
}

// applyCachedReply populates cmd from a previously captured raw RESP reply by
// replaying it through the command's own readReply.
func applyCachedReply(cmd Cmder, raw []byte) error {
	return cmd.readReply(proto.NewReader(bytes.NewReader(raw)))
}

// cscDrainSkipWindow is the default SharedTracking drain period (overridable via
// ClientSideCacheConfig.DrainInterval). A buffered invalidation is picked up within
// roughly one round; MaxStaleness, when configured, is the hard time-based backstop.
const cscDrainSkipWindow = 5 * time.Millisecond

// cscDrainHardReadCap is the hard socket read deadline the drainer applies via
// Conn.WithReaderHardDeadline. It bounds only a rare partial-frame mid-read. A
// var (not const) so the tuning harness can sweep it.
var cscDrainHardReadCap = 50 * time.Microsecond

// cscDrainCustomErrCap is the number of CONSECUTIVE fatal custom-processor
// drain errors after which the drainer disables CSC instead of removing (and
// redialing) a connection per tick indefinitely.
const cscDrainCustomErrCap = 8

// processCached runs the Get-Reserve-Fulfill lifecycle for a cacheable command.
// Only invoked after process has verified that CSC is active and cmd is
// eligible.
func (c *baseClient) processCached(ctx context.Context, cmd Cmder) error {
	// Once the drainer has stopped (owner Close, or the owner dropped without
	// Close), no invalidations flow — a surviving clone must not serve stale hits.
	if a := c.cscActive; a != nil && !a.Load() {
		return c.processWithRetry(ctx, cmd, nil)
	}

	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		return c.processWithRetry(ctx, cmd, nil)
	}

	redisKeys := extractRedisKeys(cmd)
	if len(redisKeys) == 0 {
		// Without a key list we cannot react to invalidations for this command.
		return c.processWithRetry(ctx, cmd, nil)
	}

	db := c.opt.DB
	key := dbNamespacedKey(db, rawKey)
	nsRedisKeys := make([]string, len(redisKeys))
	for i, k := range redisKeys {
		nsRedisKeys[i] = dbNamespacedKey(db, k)
	}

	// Serve hits straight from the cache.
	if data, ok := c.csc.Get(ctx, key); ok {
		if err := applyCachedReply(cmd, data); err == nil {
			commandHits.Add(1)
			return nil
		}
		c.csc.DeleteByCacheKey(key)
	}

	token, shouldFetch := c.csc.Reserve(key, nsRedisKeys)
	if !shouldFetch {
		// Another goroutine is fetching; Reserve blocks until it completes.
		if data, ok := c.csc.Get(ctx, key); ok {
			if err := applyCachedReply(cmd, data); err == nil {
				commandHits.Add(1)
				return nil
			}
			c.csc.DeleteByCacheKey(key)
		}
		// Original fetcher cancelled or its value was invalidated; try to take
		// over so later waiters still benefit from the cache.
		token, shouldFetch = c.csc.Reserve(key, nsRedisKeys)
	}

	var fc cscFetchCapture
	var capture *cscFetchCapture
	if shouldFetch {
		capture = &fc
		// Release the placeholder if processWithRetry panics; Cancel on a
		// stale token is a no-op.
		defer func() {
			if capture != nil {
				c.csc.Cancel(key, token)
			}
		}()
	}

	err := c.processWithRetry(ctx, cmd, capture)

	if shouldFetch {
		capture = nil // disarm the deferred Cancel
		if err == nil {
			if !c.fulfillCached(key, token, &fc) {
				commandCacheRejects.Add(1)
			}
		} else {
			c.csc.Cancel(key, token)
		}
	}
	// Count a miss only when the command completed; a network/command error is
	// a failure, not a miss, and would skew the hit-rate metric.
	if err == nil {
		commandMisses.Add(1)
	}
	return err
}

// fulfillCached stores a fetched value, attributing it to its serving conn when
// an evict-on-remove hook is active so EvictByConn can drop it if that conn is
// removed. It also closes the attribute-vs-coverage races: the conn is released
// before this runs, so its OnRemove eviction — or a handoff re-init's scoped
// eviction — may fire before the entry exists. After attributing we re-check
// (removed-ring and init generation, via coverageLostSince) against the state
// captured at reply time, and drop the entry if its invalidation coverage was
// lost in between.
func (c *baseClient) fulfillCached(key string, token uint64, fc *cscFetchCapture) bool {
	if hook := c.cscHook(); hook != nil {
		if fc.connID == 0 {
			// Invariant: an active hook always gets a real conn id (>=1). A zero id
			// would leave the entry unattributed and un-evictable, so fail closed.
			c.csc.Cancel(key, token)
			return false
		}
		owner := hook.evictor
		done := owner.FulfillOwned(key, token, fc.connID, fc.raw)
		if done && hook.coverageLostSince(fc.connID, fc.initGen) {
			// Scoped to this key: EvictByConn here would also drop entries the
			// conn's NEW socket legitimately tracks after a handoff re-init.
			c.csc.DeleteByCacheKey(key)
		}
		return done
	}
	return c.csc.Fulfill(key, token, fc.raw)
}
