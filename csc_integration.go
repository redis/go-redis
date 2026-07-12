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
// its background CSC goroutines. The cleanups capture only the handle/sidecar
// (never *Client, which the goroutines don't reference) so the wrapper can be
// collected, and only signal stop (a GC cleanup must not block); both signals
// are idempotent, so a later explicit Close is safe.
func cscRegisterCleanups(c *Client) {
	if h := c.baseClient.cscDrainHandle; h != nil {
		runtime.AddCleanup(c, func(h *cscDrainHandle) { h.signalStop() }, h)
	}
	if s := c.baseClient.cscSidecar; s != nil {
		runtime.AddCleanup(c, func(s *broadcastSidecar) { s.signalShutdown() }, s)
	}
}

// commandHits / commandMisses count cache outcomes once per processCached call,
// regardless of how many internal cache.Get probes it issues.
var (
	commandHits   atomic.Uint64
	commandMisses atomic.Uint64

	// commandCacheRejects counts admission rejections (Fulfill returned false).
	// A high rate with a low hit rate usually means MaxMemoryBytes is too small
	// for the reply sizes (each shard admits at most its 1/16 share).
	commandCacheRejects atomic.Uint64
)

// CacheAdmissionRejects returns the cumulative count of cache-admission
// rejections since process start. Process-wide, like CommandStats.
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
type invalidateHandler struct {
	cache Cache
	db    int
}

// HandlePushNotification decodes ["invalidate", <keys>] notifications. A nil
// <keys> payload is emitted on FLUSHDB/FLUSHALL and triggers a full cache flush.
func (h *invalidateHandler) HandlePushNotification(
	_ context.Context, _ push.NotificationHandlerContext, notification []interface{},
) error {
	if h.cache == nil || len(notification) < 2 {
		return nil
	}

	switch payload := notification[1].(type) {
	case nil:
		h.cache.Flush()
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
			h.cache.DeleteByRedisKey(dbNamespacedKey(h.db, name))
		}
	}
	return nil
}

func registerInvalidateHandler(p push.NotificationProcessor, cache Cache, db int) error {
	if p == nil || cache == nil {
		return nil
	}
	// A derived client (Client.Conn) shares the parent's processor, which
	// already routes "invalidate" to this exact cache: treat that as success so
	// the derived client shares the cache. A handler bound to a different cache
	// is an error (piggybacking would leave this cache uninvalidated).
	if existing := p.GetHandler(invalidatePushName); existing != nil {
		if h, ok := existing.(*invalidateHandler); ok && h.cache == cache && h.db == db {
			return nil
		}
		return errors.New("csc: a different \"invalidate\" push handler is already registered")
	}
	// VoidProcessor (RESP2) returns an error here; the caller treats it as
	// "CSC not available" rather than fatal.
	return p.RegisterHandler(invalidatePushName, &invalidateHandler{cache: cache, db: db}, true)
}

// attachCSC wires the cache and invalidate handler onto this baseClient. Safe
// with a nil cache; on registration failure c.csc stays nil (commands fall back
// to normal round-trips).
//
// CSC is DB-0 only: CLIENT TRACKING is bound to the connection's DB at tracking
// time and a runtime SELECT does not re-key the server's tracking table, so
// multi-DB use would serve stale data. Run one client per DB instead.
func (c *baseClient) attachCSC(ctx context.Context, cache Cache) {
	if cache == nil || c.opt.Protocol != 3 {
		return
	}
	// PerConnection owns the invalidate handler and keeps c.csc nil; wiring the
	// shared cache here would run two caching paths at once.
	if c.opt.ClientSideCacheStrategy == CSCStrategyPerConnection {
		return
	}
	if c.opt.DB != 0 {
		internal.Logger.Printf(ctx,
			"csc: client-side caching is restricted to DB 0; disabling CSC for client configured with DB=%d. "+
				"Use one client per DB if you need caching against non-zero databases.", c.opt.DB)
		return
	}
	// SharedTracking hits are only safe if something applies the invalidations
	// buffered on pool conns. A pooler without idle-conn draining (e.g. the
	// StickyConnPool behind Client.Conn) has no drainer, so stay uncached.
	// Broadcast is exempt: its sidecar delivers invalidations independently.
	if c.opt.ClientSideCacheStrategy == CSCStrategySharedTracking {
		if _, ok := c.connPool.(idleConnDrainer); !ok {
			return
		}
	}
	if err := registerInvalidateHandler(c.pushProcessor, cache, c.opt.DB); err != nil {
		internal.Logger.Printf(ctx, "csc: failed to register invalidate handler: %v", err)
		return
	}
	c.csc = cache
	// Only SharedTracking drains pool conns (Broadcast uses the sidecar;
	// PerConnection never reaches here).
	if c.opt.ClientSideCacheStrategy == CSCStrategySharedTracking {
		c.startBackgroundDrainer()
		c.registerConnEvictHook(cache)
	}
}

// poolHookRegistrar is the subset of *pool.ConnPool used to (de)register the
// evict-on-remove hook. Without it a pooler gets no per-conn eviction
// (bounded by MaxStaleness instead).
type poolHookRegistrar interface {
	AddPoolHook(hook pool.PoolHook)
	RemovePoolHook(hook pool.PoolHook)
}

// cscRecentRemovedRing bounds the recently-removed conn-id window that closes
// the fulfill-vs-removal race (see fulfillCached); only a handful of removals
// can fall in that microsecond window.
const cscRecentRemovedRing = 64

// cscEvictOnRemoveHook evicts a connection's owned entries when the pool removes
// it (the server stops delivering their invalidations — Window 2), and records
// recently-removed conn ids so fulfillCached can catch a value whose owning conn
// was removed mid-fetch.
type cscEvictOnRemoveHook struct {
	evictor connCacheOwner

	mu     sync.Mutex
	recent [cscRecentRemovedRing]uint64
	ridx   int
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
	id := cn.GetID()
	// Record BEFORE evicting: if we evicted first, a concurrent fulfill could
	// create the entry after the eviction yet read the ring before the record,
	// leaving it orphaned. Recording first guarantees fulfillCached sees the
	// removal and drops the entry.
	h.mu.Lock()
	h.recent[h.ridx%cscRecentRemovedRing] = id
	h.ridx++
	h.mu.Unlock()
	h.evictor.EvictByConn(id)
}

// wasRecentlyRemoved reports whether connID is in the recently-removed ring.
func (h *cscEvictOnRemoveHook) wasRecentlyRemoved(connID uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.recent {
		if v == connID {
			return true
		}
	}
	return false
}

// registerConnEvictHook wires cscEvictOnRemoveHook when the cache supports
// owning-connection attribution and the pool supports hooks. No-op otherwise
// (custom caches / poolers fall back to the MaxStaleness backstop).
func (c *baseClient) registerConnEvictHook(cache Cache) {
	owner, ok := cache.(connCacheOwner)
	if !ok {
		return
	}
	reg, ok := c.connPool.(poolHookRegistrar)
	if !ok {
		return
	}
	h := &cscEvictOnRemoveHook{evictor: owner}
	reg.AddPoolHook(h)
	c.cscPoolHook = h
}

// cscEvictOwnedEntries drops the shared-cache entries owned by connID, when the
// cache supports owning-connection attribution. Called from initConn on
// (re)init/handoff; no-op for caches/strategies without attribution.
func (c *baseClient) cscEvictOwnedEntries(connID uint64) {
	if c.csc == nil {
		return
	}
	if owner, ok := c.csc.(connCacheOwner); ok {
		owner.EvictByConn(connID)
	}
}

// cscDrainHandle holds the drainer goroutine's lifecycle channels: stop signals
// shutdown; done is closed on exit so stopBackgroundDrainer can join.
type cscDrainHandle struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
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

// idleConnDrainer is the optional pooler capability the SharedTracking drainer
// needs; *pool.ConnPool implements it. Poolers without it get no draining, so
// their cached reads are bounded only by CacheConfig.MaxStaleness.
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
	interval := c.cscDrainInterval()
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// st persists round/visited across ticks; single-goroutine, no lock.
		var st pool.DrainState
		drain := func(cn *pool.Conn) error { return c.drainPushNotifications(cn) }
		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
				// ctx bounds the whole pass; the drain read has its own hard deadline.
				cycleCtx, cancel := context.WithTimeout(context.Background(), interval/2)
				cp.DrainIdleConns(cycleCtx, &st, drain)
				cancel()
			}
		}
	}()
}

// stopBackgroundDrainer stops the drainer goroutine and joins it (so no pass
// touches the pool after Close), then flushes the cache if this client solely
// owns it — with the drainer gone nothing would apply invalidations, so an
// emptied cache can't serve stale. Idempotent; injected/shared caches are left
// intact for other clients.
func (c *baseClient) stopBackgroundDrainer() {
	// Deregister the hook first so pool teardown can't call back into the cache.
	// Independent of the handle: a cache without connCacheOwner has a drainer
	// but no hook, and vice versa.
	if c.cscPoolHook != nil {
		if reg, ok := c.connPool.(poolHookRegistrar); ok {
			reg.RemovePoolHook(c.cscPoolHook)
		}
		c.cscPoolHook = nil
	}
	h := c.cscDrainHandle
	if h == nil {
		return
	}
	c.cscDrainHandle = nil
	h.signalStop()
	<-h.done
	if c.cscOwnsCache && c.csc != nil {
		c.csc.Flush()
	}
}

// applyCachedReply populates cmd from a previously captured raw RESP reply by
// replaying it through the command's own readReply.
func applyCachedReply(cmd Cmder, raw []byte) error {
	return cmd.readReply(proto.NewReader(bytes.NewReader(raw)))
}

// cscDrainSkipWindow is the default SharedTracking drain period (overridable via
// ClientSideCacheConfig.DrainInterval). A buffered invalidation is picked up within
// roughly one round; MaxStaleness is the hard backstop.
const cscDrainSkipWindow = 5 * time.Millisecond

// cscDrainHardReadCap is the hard socket read deadline the drainer applies via
// Conn.WithReaderHardDeadline. It bounds only a rare partial-frame mid-read. A
// var (not const) so the tuning harness can sweep it.
var cscDrainHardReadCap = 50 * time.Microsecond

// processCached runs the Get-Reserve-Fulfill lifecycle for a cacheable command.
// Only invoked after process has verified that CSC is active and cmd is
// eligible.
func (c *baseClient) processCached(ctx context.Context, cmd Cmder) error {
	// Broadcast: while the sidecar is down no invalidations flow, so bypass the
	// cache entirely until it reconnects (hits could be stale, and new entries
	// would have no invalidate coverage).
	if r := c.cscBcastReady; r != nil && !r.Load() {
		return c.processWithRetry(ctx, cmd, nil, nil)
	}

	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		return c.processWithRetry(ctx, cmd, nil, nil)
	}

	redisKeys := extractRedisKeys(cmd)
	if len(redisKeys) == 0 {
		// Without a key list we cannot react to invalidations for this command.
		return c.processWithRetry(ctx, cmd, nil, nil)
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

	var raw []byte
	var capture *[]byte
	var connID uint64
	var connIDOut *uint64
	if shouldFetch {
		capture = &raw
		// Capture the serving conn id only when an evict-on-remove hook is
		// active; without it the attribution is unactionable.
		if c.cscPoolHook != nil {
			connIDOut = &connID
		}
		// Release the placeholder if processWithRetry panics; Cancel on a
		// stale token is a no-op.
		defer func() {
			if capture != nil {
				c.csc.Cancel(key, token)
			}
		}()
	}

	err := c.processWithRetry(ctx, cmd, capture, connIDOut)

	if shouldFetch {
		capture = nil // disarm the deferred Cancel
		if err == nil {
			if !c.fulfillCached(key, token, connID, raw) {
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
// removed. It also closes the attribute-vs-removal race: the conn is released
// before this runs, so its OnRemove eviction may fire before the entry exists —
// after attributing we re-check and drop the entry if the conn was just removed.
func (c *baseClient) fulfillCached(key string, token, connID uint64, raw []byte) bool {
	if connID != 0 {
		if hook, ok := c.cscPoolHook.(*cscEvictOnRemoveHook); ok {
			owner := hook.evictor
			done := owner.FulfillOwned(key, token, connID, raw)
			if done && hook.wasRecentlyRemoved(connID) {
				owner.EvictByConn(connID)
			}
			return done
		}
	}
	return c.csc.Fulfill(key, token, raw)
}
