package redis

// PerConnection client-side caching: a private cache per pool connection.
//
// When Options.ClientSideCacheStrategy is CSCStrategyPerConnection, every
// cacheable command is dispatched through this path first. It registers the
// "invalidate" push handler before attachCSC installs the shared-cache one,
// so invalidations are routed to per-connection caches and the shared cache
// (baseClient.csc) is intentionally left nil.
//
// Differences from the shared-cache strategies (Broadcast / SharedTracking):
//
//   - Each *pool.Conn owns a private *localCache. Tracking entries the server
//     associates with a given client connection invalidate only that
//     connection's private cache, never another connection's.
//   - Cache lookups are scoped to the connection that will execute the
//     command. A hit on Conn-1 cannot serve a request that happens to be
//     borrowed onto Conn-2 — trading hit rate and duplicated memory for
//     isolation.
//   - Connection-lifecycle staleness is eliminated for each closed connection's
//     own entries: on close we Flush() the connection's private cache,
// 	   and the server has simultaneously dropped all its tracking state for
//     that client conn.

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// cscPerConnDrainSkipWindow is the recency window inside which the
// cache-hit path can skip an explicit peekAndProcessPushNotifications call.
// A drain that succeeded within this window stamps cn.lastPushDrainAt; the
// next cacheable op on the same conn trusts that stamp and avoids the
// MaybeHasData() syscall.
const cscPerConnDrainSkipWindow = 5 * time.Millisecond

// perConnState holds the dispatch table that maps a connection ID to its
// private *localCache. The map is shared across one baseClient instance.
// Entries are removed when the connection's onClose hook fires (or when a
// reinit replaces the cache before the old conn is closed).
type perConnState struct {
	mu     sync.RWMutex
	caches map[uint64]Cache
	cfg    CacheConfig
	db     int

	// owner is the baseClient whose Close tears down the cache table.
	// Derived clients (Conn's sticky client, WithTimeout clones) share the
	// state pointer but must not wipe it when they close.
	owner *baseClient

	// metrics
	hits   atomic.Uint64
	misses atomic.Uint64

	// badCtxWarn ensures the "unusable push-handler Conn" warning is logged at
	// most once per client (see perConnInvalidateHandler.HandlePushNotification).
	badCtxWarn sync.Once
}

func newPerConnState(cfg CacheConfig, db int, owner *baseClient) *perConnState {
	return &perConnState{
		caches: make(map[uint64]Cache),
		cfg:    cfg,
		db:     db,
		owner:  owner,
	}
}

func (s *perConnState) putCache(id uint64, cache Cache) {
	s.mu.Lock()
	if prior, ok := s.caches[id]; ok && prior != cache {
		prior.Flush()
	}
	s.caches[id] = cache
	s.mu.Unlock()
}

func (s *perConnState) getCache(id uint64) Cache {
	s.mu.RLock()
	c := s.caches[id]
	s.mu.RUnlock()
	return c
}

func (s *perConnState) removeCache(id uint64) Cache {
	s.mu.Lock()
	cache := s.caches[id]
	delete(s.caches, id)
	s.mu.Unlock()
	return cache
}

// len reports the number of active per-connection caches. Exposed for
// tests/benchmarks only.
func (s *perConnState) len() int {
	s.mu.RLock()
	n := len(s.caches)
	s.mu.RUnlock()
	return n
}

// perConnStateFor returns the client's per-connection dispatch state, nil
// when PerConnection is not active. The state lives in a baseClient field
// (copied by clone(), set by cscPerConnPropagateTo for Conn()) rather than a
// package-global registry keyed by *baseClient: a global map pins every
// WithTimeout clone forever (they are never Closed) and lets a derived
// client's Close observe — and previously wipe — the parent's state.
func perConnStateFor(c *baseClient) *perConnState {
	if c == nil {
		return nil
	}
	return c.cscPerConnState
}

// cscPerConnEnabled reports whether the per-connection cache approach is
// active for this baseClient. True iff cscPerConnInit installed state.
func (c *baseClient) cscPerConnEnabled() bool {
	return perConnStateFor(c) != nil
}

// cscPerConnInit is invoked from NewClient (before attachCSC). When CSC
// is configured it claims the "invalidate" push handler so its per-conn
// dispatcher receives invalidations. Subsequent attachCSC will see the
// handler already registered, fail to register the shared-cache one, and
// leave baseClient.csc nil — disabling the shared-cache hot path.
//
// We only enable PerConnection when:
//   - ClientSideCacheStrategy == CSCStrategyPerConnection;
//   - Protocol == 3 (RESP3 is required for invalidation push frames);
//   - A CSC config (or explicit Cache) was supplied via Options.
func (c *baseClient) cscPerConnInit() {
	if c.opt.ClientSideCacheStrategy != CSCStrategyPerConnection {
		return
	}
	if c.opt.Protocol != 3 {
		return
	}
	if c.opt.DB != 0 {
		internal.Logger.Printf(context.Background(),
			"csc per-connection: client-side caching is restricted to DB 0; disabling per-connection CSC for client configured with DB=%d. "+
				"Use one client per DB if you need caching against non-zero databases.", c.opt.DB)
		return
	}

	var cfg CacheConfig
	switch {
	case c.opt.ClientSideCacheConfig != nil:
		cfg = *c.opt.ClientSideCacheConfig
	case c.opt.ClientSideCache != nil:
		// Explicit Cache instance is incompatible with per-conn caching.
		// Fall back to default CacheConfig and warn.
		internal.Logger.Printf(
			context.Background(),
			"csc per-connection: ClientSideCache is ignored under per-connection mode; "+
				"using default CacheConfig per connection.",
		)
	default:
		return
	}

	state := newPerConnState(cfg, c.opt.DB, c)
	c.cscPerConnState = state

	// Register the per-conn dispatcher. NewClient gates attachCSC away from
	// PerConnection (and attachCSC itself refuses the strategy), so this
	// registration does not race the shared-cache handler for the slot.
	if err := c.pushProcessor.RegisterHandler(
		invalidatePushName,
		&perConnInvalidateHandler{state: state},
		true,
	); err != nil {
		// Most likely cause: the processor is a VoidProcessor (RESP2) or
		// another component already registered an invalidate handler.
		// Either way, fall back to no caching for this client.
		internal.Logger.Printf(context.Background(),
			"csc per-connection: failed to register invalidate handler: %v", err)
		c.cscPerConnState = nil
	}
}

// cscPerConnOnInitConn runs from initConn after CLIENT TRACKING ON has
// been issued on cn. It allocates a private *localCache for this conn and
// hooks the conn's CSC close hook to flush+detach the cache when the conn is
// closed.
func (c *baseClient) cscPerConnOnInitConn(_ context.Context, cn *pool.Conn) {
	state := perConnStateFor(c)
	if state == nil || cn == nil {
		return
	}
	cache := NewLocalCache(state.cfg)
	state.putCache(cn.GetID(), cache)

	id := cn.GetID()
	// Dedicated CSC slot: SetOnClose would clobber the streaming-credentials
	// unsubscribe installed earlier in the same initConn.
	cn.SetOnCscClose(func() error {
		if removed := state.removeCache(id); removed != nil {
			removed.Flush()
		}
		return nil
	})
}

// perConnInvalidateHandler dispatches RESP3 ["invalidate", <keys>]
// notifications to the per-connection cache that received them.
type perConnInvalidateHandler struct {
	state *perConnState
}

func (h *perConnInvalidateHandler) HandlePushNotification(
	_ context.Context,
	hctx push.NotificationHandlerContext,
	notification []interface{},
) error {
	if h.state == nil || len(notification) < 2 {
		return nil
	}

	// Locate this connection's cache; hctx.Conn is set by the reading hot path.
	cn, _ := hctx.Conn.(*pool.Conn)
	if cn == nil {
		// A custom push processor with no usable *pool.Conn can't be routed to a
		// per-conn cache, so invalidations would be dropped while hits keep
		// being served (stale). Warn once: PerConnection needs the built-in
		// processor.
		h.state.badCtxWarn.Do(func() {
			internal.Logger.Printf(context.Background(),
				"csc per-connection: push handler context has no *pool.Conn; "+
					"invalidations cannot be routed — PerConnection caching requires the built-in push processor")
		})
		return nil
	}
	cache := h.state.getCache(cn.GetID())
	if cache == nil {
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
			cache.DeleteByRedisKey(dbNamespacedKey(h.state.db, name))
		}
	}
	return nil
}

// cscPerConnTryProcess is the hot-path hook called from baseClient.process
// in place of the shared-cache processCached. Returns (true, err) if the
// command was handled (success or terminal failure); (false, nil) means the
// command is not eligible for PerConnection and the caller should run the
// normal command path (which will skip the shared-cache CSC path since
// baseClient.csc is nil under this build tag).
func (c *baseClient) cscPerConnTryProcess(ctx context.Context, cmd Cmder) (bool, error) {
	state := perConnStateFor(c)
	if state == nil || !isCacheable(cmd) {
		return false, nil
	}
	return true, c.processPerConn(ctx, cmd, state)
}

// processPerConn is the PerConnection analogue of processCached. It binds
// the command to a specific pool.Conn, looks up that conn's private cache,
// and runs the Get -> Reserve -> Fulfill flow scoped to that cache.
func (c *baseClient) processPerConn(ctx context.Context, cmd Cmder, state *perConnState) error {
	rawKey, ok := buildCacheKey(cmd)
	if !ok {
		return c.processWithRetry(ctx, cmd, nil, nil)
	}
	redisKeys := extractRedisKeys(cmd)
	if len(redisKeys) == 0 {
		return c.processWithRetry(ctx, cmd, nil, nil)
	}

	db := state.db
	key := dbNamespacedKey(db, rawKey)
	nsRedisKeys := make([]string, len(redisKeys))
	for i, k := range redisKeys {
		nsRedisKeys[i] = dbNamespacedKey(db, k)
	}

	// Bind the lookup to one connection (a hit cached on conn X is invisible to
	// conn Y, so we must not release+re-borrow).
	cn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	// Release the conn exactly once. The deferred call is a panic backstop
	// (the shared-cache path gets this from withConn's defer; PerConnection
	// must do it explicitly) so a panic can't leak the conn or its pool turn.
	released := false
	release := func(e error) {
		if !released {
			released = true
			c.releaseConn(ctx, cn, e)
		}
	}
	defer func() { release(nil) }()

	// Drain pending invalidations on this conn before consulting its cache, so
	// a hit can't race ahead of an already-parsed invalidation. Skip the
	// MaybeHasData() syscall if a drain ran within cscPerConnDrainSkipWindow.
	// Use drainPushNotifications (not the peek variant) so read errors surface:
	// a mid-frame deadline desyncs the conn, so discard it rather than reuse it
	// (releaseConn removes it — drain errors satisfy isBadConn).
	nowNs := pool.GetCachedTimeNs()
	if nowNs-cn.LastPushDrainAtNs() >= int64(cscPerConnDrainSkipWindow) {
		if drainErr := c.drainPushNotifications(cn); drainErr != nil {
			internal.Logger.Printf(ctx,
				"csc per-connection: drain failed, discarding conn: %v", drainErr)
			release(drainErr)
			return c.processWithRetry(ctx, cmd, nil, nil)
		}
	}

	cache := state.getCache(cn.GetID())
	if cache == nil {
		// Conn was set up before PerConnection claimed it (e.g. initConn ran
		// for a non-tracking-enabled client). Release the conn and fall
		// back to a plain command path.
		release(nil)
		return c.processWithRetry(ctx, cmd, nil, nil)
	}

	// serveHit replays a cached value into cmd if present, returning true when
	// it served the command. A corrupt entry is evicted and treated as a miss.
	serveHit := func() bool {
		data, ok := cache.Get(ctx, key)
		if !ok {
			return false
		}
		if applyCachedReply(cmd, data) != nil {
			cache.DeleteByCacheKey(key)
			return false
		}
		state.hits.Add(1)
		commandHits.Add(1)
		return true
	}

	if serveHit() {
		release(nil)
		return nil
	}

	// Reserve a slot and execute on the same conn. We don't call
	// processWithRetry here because retries borrow new connections from the
	// pool, breaking the conn->cache binding. PerConnection trades retry
	// affinity for cache-locality; a single attempt is acceptable.
	token, shouldFetch := cache.Reserve(key, nsRedisKeys)
	if !shouldFetch {
		// Another goroutine on this conn is fetching; wait via Get, then try to
		// take over. If takeover also fails (entry became Valid, or a third
		// goroutine now owns the fetch), read once more before falling through
		// to a single uncached round-trip.
		if serveHit() {
			release(nil)
			return nil
		}
		if token, shouldFetch = cache.Reserve(key, nsRedisKeys); !shouldFetch && serveHit() {
			release(nil)
			return nil
		}
	}

	var raw []byte
	var capture *[]byte
	if shouldFetch {
		capture = &raw
		defer func() {
			if capture != nil {
				cache.Cancel(key, token)
			}
		}()
	}

	execErr := c.executeOnConn(ctx, cn, cmd, capture)
	release(execErr)

	if shouldFetch {
		capture = nil // disarm deferred Cancel
		if execErr == nil {
			if !cache.Fulfill(key, token, raw) {
				commandCacheRejects.Add(1)
			}
		} else {
			cache.Cancel(key, token)
		}
	}
	// Count a miss only when the command completed (mirrors processCached); a
	// network/command failure is not a miss and would skew the hit rate.
	if execErr == nil {
		state.misses.Add(1)
		commandMisses.Add(1)
	}
	return execErr
}

// executeOnConn writes cmd on cn, reads the reply, and optionally captures
// the raw bytes. This is a single-attempt mirror of _process that operates
// on a caller-provided connection so PerConnection can keep the cache lookup
// and the command execution bound to the same conn.
func (c *baseClient) executeOnConn(ctx context.Context, cn *pool.Conn, cmd Cmder, rawReplyCapture *[]byte) error {
	if err := c.processPushNotifications(ctx, cn); err != nil {
		internal.Logger.Printf(ctx, "csc per-connection: pending notifications error: %v", err)
	}

	if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, cmd)
	}); err != nil {
		return err
	}

	readReplyFunc := cmd.readReply
	if c.opt.Protocol != 2 {
		useRawReply, err := c.assertUnstableCommand(cmd)
		if err != nil {
			return err
		}
		if useRawReply {
			readReplyFunc = cmd.readRawReply
		}
	}
	if rawReplyCapture != nil {
		origRead := readReplyFunc
		readReplyFunc = func(rd *proto.Reader) error {
			raw, err := rd.ReadRawReply()
			if err != nil {
				return err
			}
			if err := origRead(proto.NewReader(bytes.NewReader(raw))); err != nil {
				return err
			}
			*rawReplyCapture = raw
			return nil
		}
	}

	return cn.WithReader(c.context(ctx), c.cmdTimeout(cmd), func(rd *proto.Reader) error {
		if err := c.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
			internal.Logger.Printf(ctx, "csc per-connection: pending notifications (reader) error: %v", err)
		}
		return readReplyFunc(rd)
	})
}

// perConnStats returns (hits, misses, numConns) for tests/benchmarks.
// Unused by production code paths.
func (c *baseClient) perConnStats() (uint64, uint64, int) {
	state := perConnStateFor(c)
	if state == nil {
		return 0, 0, 0
	}
	return state.hits.Load(), state.misses.Load(), state.len()
}

// cscPerConnOnClose flushes every per-connection cache owned by c. Idempotent
// and safe to call from baseClient.Close even when PerConnection was never
// activated. Derived clients (Conn/WithTimeout) share the state pointer but do
// NOT own it: closing them must not wipe the parent's cache table — the
// caches belong to pool connections that remain live in the parent's pool.
func (c *baseClient) cscPerConnOnClose() {
	state := c.cscPerConnState
	if state == nil || state.owner != c {
		return
	}
	state.mu.Lock()
	for id, cache := range state.caches {
		if cache != nil {
			cache.Flush()
		}
		delete(state.caches, id)
	}
	state.mu.Unlock()
}

// cscPerConnPropagateTo shares c's per-connection dispatch state with target
// so that its hot-path lookups see the same per-connection caches. It also
// wires target's pushProcessor with the same invalidate dispatcher (a no-op
// when the processor is shared with the parent, which already has it), so
// invalidations parsed via target still route to the shared dispatch table.
//
// target does NOT become an owner: its Close leaves the cache table intact.
// Invoked from Client.Conn(); WithTimeout clones inherit the field via clone().
func (c *baseClient) cscPerConnPropagateTo(target *baseClient) {
	state := perConnStateFor(c)
	if state == nil || target == nil {
		return
	}
	target.cscPerConnState = state
	if target.pushProcessor != nil {
		_ = target.pushProcessor.RegisterHandler(
			invalidatePushName,
			&perConnInvalidateHandler{state: state},
			true,
		)
	}
}
