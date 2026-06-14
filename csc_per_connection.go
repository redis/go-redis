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

	// metrics
	hits          atomic.Uint64
	misses        atomic.Uint64
	drainsSkipped atomic.Uint64 // cache-hit-path drains skipped via lastPushDrainAt window
}

func newPerConnState(cfg CacheConfig, db int) *perConnState {
	return &perConnState{
		caches: make(map[uint64]Cache),
		cfg:    cfg,
		db:     db,
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

// perConnStateField attaches per-client state without growing baseClient.
// Key: *baseClient. Value: *perConnState.
var perConnStateField sync.Map

func perConnStateFor(c *baseClient) *perConnState {
	v, ok := perConnStateField.Load(c)
	if !ok {
		return nil
	}
	return v.(*perConnState)
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

	state := newPerConnState(cfg, c.opt.DB)
	perConnStateField.Store(c, state)

	// Register the per-conn dispatcher. This succeeds because attachCSC has
	// not yet run; afterwards attachCSC will fail to register the shared-cache
	// handler and leave baseClient.csc unset, which is exactly what we want.
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
		perConnStateField.Delete(c)
	}
}

// cscPerConnOnInitConn runs from initConn after CLIENT TRACKING ON has
// been issued on cn. It allocates a private *localCache for this conn and
// hooks the conn's onClose to flush+detach the cache when the conn is closed.
func (c *baseClient) cscPerConnOnInitConn(_ context.Context, cn *pool.Conn) {
	state := perConnStateFor(c)
	if state == nil || cn == nil {
		return
	}
	cache := NewLocalCache(state.cfg)
	state.putCache(cn.GetID(), cache)

	id := cn.GetID()
	cn.SetOnClose(func() error {
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

	// Locate the cache for this connection. handlerCtx.Conn is set by the
	// hot path that read the push frame — we type-assert to *pool.Conn.
	cn, _ := hctx.Conn.(*pool.Conn)
	if cn == nil {
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
		var bytesConsumed int64
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
			bytesConsumed += int64(len(name))
			cache.DeleteByRedisKey(dbNamespacedKey(h.state.db, name))
		}
		if bytesConsumed > 0 {
			proto.InvalidationBytesRead.Add(bytesConsumed)
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
		return c.processWithRetry(ctx, cmd, nil)
	}
	redisKeys := extractRedisKeys(cmd)
	if len(redisKeys) == 0 {
		return c.processWithRetry(ctx, cmd, nil)
	}

	db := state.db
	key := dbNamespacedKey(db, rawKey)
	nsRedisKeys := make([]string, len(redisKeys))
	for i, k := range redisKeys {
		nsRedisKeys[i] = dbNamespacedKey(db, k)
	}

	// PerConnection binds the lookup to a specific connection. We borrow one
	// from the pool via getConn.
	// We must NOT release+re-borrow because a hit cached on conn X is
	// invisible to conn Y.
	cn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	// Drain any pending invalidations on this conn before consulting its
	// cache; otherwise a hit could race ahead of an in-flight server
	// invalidation that has already been parsed but not yet delivered.
	//
	// if a successful drain or socket-peek ran on this
	// conn very recently (cscPerConnDrainSkipWindow), trust that stamp and
	// avoid the MaybeHasData() syscall. Invalidations that race into the
	// skip window are picked up on the next op past it; the per-conn cache
	// is still flushed on close.
	nowNs := pool.GetCachedTimeNs()
	if nowNs-cn.LastPushDrainAtNs() >= int64(cscPerConnDrainSkipWindow) {
		if drainErr := c.peekAndProcessPushNotifications(ctx, cn); drainErr != nil {
			internal.Logger.Printf(ctx, "csc per-connection: drain pending invalidations failed: %v", drainErr)
		}
	} else {
		state.drainsSkipped.Add(1)
	}

	cache := state.getCache(cn.GetID())
	if cache == nil {
		// Conn was set up before PerConnection claimed it (e.g. initConn ran
		// for a non-tracking-enabled client). Release the conn and fall
		// back to a plain command path.
		c.releaseConn(ctx, cn, nil)
		return c.processWithRetry(ctx, cmd, nil)
	}

	// Fast-path hit. Only return early on a successful replay; on any
	// replay failure we evict and execute fresh on the same conn.
	if data, ok := cache.Get(ctx, key); ok {
		if applyErr := applyCachedReply(cmd, data); applyErr == nil {
			state.hits.Add(1)
			commandHits.Add(1)
			c.releaseConn(ctx, cn, nil)
			return nil
		}
		cache.DeleteByCacheKey(key)
	}

	// Reserve a slot and execute on the same conn. We don't call
	// processWithRetry here because retries borrow new connections from
	// the pool, breaking the conn->cache binding. PerConnection trades retry
	// affinity for cache-locality; a single attempt is acceptable because
	// the caller (process) and outer hooks already provide their own
	// error semantics.
	token, shouldFetch := cache.Reserve(key, nsRedisKeys)
	if !shouldFetch {
		// Another goroutine on this same conn is fetching; wait.
		if data, ok := cache.Get(ctx, key); ok {
			if applyErr := applyCachedReply(cmd, data); applyErr == nil {
				state.hits.Add(1)
				commandHits.Add(1)
				c.releaseConn(ctx, cn, nil)
				return nil
			}
			cache.DeleteByCacheKey(key)
		}
		// Try to take over.
		token, shouldFetch = cache.Reserve(key, nsRedisKeys)
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
	c.releaseConn(ctx, cn, execErr)

	if shouldFetch {
		capture = nil // disarm deferred Cancel
		if execErr == nil {
			cache.Fulfill(key, token, raw)
		} else {
			cache.Cancel(key, token)
		}
	}
	// Reaching here means we executed against Redis (the miss path). Both the
	// per-state and command-level miss counters are bumped at this exit
	// boundary: a Reserve-takeover that ended up serving from cache returns via
	// an early hit path above, so it can never be double-counted as miss+hit.
	state.misses.Add(1)
	commandMisses.Add(1)
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

// perConnDrainSkips returns the number of times the cache-hit-path drain
// was skipped because cn.LastPushDrainAtNs() fell inside
// cscPerConnDrainSkipWindow. Useful for benchmark instrumentation.
func (c *baseClient) perConnDrainSkips() uint64 {
	state := perConnStateFor(c)
	if state == nil {
		return 0
	}
	return state.drainsSkipped.Load()
}

// cscPerConnOnClose flushes every per-connection cache owned by c and
// removes c's entry from the dispatch table. Idempotent and safe to call
// from baseClient.Close even when PerConnection was never activated.
func (c *baseClient) cscPerConnOnClose() {
	v, ok := perConnStateField.LoadAndDelete(c)
	if !ok {
		return
	}
	state := v.(*perConnState)
	state.mu.Lock()
	for id, cache := range state.caches {
		if cache != nil {
			cache.Flush()
		}
		delete(state.caches, id)
	}
	state.mu.Unlock()
}

// cscPerConnPropagateTo registers target so that its hot-path lookups see
// the same per-connection caches as c. It also wires target's (separate)
// pushProcessor with the same invalidate dispatcher, so invalidations that
// happen to be parsed via target still route to the shared dispatch table.
//
// This is invoked from Client.Conn() (sticky single-conn derived clients).
// In the baseline build the stub version is a no-op.
func (c *baseClient) cscPerConnPropagateTo(target *baseClient) {
	state := perConnStateFor(c)
	if state == nil || target == nil {
		return
	}
	perConnStateField.Store(target, state)
	if target.pushProcessor != nil {
		_ = target.pushProcessor.RegisterHandler(
			invalidatePushName,
			&perConnInvalidateHandler{state: state},
			true,
		)
	}
}
