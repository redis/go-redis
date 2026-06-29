package redis

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// RESPInvalidationBytesRead returns the cumulative byte count of key names
// received in "invalidate" push frames since process start. This counter is
// shared across all clients in the process and is never reset by the library.
func RESPInvalidationBytesRead() int64 {
	return proto.InvalidationBytesRead.Load()
}

// commandHits / commandMisses count cache outcomes at the SERVED-COMMAND
// boundary — exactly one increment per `processCached` invocation,
// regardless of how many internal cache.Get probes it issues.
var (
	commandHits   atomic.Uint64
	commandMisses atomic.Uint64
)

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
	// cscNamespaceSep separates the DB-number prefix from the rest of a
	// namespaced cache/redis key. NUL is technically a legal byte inside a
	// Redis key (keys are binary-safe), so this prefix is not collision-proof
	// against an adversarial key like "0\x00foo".
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
	// VoidProcessor (RESP2) returns an error here; the caller treats it as
	// "CSC not available" rather than fatal.
	return p.RegisterHandler(invalidatePushName, &invalidateHandler{cache: cache, db: db}, true)
}

// attachCSC wires a client-side cache to this baseClient and registers the
// invalidate handler. Safe to call with a nil cache. On registration failure
// the cache reference is left unset so cacheable commands fall back to normal
// round-trips.
//
// CSC is only enabled when Options.DB == 0. Redis CLIENT TRACKING is
// per-connection and bound to the database the connection was on when tracking
// was enabled; a runtime SELECT mid-session changes the active DB but does not
// re-key the server's tracking table. Cache entries written under one DB would
// then be invalidated by writes against a different DB, silently serving stale
// data. Users that need multi-DB caching must run one client per DB.
func (c *baseClient) attachCSC(ctx context.Context, cache Cache) {
	if cache == nil || c.opt.Protocol != 3 {
		return
	}
	if c.opt.DB != 0 {
		internal.Logger.Printf(ctx,
			"csc: client-side caching is restricted to DB 0; disabling CSC for client configured with DB=%d. "+
				"Use one client per DB if you need caching against non-zero databases.", c.opt.DB)
		return
	}
	if err := registerInvalidateHandler(c.pushProcessor, cache, c.opt.DB); err != nil {
		internal.Logger.Printf(ctx, "csc: failed to register invalidate handler: %v", err)
		return
	}
	c.csc = cache
	// Only SharedTracking delivers invalidations to pool connections, so only it
	// runs the drainer (Broadcast uses the sidecar; PerConnection never gets here).
	if c.opt.ClientSideCacheStrategy == CSCStrategySharedTracking {
		c.startBackgroundDrainer()
	}
}

// cscDrainHandle holds a drainer goroutine's lifecycle channels: stop signals
// shutdown; done is closed on exit so stopBackgroundDrainer can JOIN before teardown.
type cscDrainHandle struct {
	stop chan struct{}
	done chan struct{}
}

// cscDrainHandles maps *baseClient -> *cscDrainHandle for its background drainer.
// The entry is removed (and the goroutine joined) on Close.
var cscDrainHandles sync.Map

// cscDrainInterval returns ClientSideCacheConfig.DrainInterval, or the default
// (cscDrainSkipWindow, 5ms) when unset.
func (c *baseClient) cscDrainInterval() time.Duration {
	if cfg := c.opt.ClientSideCacheConfig; cfg != nil && cfg.DrainInterval > 0 {
		return cfg.DrainInterval
	}
	return cscDrainSkipWindow
}

// startBackgroundDrainer launches the per-client invalidation drainer. Each tick
// (ClientSideCacheConfig.DrainInterval, default 5ms) runs one pass of
// pool.DrainIdleConns — claiming one idle connection at a time, draining its
// buffered push frames, and returning it via Put (so OnPut can queue a maintenance
// handoff). Only the standard *pool.ConnPool is supported; other poolers no-op.
func (c *baseClient) startBackgroundDrainer() {
	cp, ok := c.connPool.(*pool.ConnPool)
	if !ok {
		return
	}
	h := &cscDrainHandle{stop: make(chan struct{}), done: make(chan struct{})}
	if _, loaded := cscDrainHandles.LoadOrStore(c, h); loaded {
		return // already running
	}
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

// stopBackgroundDrainer halts the client's drainer goroutine and JOINS it before
// returning, so a pass in progress cannot touch the pool after baseClient.Close
// tears it down. Idempotent; called from baseClient.Close.
func (c *baseClient) stopBackgroundDrainer() {
	if v, ok := cscDrainHandles.LoadAndDelete(c); ok {
		h := v.(*cscDrainHandle)
		close(h.stop)
		<-h.done
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

// cscDrainHardReadCap is the HARD socket read deadline the drainer applies via
// Conn.WithReaderHardDeadline (a relaxed maintenance timeout can't extend it). The
// drain reads only buffered frames and stops when empty, so this bounds only a rare
// partial-frame mid-read. A var (not const) so the tuning harness can sweep it.
var cscDrainHardReadCap = 50 * time.Microsecond

// processCached runs the Get-Reserve-Fulfill lifecycle for a cacheable command.
// Only invoked after process has verified that CSC is active and cmd is
// eligible.
func (c *baseClient) processCached(ctx context.Context, cmd Cmder) error {
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

	var raw []byte
	var capture *[]byte
	if shouldFetch {
		capture = &raw
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
			c.csc.Fulfill(key, token, raw)
		} else {
			c.csc.Cancel(key, token)
		}
	}
	// Reaching here means the command's reply came from Redis (the miss
	// path). Count as a command-level miss.
	commandMisses.Add(1)
	return err
}
