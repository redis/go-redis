package redis

import (
	"bytes"
	"context"
	"strconv"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// ClientSideCacheConfig configures the built-in client-side cache. Pass a
// non-nil value to Options.ClientSideCacheConfig to enable caching on a RESP3
// client.
type ClientSideCacheConfig = CacheConfig

const (
	invalidatePushName = "invalidate"
	// cscNamespaceSep is a NUL byte so it cannot occur inside a Redis key name.
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
func (c *baseClient) attachCSC(ctx context.Context, cache Cache) {
	if cache == nil || c.opt.Protocol != 3 {
		return
	}
	if err := registerInvalidateHandler(c.pushProcessor, cache, c.opt.DB); err != nil {
		internal.Logger.Printf(ctx, "csc: failed to register invalidate handler: %v", err)
		return
	}
	c.csc = cache
}

// applyCachedReply populates cmd from a previously captured raw RESP reply by
// replaying it through the command's own readReply.
func applyCachedReply(cmd Cmder, raw []byte) error {
	return cmd.readReply(proto.NewReader(bytes.NewReader(raw)))
}

// drainPendingInvalidations processes buffered push notifications across the
// client's currently idle pool connections. Invalidations are delivered to the
// specific connection that read the tracked key, so a hit served from a
// different connection could otherwise race ahead of a pending "invalidate".
// The walk is bounded by an IdleLen snapshot and dedup'd by connection ID so a
// rotating pool (FIFO/LIFO) is not traversed indefinitely, and each connection
// is peeked via peekAndProcessPushNotifications so the recent-health-check
// shortcut does not suppress drainage on this non-reply-read path.
func (c *baseClient) drainPendingInvalidations(ctx context.Context) {
	if c.connPool == nil || c.opt.Protocol != 3 {
		return
	}
	remaining := c.connPool.IdleLen()
	if remaining < 1 {
		remaining = 1
	}

	seen := make(map[uint64]struct{}, remaining)
	borrowed := make([]*pool.Conn, 0, remaining)
	for i := 0; i < remaining; i++ {
		cn, err := c.connPool.Get(ctx)
		if err != nil {
			break
		}
		if _, dup := seen[cn.GetID()]; dup {
			c.connPool.Put(ctx, cn)
			break
		}
		seen[cn.GetID()] = struct{}{}
		borrowed = append(borrowed, cn)
		if err := c.peekAndProcessPushNotifications(ctx, cn); err != nil {
			internal.Logger.Printf(ctx, "csc: error draining invalidations on cache hit: %v", err)
		}
	}
	for _, cn := range borrowed {
		c.connPool.Put(ctx, cn)
	}
}

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

	if _, ok := c.csc.Get(ctx, key); ok {
		// Drain pending invalidations and re-check so the hit path cannot
		// serve a value the server has already marked stale.
		c.drainPendingInvalidations(ctx)
		if data, ok := c.csc.Get(ctx, key); ok {
			if err := applyCachedReply(cmd, data); err == nil {
				return nil
			}
			c.csc.DeleteByCacheKey(key)
		}
	}

	token, shouldFetch := c.csc.Reserve(key, nsRedisKeys)
	if !shouldFetch {
		// Another goroutine is fetching; Reserve blocks until it completes.
		if data, ok := c.csc.Get(ctx, key); ok {
			if err := applyCachedReply(cmd, data); err == nil {
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
	return err
}
