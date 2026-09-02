package redis

import "sync"

// Support shims for refresh-on-invalidate and reader-miss coalescing, kept in
// one file so the feature is a clean addition over the CSC base.

// cscRevalidateHandle is the stop/join handle for a background CSC goroutine.
type cscRevalidateHandle struct {
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// signalStop closes stop at most once (so stopCSCRefresher and the AddCleanup
// safety net cannot double-close) and does not join — a GC cleanup must not block.
func (h *cscRevalidateHandle) signalStop() {
	h.stopOnce.Do(func() { close(h.stop) })
}

// LRUClock returns the current global recency token; the refresher uses it as
// the "recently read" horizon.
func (c *LocalCache) LRUClock() int64 { return lruSequence.Load() }

// InvalidationStats reports INCOMING invalidation pushes: the count of keys named
// in server invalidation messages, tallied at the handler before dedup/batching.
func (c *LocalCache) InvalidationStats() (invalidations uint64) {
	return c.invalidations.Load()
}

// DeletionStats reports APPLIED invalidations. deletions counts keys the cache
// actually processed for removal (post-dedup, so <= InvalidationStats under
// duplicate pushes); noop counts those that matched no live entry, the direct
// duplicate-invalidation signature.
func (c *LocalCache) DeletionStats() (deletions, noop uint64) {
	return c.deletions.Load(), c.deletionsNoop.Load()
}

// deleteByRedisKeyCollectingHot deletes every cache entry tracked under redisKey
// and returns those that were VALID and read since sinceToken, as refetch
// targets. Delete and collect happen under one shard lock: the entry's cache key
// and recency are known only there, and it is about to be removed.
func (c *LocalCache) deleteByRedisKeyCollectingHot(redisKey string, sinceToken int64, dst []cscRefreshTarget) []cscRefreshTarget {
	removed := 0
	for i := range c.shards {
		var n int
		dst, n = c.shards[i].collectHotAndDelete(redisKey, sinceToken, dst)
		removed += n
	}
	// Applied-delete accounting (refresh-on path). Twin of DeleteByRedisKey; the
	// incoming push was already counted at the handler.
	c.deletions.Add(1)
	if removed == 0 {
		c.deletionsNoop.Add(1)
	}
	return dst
}

func (s *cacheShard) collectHotAndDelete(redisKey string, sinceToken int64, dst []cscRefreshTarget) ([]cscRefreshTarget, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cacheKeys, ok := s.byRedisKey[redisKey]
	if !ok {
		return dst, 0
	}
	toRemove := make([]string, 0, len(cacheKeys))
	for cacheKey := range cacheKeys {
		toRemove = append(toRemove, cacheKey)
	}
	removed := 0
	for _, cacheKey := range toRemove {
		if entry, exists := s.entries[cacheKey]; exists &&
			entry.state == cacheEntryValid && entry.lastAccessNs.Load() > sinceToken {
			keys := make([]string, len(entry.redisKeys))
			copy(keys, entry.redisKeys)
			dst = append(dst, cscRefreshTarget{
				cacheKey:  cacheKey,
				redisKeys: keys,
				// The dying entry's payload size approximates the refetch REPLY size —
				// the refresher chunks round trips by expected reply bytes with it (see
				// cscRefreshChunkEnd). Known for free here, under the same shard lock.
				valBytes: len(entry.value),
			})
		}
		if s.removeEntryLocked(cacheKey) {
			removed++
		}
	}
	return dst, removed
}
