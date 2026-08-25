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

// InvalidationStats reports invalidation-push accounting. invalidations counts
// keys named in incoming pushes; noop counts those that matched no live entry
// (the duplicate-invalidation signature).
func (c *LocalCache) InvalidationStats() (invalidations, noop uint64) {
	return c.invalidations.Load(), c.invalidationsNoop.Load()
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
	c.invalidations.Add(1)
	if removed == 0 {
		c.invalidationsNoop.Add(1)
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
			dst = append(dst, cscRefreshTarget{cacheKey: cacheKey, redisKeys: keys})
		}
		if s.removeEntryLocked(cacheKey) {
			removed++
		}
	}
	return dst, removed
}
