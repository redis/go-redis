package redis

import (
	"sync"
	"time"
)

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

// cscRecencyRing holds the last N per-tick LRUClock() snapshots, giving
// startCSCRefresher's sinceToken horizon a bounded multi-tick lookback
// instead of the single most-recent tick, so
// Options.ClientSideCacheRefreshRecencyWindow can span more than one
// cscRefreshRecencyTick. oldest() is the horizon: once the ring has filled,
// an entry read since oldest() was captured is guaranteed to fall within the
// configured window, regardless of where an invalidation lands relative to
// the tick phase (see the ring-sizing comment in startCSCRefresher). Owned
// solely by the refresher goroutine — push/oldest are not called
// concurrently, so no lock.
type cscRecencyRing struct {
	buf []int64
	pos int
	n   int
}

// cscRecencyRingMaxSize caps the ring allocation newCscRecencyRing builds. At
// the 200ms tick this is ~58 hours of coverage — a pathological
// ClientSideCacheRefreshRecencyWindow (a duration typo, or math.MaxInt64)
// degrades to that cap instead of an oversized or panicking make([]int64, N).
const cscRecencyRingMaxSize = 1 << 20

// newCscRecencyRing builds a ring of the given size, clamped to
// [1, cscRecencyRingMaxSize].
func newCscRecencyRing(size int) *cscRecencyRing {
	if size < 1 {
		size = 1
	}
	if size > cscRecencyRingMaxSize {
		size = cscRecencyRingMaxSize
	}
	return &cscRecencyRing{buf: make([]int64, size)}
}

// cscRefreshWindowTicks converts a requested
// Options.ClientSideCacheRefreshRecencyWindow into a cscRecencyRing size.
// Ring size N gives a GUARANTEED-minimum lookback of (N-1)*cscRefreshRecencyTick
// once the ring has filled (oldest() is then N-1 ticks behind the latest
// push) — the tick phase relative to an arbitrary invalidation can cost up to
// one full tick of coverage, so N-1 must itself already be >= ceil(w/tick)
// for the enforced window to never fall short of w. Hence N = ceil(w/tick)+1,
// giving an enforced window in [w, w+tick). Extracted pure so the rounding is
// unit-tested; w<=0 (unbounded mode) never reaches this function.
func cscRefreshWindowTicks(w time.Duration) int {
	ticks := int(w / cscRefreshRecencyTick)
	if w%cscRefreshRecencyTick != 0 {
		ticks++
	}
	return ticks + 1
}

// push records the latest per-tick snapshot, evicting the oldest once full.
func (r *cscRecencyRing) push(v int64) {
	r.buf[r.pos] = v
	r.pos = (r.pos + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
}

// oldest returns the least-recent snapshot currently held. Before the ring
// fills (just after the refresher starts), it returns the earliest snapshot
// taken so far, so every access since the refresher started counts as hot
// until the configured window has had time to mature. Once full, r.pos
// always points at the slot about to be overwritten next — the oldest value
// held — a standard circular-buffer property.
func (r *cscRecencyRing) oldest() int64 {
	if r.n < len(r.buf) {
		return r.buf[0]
	}
	return r.buf[r.pos]
}

// InvalidationStats reports INCOMING invalidation pushes: the count of keys named
// in server invalidation messages, tallied at the handler before dedup/batching.
func (c *LocalCache) InvalidationStats() (invalidations uint64) {
	return c.invalidations.Load()
}

// DeletionStats reports APPLIED invalidations. deletions counts keys the cache
// actually processed for removal (post-dedup, so <= InvalidationStats under
// duplicate pushes); noop counts those that removed no live entry — a key that
// matched nothing, or one whose only match was skipped by the fetch-order guard
// because it was refetched after the invalidation (see collectHotAndDelete).
// Both are the duplicate/stale-invalidation signature.
func (c *LocalCache) DeletionStats() (deletions, noop uint64) {
	return c.deletions.Load(), c.deletionsNoop.Load()
}

// deleteByRedisKeyCollectingHot deletes every cache entry tracked under redisKey
// and returns those that were VALID and read since sinceToken, as refetch
// targets. Delete and collect happen under one shard lock: the entry's cache key
// and recency are known only there, and it is about to be removed. fetchSnap is
// the cscFetchSeq value observed when this invalidation arrived; a Valid entry
// whose fetch was ISSUED after that (entry.fetchSeq > fetchSnap) is kept — it was
// refetched after the write, so evicting it would be a spurious miss (see
// cacheEntry.fetchSeq).
func (c *LocalCache) deleteByRedisKeyCollectingHot(redisKey string, sinceToken int64, fetchSnap uint64, dst []cscRefreshTarget) []cscRefreshTarget {
	removed := 0
	for i := range c.shards {
		var n int
		dst, n = c.shards[i].collectHotAndDelete(redisKey, sinceToken, fetchSnap, dst)
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

func (s *cacheShard) collectHotAndDelete(redisKey string, sinceToken int64, fetchSnap uint64, dst []cscRefreshTarget) ([]cscRefreshTarget, int) {
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
		entry, exists := s.entries[cacheKey]
		// Fetch-order guard: an entry whose fetch was ISSUED after this invalidation
		// was observed (fetchSeq > fetchSnap) cannot hold the pre-write value, so keep
		// it — whether already Valid or still in-progress. Evicting a Valid one would
		// be a spurious miss (and a stale queued duplicate straddling a refetch must
		// not undo the fresh value, #3965); canceling an in-progress one would fail its
		// racing Fulfill, wake every waiter as a miss, and defeat miss coalescing. An
		// entry reserved at or before the observe (fetchSeq <= fetchSnap) may predate
		// the write — possibly fetched on a different stream (see deleteByRedisKey) —
		// so it is removed, and any racing Fulfill for it correctly fails.
		if exists && entry.fetchSeq > fetchSnap {
			continue
		}
		if exists && entry.state == cacheEntryValid && entry.lastAccessNs.Load() > sinceToken {
			keys := make([]string, len(entry.redisKeys))
			copy(keys, entry.redisKeys)
			dst = append(dst, cscRefreshTarget{
				cacheKey:  cacheKey,
				redisKeys: keys,
				// Preserve the reader-access token so the refresh republish can restore it
				// and not renew demand (see cscRefreshTarget.accessNs / restoreAccessToken).
				// Re-load is safe: the shard lock is held, so no writer intervenes.
				accessNs: entry.lastAccessNs.Load(),
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
