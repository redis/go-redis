package redis

import (
	"sync"
	"sync/atomic"
	"time"
)

// Windowed background invalidation batcher.
//
// Normally the invalidate handler deletes cache entries INLINE, on whatever
// goroutine read the RESP3 "invalidate" push — which, for the coalescer's
// reply reader, means invalidation work steals time from reading miss replies
// and inflates the low-concurrency churn p99 tail.
//
// When Options.ClientSideCacheInvalidationBatchWindow>0 the handler instead
// ENQUEUES invalidated keys (cheap, off the read path) and a single background
// goroutine applies the deletes in batches once per window. Deferring an
// invalidation's application by <= window means a reader may see the
// pre-invalidation value for <= window, which is exactly what MaxStaleness=window
// already licenses; set the window <= MaxStaleness to stay within contract (a
// nonzero window with MaxStaleness=0 is an explicit strictness relaxation).

const cscInvalBatchMax = 4096 // size-cap flush regardless of the timer

// cscInvalItem is one queued invalidation, tagged with the batcher epoch it
// was enqueued under so a full-cache flush can supersede it (see drop()).
type cscInvalItem struct {
	key   string
	epoch uint64
}

type cscInvalBatcher struct {
	window time.Duration
	// cache/refresh are snapshotted at creation (batcher lifetime is inside the
	// binding lifetime): the release-time stop-drain must still apply queued
	// deletes AFTER releaseLocked nils h.cache, or a successor reusing the
	// shared cache serves stale until TTL/MaxStaleness.
	cache   Cache
	refresh *cscRefreshQueue

	ch       chan cscInvalItem
	stopCh   chan struct{}
	stopOnce sync.Once
	// epoch versions enqueued invalidations against full-cache flushes: drop()
	// bumps it, and apply skips items from an older epoch. See drop().
	epoch atomic.Uint64
	// stopMu/stopped interlock enqueue against stop: a handler can hold a stale
	// batcher pointer across a rebuild. enqueue sends under the read side, so a
	// sent key lands BEFORE stop closes stopCh (the stop-drain sees it); once
	// stopped, enqueue applies inline — no delete is ever parked in a channel
	// nothing drains.
	stopMu  sync.RWMutex
	stopped bool
}

// stop signals run() to flush and exit; idempotent, never touches h.mu, does
// not wait — safe under the handler lock. Lock order h.mu -> stopMu, never
// reversed (enqueue's stopMu section is only the flag check and the send).
func (b *cscInvalBatcher) stop() {
	b.stopOnce.Do(func() {
		b.stopMu.Lock()
		b.stopped = true
		b.stopMu.Unlock()
		close(b.stopCh)
	})
}

// drop marks everything enqueued so far as superseded by a full cache Flush:
// bumping the epoch makes apply skip stale-epoch items — both queued and in an
// in-progress batch — without draining anything. The old drain-based drop
// could discard a NEWER per-key invalidation racing in from another tracked
// connection after the flush, losing its delete and leaving a repopulated
// entry stale; with epochs that item carries the new epoch and survives.
// Callers bump BEFORE flushing the cache, so a stale epoch always means
// "enqueued before the flush", where the delete is redundant by the flush.
func (b *cscInvalBatcher) drop() {
	b.epoch.Add(1)
}

// enqueue hands a namespaced key to the batcher without blocking the caller. On
// a full queue — or once the batcher is stopped (see stopMu) — it applies the
// delete inline so an invalidation is never dropped.
func (b *cscInvalBatcher) enqueue(nsKey string) {
	it := cscInvalItem{key: nsKey, epoch: b.epoch.Load()}
	b.stopMu.RLock()
	if b.stopped {
		b.stopMu.RUnlock()
		b.apply([]cscInvalItem{it})
		return
	}
	select {
	case b.ch <- it:
		b.stopMu.RUnlock()
	default:
		b.stopMu.RUnlock()
		b.apply([]cscInvalItem{it})
	}
}

// apply deletes the namespaced keys and feeds evicted-hot entries to the
// refresher, using the creation-time snapshot (see the struct fields).
func (b *cscInvalBatcher) apply(items []cscInvalItem) {
	cache, refresh := b.cache, b.refresh
	if cache == nil {
		return
	}
	cur := b.epoch.Load()
	lc, canRefresh := cache.(*LocalCache)
	canRefresh = canRefresh && refresh != nil
	var hot []cscRefreshTarget
	for _, it := range items {
		// Stale epoch: enqueued before a full cache Flush that superseded it
		// (see drop()); applying it would only evict a post-flush repopulation.
		if it.epoch != cur {
			continue
		}
		k := it.key
		if !canRefresh {
			cache.DeleteByRedisKey(k)
			continue
		}
		hot = lc.deleteByRedisKeyCollectingHot(k, refresh.sinceToken.Load(), hot[:0])
		for i := range hot {
			refresh.offer(hot[i])
		}
	}
}

func (b *cscInvalBatcher) run() {
	t := time.NewTimer(b.window)
	defer t.Stop()
	pending := make([]cscInvalItem, 0, 256)
	// seen dedups by key WITHIN an epoch: the same key arriving again after a
	// flush bumped the epoch must be re-appended (the old occurrence will be
	// skipped by apply), or its post-flush delete would be lost to dedup.
	seen := make(map[string]uint64, 256)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		b.apply(pending)
		pending = pending[:0]
		for k := range seen {
			delete(seen, k)
		}
	}
	for {
		select {
		case <-b.stopCh:
			// Stopped (last release, or a window-change rebuild where queued deletes
			// are still live and must not be lost): drain ch into the batch, flush
			// once, exit.
			for draining := true; draining; {
				select {
				case it := <-b.ch:
					if e, dup := seen[it.key]; !dup || e != it.epoch {
						seen[it.key] = it.epoch
						pending = append(pending, it)
					}
				default:
					draining = false
				}
			}
			flush()
			return
		case it := <-b.ch:
			if e, dup := seen[it.key]; !dup || e != it.epoch {
				seen[it.key] = it.epoch
				pending = append(pending, it)
			}
			if len(pending) >= cscInvalBatchMax {
				flush()
				t.Reset(b.window)
			}
		case <-t.C:
			flush()
			t.Reset(b.window)
		}
	}
}
