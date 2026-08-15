package redis

import (
	"sync"
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

type cscInvalBatcher struct {
	window time.Duration
	// cache/refresh are snapshotted at creation (batcher lifetime is inside the
	// binding lifetime): the release-time stop-drain must still apply queued
	// deletes AFTER releaseLocked nils h.cache, or a successor reusing the
	// shared cache serves stale until TTL/MaxStaleness.
	cache   Cache
	refresh *cscRefreshQueue

	ch       chan string
	stopCh   chan struct{}
	dropCh   chan struct{} // cap-1: signal run() to discard its in-progress batch
	stopOnce sync.Once
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

// drop discards everything queued (channel + the run loop's in-progress batch)
// WITHOUT applying: after a full cache Flush the per-key deletes are redundant
// and would evict post-flush repopulations. Best-effort against racing
// enqueues.
func (b *cscInvalBatcher) drop() {
	// Drain queued keys (best-effort, non-blocking).
	for draining := true; draining; {
		select {
		case <-b.ch:
		default:
			draining = false
		}
	}
	// Signal run() to clear its in-progress batch; the cap-1 buffer means a signal
	// is never lost even if run() is not currently selecting.
	select {
	case b.dropCh <- struct{}{}:
	default:
	}
}

// enqueue hands a namespaced key to the batcher without blocking the caller. On
// a full queue — or once the batcher is stopped (see stopMu) — it applies the
// delete inline so an invalidation is never dropped.
func (b *cscInvalBatcher) enqueue(nsKey string) {
	b.stopMu.RLock()
	if b.stopped {
		b.stopMu.RUnlock()
		b.apply([]string{nsKey})
		return
	}
	select {
	case b.ch <- nsKey:
		b.stopMu.RUnlock()
	default:
		b.stopMu.RUnlock()
		b.apply([]string{nsKey})
	}
}

// apply deletes the namespaced keys and feeds evicted-hot entries to the
// refresher, using the creation-time snapshot (see the struct fields).
func (b *cscInvalBatcher) apply(keys []string) {
	cache, refresh := b.cache, b.refresh
	if cache == nil {
		return
	}
	lc, canRefresh := cache.(*LocalCache)
	canRefresh = canRefresh && refresh != nil
	var hot []cscRefreshTarget
	for _, k := range keys {
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
	pending := make([]string, 0, 256)
	seen := make(map[string]struct{}, 256)
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
				case k := <-b.ch:
					if _, dup := seen[k]; !dup {
						seen[k] = struct{}{}
						pending = append(pending, k)
					}
				default:
					draining = false
				}
			}
			flush()
			return
		case <-b.dropCh:
			// A full cache Flush superseded the queued deletes: discard the
			// in-progress batch without applying it (see drop()).
			pending = pending[:0]
			clear(seen)
		case k := <-b.ch:
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				pending = append(pending, k)
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
