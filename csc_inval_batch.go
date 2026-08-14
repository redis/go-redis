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
	// cache/refresh are snapshotted at creation rather than re-read from the
	// binding on every apply: the batcher's lifetime is strictly inside one
	// binding (releaseLocked stops it before users hits 0 clears the binding),
	// and the snapshot is what lets the release-time stop-drain still apply
	// queued deletes AFTER releaseLocked has nilled h.cache — dropping them
	// there would let a successor reusing the same shared cache serve
	// pre-invalidation values until TTL/MaxStaleness.
	cache   Cache
	refresh *cscRefreshQueue

	ch       chan string
	stopCh   chan struct{}
	dropCh   chan struct{} // cap-1: signal run() to discard its in-progress batch
	stopOnce sync.Once
	// stopMu/stopped interlock enqueue against stop: a handler goroutine can hold
	// a batcher pointer across a concurrent stop (a window-change rebuild while
	// HandlePushNotification is mid-enqueue-loop, or ensureBatcher's RLock fast
	// path returning a just-stopped instance). enqueue sends while holding the
	// read side, so every key sent lands BEFORE stop's write side closes stopCh —
	// guaranteeing run()'s stop-drain sees it; once stopped, enqueue applies
	// inline instead, so no delete is ever parked in a channel nothing drains
	// (which would serve pre-invalidation values until TTL/MaxStaleness).
	stopMu  sync.RWMutex
	stopped bool
}

// stop signals run() to flush and exit; idempotent. It closes a channel and
// flips the stopped flag — it never touches h.mu and does not wait for the
// goroutine — so it is safe to call while holding the handler lock (see
// releaseLocked). Lock order is h.mu → stopMu, never the reverse: enqueue's
// stopMu critical section contains only the flag check and a channel send.
func (b *cscInvalBatcher) stop() {
	b.stopOnce.Do(func() {
		b.stopMu.Lock()
		b.stopped = true
		b.stopMu.Unlock()
		close(b.stopCh)
	})
}

// drop discards everything the batcher currently has queued — the channel plus
// the run loop's in-progress batch — WITHOUT applying it. Called after a full
// cache Flush (FLUSHDB/FLUSHALL) has already removed every entry, so the queued
// per-key deletes are redundant; applying them would evict entries a reader
// repopulated after the flush (an extra miss within the window). Best-effort: a
// key racing in right after is treated as a normal post-flush invalidation.
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

// apply deletes the given namespaced keys and feeds any evicted-hot entries to
// the refresher. Uses the creation-time snapshot (see the struct fields), NOT a
// re-read of the live binding: releaseLocked nils the binding before this
// goroutine's final stop-drain flush runs, and these deletes must still land on
// the (possibly shared, successor-reused) cache.
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
			// Stopped: by the last user releasing the binding (deletes are then a
			// no-op on the nil cache) or by a window change rebuilding the batcher
			// (setInvalBatchWindow), where the queued deletes are still live and
			// must not be lost — the cache would serve pre-invalidation values
			// until TTL/MaxStaleness. Drain what is still buffered in ch into the
			// batch, then flush once and exit.
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
