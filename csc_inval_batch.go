package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
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
	// spill absorbs invalidations that arrive while ch is full, so an overflow
	// never applies a delete inline on the producer — critically the coalescer's
	// reply reader, where inline apply (lock-held cache work) stalls miss replies
	// (head-of-line). The worker drains spill through the SAME seen/pending dedup
	// pipeline as ch, so a burst of duplicate keys collapses to one delete per
	// key; distinct keys are bounded by the tracked keyset (⊂ cache capacity), so
	// spill needs no hard cap — the worker catches up. wake nudges the worker to
	// drain promptly (cap 1, coalescing: one wake drains the whole slice).
	spillMu sync.Mutex
	spill   []cscInvalItem
	wake    chan struct{}
	// spilled counts overflow events (ch full at send time). Internal; read by
	// tests and available for a future stat, so the feature's degradation under
	// invalidation bursts is observable instead of silent.
	spilled atomic.Uint64
	// epoch versions enqueued invalidations against full-cache flushes: drop()
	// bumps it, and apply skips items from an older epoch. applyMu serializes
	// the two: without it, apply could snapshot the epoch, a concurrent
	// drop()+Flush() land mid-loop, and the remaining stale items would still
	// be applied AFTER the flush — evicting post-flush repopulations, the exact
	// case the epoch exists to prevent. drop() holding applyMu means any
	// in-flight batch finishes BEFORE the flush (harmless: the flush wipes
	// everything anyway), and every later apply sees the new epoch.
	epoch   atomic.Uint64
	applyMu sync.Mutex
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
	// Serialize with apply (see applyMu): after drop returns, no stale-epoch
	// delete can run — the caller may flush the cache immediately.
	b.applyMu.Lock()
	b.epoch.Add(1)
	b.applyMu.Unlock()
}

// enqueue hands a namespaced key to the batcher without blocking the caller and
// without ever applying a delete inline on the producer (see the spill field):
// on a full ch it appends to spill and nudges the worker; only once the batcher
// is stopped (no worker left to drain, see stopMu) does it apply inline so an
// invalidation is never dropped.
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
		// ch full: park on spill (off the producer's read path) for the worker
		// to drain, instead of applying inline here. Correctness is unchanged —
		// the item carries its enqueue-time epoch and the worker applies it under
		// applyMu with the same epoch check as the ch path.
		b.spillMu.Lock()
		b.spill = append(b.spill, it)
		b.spillMu.Unlock()
		b.stopMu.RUnlock()
		b.spilled.Add(1)
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}
}

// takeSpill swaps out the accumulated spill for the worker to drain. Returns nil
// when empty. The swapped slice is owned by the caller; b.spill starts fresh so
// concurrent enqueues never race the drain.
func (b *cscInvalBatcher) takeSpill() []cscInvalItem {
	b.spillMu.Lock()
	s := b.spill
	b.spill = nil
	b.spillMu.Unlock()
	return s
}

// apply deletes the namespaced keys and feeds evicted-hot entries to the
// refresher, using the creation-time snapshot (see the struct fields).
func (b *cscInvalBatcher) apply(items []cscInvalItem) {
	cache, refresh := b.cache, b.refresh
	if cache == nil {
		return
	}
	// Held for the whole batch so a concurrent drop()+Flush() cannot land
	// mid-loop and leave stale-epoch deletes running post-flush (see applyMu).
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
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
		// Recover so a panic in a cache/refresh call can never kill the worker:
		// overflow is parked on the unbounded spill buffer, so a dead worker would
		// let spill grow without bound. Log and keep looping instead.
		func() {
			defer func() {
				if r := recover(); r != nil {
					internal.Logger.Printf(context.Background(),
						"redis: csc invalidation batch apply panic: %v", r)
				}
			}()
			b.apply(pending)
		}()
		pending = pending[:0]
		for k := range seen {
			delete(seen, k)
		}
	}
	// add runs one item through the epoch-scoped dedup and size-flushes at the
	// cap. Shared by the ch, spill, and stop-drain paths so spilled items get the
	// SAME dedup as fast-path items — a burst of duplicate keys collapses to one
	// delete per key.
	add := func(it cscInvalItem) {
		if e, dup := seen[it.key]; !dup || e != it.epoch {
			seen[it.key] = it.epoch
			pending = append(pending, it)
		}
		if len(pending) >= cscInvalBatchMax {
			flush()
			t.Reset(b.window)
		}
	}
	drainSpill := func() {
		for _, it := range b.takeSpill() {
			add(it)
		}
	}
	for {
		select {
		case <-b.stopCh:
			// Stopped (last release, or a window-change rebuild where queued deletes
			// are still live and must not be lost): drain spill AND ch into the
			// batch, flush once, exit. enqueue appends spill under stopMu.RLock, so
			// anything spilled before stop set stopped=true is visible here.
			drainSpill()
			for draining := true; draining; {
				select {
				case it := <-b.ch:
					add(it)
				default:
					draining = false
				}
			}
			drainSpill() // catch keys spilled during the ch drain
			flush()
			return
		case it := <-b.ch:
			add(it)
		case <-b.wake:
			// An overflow parked keys on spill; drain them off the producer.
			drainSpill()
		case <-t.C:
			drainSpill()
			flush()
			t.Reset(b.window)
		}
	}
}
