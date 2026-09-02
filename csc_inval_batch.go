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

// cscInvalSpillMax hard-caps the overflow spill buffer. The spill is normally
// self-limiting (duplicate keys collapse in the worker's dedup), but the set of
// keys a server can invalidate is NOT bounded by the local cache size: Redis
// keeps CLIENT TRACKING for every key a connection read, even after the local
// LRU evicted it (no per-key untrack), so a workload that reads far more than
// MaxEntries and then invalidates them can grow the spill without bound. At this
// many buffered items the batcher stops spilling and schedules ONE full cache
// Flush instead — a correctness-preserving, O(1)-memory fallback (a flush can
// never serve stale) that also keeps the reader unblocked.
const cscInvalSpillMax = 1 << 16 // 65536

// cscInvalSpillMaxBytes hard-caps the RETAINED KEY BYTES in the overflow spill.
// The item cap alone bounds the spill by COUNT, not memory: the spill holds
// namespaced key STRINGS (no values), but a burst of distinct large keys can
// retain GBs while the item count stays far below cscInvalSpillMax. When the
// retained bytes would cross this bound the batcher takes the SAME full-Flush
// fallback the item cap triggers (a single oversized key crosses it alone). 8
// MiB of key strings is already tens of thousands of large keys — in the spirit
// of the refresh queue's key-byte cap (cscRefreshTargetMaxBytes).
const cscInvalSpillMaxBytes = 8 << 20 // 8 MiB

// cscInvalItem is one queued invalidation, tagged with the batcher epoch it
// was enqueued under so a full-cache flush can supersede it (see drop()).
type cscInvalItem struct {
	key   string
	epoch uint64
	// sinceToken is the refresh "recently-read" horizon snapshotted at ENQUEUE,
	// used by apply for the hot-entry check instead of a live load. A batch window
	// >= the recency tick (200ms) would otherwise let the horizon advance during
	// the wait, so a key that was hot when the invalidation arrived fails the
	// check by apply time and silently degrades to plain eviction. Free size-wise:
	// it fits the 8-byte alignment padding the struct already carries.
	//
	// cscInvalNoHorizon (-1) when refresh was OFF at enqueue. apply re-reads the
	// refresh binding at APPLY time, and a refresh-enabled client attaching in
	// between (setRefreshQueue repoints the batcher, then stop-drains it) would
	// otherwise see horizon 0 and treat EVERY valid entry as hot — the cold-key
	// refresh loop the horizon exists to prevent. With the sentinel, apply falls
	// back to the live horizon (seeded at that client's start), which can only
	// under-refresh, never chase cold keys. LRUClock is a monotonic sequence >= 0,
	// so -1 cannot collide with a real horizon.
	sinceToken int64
}

// cscInvalNoHorizon marks an invalidation enqueued while no refresh binding
// existed (see cscInvalItem.sinceToken).
const cscInvalNoHorizon = -1

type cscInvalBatcher struct {
	window time.Duration
	// cache is snapshotted at creation (batcher lifetime is inside the binding
	// lifetime): the release-time stop-drain must still apply queued deletes AFTER
	// releaseLocked nils h.cache, or a successor reusing the shared cache serves
	// stale until TTL/MaxStaleness.
	cache Cache
	// refresh is the active refresh binding. Atomic (not a plain snapshot) because
	// set/clearRefreshQueue can REPOINT it — under h.mu, before stopping the
	// batcher — at the surviving/new binding, so the stop-drain feeds hot keys to
	// the live refresher instead of the dead one the batcher was created with. Read
	// by both the producer (enqueue) and the worker (apply).
	refresh atomic.Pointer[cscRefreshQueue]

	ch       chan cscInvalItem
	stopCh   chan struct{}
	stopOnce sync.Once
	// spill absorbs invalidations that arrive while ch is full, so an overflow
	// never applies a delete inline on the producer — critically the coalescer's
	// reply reader, where inline apply (lock-held cache work) stalls miss replies
	// (head-of-line). The worker drains spill through the SAME seen/pending dedup
	// pipeline as ch, so a burst of duplicate keys collapses to one delete per
	// key. The worker normally catches up, but the spill is hard-capped at
	// cscInvalSpillMax: the invalidatable keyset is NOT bounded by cache capacity
	// (the server tracks keys past local LRU eviction), so at the cap the batcher
	// schedules one full Flush (flushReq) instead of growing spill. wake nudges
	// the worker to drain promptly (cap 1, coalescing: one wake drains the whole
	// slice).
	spillMu sync.Mutex
	spill   []cscInvalItem
	// spillBytes tracks the retained KEY bytes currently held in spill, guarded by
	// spillMu alongside spill itself. Bounds spill memory by bytes as well as by
	// item count (see cscInvalSpillMaxBytes); reset with spill on drain/flush.
	spillBytes int
	// chBytes tracks the retained KEY bytes currently sitting in ch. ch is bounded
	// by item COUNT (its slot count) but not memory, so a burst of large keys — or
	// one key larger than the whole byte cap — would retain unbounded bytes before
	// the spill path (and its byte cap) ever engages. enqueue RESERVES atomically
	// before the send (mirroring reserveWireBytes): it charges keyLen up front and
	// rolls back if the total crosses cscInvalSpillMaxBytes, so concurrent producers
	// can no longer each read a stale below-cap Load and all overshoot. A send that
	// then finds ch full rolls the reservation back too (those bytes are tracked on
	// the spill path via spillBytes); the worker subtracts on consume. Written by
	// producers (reserve/rollback) and the worker (consume); read by the gate.
	chBytes atomic.Int64
	wake    chan struct{}
	// flushReq: spill hit cscInvalSpillMax, so the worker must drop() + full-Flush
	// the cache instead of applying a huge backlog. Set by enqueue, consumed by the
	// worker (CAS to false).
	flushReq atomic.Bool
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
	// done is closed when run() exits (after its stop-drain and final flush);
	// join() waits on it so stop+join is a synchronous teardown. Nil only in
	// test-constructed literals whose worker never starts.
	done chan struct{}
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

// join blocks until run() has exited — i.e. the stop-drain and final flush have
// fully applied. Callers pair it with stop() so a teardown or rebuild is
// SYNCHRONOUS: without the join, the old worker's drain ran after Close (or
// after a rebuild installed a new batcher with a new window contract), leaving
// a straggler goroutine past Close and letting a late apply evict entries a
// successor just repopulated. Safe under h.mu: run() never takes handler locks
// (its drain touches only the cache shards, applyMu, and non-blocking refresh
// offers). No-op for a batcher whose worker was never started (test literals).
func (b *cscInvalBatcher) join() {
	if b.done == nil {
		return
	}
	<-b.done
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
	it := cscInvalItem{key: nsKey, epoch: b.epoch.Load(), sinceToken: cscInvalNoHorizon}
	// Snapshot the refresh recency horizon NOW (enqueue time), so a batch-window
	// delay can't advance it out from under apply's hot-entry check (see the field
	// doc). Cheap: one atomic load, no lock. Stays cscInvalNoHorizon when refresh
	// is off, so a binding that appears before apply cannot read it as horizon 0.
	if r := b.refresh.Load(); r != nil {
		it.sinceToken = r.sinceToken.Load()
	}
	keyLen := int64(len(it.key))
	b.stopMu.RLock()
	if b.stopped {
		b.stopMu.RUnlock()
		b.apply([]cscInvalItem{it})
		return
	}
	// Pre-admission byte gate: ch is bounded by item COUNT, not memory. When this
	// key alone exceeds the byte cap, or the bytes already retained in ch plus this
	// key would cross it, skip ch entirely and take the full-Flush fallback (below)
	// — otherwise a distinct-large-key flood retains unbounded bytes in ch before
	// the spill path ever engages. RESERVE the bytes atomically before the send
	// (mirroring reserveWireBytes): a stale Load-then-Add let concurrent producers
	// each read a below-cap value and all overshoot the cap, so charge keyLen up
	// front and roll back when it crosses.
	overBytes := keyLen > cscInvalSpillMaxBytes
	if !overBytes {
		if b.chBytes.Add(keyLen) > cscInvalSpillMaxBytes {
			b.chBytes.Add(-keyLen) // over cap: divert to the full-Flush fallback below
			overBytes = true
		} else {
			select {
			case b.ch <- it:
				b.stopMu.RUnlock()
				return
			default:
				// ch full: release the reservation (the spill path tracks these bytes
				// via spillBytes) and fall through to the spill path below.
				b.chBytes.Add(-keyLen)
			}
		}
	}
	{
		// Not admitted to ch (full, or the byte gate diverted here): park on spill
		// (off the producer's read path) for the worker to drain, instead of applying
		// inline. Correctness is unchanged — the item carries its enqueue-time epoch
		// and the worker applies it under applyMu with the same epoch check as the ch
		// path.
		//
		// At either hard cap, stop growing spill: drop this item, clear the backlog,
		// and ask the worker to full-Flush the cache. Safe because a Flush drops
		// everything (nothing stale can survive), and correct because the capped case
		// is a pathological invalidation flood where the cache is churning wholesale
		// anyway. Needed because the invalidatable keyset is not bounded by cache size
		// (the server tracks keys past local LRU eviction).
		b.spillMu.Lock()
		// Trip on ANY bound: the byte gate above (overBytes), the spill item count, or
		// the spill retained-key bytes. Bytes matter because the invalidatable keyset
		// is not bounded by cache size and keys can be large, so a distinct-large-key
		// flood grows memory long before the item cap. A single oversized key trips it
		// alone (overBytes).
		if overBytes || len(b.spill) >= cscInvalSpillMax ||
			b.spillBytes+len(it.key) > cscInvalSpillMaxBytes {
			// The cleared backlog plus this item are superseded by the coming
			// full-Flush and never reach deleteByRedisKeyCollectingHot. They were
			// already counted as incoming invalidations at the handler; they simply
			// won't become deletions — the Flush supersedes them wholesale.
			b.spill = b.spill[:0]
			b.spillBytes = 0
			// Set flushReq BEFORE releasing stopMu. stop() takes stopMu.Lock, so it
			// cannot close stopCh until this store is visible; the worker's stop-drain
			// then sees the request via fullFlushIfRequested instead of exiting and
			// losing the flush (which would leave the flood's entries stale). Setting
			// it after RUnlock would race stop() and drop the flush.
			b.flushReq.Store(true)
		} else {
			b.spill = append(b.spill, it)
			b.spillBytes += len(it.key)
		}
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
	b.spillBytes = 0
	b.spillMu.Unlock()
	return s
}

// apply deletes the namespaced keys and feeds evicted-hot entries to the current
// refresh binding (b.refresh, which set/clearRefreshQueue may have repointed at a
// survivor before the stop-drain — see the struct field).
func (b *cscInvalBatcher) apply(items []cscInvalItem) {
	cache, refresh := b.cache, b.refresh.Load()
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
		// Use the horizon snapshotted at ENQUEUE, not a live load: a batch-window
		// delay advances the live horizon and would chill keys that were hot when
		// the invalidation arrived (see cscInvalItem.sinceToken). An item enqueued
		// with no refresh binding carries cscInvalNoHorizon; the binding that
		// exists NOW (a client attached in between) supplies the live horizon, which
		// is a real recency test — never horizon 0, which would mark every entry hot.
		since := it.sinceToken
		if since == cscInvalNoHorizon {
			since = refresh.sinceToken.Load()
		}
		hot = lc.deleteByRedisKeyCollectingHot(k, since, hot[:0])
		for i := range hot {
			refresh.offer(hot[i])
		}
	}
}

func (b *cscInvalBatcher) run() {
	// Closed LAST (deferred first): join() unblocks only after the stop-drain and
	// final flush below have fully applied, making a stop+join teardown synchronous.
	defer close(b.done)
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
		// A duplicate key in the same epoch is dropped here: apply() deletes the key
		// once, so it becomes a single deletion. The incoming push was already
		// counted at the handler, so the dropped duplicate needs no accounting — it
		// just won't add a deletion, which is exactly the dedup signal.
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
	// fullFlushIfRequested consumes a flushReq set by enqueue when spill hit its
	// cap: drop the whole backlog (epoch bump skips any stale-epoch delete still
	// queued/in-flight) and Flush the cache — the O(1)-memory, correctness-
	// preserving fallback for an invalidation flood. Recover so a Flush panic can't
	// kill the worker (its death would let spill grow unbounded again).
	fullFlushIfRequested := func() {
		if !b.flushReq.CompareAndSwap(true, false) {
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					internal.Logger.Printf(context.Background(),
						"redis: csc invalidation full-flush panic: %v", r)
				}
			}()
			b.drop()
			if b.cache != nil {
				b.cache.Flush()
			}
		}()
		// The pending batch is superseded by the Flush and never applied as
		// individual deletions (the Flush invalidates wholesale). Incoming pushes
		// were already counted at the handler, so just reset the batch.
		pending = pending[:0]
		for k := range seen {
			delete(seen, k)
		}
	}
	for {
		select {
		case <-b.stopCh:
			// Stopped (last release, or a window-change rebuild where queued deletes
			// are still live and must not be lost): drain spill AND ch into the
			// batch, flush once, exit. enqueue appends spill under stopMu.RLock, so
			// anything spilled before stop set stopped=true is visible here.
			fullFlushIfRequested()
			drainSpill()
			for draining := true; draining; {
				select {
				case it := <-b.ch:
					b.chBytes.Add(-int64(len(it.key)))
					add(it)
				default:
					draining = false
				}
			}
			drainSpill() // catch keys spilled during the ch drain
			flush()
			return
		case it := <-b.ch:
			b.chBytes.Add(-int64(len(it.key)))
			add(it)
		case <-b.wake:
			// An overflow parked keys on spill (or hit the cap and asked for a full
			// flush); handle the flush request first, then drain what's left.
			fullFlushIfRequested()
			drainSpill()
		case <-t.C:
			fullFlushIfRequested()
			drainSpill()
			flush()
			t.Reset(b.window)
		}
	}
}
