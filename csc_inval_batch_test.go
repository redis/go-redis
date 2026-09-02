package redis

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingCache implements just enough of Cache for the batcher's apply path
// (refresh==nil + a non-*LocalCache cache => apply calls DeleteByRedisKey once
// per unique key). Other Cache methods are never reached; the embedded nil Cache
// would panic if one were, which is the intended tripwire.
type countingCache struct {
	Cache
	deletes atomic.Int64
	flushes atomic.Int64
	mu      sync.Mutex
	keys    []string
}

func (c *countingCache) DeleteByRedisKey(k string) int {
	c.deletes.Add(1)
	c.mu.Lock()
	c.keys = append(c.keys, k)
	c.mu.Unlock()
	return 1
}

func (c *countingCache) Flush() int { c.flushes.Add(1); return 0 }

func newTestBatcher(cache Cache, chCap int, window time.Duration) *cscInvalBatcher {
	return &cscInvalBatcher{
		window: window,
		cache:  cache,
		ch:     make(chan cscInvalItem, chCap),
		wake:   make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// TestInvalBatcherStopJoinIsSynchronous pins the stop+join teardown contract:
// once join returns, the stop-drain has fully applied — every enqueued delete is
// visible, with no Eventually. Without the join the drain ran asynchronously
// after Close returned (straggler goroutine; a late apply could evict what a
// successor client on a shared cache just repopulated).
func TestInvalBatcherStopJoinIsSynchronous(t *testing.T) {
	cache := &countingCache{}
	b := newTestBatcher(cache, 8, time.Hour) // window never fires; only the stop-drain applies
	go b.run()

	b.enqueue("k1")
	b.enqueue("k2")
	b.stop()
	b.join()

	if got := cache.deletes.Load(); got != 2 {
		t.Fatalf("deletes after stop+join = %d, want 2 (drain must be synchronous)", got)
	}
}

// TestInvalBatcherEnqueueSnapshotsSinceToken pins the #16 horizon snapshot across
// the atomic.Pointer conversion of the batcher's refresh field: enqueue must read
// the CURRENT refresh binding's sinceToken and stamp it on the item, so a
// batch-window delay cannot chill a key that was hot when the invalidation
// arrived.
func TestInvalBatcherEnqueueSnapshotsSinceToken(t *testing.T) {
	b := newTestBatcher(&countingCache{}, 4, time.Hour) // worker not started: item stays on ch
	q := &cscRefreshQueue{}
	q.sinceToken.Store(4242)
	b.refresh.Store(q)

	b.enqueue("k")

	select {
	case it := <-b.ch:
		if it.sinceToken != 4242 {
			t.Fatalf("enqueued item sinceToken = %d, want 4242 (snapshot at enqueue)", it.sinceToken)
		}
	default:
		t.Fatal("enqueue did not land on ch")
	}
}

// TestInvalBatcherRefreshAttachUsesLiveHorizon pins the recency gate across a
// refresh ATTACH: an item enqueued while the batcher had no refresh binding must
// not be applied as "horizon 0" once a binding appears before apply (setRefreshQueue
// repoints the batcher, then stop-drains it). Horizon 0 would mark every valid
// entry hot and refetch cold keys — the loop the horizon exists to prevent. The
// item carries cscInvalNoHorizon and apply falls back to the live horizon, so a
// COLD entry is deleted but NOT offered, while a hot one still is.
func TestInvalBatcherRefreshAttachUsesLiveHorizon(t *testing.T) {
	lc := NewLocalCache(CacheConfig{MaxEntries: 64})
	mkValid := func(cacheKey, redisKey string) {
		tok, fetch := lc.Reserve(cacheKey, []string{redisKey})
		if tok == 0 || !fetch {
			t.Fatalf("Reserve(%q) = token %d, shouldFetch %v", cacheKey, tok, fetch)
		}
		if !lc.fulfill(cacheKey, tok, 0, []byte("v")) {
			t.Fatalf("fulfill(%q) failed", cacheKey)
		}
	}

	// Cold entry: populated BEFORE the attaching client's horizon exists.
	mkValid("ck:cold", "rk:cold")

	// Batcher built with refresh OFF; worker not started so items stay on ch.
	b := newTestBatcher(lc, 4, time.Hour)
	b.enqueue("rk:cold")
	var cold cscInvalItem
	select {
	case cold = <-b.ch:
	default:
		t.Fatal("enqueue did not land on ch")
	}
	if cold.sinceToken != cscInvalNoHorizon {
		t.Fatalf("item enqueued with refresh off: sinceToken = %d, want cscInvalNoHorizon (%d)",
			cold.sinceToken, cscInvalNoHorizon)
	}

	// A refresh-enabled client attaches: its horizon is seeded NOW, after the cold
	// entry, and the batcher is repointed at its queue (as setRefreshQueue does
	// before the stop-drain).
	q := &cscRefreshQueue{ch: make(chan cscRefreshTarget, 8)}
	q.sinceToken.Store(lc.LRUClock())
	b.refresh.Store(q)

	// Hot control: read after the horizon, enqueued with a real snapshot.
	mkValid("ck:hot", "rk:hot")
	b.enqueue("rk:hot")
	var hot cscInvalItem
	select {
	case hot = <-b.ch:
	default:
		t.Fatal("enqueue did not land on ch")
	}

	// The stop-drain applies both under the NEW binding.
	b.apply([]cscInvalItem{cold, hot})

	// Both deleted ...
	ctx := context.Background()
	if _, ok := lc.Get(ctx, "ck:cold"); ok {
		t.Fatal("cold entry not deleted")
	}
	if _, ok := lc.Get(ctx, "ck:hot"); ok {
		t.Fatal("hot entry not deleted")
	}
	// ... but only the hot one is offered for refetch. Without the sentinel the
	// cold item would apply with horizon 0 and be offered too (len == 2).
	if n := len(q.ch); n != 1 {
		t.Fatalf("refresh targets offered = %d, want 1 (hot only); cold key must not be refetched", n)
	}
	if got := <-q.ch; got.cacheKey != "ck:hot" {
		t.Fatalf("offered target = %q, want ck:hot", got.cacheKey)
	}
}

// A full ch must send overflow to spill, never apply a delete inline on the
// caller (which, on the coalescer's reply reader, would stall miss replies).
func TestInvalBatcherOverflowSpillsNotInline(t *testing.T) {
	cache := &countingCache{}
	// Worker NOT started: ch never drains, so enqueues past its cap must overflow.
	b := newTestBatcher(cache, 2, time.Hour)

	b.enqueue("k1") // -> ch
	b.enqueue("k2") // -> ch (now full)
	b.enqueue("k3") // ch full -> spill
	b.enqueue("k4") // ch full -> spill

	if got := cache.deletes.Load(); got != 0 {
		t.Fatalf("overflow applied %d deletes inline on the caller; want 0 (must spill)", got)
	}
	if got := b.spilled.Load(); got != 2 {
		t.Fatalf("spilled counter = %d, want 2", got)
	}
	b.spillMu.Lock()
	n := len(b.spill)
	b.spillMu.Unlock()
	if n != 2 {
		t.Fatalf("spill holds %d items, want 2", n)
	}
}

// The worker drains BOTH ch and spill through the shared seen/pending dedup, so a
// burst of duplicate keys collapses to one delete per unique key.
func TestInvalBatcherWorkerDrainsSpillDedup(t *testing.T) {
	cache := &countingCache{}
	// Large window so only the stop-drain flushes (deterministic single apply).
	b := newTestBatcher(cache, 2, time.Hour)

	const dups = 200
	for i := 0; i < dups; i++ {
		b.enqueue("hot") // fills ch (cap 2) then overflows to spill
	}
	b.enqueue("cold")

	if b.spilled.Load() == 0 {
		t.Fatal("setup: expected overflow to spill")
	}
	if got := cache.deletes.Load(); got != 0 {
		t.Fatalf("setup: %d inline deletes before worker start; want 0", got)
	}

	go b.run()
	b.stop() // stop-drain: drains spill + ch, flushes once, exits

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cache.deletes.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	if got := cache.deletes.Load(); got != 2 {
		t.Fatalf("applied %d deletes; want 2 (dedup must collapse %d 'hot' + 1 'cold')", got, dups)
	}
}

// Past the spill hard cap, enqueue stops growing spill and asks the worker to
// full-Flush the cache instead (bounded-memory fallback for an invalidation
// flood on a keyset larger than the local cache).
func TestInvalBatcherSpillCapTriggersFlush(t *testing.T) {
	cache := &countingCache{}
	b := newTestBatcher(cache, 2, time.Hour)

	// Worker not running: fill ch, then overflow past the cap with distinct keys.
	for i := 0; i < cscInvalSpillMax+16; i++ {
		b.enqueue("k" + strconv.Itoa(i))
	}

	if !b.flushReq.Load() {
		t.Fatal("spill exceeded the cap but flushReq was not set")
	}
	b.spillMu.Lock()
	n := len(b.spill)
	b.spillMu.Unlock()
	if n >= cscInvalSpillMax {
		t.Fatalf("spill = %d, want < cap %d (cap must clear the backlog)", n, cscInvalSpillMax)
	}

	// The worker must consume flushReq and Flush the whole cache exactly.
	go b.run()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cache.flushes.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	b.stop()
	if cache.flushes.Load() == 0 {
		t.Fatal("worker did not full-Flush the cache after the spill cap was hit")
	}
}

// Past the spill RETAINED-BYTE bound, enqueue takes the same full-Flush fallback as
// the item cap — a distinct-large-key flood can pin GBs while the item count stays
// far below cscInvalSpillMax, so the byte bound must trip first.
func TestInvalBatcherSpillByteCapTriggersFlush(t *testing.T) {
	cache := &countingCache{}
	b := newTestBatcher(cache, 2, time.Hour)

	// 1 MiB per key: a dozen distinct keys past ch cross cscInvalSpillMaxBytes (8 MiB)
	// while the item count stays a few, far under the 65536 item cap.
	bigKey := strings.Repeat("x", 1<<20)
	const n = 2 + 12 // ch cap 2, then 12 overflow to spill
	for i := 0; i < n; i++ {
		b.enqueue(bigKey + strconv.Itoa(i))
	}

	if !b.flushReq.Load() {
		t.Fatal("large-key spill crossed the byte bound but flushReq was not set")
	}
	b.spillMu.Lock()
	items := len(b.spill)
	b.spillMu.Unlock()
	if items >= cscInvalSpillMax {
		t.Fatalf("spill held %d items (>= item cap %d); the BYTE bound should have tripped first",
			items, cscInvalSpillMax)
	}

	// The worker must consume flushReq and full-Flush the cache.
	go b.run()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cache.flushes.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	b.stop()
	if cache.flushes.Load() == 0 {
		t.Fatal("worker did not full-Flush the cache after the spill byte bound was hit")
	}
}

// TestClearRefreshQueueDetachesBatcherWithoutJoin pins the Group-A contract: the
// handler paths that tear a batcher down (here clearRefreshQueue; also
// setRefreshQueue/releaseLocked/setInvalBatchWindow) must DETACH and SIGNAL the
// batcher under h.mu but NOT join it there — join waits on the stop-drain, which
// would stall a sibling on the hot path and must never block the GC finalizer. The
// batcher's run() is never started, so its done channel never closes: the old code
// (b.join() under the lock) would hang forever, the fixed code returns the detached
// batcher for the caller to join OUTSIDE the lock.
func TestClearRefreshQueueDetachesBatcherWithoutJoin(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	h := &invalidateHandler{}
	if err := h.bindTo(cache, "p:"); err != nil {
		t.Fatalf("bindTo: %v", err)
	}
	q := &cscRefreshQueue{}
	h.setRefreshQueue(q) // active binding (no batcher yet, returns nil)

	// A batcher whose run() was NEVER started: done never closes, so join() blocks.
	b := newTestBatcher(cache, 1, time.Hour)
	h.mu.Lock()
	h.batcher = b
	h.mu.Unlock()

	var got *cscInvalBatcher
	done := make(chan struct{})
	go func() {
		got = h.clearRefreshQueue(q)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("clearRefreshQueue blocked: it joined the batcher under h.mu " +
			"(a never-started worker); the fix must detach+signal only")
	}
	if got != b {
		t.Fatalf("clearRefreshQueue returned %p, want the detached batcher %p", got, b)
	}
	if !b.stopped {
		t.Fatal("clearRefreshQueue did not signal the batcher to stop")
	}
	if h.batcher != nil {
		t.Fatal("clearRefreshQueue did not detach the batcher from the handler")
	}
}
