package redis

import (
	"strconv"
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
