package redis

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// erroringPooler fails every Get, so refreshInvalidatedBatch (via withConn)
// returns an error deterministically without a live server.
type erroringPooler struct{ pool.Pooler }

func (*erroringPooler) Get(context.Context) (*pool.Conn, error) {
	return nil, errors.New("csc test: pool get always fails")
}

// TestCSCRefresherStopDrainBailsOnFirstFailure pins #3965 F2: the Close stop-drain
// must BAIL after the first failed refresh chunk rather than give each of many
// chunks a fresh cscRefreshBatchTimeout against a dead/stalled server (which could
// delay Close by minutes). With several chunks queued and every refresh failing,
// exactly ONE chunk is attempted.
func TestCSCRefresherStopDrainBailsOnFirstFailure(t *testing.T) {
	// No cscRefreshWindow mutation: runCSCRefresher runs synchronously below with
	// stop already signalled, so the drain completes in microseconds against the
	// failing pool — the default window/recency timers never fire, and mutating the
	// shared global would risk a -race read against another test's live refresher.
	lc := NewLocalCache(CacheConfig{MaxEntries: 10000})
	c := &baseClient{
		opt:          &Options{},
		csc:          lc,
		cscKeyPrefix: "p:",
		connPool:     &erroringPooler{},
	}
	q := &cscRefreshQueue{
		ch:       make(chan cscRefreshTarget, 1024),
		demandCh: make(chan uint64, 1),
	}
	q.sinceToken.Store(lc.LRUClock())
	h := &cscRevalidateHandle{stop: make(chan struct{}), done: make(chan struct{})}

	// More than one chunk's worth (cscRefreshBatchMax) but within one drainQueue
	// round (< cscRefreshWindowMaxKeys) so the whole backlog is flushed once; a
	// buggy drain would then attempt every chunk.
	const chunks = 3
	const targets = chunks * cscRefreshBatchMax
	for i := 0; i < targets; i++ {
		key := "p:" + strconv.Itoa(i)
		q.ch <- cscRefreshTarget{cacheKey: key, redisKeys: []string{key}}
	}

	// Signal stop up front, then run the refresher synchronously: it takes the
	// stop-drain path, flushes the backlog, and returns.
	close(h.stop)
	c.runCSCRefresher(h, lc, q)

	if got := q.refreshFailed.Load(); got != 1 {
		t.Fatalf("refreshFailed = %d; want 1 — the stop-drain must bail after the first "+
			"failed chunk, not attempt all %d chunks (#3965 F2)", got, chunks)
	}
}

// stopClosingPooler closes stop on its first Get, then fails every Get, counting calls.
// It drives a NORMAL (non-stopping) refresh flush that is interrupted by Close mid-loop.
type stopClosingPooler struct {
	pool.Pooler
	stop chan struct{}
	gets int
}

func (p *stopClosingPooler) Get(context.Context) (*pool.Conn, error) {
	p.gets++
	if p.gets == 1 {
		close(p.stop) // Close arrives while this normal flush is already running
	}
	return nil, errors.New("csc test: pool get always fails")
}

// TestCSCRefresherNormalFlushAbortsOnClose pins r3937722481: a normal (non-stopping)
// refresh flush already running when Close begins must abort BETWEEN chunks instead of
// giving each remaining chunk a fresh cscRefreshBatchTimeout against a dead/stalled
// server (which, with reply budgeting producing one-target chunks, could hold Close for
// tens of minutes). A full window is 4 chunks; the first chunk's Get closes h.stop, so
// the between-chunks check must bail the rest — exactly ONE chunk is attempted.
//
// Red-check: drop the `if !stopping { select <-h.stop }` guard in flush's chunk loop —
// the normal flush ignores the now-closed stop and attempts all 4 chunks (gets == 4).
func TestCSCRefresherNormalFlushAbortsOnClose(t *testing.T) {
	lc := NewLocalCache(CacheConfig{MaxEntries: 100000})
	h := &cscRevalidateHandle{stop: make(chan struct{}), done: make(chan struct{})}
	p := &stopClosingPooler{stop: h.stop}
	c := &baseClient{opt: &Options{}, csc: lc, cscKeyPrefix: "p:", connPool: p}
	q := &cscRefreshQueue{
		ch:       make(chan cscRefreshTarget, cscRefreshWindowMaxKeys+16),
		demandCh: make(chan uint64, 1),
	}
	q.sinceToken.Store(lc.LRUClock())

	// A full window (4 * cscRefreshBatchMax) triggers a NORMAL flush(false,false) from the
	// q.ch case — this is NOT the stop-drain path (h.stop is not pre-closed).
	for i := 0; i < cscRefreshWindowMaxKeys; i++ {
		key := "p:" + strconv.Itoa(i)
		q.ch <- cscRefreshTarget{cacheKey: key, redisKeys: []string{key}}
	}

	done := make(chan struct{})
	go func() { defer close(done); c.runCSCRefresher(h, lc, q) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("refresher did not converge — a normal flush kept refetching after Close began")
	}

	if p.gets != 1 {
		t.Fatalf("pool Gets = %d; want 1 — a normal flush must abort mid-loop once Close begins "+
			"(r3937722481), not attempt every chunk", p.gets)
	}
}

// TestCloneSharesRefreshQueueForDemand pins #3965 F4: clone() (WithTimeout/
// WithContext) must SHARE the owner's refresh queue so a derived client's
// processCached can signal demand on it. Without the share the clone's field is
// nil and signalDemand no-ops, so a clone's miss waits the full window instead of
// nudging the owner's in-window batch. Lifecycle stays owner-only: the clone only
// signals (nil-safe, non-blocking) and never stops the shared refresher.
func TestCloneSharesRefreshQueueForDemand(t *testing.T) {
	parent := &baseClient{
		opt:             &Options{},
		cscRefreshQueue: &cscRefreshQueue{demandCh: make(chan uint64, 1)},
	}
	// A key sitting in the refresher's window: signalDemand nudges only for these.
	parent.cscRefreshQueue.pendingSet.Store("ck:x", struct{}{})

	clone := parent.clone()
	if clone.cscRefreshQueue != parent.cscRefreshQueue {
		t.Fatal("clone did not share the owner's refresh queue; a clone's signalDemand " +
			"would no-op on a nil queue (#3965 F4)")
	}

	// A demand nudge through the clone's pointer must reach the SHARED queue.
	clone.cscRefreshQueue.signalDemand("ck:x")
	select {
	case <-parent.cscRefreshQueue.demandCh:
		// The clone's signal landed on the owner's window.
	default:
		t.Fatal("clone signalDemand did not reach the shared queue's demand channel (#3965 F4)")
	}
}

func TestCSCRefreshOnInvalidateTurnsAMissIntoAHit(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "enabled_next_read_is_a_hit"
		if !enabled {
			name = "disabled_next_read_is_a_miss"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cached := NewClient(&Options{
				Addr:     internalTestRedisAddr(),
				Protocol: 3,
				// Default DrainInterval: the invalidation must actually arrive.
				ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 1000},
				ClientSideCacheRefreshOnInvalidate: enabled,
			})
			seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
			if err := cached.Ping(ctx).Err(); err != nil {
				cached.Close()
				seeder.Close()
				t.Skipf("no redis: %v", err)
			}
			defer cached.Close()
			defer seeder.Close()

			key := "cscrefresh:" + name
			t.Cleanup(func() { seeder.Del(context.Background(), key) })

			// Retry loop: the "recently read" horizon advances on a ticker, so a read
			// can fall just the wrong side of it. Each attempt re-reads immediately
			// before the write to put the entry firmly inside the window.
			var observed bool
			for attempt := 0; attempt < 12 && !observed; attempt++ {
				if err := seeder.Set(ctx, key, "v1", 0).Err(); err != nil {
					t.Fatal(err)
				}
				// Warm it, and mark it recently read.
				if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v1" {
					t.Fatalf("warm read = %q, %v; want v1", got, err)
				}

				beforeRefresh := cached.CSCRefreshStats().Refreshed
				if err := seeder.Set(ctx, key, "v2", 0).Err(); err != nil {
					t.Fatal(err)
				}

				if !enabled {
					// Give the drainer time to apply the invalidation, then confirm the
					// read is a MISS: this is the behaviour the feature improves on.
					time.Sleep(150 * time.Millisecond)
					missesBefore := cached.CSCStats().Misses
					if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v2" {
						t.Fatalf("read after invalidation = %q, %v; want v2", got, err)
					}
					if cached.CSCStats().Misses == missesBefore {
						t.Fatal("with refresh-on-invalidate DISABLED the read after an " +
							"invalidation was a cache HIT; this case cannot detect the " +
							"feature working")
					}
					observed = true
					break
				}

				// Enabled: wait for the background refresh to republish the entry.
				deadline := time.Now().Add(2 * time.Second)
				for time.Now().Before(deadline) {
					if cached.CSCRefreshStats().Refreshed > beforeRefresh {
						observed = true
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if !observed {
					continue // the entry fell outside the recency window; try again
				}

				// The refreshed entry must serve the NEW value, with no miss.
				missesBefore := cached.CSCStats().Misses
				got, err := cached.Get(ctx, key).Result()
				if err != nil {
					t.Fatal(err)
				}
				if got != "v2" {
					t.Fatalf("read after refresh = %q; want v2 — the refresh republished "+
						"a stale value", got)
				}
				if cached.CSCStats().Misses != missesBefore {
					t.Fatalf("read after refresh was a MISS; the whole point is that the "+
						"background refresh had already put the new value in place "+
						"(refreshed=%d)", cached.CSCRefreshStats().Refreshed)
				}
			}
			if !observed {
				t.Fatal("no attempt produced the behaviour under test in 12 tries")
			}
		})
	}
}

func TestCSCRefreshDemandTriggerFlushesWindowEarly(t *testing.T) {
	origWindow := cscRefreshWindow
	cscRefreshWindow = 5 * time.Second // timer must not be the cause
	defer func() { cscRefreshWindow = origWindow }()

	ctx := context.Background()
	cached := NewClient(&Options{
		Addr:                               internalTestRedisAddr(),
		Protocol:                           3,
		ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 1000},
		ClientSideCacheRefreshOnInvalidate: true,
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := cached.Ping(ctx).Err(); err != nil {
		cached.Close()
		seeder.Close()
		t.Skipf("no redis: %v", err)
	}
	defer cached.Close()
	defer seeder.Close()

	const demandKey = "cscrefresh:demand:trigger"
	const siblingKey = "cscrefresh:demand:sibling"
	t.Cleanup(func() { seeder.Del(context.Background(), demandKey, siblingKey) })

	var observed bool
	for attempt := 0; attempt < 12 && !observed; attempt++ {
		if err := seeder.MSet(ctx, demandKey, "d1", siblingKey, "s1").Err(); err != nil {
			t.Fatal(err)
		}
		// Warm both and mark them recently read, so both are collected (the
		// refresher ignores cold keys).
		if _, err := cached.Get(ctx, demandKey).Result(); err != nil {
			t.Fatal(err)
		}
		if got, err := cached.Get(ctx, siblingKey).Result(); err != nil || got != "s1" {
			t.Fatalf("warm sibling = %q, %v; want s1", got, err)
		}

		beforeDemand := cached.CSCRefreshStats().DemandFlushes
		beforeRefresh := cached.CSCRefreshStats().Refreshed

		// Invalidate BOTH: each push deletes its entry and collects it into the
		// window (both are recently read).
		if err := seeder.MSet(ctx, demandKey, "d2", siblingKey, "s2").Err(); err != nil {
			t.Fatal(err)
		}
		// Let the drainer apply both invalidations and the refresher collect them.
		time.Sleep(120 * time.Millisecond)

		// Read the demand key: it is a miss (just invalidated), which fires the
		// demand signal and must flush the whole window — refetching the sibling.
		if got, err := cached.Get(ctx, demandKey).Result(); err != nil || got != "d2" {
			t.Fatalf("read demand key = %q, %v; want d2", got, err)
		}

		// The sibling must now refresh WELL within the 5s window. If it does, the
		// timer cannot be responsible — only the demand flush.
		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if cached.CSCRefreshStats().Refreshed > beforeRefresh {
				observed = true
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if !observed {
			continue // entries fell outside the recency horizon; retry
		}

		if got := cached.CSCRefreshStats().DemandFlushes; got <= beforeDemand {
			t.Fatalf("DemandFlushes did not advance (%d -> %d): the sibling refreshed "+
				"but not via the demand trigger", beforeDemand, got)
		}
		// The sibling read must now be a HIT serving the new value: the demand
		// flush refetched it a window early.
		missesBefore := cached.CSCStats().Misses
		if got, err := cached.Get(ctx, siblingKey).Result(); err != nil || got != "s2" {
			t.Fatalf("sibling after demand flush = %q, %v; want s2", got, err)
		}
		if cached.CSCStats().Misses != missesBefore {
			t.Fatal("sibling read after the demand flush was a MISS; the demand trigger " +
				"should have refetched it when its neighbor was read")
		}
	}
	if !observed {
		t.Fatal("no attempt produced a demand-triggered refresh in 12 tries")
	}
}

// TestCSCRefreshRepublishedEntryIsInvalidatable pins the invariant that the
// background refresh path publishes its entry on a CSC-TRACKED connection.
// Refresh reads run on a background pipelined connection (withPipelineConn),
// which on this base falls back to / draws from a pool whose connections all
// run CLIENT TRACKING while CSC is active. If a refetch ever published on an
// untracked connection, the republished entry could never be invalidated and
// would serve stale until TTL — the exact trap the coalescer's main-pool
// choice avoids. This test would catch that regression: it refreshes an entry,
// then mutates the key again from a second client and requires the caching
// client to converge to the new value. With no TTL set, convergence is only
// possible if the second invalidation was actually delivered, i.e. the
// republished entry was tracked.
func TestCSCRefreshRepublishedEntryIsInvalidatable(t *testing.T) {
	ctx := context.Background()
	cached := NewClient(&Options{
		Addr:                               internalTestRedisAddr(),
		Protocol:                           3,
		ClientSideCacheConfig:              &ClientSideCacheConfig{MaxEntries: 1000},
		ClientSideCacheRefreshOnInvalidate: true,
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := cached.Ping(ctx).Err(); err != nil {
		cached.Close()
		seeder.Close()
		t.Skipf("no redis: %v", err)
	}
	defer cached.Close()
	defer seeder.Close()

	key := "cscrefresh:republished-invalidatable"
	t.Cleanup(func() { seeder.Del(context.Background(), key) })

	var observed bool
	for attempt := 0; attempt < 12 && !observed; attempt++ {
		if err := seeder.Set(ctx, key, "v1", 0).Err(); err != nil {
			t.Fatal(err)
		}
		// Warm it and mark it recently read so the refresher will collect it.
		if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v1" {
			t.Fatalf("warm read = %q, %v; want v1", got, err)
		}

		beforeRefresh := cached.CSCRefreshStats().Refreshed
		if err := seeder.Set(ctx, key, "v2", 0).Err(); err != nil {
			t.Fatal(err)
		}
		// Wait for the background refresh to REPUBLISH the entry.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cached.CSCRefreshStats().Refreshed > beforeRefresh {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if cached.CSCRefreshStats().Refreshed == beforeRefresh {
			continue // the entry fell outside the recency window; retry
		}
		if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v2" {
			t.Fatalf("read after refresh = %q, %v; want v2", got, err)
		}

		// Discriminating check: mutate the key again from the second client. The
		// republished entry must be invalidated and the client must converge to
		// v3. No TTL is set, so if the republished entry were published on an
		// untracked connection no invalidation would arrive and the client would
		// serve v2 forever — this poll would then time out.
		if err := seeder.Set(ctx, key, "v3", 0).Err(); err != nil {
			t.Fatal(err)
		}
		converge := time.Now().Add(2 * time.Second)
		for time.Now().Before(converge) {
			if got, _ := cached.Get(ctx, key).Result(); got == "v3" {
				observed = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !observed {
			t.Fatal("after a background refresh republished the entry, a later " +
				"invalidation never reached the client: the republished entry was " +
				"NOT server-side tracked and would serve stale until TTL")
		}
	}
	if !observed {
		t.Fatal("no attempt produced a background refresh to test in 12 tries")
	}
}

// TestRefreshInvalidatedBatchCancelsReservationsOnSizerPanic pins #3989: if a user
// CacheSizer panics inside Reserve mid reservation loop, the targets reserved so far
// must be cancelled during unwind (the cancellation defer is armed BEFORE the loop),
// not left IN_PROGRESS for readers to block on until StaleTimeout.
func TestRefreshInvalidatedBatchCancelsReservationsOnSizerPanic(t *testing.T) {
	calls := 0
	sizer := func(_ string, _ []string, value []byte) int64 {
		calls++
		if calls == 2 {
			panic("boom: CacheSizer panic mid reservation loop")
		}
		return int64(len(value))
	}
	lc := NewLocalCache(CacheConfig{MaxEntries: 64, Sizer: sizer})
	c := &baseClient{csc: lc, cscKeyPrefix: "p"}

	targets := []cscRefreshTarget{
		{cacheKey: "ck:1", redisKeys: []string{"rk:1"}},
		{cacheKey: "ck:2", redisKeys: []string{"rk:2"}},
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the sizer panic to propagate (the outer refresher recovers it)")
			}
		}()
		_, _ = c.refreshInvalidatedBatch(context.Background(), targets)
	}()

	// ck:1 was reserved before the panic on ck:2; the defer must have cancelled it, so a
	// fresh Reserve sees no lingering IN_PROGRESS placeholder (shouldFetch, non-zero tok).
	tok, shouldFetch := lc.Reserve("ck:1", []string{"rk:1"})
	if !shouldFetch || tok == 0 {
		t.Fatalf("ck:1 reservation not cancelled after the panic (Reserve = %d, %v); a reader "+
			"would block on the orphaned placeholder until StaleTimeout (#3989)", tok, shouldFetch)
	}
}
