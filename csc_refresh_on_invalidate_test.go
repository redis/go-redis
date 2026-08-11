package redis

import (
	"context"
	"testing"
	"time"
)

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
