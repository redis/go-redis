package redis

import (
	"context"
	"testing"
)

// TestCollectHotAndDeleteRecencyFilter pins the recency gate that decides which
// invalidated entries the refresher re-fetches. Every invalidated entry is
// deleted, but only those that were VALID and read since the horizon are
// returned as refetch targets. Getting the comparison or the horizon wrong
// would silently refetch every invalidated key — extra round trips, no test
// failure — so the cold case is asserted explicitly alongside the hot one.
func TestCollectHotAndDeleteRecencyFilter(t *testing.T) {
	ctx := context.Background()
	lc := NewLocalCache(CacheConfig{MaxEntries: 64})

	mkValid := func(cacheKey, redisKey string) {
		tok, fetch := lc.Reserve(cacheKey, []string{redisKey})
		if tok == 0 || !fetch {
			t.Fatalf("Reserve(%q) = token %d, shouldFetch %v; want a fresh reservation", cacheKey, tok, fetch)
		}
		if !lc.fulfill(cacheKey, tok, 0, []byte("v")) {
			t.Fatalf("fulfill(%q) failed", cacheKey)
		}
	}

	// Cold entry is created first; the horizon is snapshotted after it, so its
	// recency token is at or below the horizon.
	mkValid("ck:cold", "rk:cold")
	horizon := lruSequence.Load()

	// Hot entry is created after the horizon snapshot, so its recency token is
	// strictly above the horizon regardless of any concurrent LRU activity.
	mkValid("ck:hot", "rk:hot")

	// Cold: NOT collected (read no more recently than the horizon), but still deleted.
	if got := lc.deleteByRedisKeyCollectingHot("rk:cold", horizon, nil); len(got) != 0 {
		t.Fatalf("cold entry collected as a refetch target: %v", got)
	}
	if _, ok := lc.Get(ctx, "ck:cold"); ok {
		t.Fatal("cold entry was not deleted by the invalidation")
	}

	// Hot: collected (read after the horizon) and deleted.
	got := lc.deleteByRedisKeyCollectingHot("rk:hot", horizon, nil)
	if len(got) != 1 || got[0].cacheKey != "ck:hot" {
		t.Fatalf("hot entry not collected as the sole refetch target: %v", got)
	}
	if _, ok := lc.Get(ctx, "ck:hot"); ok {
		t.Fatal("hot entry was not deleted by the invalidation")
	}

	// Unknown redis key: nothing collected, no panic on the missing index.
	if got := lc.deleteByRedisKeyCollectingHot("rk:nonexistent", 0, nil); len(got) != 0 {
		t.Fatalf("unknown redis key produced refetch targets: %v", got)
	}
}

// TestCSCRefreshReplyCacheable pins the refresh cacheability decision on the FULL
// reply, not raw[0]. A RESP3 attribute-prefixed error leads with RespAttr ('|'),
// so a first-byte error check would publish an attributed error as a fresh entry
// (a false success that bumps Refreshed and forces the next reader to evict and
// refetch). A nil reply IS cacheable; an empty reply never is.
func TestCSCRefreshReplyCacheable(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		cacheable bool
	}{
		{"plain value", "$2\r\nhi\r\n", true},
		{"resp3 nil", "_\r\n", true},
		{"resp2 nil", "$-1\r\n", true},
		{"top-level error", "-WRONGTYPE nope\r\n", false},
		{"blob error", "!11\r\nERR bad arg\r\n", false},
		// The bug: the error frame is prefixed by a RESP3 attribute, so raw[0]=='|'.
		{"attribute-prefixed error", "|1\r\n$3\r\nttl\r\n:100\r\n-WRONGTYPE nope\r\n", false},
		{"attribute-prefixed value", "|1\r\n$3\r\nttl\r\n:100\r\n$2\r\nhi\r\n", true},
		{"empty", "", false},
	}
	for _, tc := range cases {
		if got := cscRefreshReplyCacheable([]byte(tc.raw)); got != tc.cacheable {
			t.Errorf("%s: cscRefreshReplyCacheable = %v, want %v", tc.name, got, tc.cacheable)
		}
	}
}

// TestCSCRefreshChunkEnd pins the refresh round-trip boundaries: count cap,
// request-byte budget (key wire form), and the expected-REPLY-byte budget —
// the reply side is what jams a flow-controlled path, so large evicted values
// must shrink the chunk. The first target always goes.
func TestCSCRefreshChunkEnd(t *testing.T) {
	mk := func(key string, val int) cscRefreshTarget {
		return cscRefreshTarget{cacheKey: key, valBytes: val}
	}

	// Reply budget splits the chunk: three 15KiB values fit two per chunk
	// under the 32KiB reply budget (request bytes tiny).
	big := []cscRefreshTarget{mk("get:a", 15<<10), mk("get:b", 15<<10), mk("get:c", 15<<10)}
	if got := cscRefreshChunkEnd(big, 0, 0, 1<<20); got != 2 {
		t.Fatalf("reply budget: end = %d, want 2 (two 15KiB replies fit, third exceeds 32KiB)", got)
	}
	if got := cscRefreshChunkEnd(big, 2, 0, 1<<20); got != 3 {
		t.Fatalf("tail: end = %d, want 3", got)
	}

	// A single oversized value still goes (progress guarantee).
	huge := []cscRefreshTarget{mk("get:x", 1<<20), mk("get:y", 1)}
	if got := cscRefreshChunkEnd(huge, 0, 0, 1<<20); got != 1 {
		t.Fatalf("oversized first: end = %d, want 1 (always include the first target)", got)
	}

	// Request-byte budget still enforced: long keys, zero-size values.
	longKey := make([]byte, 100)
	for i := range longKey {
		longKey[i] = 'k'
	}
	keys := []cscRefreshTarget{mk(string(longKey), 0), mk(string(longKey), 0), mk(string(longKey), 0)}
	if got := cscRefreshChunkEnd(keys, 0, 0, 150); got != 1 {
		t.Fatalf("request budget: end = %d, want 1 (second 100B key exceeds 150B budget)", got)
	}

	// Count cap: many tiny targets stop at cscRefreshBatchMax.
	many := make([]cscRefreshTarget, cscRefreshBatchMax+10)
	for i := range many {
		many[i] = mk("get:k", 1)
	}
	if got := cscRefreshChunkEnd(many, 0, 0, 1<<20); got != cscRefreshBatchMax {
		t.Fatalf("count cap: end = %d, want %d", got, cscRefreshBatchMax)
	}
}

// TestCSCRefreshDemandGenerationIgnoresStaleSignal pins the demand generation gate:
// a demand nudge is stamped with the window generation it belongs to, and the
// refresher ignores a nudge whose generation was retired — so a nudge for an
// already-flushed window cannot flush the NEXT one early (which would defeat the
// coalescing window and inflate DemandFlushes). Pins signalDemand's stamping and the
// demandIsCurrent truth table (the goroutine race lives inside runCSCRefresher).
func TestCSCRefreshDemandGenerationIgnoresStaleSignal(t *testing.T) {
	if !cscDemandRefresh {
		t.Skip("demand refresh disabled")
	}
	q := &cscRefreshQueue{demandCh: make(chan uint64, 1)}

	q.pendingSet.Store("ck", struct{}{})
	q.signalDemand("ck")
	var stale uint64
	select {
	case stale = <-q.demandCh:
	default:
		t.Fatal("signalDemand did not stamp a nudge for a pending key")
	}
	if !q.demandIsCurrent(stale) {
		t.Fatal("a nudge for the CURRENT window must be honored")
	}

	q.pendingSet.Delete("ck")
	q.demandGen.Add(1) // refresher clears window N and retires its generation

	if q.demandIsCurrent(stale) {
		t.Fatal("a nudge from a retired generation must be ignored")
	}

	q.pendingSet.Store("ck2", struct{}{})
	q.signalDemand("ck2")
	select {
	case g := <-q.demandCh:
		if !q.demandIsCurrent(g) {
			t.Fatal("a nudge for the new current window must be honored")
		}
	default:
		t.Fatal("signalDemand did not stamp a nudge for the new window's key")
	}
}
