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
