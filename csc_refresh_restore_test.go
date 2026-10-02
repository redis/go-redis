package redis

import (
	"context"
	"testing"
)

// TestRefreshRepublishSurvivesConcurrentInsert pins that a refresh republish
// keeps the invalidated entry's recency in the same lock as the publish. It
// used to publish with the second-chance bit clear and restore the bit in a
// second lock. On a warm shard the republished entry was then the only
// clear-bit victim, so an insert in that gap evicted the key the refresh had
// just fetched.
func TestRefreshRepublishSurvivesConcurrentInsert(t *testing.T) {
	ctx := context.Background()
	lc := NewLocalCache(CacheConfig{MaxEntries: 2})
	fill := func(key, rk, val string) {
		t.Helper()
		tok, _ := lc.Reserve(key, []string{rk})
		if !lc.fulfill(key, tok, 0, []byte(val)) {
			t.Fatalf("fill %s failed", key)
		}
	}
	// b is older than a; both are read, so the shard is warm.
	fill("get:b", "b", "vb")
	fill("get:a", "a", "va")
	for _, k := range []string{"get:a", "get:b"} {
		if _, ok := lc.Get(ctx, k); !ok {
			t.Fatalf("%s missing", k)
		}
	}

	// Refresh a: collect its target, reserve, stage the kept recency, publish.
	targets := lc.deleteByRedisKeyCollectingHot("a", cscInvalNoHorizon, ^uint64(0), nil)
	if len(targets) != 1 || !targets[0].read {
		t.Fatalf("want 1 hot target carrying the read bit, got %+v", targets)
	}
	tok, _ := lc.Reserve("get:a", []string{"a"})
	lc.stageRefreshAccess("get:a", tok, targets[0].accessNs, targets[0].read)
	if !lc.fulfill("get:a", tok, 0, []byte("va2")) {
		t.Fatal("republish fulfill failed")
	}

	// An insert right after the publish. Both residents were read, so the
	// sweep consumes their second chances and evicts the older one, b.
	if tok, _ := lc.Reserve("get:c", []string{"c"}); tok == 0 {
		t.Fatal("reserve c failed")
	}
	if _, ok := lc.Get(ctx, "get:a"); !ok {
		t.Fatal("the refreshed key was evicted by the next insert")
	}
}
