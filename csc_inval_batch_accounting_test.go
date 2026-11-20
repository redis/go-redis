package redis

import (
	"context"
	"testing"
)

// The batched invalidation apply must report deletions exactly as the
// single-key path does, including the no-op count.
//
// The first version of the batch path derived no-ops from the batch total --
// "nothing in this batch matched" -- which silently undercounted the common
// case where some keys match and others do not. DeletionStats is public, so
// that is a user-visible metric regression, and it is invisible in a
// throughput benchmark.
func TestDeleteManyByRedisKeyDeletionAccounting(t *testing.T) {
	newCache := func() *LocalCache {
		return NewLocalCache(CacheConfig{
			MaxEntries: 128,
		})
	}
	ctx := context.Background()

	// Seed one entry under redis key "present".
	seed := func(c *LocalCache, cacheKey, redisKey string) {
		tok, ok := c.Reserve(cacheKey, []string{redisKey})
		if !ok || tok == 0 {
			t.Fatalf("Reserve(%q) failed", cacheKey)
		}
		if !c.fulfill(cacheKey, tok, 0, []byte("v")) {
			t.Fatalf("Fulfill(%q) failed", cacheKey)
		}
		if _, hit := c.Get(ctx, cacheKey); !hit {
			t.Fatalf("expected %q cached", cacheKey)
		}
	}

	t.Run("mixed batch counts one no-op per unmatched key", func(t *testing.T) {
		c := newCache()
		seed(c, "ck1", "present")

		// One key matches, four do not.
		keys := []string{"present", "absent1", "absent2", "absent3", "absent4"}
		since := make([]int64, len(keys))
		snaps := make([]uint64, len(keys))
		for i := range snaps {
			snaps[i] = ^uint64(0)
		}
		_, removed := c.deleteManyByRedisKeyCollectingHot(keys, since, snaps, nil)
		if removed != 1 {
			t.Fatalf("removed = %d, want 1", removed)
		}
		del, noop := c.DeletionStats()
		if del != uint64(len(keys)) {
			t.Errorf("deletions = %d, want %d", del, len(keys))
		}
		// Four keys matched nothing. Deriving this from the batch total
		// would report 0 here.
		if noop != 4 {
			t.Errorf("noop = %d, want 4 (one per unmatched key)", noop)
		}
	})

	t.Run("matches the single-key path", func(t *testing.T) {
		batch := newCache()
		single := newCache()
		seed(batch, "ck1", "present")
		seed(single, "ck1", "present")

		keys := []string{"present", "absent1", "absent2"}
		since := make([]int64, len(keys))
		snaps := make([]uint64, len(keys))
		for i := range snaps {
			snaps[i] = ^uint64(0)
		}
		batch.deleteManyByRedisKeyCollectingHot(keys, since, snaps, nil)
		for _, k := range keys {
			single.DeleteByRedisKey(k)
		}

		bd, bn := batch.DeletionStats()
		sd, sn := single.DeletionStats()
		if bd != sd || bn != sn {
			t.Errorf("batch (del=%d noop=%d) != single (del=%d noop=%d)", bd, bn, sd, sn)
		}
	})

	t.Run("whole batch unmatched", func(t *testing.T) {
		c := newCache()
		keys := []string{"a", "b", "c"}
		since := make([]int64, len(keys))
		snaps := make([]uint64, len(keys))
		for i := range snaps {
			snaps[i] = ^uint64(0)
		}
		_, removed := c.deleteManyByRedisKeyCollectingHot(keys, since, snaps, nil)
		if removed != 0 {
			t.Fatalf("removed = %d, want 0", removed)
		}
		del, noop := c.DeletionStats()
		if del != 3 || noop != 3 {
			t.Errorf("deletions=%d noop=%d, want 3/3", del, noop)
		}
	})
}
