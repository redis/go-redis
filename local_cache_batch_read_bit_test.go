package redis

import (
	"context"
	"testing"
)

// The batched invalidation path must hand the refresher the same target the
// single-key path does, second-chance bit included: stageRefreshAccess
// republishes the entry with it. The batch collector left `read` false, so
// with the batch window and refresh on, a refreshed hot key came back with its
// bit clear and was the next eviction victim in a warm shard.
func TestBatchedInvalidationKeepsReadBit(t *testing.T) {
	ctx := context.Background()
	for _, batched := range []bool{false, true} {
		// A full shard, so the read is recorded even under the pressure gate.
		lc := NewLocalCache(CacheConfig{MaxEntries: 1})
		const key, rk = "get:k", "rk"
		tok, _ := lc.Reserve(key, []string{rk})
		if !lc.fulfill(key, tok, 0, []byte("v")) {
			t.Fatal("seed fulfill failed")
		}
		if _, ok := lc.Get(ctx, key); !ok {
			t.Fatal("seeded key missing")
		}
		var targets []cscRefreshTarget
		if batched {
			targets, _ = lc.deleteManyByRedisKeyCollectingHot([]string{rk}, []int64{cscInvalNoHorizon}, []uint64{^uint64(0)}, nil)
		} else {
			targets = lc.deleteByRedisKeyCollectingHot(rk, cscInvalNoHorizon, ^uint64(0), nil)
		}
		if len(targets) != 1 {
			t.Fatalf("batched=%v: %d targets, want 1", batched, len(targets))
		}
		if !targets[0].read {
			t.Fatalf("batched=%v: the target lost the read bit the entry had", batched)
		}
	}
}
