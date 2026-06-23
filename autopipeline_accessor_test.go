package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineSecondaryAccessors guards a regression where the deferred
// await() was injected only on the canonical Val()/Result() of each command
// type, leaving secondary value-accessors (Int, Bytes, Bool, Float, the generic
// Cmd type, SliceCmd.Val, map getters, ...) reading cmd.val before the batch
// executed. Each accessor below must block until its command runs and return the
// real value, not the zero value. Run under -race to catch the missing barrier.
func TestAutoPipelineSecondaryAccessors(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	ap := c.AutoPipeline()
	defer ap.Close()

	const G = 50
	var wg sync.WaitGroup
	var bad int64
	fail := func(cond bool) {
		if cond {
			atomic.AddInt64(&bad, 1)
		}
	}

	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			ik := fmt.Sprintf("acc:int:%d", id)
			sk := fmt.Sprintf("acc:str:%d", id)
			hk := fmt.Sprintf("acc:hash:%d", id)

			// Seed values via the autopipeline (these return *StatusCmd/*IntCmd).
			ap.Set(ctx, sk, "12345", 0)
			ap.Set(ctx, ik, "0", 0)
			ap.HSet(ctx, hk, "f1", "v1", "f2", "v2")

			// StringCmd secondary converters — previously missing await().
			n, err := ap.Get(ctx, sk).Int()
			fail(err != nil || n != 12345)
			i64, err := ap.Get(ctx, sk).Int64()
			fail(err != nil || i64 != 12345)
			b, err := ap.Get(ctx, sk).Bytes()
			fail(err != nil || string(b) != "12345")

			// IntCmd path + IntCmd.Uint64 (previously missing await()).
			incr := ap.Incr(ctx, ik)
			u, err := incr.Uint64()
			fail(err != nil || u != 1)

			// Generic Cmd via Do() — the whole *Cmd type was missing await().
			doCmd := ap.Do(ctx, "GET", sk).(*redis.Cmd)
			v, err := doCmd.Int()
			fail(err != nil || v != 12345)
			txt, err := ap.Do(ctx, "GET", sk).(*redis.Cmd).Text()
			fail(err != nil || txt != "12345")

			// SliceCmd.Val()/Result() — previously missing await().
			mget := ap.MGet(ctx, sk, ik)
			vals := mget.Val()
			fail(len(vals) != 2 || vals[0] != "12345")

			// MapStringStringCmd via HGetAll (Val()/Result() are covered, but
			// exercise it as the typical map path).
			m, err := ap.HGetAll(ctx, hk).Result()
			fail(err != nil || m["f1"] != "v1" || m["f2"] != "v2")
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("secondary accessors: %d wrong/zero reads (await regression)", bad)
	}
}
