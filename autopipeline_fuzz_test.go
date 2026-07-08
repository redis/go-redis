package redis_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineDifferentialFuzz runs the same randomized, type-diverse command
// stream through a plain client and through an AutoPipeliner (blocking face, so
// per-call order matches the sequential plain run) and diffs every result. This
// is the highest-leverage correctness net for a batching layer: it re-covers
// cross-talk, ordering, and reply-shape demux across arbitrary command mixes,
// including nil/empty/large/binary values. Both types satisfy redis.Cmdable, so
// the exact same op sequence drives both.
func TestAutoPipelineDifferentialFuzz(t *testing.T) {
	ctx := context.Background()
	// Dedicated DB so FlushDB between runs can't touch anything else.
	client := redis.NewClient(&redis.Options{Addr: ":6379", DB: 15})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	for _, seed := range []int64{1, 7, 42, 1337} {
		ops := genOps(rand.New(rand.NewSource(seed)), 400)

		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		plainRes := runOps(ctx, client, ops)

		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		ap, err := client.AutoPipeline(nil)
		if err != nil {
			t.Fatal(err)
		}
		apRes := runOps(ctx, ap, ops)
		ap.Close()

		for i := range ops {
			if plainRes[i] != apRes[i] {
				t.Fatalf("seed=%d op#%d %s(%s): plain=%q autopipeline=%q",
					seed, i, ops[i].kind, ops[i].key, plainRes[i], apRes[i])
			}
		}
	}
	client.FlushDB(ctx)
}

type fuzzOp struct {
	kind string
	key  string
	val  string
	n    int64
}

func genOps(rng *rand.Rand, n int) []fuzzOp {
	kinds := []string{"set", "get", "getdel", "incr", "incrby", "append", "strlen",
		"exists", "del", "hset", "hget", "hgetall", "lpush", "lrange", "llen",
		"expire", "ttl", "type", "setbig", "setbin"}
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("fz:%d", i)
	}
	ops := make([]fuzzOp, n)
	for i := range ops {
		k := keys[rng.Intn(len(keys))]
		ops[i] = fuzzOp{kind: kinds[rng.Intn(len(kinds))], key: k,
			val: fmt.Sprintf("v%d", rng.Intn(1000)), n: int64(rng.Intn(50))}
	}
	return ops
}

// runOps executes the op sequence via the Cmdable interface and returns a
// normalized "value|error" string per op for cross-run comparison.
func runOps(ctx context.Context, c redis.Cmdable, ops []fuzzOp) []string {
	out := make([]string, len(ops))
	res := func(v interface{}, err error) string {
		if err != nil {
			return "ERR:" + err.Error()
		}
		return fmt.Sprintf("%v", v)
	}
	for i, op := range ops {
		switch op.kind {
		case "set":
			out[i] = res(c.Set(ctx, op.key, op.val, 0).Result())
		case "get":
			out[i] = res(c.Get(ctx, op.key).Result())
		case "getdel":
			out[i] = res(c.GetDel(ctx, op.key).Result())
		case "incr":
			out[i] = res(c.Incr(ctx, op.key).Result())
		case "incrby":
			out[i] = res(c.IncrBy(ctx, op.key, op.n).Result())
		case "append":
			out[i] = res(c.Append(ctx, op.key, op.val).Result())
		case "strlen":
			out[i] = res(c.StrLen(ctx, op.key).Result())
		case "exists":
			out[i] = res(c.Exists(ctx, op.key).Result())
		case "del":
			out[i] = res(c.Del(ctx, op.key).Result())
		case "hset":
			out[i] = res(c.HSet(ctx, op.key, op.val, op.n).Result())
		case "hget":
			out[i] = res(c.HGet(ctx, op.key, op.val).Result())
		case "hgetall":
			out[i] = res(c.HGetAll(ctx, op.key).Result())
		case "lpush":
			out[i] = res(c.LPush(ctx, op.key, op.val).Result())
		case "lrange":
			out[i] = res(c.LRange(ctx, op.key, 0, -1).Result())
		case "llen":
			out[i] = res(c.LLen(ctx, op.key).Result())
		case "expire":
			out[i] = res(c.Expire(ctx, op.key, 1000).Result())
		case "ttl":
			// TTL varies with wall clock; normalize to "set"/"unset".
			d, err := c.TTL(ctx, op.key).Result()
			if err != nil {
				out[i] = "ERR:" + err.Error()
			} else if d > 0 {
				out[i] = "ttl>0"
			} else {
				out[i] = fmt.Sprintf("%v", d)
			}
		case "type":
			out[i] = res(c.Type(ctx, op.key).Result())
		case "setbig":
			out[i] = res(c.Set(ctx, op.key, strings.Repeat("Z", 70000), 0).Result())
		case "setbin":
			out[i] = res(c.Set(ctx, op.key, "b\x00\r\n\x01\xffX", 0).Result())
		}
	}
	return out
}
