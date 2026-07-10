package redis_test

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Reply-shape demux: a single batch mixing heterogeneous reply types must keep
// the reader in sync so every command gets ITS OWN correctly-typed reply. Prior
// correctness tests batched only homogeneous simple commands (SET/GET/INCR),
// which proves key/value pairing but not that the parser stays aligned across
// bulk + array + int + map + nil + double + large(multi-read) + binary-safe
// values. One demux slip corrupts every later reply in the batch. AutoPipeline
// dispatches through the same execer, so it inherits this.

func seedReplyShapeData(t *testing.T, ctx context.Context, c *redis.Client) (big, binary string) {
	t.Helper()
	big = strings.Repeat("A", 100_000)     // > bufio buffer -> multi-read
	binary = "x\x00y\r\nz\x01\xffend"      // embedded NUL, CRLF, high bytes
	c.Del(ctx, "rs:list", "rs:hash", "rs:big", "rs:bin", "rs:s", "rs:n", "rs:f")
	if err := c.RPush(ctx, "rs:list", "a", "b", "c").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, "rs:hash", "f1", "v1", "f2", "v2").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "rs:big", big, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "rs:bin", binary, 0).Err(); err != nil {
		t.Fatal(err)
	}
	return big, binary
}

func assertReplyShapes(t *testing.T, cSet *redis.StatusCmd, cGet *redis.StringCmd,
	cIncr *redis.IntCmd, cArr *redis.StringSliceCmd, cMap *redis.MapStringStringCmd,
	cNil *redis.StringCmd, cBig, cBin *redis.StringCmd, cFloat *redis.FloatCmd,
	big, binary string) {
	t.Helper()
	if cSet.Val() != "OK" {
		t.Errorf("status: %q, want OK", cSet.Val())
	}
	if cGet.Val() != "v" {
		t.Errorf("bulk: %q, want v", cGet.Val())
	}
	if cIncr.Val() != 1 {
		t.Errorf("int: %d, want 1", cIncr.Val())
	}
	if got := cArr.Val(); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("array: %v, want [a b c]", got)
	}
	if got := cMap.Val(); got["f1"] != "v1" || got["f2"] != "v2" {
		t.Errorf("map: %v", got)
	}
	if cNil.Err() != redis.Nil {
		t.Errorf("nil: err=%v, want redis.Nil", cNil.Err())
	}
	if cBig.Val() != big {
		t.Errorf("large: len=%d, want %d", len(cBig.Val()), len(big))
	}
	if cBin.Val() != binary {
		t.Errorf("binary-safe: %q, want %q", cBin.Val(), binary)
	}
	if cFloat.Val() != 1.5 {
		t.Errorf("double: %v, want 1.5", cFloat.Val())
	}
}

func TestPipelineHeterogeneousReplyDemux(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	pipe := c.Pipeline()
	cSet := pipe.Set(ctx, "rs:s", "v", 0)
	cGet := pipe.Get(ctx, "rs:s")
	cIncr := pipe.Incr(ctx, "rs:n")
	cArr := pipe.LRange(ctx, "rs:list", 0, -1)
	cMap := pipe.HGetAll(ctx, "rs:hash")
	cNil := pipe.Get(ctx, "rs:missing")
	cBig := pipe.Get(ctx, "rs:big")
	cBin := pipe.Get(ctx, "rs:bin")
	cFloat := pipe.IncrByFloat(ctx, "rs:f", 1.5)
	_, _ = pipe.Exec(ctx) // Exec returns the first error (the redis.Nil); check per-cmd

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}

// TestPipelineHeterogeneousReplyDemuxRESP2 runs the same demux under RESP2, which
// parses maps/doubles/verbatim differently on the wire — the reader must still
// stay aligned across the mixed batch.
func TestPipelineHeterogeneousReplyDemuxRESP2(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379", Protocol: 2})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	pipe := c.Pipeline()
	cSet := pipe.Set(ctx, "rs:s", "v", 0)
	cGet := pipe.Get(ctx, "rs:s")
	cIncr := pipe.Incr(ctx, "rs:n")
	cArr := pipe.LRange(ctx, "rs:list", 0, -1)
	cMap := pipe.HGetAll(ctx, "rs:hash")
	cNil := pipe.Get(ctx, "rs:missing")
	cBig := pipe.Get(ctx, "rs:big")
	cBin := pipe.Get(ctx, "rs:bin")
	cFloat := pipe.IncrByFloat(ctx, "rs:f", 1.5)
	_, _ = pipe.Exec(ctx)

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}

func TestAutoPipelineHeterogeneousReplyDemux(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	big, binary := seedReplyShapeData(t, ctx, c)

	ap, err := c.AutoPipeline() // blocking face: results ready on call
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	cSet := ap.Set(ctx, "rs:s", "v", 0)
	cGet := ap.Get(ctx, "rs:s")
	cIncr := ap.Incr(ctx, "rs:n")
	cArr := ap.LRange(ctx, "rs:list", 0, -1)
	cMap := ap.HGetAll(ctx, "rs:hash")
	cNil := ap.Get(ctx, "rs:missing")
	cBig := ap.Get(ctx, "rs:big")
	cBin := ap.Get(ctx, "rs:bin")
	cFloat := ap.IncrByFloat(ctx, "rs:f", 1.5)

	assertReplyShapes(t, cSet, cGet, cIncr, cArr, cMap, cNil, cBig, cBin, cFloat, big, binary)
}
