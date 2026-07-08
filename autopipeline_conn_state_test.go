package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineBlockingCommandIsolation verifies a blocking command (detected
// via readTimeout at the submit gate, autopipeline.go:679) runs outside the
// pipeline and does NOT corrupt batched commands interleaved around it: the
// batched SET/GET before and after the BLPOP all return their correct replies.
func TestAutoPipelineBlockingCommandIsolation(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.Del(ctx, "cs:list", "cs:a", "cs:b")
	if err := client.RPush(ctx, "cs:list", "item").Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline(nil) // blocking face
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	if v := ap.Set(ctx, "cs:a", "1", 0).Val(); v != "OK" {
		t.Fatalf("set a before blocking: %q", v)
	}
	// Blocking command interleaved with batched work.
	blp, err := ap.BLPop(ctx, time.Second, "cs:list").Result()
	if err != nil {
		t.Fatalf("blpop: %v", err)
	}
	if len(blp) != 2 || blp[1] != "item" {
		t.Fatalf("blpop = %v, want [cs:list item]", blp)
	}
	// Batched work after a blocking command still works and stays uncorrupted.
	if v := ap.Set(ctx, "cs:b", "2", 0).Val(); v != "OK" {
		t.Fatalf("set b after blocking: %q", v)
	}
	if v := ap.Get(ctx, "cs:a").Val(); v != "1" {
		t.Fatalf("get a after blocking: %q, want 1", v)
	}
	if v := ap.Get(ctx, "cs:b").Val(); v != "2" {
		t.Fatalf("get b after blocking: %q, want 2", v)
	}
	client.Del(ctx, "cs:a", "cs:b")
}

// TestAutoPipelineDoubleClose verifies Close is idempotent — a second Close does
// not panic (it may return an error, but must not crash or hang).
func TestAutoPipelineDoubleClose(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// Second close must not panic; ErrClosed or nil are both acceptable.
	_ = ap.Close()
}
