package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestUniversalClientAutoPipeline verifies AutoPipeline is reachable through the
// UniversalClient interface: a standalone/cluster-backed UniversalClient returns
// a working AutoPipeliner, while Ring (also a UniversalClient) returns an error
// rather than being silently absent.
func TestUniversalClientAutoPipeline(t *testing.T) {
	ctx := context.Background()

	var uc redis.UniversalClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	defer uc.Close()
	if err := uc.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	ap, err := uc.AutoPipeline(nil)
	if err != nil {
		t.Fatalf("UniversalClient.AutoPipeline: %v", err)
	}
	defer ap.Close()
	if v := ap.Set(ctx, "uc:k", "v", 0).Val(); v != "OK" {
		t.Fatalf("set via UniversalClient autopipeline = %q, want OK", v)
	}
	if v := ap.Get(ctx, "uc:k").Val(); v != "v" {
		t.Fatalf("get via UniversalClient autopipeline = %q, want v", v)
	}
	uc.Del(ctx, "uc:k")

	// Ring: AutoPipeline is unsupported and must return an error (directly and
	// through the UniversalClient interface).
	ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"s1": ":6379"}})
	defer ring.Close()
	if _, err := ring.AutoPipeline(nil); err == nil {
		t.Fatal("Ring.AutoPipeline should return an error (unsupported)")
	}
	if _, err := ring.AsyncAutoPipeline(nil); err == nil {
		t.Fatal("Ring.AsyncAutoPipeline should return an error (unsupported)")
	}
	var ru redis.UniversalClient = ring
	if _, err := ru.AutoPipeline(nil); err == nil {
		t.Fatal("Ring via UniversalClient.AutoPipeline should return an error")
	}
}
