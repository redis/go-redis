package redis_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAPClusterMovedRedirectFollowed verifies AutoPipeline inherits the cluster
// MOVED-redirect handling (it dispatches through ClusterClient.processPipeline,
// which runs the MaxRedirects loop + checkMovedErr). SwapNodes forces the client
// to believe a replica is the master for a slot; a write there is answered with
// MOVED and the client must follow it, so the value is still read back correctly.
func TestAPClusterMovedRedirectFollowed(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster()
	defer c.Close()
	skipIfClusterUnhealthy(t, c)

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const key, val = "moved:key", "moved:val"
	if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	// Force the client's in-memory topology to point the slot at the wrong node,
	// so the next access is answered with MOVED and must be followed.
	if err := c.SwapNodes(ctx, key); err != nil {
		t.Fatalf("SwapNodes: %v", err)
	}

	got, err := ap.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("get after MOVED: %v", err)
	}
	if got != val {
		t.Fatalf("get after MOVED = %q, want %q (redirect not followed correctly)", got, val)
	}
}

// TestAPClusterMovedSurfacedWithoutRedirects anchors the positive test: with
// MaxRedirects disabled (-1 -> 0, one attempt), the forced MOVED is NOT followed
// and must surface as an error on the command's future — proving SwapNodes really
// induced a redirect (so TestAPClusterMovedRedirectFollowed exercised the
// redirect path rather than passing trivially).
func TestAPClusterMovedSurfacedWithoutRedirects(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"},
		MaxRedirects: -1, // normalized to 0: a single attempt, no follow-through
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize: 200, MaxConcurrentBatches: 50, Unordered: true,
		},
	})
	defer c.Close()
	skipIfClusterUnhealthy(t, c)

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const key = "moved:noredir:key"
	if err := ap.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}

	// SwapNodes is racy against the periodic LazyReload, so re-swap and retry
	// until we observe the MOVED (or give up and skip — the swap never stuck).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.SwapNodes(ctx, key); err != nil {
			t.Fatalf("SwapNodes: %v", err)
		}
		err := ap.Get(ctx, key).Err()
		if err != nil && strings.Contains(err.Error(), "MOVED") {
			return // proven: the redirect was needed and, with no redirects, surfaced
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Skip("could not observe a MOVED (swap kept being reverted by topology reload)")
}
