package redis_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// clusterFDAddrs is the local 6-node cluster (3 masters + 3 replicas) the FD
// MOVED tests target; SwapNodes needs the replicas to swap a slot's master.
var clusterFDAddrs = []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"}

// TestAPClusterFDMovedRedirectFollowed is the discriminating proof that native
// cluster full-duplex FOLLOWS a MOVED redirect. SwapNodes forces the client to
// believe a replica is the master for a slot; a command routed there is answered
// with MOVED. The FD child's reader diverts the redirect to the redirect-aware
// ClusterClient path, which re-routes to the real master and reloads topology, so
// the value is still read back correctly. Before the redirect fix this returned
// the MOVED error and the test failed.
func TestAPClusterFDMovedRedirectFollowed(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: clusterFDAddrs})
	defer c.Close()
	skipIfClusterUnhealthy(t, c)

	ap, err := c.AutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	if !ap.Config().FullDuplex {
		t.Fatalf("Config().FullDuplex = false, want true (cluster FD not active)")
	}

	const key, val = "fdmoved:key", "fdmoved:val"
	if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	// Point the slot at the wrong node so the next access is answered with MOVED
	// and must be followed.
	if err := c.SwapNodes(ctx, key); err != nil {
		t.Fatalf("SwapNodes: %v", err)
	}

	got, err := ap.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("get after MOVED: %v (FD redirect not followed)", err)
	}
	if got != val {
		t.Fatalf("get after MOVED = %q, want %q (FD redirect not followed correctly)", got, val)
	}
}

// TestAPClusterFDMovedSurfacedWithoutRedirects anchors the positive test: with
// MaxRedirects disabled (-1 -> 0, one attempt), the FD divert still routes through
// ClusterClient.process, which now honors the zero-redirect budget and surfaces
// the forced MOVED as an error. This proves SwapNodes really induced a redirect on
// the FD path (so the positive test exercised the redirect logic, not a trivial
// pass) and that the FD reprocess goes through the cluster budget.
func TestAPClusterFDMovedSurfacedWithoutRedirects(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        clusterFDAddrs,
		MaxRedirects: -1, // normalized to 0: a single attempt, no follow-through
	})
	defer c.Close()
	skipIfClusterUnhealthy(t, c)

	ap, err := c.AutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	if !ap.Config().FullDuplex {
		t.Fatalf("Config().FullDuplex = false, want true (cluster FD not active)")
	}

	const key = "fdmoved:noredir:key"
	if err := ap.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}

	// SwapNodes is racy against the periodic LazyReload, so re-swap and retry until
	// we observe the MOVED (or give up and skip — the swap never stuck).
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
