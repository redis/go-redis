package redis_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/hashtag"
)

func newTestCluster() *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"},
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize: 200, MaxConcurrentBatches: 50, Unordered: true,
		},
	})
}

// skipIfClusterUnhealthy skips the test unless a real, formed cluster answers.
// Ping alone is insufficient: in CI's standalone test job a Redis may answer on
// the cluster ports while the cluster is not formed (CLUSTER INFO reports
// cluster_state:fail / CLUSTERDOWN, and commands return MOVED), which would turn
// "no cluster available" into spurious failures. Gate on cluster_state:ok.
func skipIfClusterUnhealthy(t *testing.T, c *redis.ClusterClient) {
	t.Helper()
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("cluster not reachable: %v", err)
	}
	info, err := c.ClusterInfo(ctx).Result()
	if err != nil {
		t.Skipf("cluster not reachable (CLUSTER INFO): %v", err)
	}
	if !strings.Contains(info, "cluster_state:ok") {
		t.Skipf("cluster not healthy (no cluster_state:ok)")
	}
}

// Validates cluster routing: a single autopipeline batch contains keys that
// hash to MANY different slots/shards. Each command must route to the correct
// node and return its own correct value. If routing were wrong, GETs would
// miss or return another key's value.
func TestAPClusterCrossSlotRouting(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster()
	defer c.Close()
	skipIfClusterUnhealthy(t, c)
	_ = c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		return m.FlushAll(ctx).Err()
	})

	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 100
	const perG = 50
	var wg sync.WaitGroup
	var mismatch, slotsSeen sync.Map
	var bad int64
	var mu sync.Mutex

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// Keys with no hashtag => spread across all 16384 slots.
				key := fmt.Sprintf("cl:%d:%d:%d", id, i, id*7919+i)
				want := fmt.Sprintf("V-%d-%d", id, i)
				slotsSeen.Store(hashtag.Slot(key), true)
				if err := ap.Set(ctx, key, want, 0).Err(); err != nil {
					mu.Lock()
					bad++
					mismatch.Store(key, "set:"+err.Error())
					mu.Unlock()
					continue
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil || got != want {
					mu.Lock()
					bad++
					mismatch.Store(key, fmt.Sprintf("got=%q want=%q err=%v", got, want, err))
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if bad > 0 {
		shown := 0
		mismatch.Range(func(k, v any) bool {
			t.Logf("MISMATCH %v: %v", k, v)
			shown++
			return shown < 15
		})
		t.Fatalf("cluster cross-slot routing failures: %d", bad)
	}

	// Sanity: we actually exercised many distinct slots (not all one shard).
	n := 0
	slotsSeen.Range(func(_, _ any) bool { n++; return true })
	if n < 100 {
		t.Fatalf("test too weak: only %d distinct slots exercised", n)
	}
	t.Logf("validated cross-slot routing across %d distinct slots", n)
}

// Validates per-key correctness on cluster with INCR ordering per goroutine,
// each goroutine on its own (randomly-slotted) key.
func TestAPClusterPerGoroutineOrder(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster()
	defer c.Close()
	skipIfClusterUnhealthy(t, c)
	// Start from a clean slate so INCR counts are deterministic across runs.
	_ = c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		return m.FlushAll(ctx).Err()
	})

	ap, err := c.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 80
	const incrs = 80
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("clord:%d:%d", id, id*104729)
			for i := 1; i <= incrs; i++ {
				v, err := ap.Incr(ctx, key).Result()
				if err != nil || v != int64(i) {
					mu.Lock()
					bad++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("cluster ordering/correctness failures: %d", bad)
	}
}
