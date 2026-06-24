package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Autopipeline + zero-copy buffer on CLUSTER (cross-slot, concurrent).
func TestAPZeroCopyBufferCluster(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"},
		AutoPipelineConfig: &redis.AutoPipelineConfig{MaxBatchSize: 100, MaxConcurrentBatches: 30, Unordered: true},
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("cluster not reachable: %v", err)
	}
	_ = c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error { return m.FlushAll(ctx).Err() })
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const g = 80
	const perG = 30
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	var errs []string
	wg.Add(g)
	for x := 0; x < g; x++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("clbuf:%d:%d:%d", id, i, id*7919+i) // spread slots
				val := fmt.Sprintf("p-%d-%d", id, i)
				if err := ap.SetFromBuffer(ctx, key, []byte(val)).Err(); err != nil {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, "set:"+err.Error())
					}
					mu.Unlock()
					continue
				}
				rbuf := make([]byte, len(val))
				cmd := ap.GetToBuffer(ctx, key, rbuf)
				n, err := cmd.Result()
				if err != nil || n != len(val) || string(cmd.Bytes()) != val {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, fmt.Sprintf("key %s n=%d got=%q want=%q err=%v", key, n, string(cmd.Bytes()), val, err))
					}
					mu.Unlock()
				}
			}
		}(x)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("cluster zero-copy buffer via autopipeline failed: %d; sample=%v", bad, errs)
	}
}
