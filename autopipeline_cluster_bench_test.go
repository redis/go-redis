package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// clusterBenchAddrs is the local osscluster (docker-compose `cluster` profile,
// formed with `redis-cli --cluster create ... --cluster-replicas 1`).
var clusterBenchAddrs = []string{"127.0.0.1:16600", "127.0.0.1:16601", "127.0.0.1:16602"}

func newClusterBenchClient(b *testing.B) *redis.ClusterClient {
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: clusterBenchAddrs, PoolSize: 500})
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		b.Skipf("cluster not reachable on %v: %v", clusterBenchAddrs, err)
	}
	return c
}

// BenchmarkClusterAutoPipelineThroughput measures executed throughput against a
// 3-master cluster. ClusterClient.AutoPipeline routes commands to shards by slot
// so each shard's batch stays on one node (keeping per-node pipelines deep), which
// is what lets a cluster scale past a single instance. As with the standalone
// benchmark, a command is counted only after its result is read.
func BenchmarkClusterAutoPipelineThroughput(b *testing.B) {
	const duration = 3 * time.Second

	b.Run("Blocking", func(b *testing.B) {
		c := newClusterBenchClient(b)
		defer c.Close()
		ap, err := c.AutoPipeline(&redis.AutoPipelineConfig{MaxBatchSize: 512, MaxConcurrentBatches: 200, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		const G = 16000
		var count int64
		dl := time.Now().Add(duration)
		ctx := context.Background()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func(id int) {
				defer wg.Done()
				i, key := 0, fmt.Sprintf("b:%d", id)
				for time.Now().Before(dl) {
					const run = 50
					for r := 0; r < run; r++ {
						i++
						ap.Set(ctx, key, i, 0).Err()
					}
					atomic.AddInt64(&count, run)
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	})

	b.Run("Windowed", func(b *testing.B) {
		c := newClusterBenchClient(b)
		defer c.Close()
		ap, err := c.AsyncAutoPipeline(&redis.AutoPipelineConfig{MaxBatchSize: 300, MaxConcurrentBatches: 96, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		const G, W = 1000, 300
		var count int64
		dl := time.Now().Add(duration)
		ctx := context.Background()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func(id int) {
				defer wg.Done()
				cmds := make([]*redis.StatusCmd, 0, W)
				for time.Now().Before(dl) {
					cmds = cmds[:0]
					for j := 0; j < W; j++ {
						cmds = append(cmds, ap.Set(ctx, fmt.Sprintf("w:%d:%d", id, j), j, 0))
					}
					for _, cm := range cmds {
						cm.Err()
					}
					atomic.AddInt64(&count, int64(W))
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	})
}
