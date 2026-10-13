package redis

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end cluster benchmarks for PR #4093 perf work. Run with
// PERF_CLUSTER_ADDRS=127.0.0.1:45100,... and -benchtime=1x. Each sub-benchmark
// runs for a fixed duration and reports ops/s plus process-wide B/cmd and
// allocs/cmd (runtime.MemStats deltas over the timed region).

const perfE2EDuration = 3 * time.Second

func perfClusterAddrs(b *testing.B) []string {
	v := os.Getenv("PERF_CLUSTER_ADDRS")
	if v == "" {
		b.Skip("PERF_CLUSTER_ADDRS not set")
	}
	return strings.Split(v, ",")
}

func newPerfE2EClient(b *testing.B) *ClusterClient {
	c := NewClusterClient(&ClusterOptions{Addrs: perfClusterAddrs(b), PoolSize: 500})
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		b.Skipf("cluster not reachable: %v", err)
	}
	return c
}

type perfMeter struct {
	start time.Time
	ms0   runtime.MemStats
}

func perfStart(b *testing.B) *perfMeter {
	runtime.GC()
	m := &perfMeter{}
	runtime.ReadMemStats(&m.ms0)
	b.ResetTimer()
	m.start = time.Now()
	return m
}

func (m *perfMeter) stop(b *testing.B, count int64) {
	el := time.Since(m.start)
	b.StopTimer()
	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)
	b.ReportMetric(float64(count)/el.Seconds(), "ops/s")
	b.ReportMetric(float64(ms1.TotalAlloc-m.ms0.TotalAlloc)/float64(count), "B/cmd")
	b.ReportMetric(float64(ms1.Mallocs-m.ms0.Mallocs)/float64(count), "allocs/cmd")
}

func perfWindowKeys(g, w int) [][]string {
	keys := make([][]string, g)
	for i := range keys {
		keys[i] = make([]string, w)
		for j := range keys[i] {
			keys[i][j] = "w:" + strconv.Itoa(i) + ":" + strconv.Itoa(j)
		}
	}
	return keys
}

// perfWindowed drives an async AP: G goroutines submit W SETs, then read all W.
func perfWindowed(b *testing.B, ap *AutoPipeliner, G, W int) {
	keys := perfWindowKeys(G, W)
	ctx := context.Background()
	var count int64
	m := perfStart(b)
	dl := m.start.Add(perfE2EDuration)
	var wg sync.WaitGroup
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			cmds := make([]*StatusCmd, 0, W)
			for time.Now().Before(dl) {
				cmds = cmds[:0]
				for j := 0; j < W; j++ {
					cmds = append(cmds, ap.Set(ctx, keys[id][j], j, 0))
				}
				for _, cm := range cmds {
					if err := cm.Err(); err != nil {
						b.Error(err)
						return
					}
				}
				atomic.AddInt64(&count, int64(W))
			}
		}(g)
	}
	wg.Wait()
	m.stop(b, count)
}

func BenchmarkPerfE2E(b *testing.B) {
	b.Run("APBlocking", func(b *testing.B) {
		c := newPerfE2EClient(b)
		defer c.Close()
		ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{MaxBatchSize: 512, MaxConcurrentBatches: 200, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		const G = 2000
		ctx := context.Background()
		var count int64
		m := perfStart(b)
		dl := m.start.Add(perfE2EDuration)
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func(id int) {
				defer wg.Done()
				i, key := 0, "b:"+strconv.Itoa(id)
				for time.Now().Before(dl) {
					const run = 50
					for r := 0; r < run; r++ {
						i++
						if err := ap.Set(ctx, key, i, 0).Err(); err != nil {
							b.Error(err)
							return
						}
					}
					atomic.AddInt64(&count, run)
				}
			}(g)
		}
		wg.Wait()
		m.stop(b, count)
	})

	b.Run("APWindowed", func(b *testing.B) {
		c := newPerfE2EClient(b)
		defer c.Close()
		ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{MaxBatchSize: 300, MaxConcurrentBatches: 96, Unordered: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		perfWindowed(b, ap, 1000, 300)
	})

	b.Run("APOrderedHD", func(b *testing.B) {
		c := newPerfE2EClient(b)
		defer c.Close()
		ap, err := c.AsyncAutoPipelineWithOptions(nil)
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		if ap.clusterFD != nil {
			b.Fatal("FD engaged on HD bench")
		}
		perfWindowed(b, ap, 256, 100)
	})

	b.Run("APFD", func(b *testing.B) {
		c := newPerfE2EClient(b)
		defer c.Close()
		ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
		if err != nil {
			b.Fatal(err)
		}
		defer ap.Close()
		if ap.clusterFD == nil {
			b.Fatal("FD not engaged")
		}
		perfWindowed(b, ap, 256, 100)
	})

	b.Run("Pipeline100", func(b *testing.B) {
		c := newPerfE2EClient(b)
		defer c.Close()
		const G, N = 64, 100
		keys := perfWindowKeys(G, N)
		ctx := context.Background()
		var count int64
		m := perfStart(b)
		dl := m.start.Add(perfE2EDuration)
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func(id int) {
				defer wg.Done()
				for time.Now().Before(dl) {
					_, err := c.Pipelined(ctx, func(p Pipeliner) error {
						for j := 0; j < N; j++ {
							p.Get(ctx, keys[id][j])
						}
						return nil
					})
					if err != nil && err != Nil {
						b.Error(err)
						return
					}
					atomic.AddInt64(&count, N)
				}
			}(g)
		}
		wg.Wait()
		m.stop(b, count)
	})
}
