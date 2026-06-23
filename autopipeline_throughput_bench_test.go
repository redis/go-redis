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

// BenchmarkAutoPipelineThroughput compares executed-command throughput three
// ways. In every variant a command is counted ONLY after its result has been
// read (the command actually executed on the server) — there is no counting of
// merely-queued commands, so the reported ops/sec is real throughput.
//
//  1. Normal — a plain client. Each Set is a blocking round-trip; throughput is
//     bounded by the connection pool and Redis's non-pipelined ceiling (~100k
//     SET/sec, matching redis-benchmark without -P).
//  2. AutoPipelineBlocking — ap.Set(...).Result() read immediately, the way the
//     normal client is used (drop-in, one command in flight per caller). Even
//     so, the flusher batches across the many concurrent callers into deep
//     pipelines, so with parallel batches it clears ~1M executed SET/sec.
//  3. AutoPipelineWindowed — submit a window of commands per caller, then read
//     their results. Keeps each pipeline deepest; the high-throughput usage,
//     reaching a few million ops/sec.
//
// The autopipeline variants use a parallel-batch config (MaxConcurrentBatches>1,
// Unordered) — that is what lets blocking usage exceed 1M. The default ordered
// config (MaxConcurrentBatches=1) serializes batch execution and caps blocking
// usage near ~500k; windowed still reaches a few million ordered.
//
// Run: go test -run '^$' -bench BenchmarkAutoPipelineThroughput -benchtime=1x
func BenchmarkAutoPipelineThroughput(b *testing.B) {
	const (
		duration   = 3 * time.Second
		goroutines = 2000
		window     = 200 // commands submitted before reading results (windowed variant)
	)

	// apConfig is a parallel-batch config: many batches execute concurrently so
	// blocking callers don't serialize behind a single flusher. Unordered is
	// required for MaxConcurrentBatches>1.
	apConfig := func() *redis.AutoPipelineConfig {
		return &redis.AutoPipelineConfig{MaxBatchSize: 300, MaxConcurrentBatches: 80, Unordered: true}
	}

	// drive runs `fn` on `goroutines` goroutines until the deadline and reports
	// executed ops/sec. fn returns how many executed commands it performed in one
	// iteration (after reading their results); it must not count un-read commands.
	drive := func(b *testing.B, fn func(id int) int) {
		var count int64
		deadline := time.Now().Add(duration)
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func(id int) {
				defer wg.Done()
				for time.Now().Before(deadline) {
					atomic.AddInt64(&count, int64(fn(id)))
				}
			}(g)
		}
		wg.Wait()
		b.StopTimer()
		b.ReportMetric(float64(count)/duration.Seconds(), "ops/sec")
	}

	ctx := context.Background()

	b.Run("Normal", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 100})
		defer c.Close()
		i := 0
		drive(b, func(id int) int {
			i++
			if err := c.Set(ctx, fmt.Sprintf("n:%d", id), i, 0).Err(); err != nil {
				b.Error(err)
			}
			return 1 // one executed command (result/err read)
		})
	})

	b.Run("AutoPipelineBlocking", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineConfig: apConfig()})
		defer c.Close()
		ap := c.AutoPipeline()
		defer ap.Close()
		i := 0
		drive(b, func(id int) int {
			i++
			// Result() blocks until executed — same call shape as a normal client.
			if _, err := ap.Set(ctx, fmt.Sprintf("b:%d", id), i, 0).Result(); err != nil {
				b.Error(err)
			}
			return 1
		})
	})

	b.Run("AutoPipelineWindowed", func(b *testing.B) {
		c := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineConfig: apConfig()})
		defer c.Close()
		ap := c.AutoPipeline()
		defer ap.Close()
		drive(b, func(id int) int {
			cmds := make([]*redis.StatusCmd, 0, window)
			for j := 0; j < window; j++ {
				cmds = append(cmds, ap.Set(ctx, fmt.Sprintf("w:%d", id), j, 0))
			}
			n := 0
			for _, cmd := range cmds {
				if _, err := cmd.Result(); err != nil { // read result = executed
					b.Error(err)
				}
				n++
			}
			return n // only commands whose result was read
		})
	})
}
