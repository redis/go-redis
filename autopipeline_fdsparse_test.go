package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestFullDuplexSparseTrafficPooledBatch drives the blocking full-duplex face
// with intentionally sparse traffic: each caller pauses longer than the idle
// timeout between commands, so the engine returns its connection to the pool and
// re-leases on the next command — every command runs in its own single-entry
// batch, repeatedly. This is the path where the pooled completion batch (buffered
// done, recycled after Wait) and the idle-return/re-lease machinery meet, so it
// guards against a mis-delivered wakeup from a recycled batch under intermittent
// execution. Correctness checks: ECHO round-trips its own token (no
// misattribution) and per-caller INCR is strictly sequential (no lost or
// duplicated completion); nothing hangs.
func TestFullDuplexSparseTrafficPooledBatch(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		Unordered:             false,
		MaxConcurrentBatches:  1,
		FullDuplexIdleTimeout: 40 * time.Millisecond,
		FullDuplexMaxHold:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const workers = 4
	const opsPerWorker = 12 // ~12 * 70ms ~= 0.85s per worker, run in parallel
	for i := 0; i < workers; i++ {
		if err := ap.Del(ctx, fmt.Sprintf("fd:sparse:%d", i)).Err(); err != nil {
			t.Fatalf("del: %v", err)
		}
	}

	done := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("fd:sparse:%d", id)
			for op := 0; op < opsPerWorker; op++ {
				// Pause past the 40ms idle timeout so the conn is returned and
				// re-leased for this command (the sparse / intermittent case).
				time.Sleep(70 * time.Millisecond)

				token := fmt.Sprintf("w%d-op%d", id, op)
				if got, err := ap.Echo(ctx, token).Result(); err != nil {
					done <- fmt.Errorf("worker %d op %d: echo: %w", id, op, err)
					return
				} else if got != token {
					done <- fmt.Errorf("worker %d op %d: ECHO misattribution: sent %q got %q", id, op, token, got)
					return
				}

				want := int64(op + 1)
				if got, err := ap.Incr(ctx, key).Result(); err != nil {
					done <- fmt.Errorf("worker %d op %d: incr: %w", id, op, err)
					return
				} else if got != want {
					done <- fmt.Errorf("worker %d op %d: INCR out of order: got %d want %d", id, op, got, want)
					return
				}
			}
			done <- nil
		}(i)
	}

	// Watchdog: sparse traffic must still complete promptly (each op is ~1 RTT +
	// the 70ms pause). A recycled-batch mis-wakeup that dropped a completion would
	// hang a caller here.
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("sparse-traffic workers did not finish (possible dropped completion)")
	}

	for i := 0; i < workers; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
