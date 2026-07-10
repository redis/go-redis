package redis_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineCloseRace stresses concurrent command submission racing with
// Close(). The hazard: a submit that reads closed==false, then stalls, then
// enqueues onto a shard whose flusher has already drained and exited during
// shutdown — leaving the command's completion signal never fired, so the
// caller's blocking accessor hangs forever. Every submitted command must
// resolve (success or ErrClosed), never block past the deadline.
//
// Best-effort: this is a stress test, not a deterministic reproducer. The exact
// interleaving (a submit preempted between the closed-check and the enqueue for
// the whole duration of Close) is narrow and may not trigger every run; the fix
// (re-checking closed under the shard lock) is justified by code analysis. The
// test guards against regressions of the obvious form and runs many iterations
// to widen the window.
func TestAutoPipelineCloseRace(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		ctx := context.Background()
		c := redis.NewClient(&redis.Options{Addr: ":6379"})
		ap, err := c.AutoPipeline(nil)
		if err != nil {
			t.Fatal(err)
		}

		const G = 64
		var wg sync.WaitGroup
		wg.Add(G)
		start := make(chan struct{})
		for g := 0; g < G; g++ {
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 20; i++ {
					// .Err() blocks until the command resolves. If the command
					// is lost (never signalled), this never returns and the
					// watchdog below fires.
					_ = ap.Set(ctx, "closerace", i, 0).Err()
				}
			}()
		}

		close(start)
		// Close concurrently with in-flight submits — the race window.
		time.Sleep(time.Millisecond)
		_ = ap.Close()

		// Watchdog: all submitters must finish promptly. A hung await() means a
		// lost command (the bug this guards).
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: submitters hung after Close — a command was lost", iter)
		}
		c.Close()
	}
}
