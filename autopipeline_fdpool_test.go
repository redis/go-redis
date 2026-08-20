package redis

import (
	"sync"
	"testing"
)

// TestFDBlockingBatchPoolReuse exercises the pooled-batch lifecycle directly:
// get → signal (as the reader would, via close) → wait → recycle, thousands of
// times with concurrent get/put churn. Run under -race: a mis-delivered wakeup
// from a recycled channel, a missed reset, or a double-signal surfaces as a race
// or a hang here without needing a live server.
func TestFDBlockingBatchPoolReuse(t *testing.T) {
	const workers = 32
	const iters = 5000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				b := getFDBlockingBatch()
				if !b.pooled {
					t.Errorf("pooled batch lost its pooled flag")
					return
				}
				if b.closed.Load() {
					t.Errorf("recycled batch came back already closed")
					return
				}
				// A completer (the reader) signals on its own goroutine while the
				// "caller" waits — the real submit/complete/Wait shape.
				done := make(chan struct{})
				go func() {
					b.close() // buffered send for pooled batches
					close(done)
				}()
				<-b.done // the caller's Wait drains the signal
				<-done
				// close is idempotent: a second completer (e.g. a racing failReqs)
				// must not send a second value or panic.
				b.close()
				select {
				case <-b.done:
					t.Errorf("second close signalled a recycled channel")
					return
				default:
				}
				putFDBlockingBatch(b)
			}
		}()
	}
	wg.Wait()
}

// TestFDBlockingBatchPoolResets guards the individual-field reset: a batch put
// back dirty must come out clean, or the next caller's completion is a no-op.
func TestFDBlockingBatchPoolResets(t *testing.T) {
	b := getFDBlockingBatch()
	// Dirty every field the reset must clear.
	b.closed.Store(true)
	b.dispGid.Store(999)
	b.nodeCount.Store(3)
	b.nodeGids = []int64{1, 2, 3}
	// Leave a stray signal in the buffered channel.
	select {
	case b.done <- struct{}{}:
	default:
	}
	putFDBlockingBatch(b)

	// The pool may hand back a different object, so loop until we observe ours
	// (or give up after a bounded number of gets) — either way every returned
	// batch must be clean.
	for i := 0; i < 64; i++ {
		g := getFDBlockingBatch()
		if g.closed.Load() || g.dispGid.Load() != 0 || g.nodeCount.Load() != 0 || g.nodeGids != nil {
			t.Fatalf("getFDBlockingBatch returned a dirty batch")
		}
		select {
		case <-g.done:
			t.Fatalf("getFDBlockingBatch returned a batch with a stray signal")
		default:
		}
		putFDBlockingBatch(g)
	}
}
