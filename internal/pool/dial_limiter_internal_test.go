package pool

import (
	"testing"
	"time"
)

// newTestLimiter builds a dial limiter driven by a controllable clock so the
// token-bucket math can be asserted deterministically without real sleeps.
func newTestLimiter(rate, burst int) (*dialLimiter, *time.Time) {
	l := newDialLimiter(rate, burst)
	now := time.Unix(1700000000, 0)
	cur := now
	l.now = func() time.Time { return cur }
	l.last = cur
	l.tokens = l.burst
	// Return a pointer the caller can advance to move the clock.
	return l, &cur
}

func TestNewDialLimiterDisabled(t *testing.T) {
	if l := newDialLimiter(0, 0); l != nil {
		t.Fatalf("rate 0 should disable the limiter, got %#v", l)
	}
	if l := newDialLimiter(-5, 10); l != nil {
		t.Fatalf("negative rate should disable the limiter, got %#v", l)
	}
}

func TestNewDialLimiterBurstDefault(t *testing.T) {
	l := newDialLimiter(7, 0)
	if l == nil {
		t.Fatal("limiter should be enabled")
	}
	if l.burst != 7 {
		t.Fatalf("burst should default to rate (7), got %v", l.burst)
	}

	l = newDialLimiter(7, 3)
	if l.burst != 3 {
		t.Fatalf("burst should be 3, got %v", l.burst)
	}
}

func TestDialLimiterAllowConsumesBurst(t *testing.T) {
	l, clock := newTestLimiter(10, 3)

	// Bucket starts full: exactly burst tokens available.
	for i := 0; i < 3; i++ {
		if !l.Allow() {
			t.Fatalf("Allow() #%d should succeed within burst", i+1)
		}
	}
	if l.Allow() {
		t.Fatal("Allow() should fail once burst is exhausted")
	}

	// After 100ms at 10 tokens/sec, exactly one token accrues.
	*clock = clock.Add(100 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("Allow() should succeed after one token has refilled")
	}
	if l.Allow() {
		t.Fatal("Allow() should fail again immediately after consuming the refill")
	}
}

func TestDialLimiterRefillCap(t *testing.T) {
	l, clock := newTestLimiter(10, 3)

	// Drain the bucket.
	for i := 0; i < 3; i++ {
		l.Allow()
	}
	// Wait far longer than needed to refill; tokens must cap at burst.
	*clock = clock.Add(10 * time.Second)
	for i := 0; i < 3; i++ {
		if !l.Allow() {
			t.Fatalf("Allow() #%d should succeed after full refill", i+1)
		}
	}
	if l.Allow() {
		t.Fatal("Allow() should fail: refill must be capped at burst (3)")
	}
}

func TestDialLimiterDelayUntilNext(t *testing.T) {
	l, clock := newTestLimiter(10, 1) // 1 token / 100ms

	if d := l.delayUntilNext(); d != 0 {
		t.Fatalf("delay should be 0 while a token is available, got %v", d)
	}

	l.Allow() // drain the single token
	d := l.delayUntilNext()
	// Need one full token at 10/sec => 100ms.
	if d < 90*time.Millisecond || d > 110*time.Millisecond {
		t.Fatalf("delay should be ~100ms, got %v", d)
	}

	// After 40ms, ~0.4 tokens accrued; remaining wait ~60ms.
	*clock = clock.Add(40 * time.Millisecond)
	d = l.delayUntilNext()
	if d < 50*time.Millisecond || d > 70*time.Millisecond {
		t.Fatalf("delay should be ~60ms after 40ms elapsed, got %v", d)
	}
}

func TestDialWaitQueueSignalAndRemove(t *testing.T) {
	q := newDialWaitQueue()
	w1 := &dialWaiter{ready: make(chan struct{}, 1)}
	w2 := &dialWaiter{ready: make(chan struct{}, 1)}
	q.enqueue(w1)
	q.enqueue(w2)

	if q.len() != 2 {
		t.Fatalf("queue len should be 2, got %d", q.len())
	}

	// signalNext wakes the oldest waiter (FIFO) and dequeues it.
	q.signalNext()
	select {
	case <-w1.ready:
	default:
		t.Fatal("w1 should have been signaled")
	}
	if q.len() != 1 {
		t.Fatalf("queue len should be 1 after signalNext, got %d", q.len())
	}

	// w1 was already dequeued -> remove reports false; w2 still present -> true.
	if q.remove(w1) {
		t.Fatal("remove(w1) should report false: already dequeued by signalNext")
	}
	if !q.remove(w2) {
		t.Fatal("remove(w2) should report true: still queued")
	}
	if q.len() != 0 {
		t.Fatalf("queue should be empty, got %d", q.len())
	}

	// signalNext on an empty queue is a no-op and must not panic.
	q.signalNext()
}
