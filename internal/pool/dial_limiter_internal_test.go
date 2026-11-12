package pool

import (
	"net"
	"testing"
	"time"
)

// len returns the number of pending (live) waiters. Test-only helper: kept out
// of dial_waiter.go because production code only needs empty().
func (q *dialWaitQueue) len() int {
	return int(q.pending.Load())
}

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

func TestDialLimiterRefund(t *testing.T) {
	l, _ := newTestLimiter(10, 2)

	// Drain the bucket, then refund one token: Allow must succeed again.
	if !l.Allow() || !l.Allow() {
		t.Fatal("burst tokens should be available")
	}
	if l.Allow() {
		t.Fatal("bucket should be empty")
	}
	l.refund()
	if !l.Allow() {
		t.Fatal("Allow should succeed after refund")
	}

	// Refund never exceeds burst: refund 5x on a full bucket, still only
	// burst tokens available.
	l.refund()
	for i := 0; i < 5; i++ {
		l.refund()
	}
	if !l.Allow() || !l.Allow() {
		t.Fatal("burst tokens should be available after refunds")
	}
	if l.Allow() {
		t.Fatal("refund must be capped at burst")
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

func TestDialWaitQueueSignalAndAbandon(t *testing.T) {
	q := newDialWaitQueue()
	w1 := &dialWaiter{ready: make(chan struct{}, 1)}
	w2 := &dialWaiter{ready: make(chan struct{}, 1)}
	q.enqueue(w1)
	q.enqueue(w2)

	if q.len() != 2 || q.empty() {
		t.Fatalf("queue should have 2 pending waiters, got %d", q.len())
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

	// w1 was already signaled -> abandon reports false; w2 still pending -> true.
	if q.abandon(w1) {
		t.Fatal("abandon(w1) should report false: already signaled by signalNext")
	}
	if !q.abandon(w2) {
		t.Fatal("abandon(w2) should report true: still pending")
	}
	if q.len() != 0 || !q.empty() {
		t.Fatalf("queue should have no pending waiters, got %d", q.len())
	}

	// signalNext must skip w2's abandoned tombstone and not panic on empty.
	q.signalNext()
	select {
	case <-w2.ready:
		t.Fatal("abandoned waiter must not be signaled")
	default:
	}
}

// TestHasAcquirableIdleConn verifies the throttled-waiter double-check only
// treats IDLE/CREATED connections as reusable: UNUSABLE conns (handoff/re-auth)
// sitting in idleConns must not count, otherwise waitForDialSlot would return
// without parking and getConn would busy-spin.
func TestHasAcquirableIdleConn(t *testing.T) {
	p := &ConnPool{}

	if p.hasAcquirableIdleConn() {
		t.Fatal("empty idle list must not report an acquirable conn")
	}

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	unusable := NewConn(c1)
	unusable.stateMachine.Transition(StateUnusable)
	p.idleConns = []*Conn{unusable}
	if p.hasAcquirableIdleConn() {
		t.Fatal("UNUSABLE idle conn must not report as acquirable (busy-spin regression)")
	}

	created := NewConn(c2) // fresh conns start in StateCreated, which popIdle can acquire
	p.idleConns = append(p.idleConns, created)
	if !p.hasAcquirableIdleConn() {
		t.Fatal("CREATED idle conn should report as acquirable")
	}

	created.stateMachine.Transition(StateIdle)
	if !p.hasAcquirableIdleConn() {
		t.Fatal("IDLE conn should report as acquirable")
	}
}

func TestDialWaitQueueSignalAll(t *testing.T) {
	q := newDialWaitQueue()
	ws := make([]*dialWaiter, 3)
	for i := range ws {
		ws[i] = &dialWaiter{ready: make(chan struct{}, 1)}
		q.enqueue(ws[i])
	}
	// One waiter departs before the drain.
	q.abandon(ws[1])

	q.signalAll()

	for i, w := range ws {
		select {
		case <-w.ready:
			if i == 1 {
				t.Fatal("abandoned waiter must not be signaled by signalAll")
			}
		default:
			if i != 1 {
				t.Fatalf("waiter %d should have been signaled by signalAll", i)
			}
		}
	}
	if q.len() != 0 || !q.empty() {
		t.Fatalf("queue should be empty after signalAll, got %d", q.len())
	}
}
