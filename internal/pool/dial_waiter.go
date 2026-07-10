package pool

import (
	"sync"
	"sync/atomic"
)

// dialWaiter states. A waiter moves from pending to exactly one of signaled or
// abandoned; the loser of the race learns about the winner from the CAS result.
const (
	dialWaiterPending   int32 = iota // parked, waiting for a signal
	dialWaiterSignaled               // dequeued by signalNext, wakeup sent on ready
	dialWaiterAbandoned              // caller left (timeout/cancel) before being signaled
)

// dialWaiter represents a Get() caller that has been throttled by the dial rate
// limiter and is parked waiting for an idle connection to be returned to the
// pool. Its ready channel is buffered (size 1) and is signaled at most once
// (guarded by the state CAS), so senders never block.
type dialWaiter struct {
	ready chan struct{}
	state atomic.Int32
}

// dialWaitQueue is a FIFO queue of parked dial waiters. When a connection is
// returned to the idle pool, signalNext wakes the oldest pending waiter so it
// can retry popping an idle connection instead of dialing a new one.
//
// Abandoned waiters are not removed in place (that would be an O(n) scan under
// the same mutex signalNext takes); abandon() just flips the waiter's state and
// the tombstone is discarded when it reaches the front of the queue (in
// signalNext or the enqueue-time trim). pending tracks the number of live
// waiters so len()/empty() are O(1) and lock-free.
type dialWaitQueue struct {
	mu      sync.Mutex
	list    []*dialWaiter
	pending atomic.Int32
}

func newDialWaitQueue() *dialWaitQueue {
	return &dialWaitQueue{}
}

func (q *dialWaitQueue) enqueue(w *dialWaiter) {
	q.pending.Add(1)
	q.mu.Lock()
	// Opportunistically trim abandoned waiters from the front so tombstones
	// can't accumulate while no signals are flowing.
	for len(q.list) > 0 && q.list[0].state.Load() != dialWaiterPending {
		q.list = q.list[1:]
	}
	q.list = append(q.list, w)
	q.mu.Unlock()
}

// abandon marks w as no longer waiting. It returns true if w had not been
// signaled yet (the caller owns its departure), and false if signalNext won the
// race and a wakeup has been (or is about to be) delivered on w.ready — in that
// case the caller must consume the signal and act on it or forward it, so the
// wakeup is not lost.
func (q *dialWaitQueue) abandon(w *dialWaiter) bool {
	if w.state.CompareAndSwap(dialWaiterPending, dialWaiterAbandoned) {
		q.pending.Add(-1)
		return true
	}
	return false
}

// signalNext wakes the oldest pending waiter, if any. The send is non-blocking:
// the state CAS guarantees each waiter is signaled at most once and its ready
// channel is buffered, so there is always room for the single value.
func (q *dialWaitQueue) signalNext() {
	q.mu.Lock()
	var w *dialWaiter
	for len(q.list) > 0 {
		head := q.list[0]
		q.list = q.list[1:]
		if head.state.CompareAndSwap(dialWaiterPending, dialWaiterSignaled) {
			w = head
			break
		}
		// Abandoned tombstone: discard and keep looking.
	}
	q.mu.Unlock()

	if w != nil {
		q.pending.Add(-1)
		w.ready <- struct{}{}
	}
}

// signalAll wakes every pending waiter. Used on pool Close() so parked Get()
// callers fail fast with ErrClosed instead of sleeping out their park timer.
func (q *dialWaitQueue) signalAll() {
	q.mu.Lock()
	list := q.list
	q.list = nil
	q.mu.Unlock()

	for _, w := range list {
		if w.state.CompareAndSwap(dialWaiterPending, dialWaiterSignaled) {
			q.pending.Add(-1)
			w.ready <- struct{}{}
		}
	}
}

// len returns the number of pending (live) waiters.
func (q *dialWaitQueue) len() int {
	return int(q.pending.Load())
}

// empty reports whether there are no pending waiters. Cheap check for the Get()
// slow path: fresh callers defer dial tokens to already-parked waiters, and the
// common case (no waiters) must cost a single atomic load.
func (q *dialWaitQueue) empty() bool {
	return q.pending.Load() == 0
}
