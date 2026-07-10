package pool

import "sync"

// dialWaiter represents a Get() caller that has been throttled by the dial rate
// limiter and is parked waiting for an idle connection to be returned to the
// pool. Its ready channel is buffered (size 1) and is signaled at most once, so
// signalNext never blocks.
type dialWaiter struct {
	ready chan struct{}
}

// dialWaitQueue is a FIFO queue of parked dial waiters. When a connection is
// returned to the idle pool, signalNext wakes the oldest waiter so it can retry
// popping an idle connection instead of dialing a new one.
type dialWaitQueue struct {
	mu   sync.Mutex
	list []*dialWaiter
}

func newDialWaitQueue() *dialWaitQueue {
	return &dialWaitQueue{}
}

func (q *dialWaitQueue) enqueue(w *dialWaiter) {
	q.mu.Lock()
	q.list = append(q.list, w)
	q.mu.Unlock()
}

// remove takes w out of the queue. It returns true if w was still queued, and
// false if it had already been dequeued (and signaled) by signalNext.
func (q *dialWaitQueue) remove(w *dialWaiter) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, x := range q.list {
		if x == w {
			q.list = append(q.list[:i], q.list[i+1:]...)
			return true
		}
	}
	return false
}

// signalNext wakes the oldest parked waiter, if any. The send is non-blocking:
// each waiter is dequeued exactly once and its ready channel is buffered, so
// there is always room for the single value.
func (q *dialWaitQueue) signalNext() {
	q.mu.Lock()
	if len(q.list) == 0 {
		q.mu.Unlock()
		return
	}
	w := q.list[0]
	q.list = q.list[1:]
	q.mu.Unlock()

	w.ready <- struct{}{}
}

func (q *dialWaitQueue) len() int {
	q.mu.Lock()
	n := len(q.list)
	q.mu.Unlock()
	return n
}
