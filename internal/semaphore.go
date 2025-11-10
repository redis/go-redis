package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var semTimers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// waiter represents a goroutine waiting for a token.
type waiter struct {
	ready     chan struct{}
	next      *waiter
	cancelled atomic.Bool // Set to true if this waiter was cancelled/timed out
}

// FastSemaphore is a counting semaphore implementation using atomic operations.
// It's optimized for the fast path (no blocking) while still supporting timeouts and context cancellation.
//
// This implementation maintains FIFO ordering of waiters using a linked list queue.
// When a token is released, the first waiter in the queue is notified.
//
// Performance characteristics:
// - Fast path (no blocking): Single atomic CAS operation
// - Slow path (blocking): FIFO queue-based waiting
// - Release: Single atomic decrement + wake up first waiter in queue
//
// This is significantly faster than a pure channel-based semaphore because:
// 1. The fast path avoids channel operations entirely (no scheduler involvement)
// 2. Atomic operations are much cheaper than channel send/receive
// 3. FIFO ordering prevents starvation
type FastSemaphore struct {
	// Current number of acquired tokens (atomic)
	count atomic.Int32

	// Maximum number of tokens (capacity)
	max int32

	// Mutex to protect the waiter queue
	lock sync.Mutex

	// Head and tail of the waiter queue (FIFO)
	head *waiter
	tail *waiter
}

// NewFastSemaphore creates a new fast semaphore with the given capacity.
func NewFastSemaphore(capacity int32) *FastSemaphore {
	return &FastSemaphore{
		max: capacity,
	}
}

// TryAcquire attempts to acquire a token without blocking.
// Returns true if successful, false if the semaphore is full.
//
// This is the fast path - just a single CAS operation.
func (s *FastSemaphore) TryAcquire() bool {
	for {
		current := s.count.Load()
		if current >= s.max {
			return false // Semaphore is full
		}
		if s.count.CompareAndSwap(current, current+1) {
			return true // Successfully acquired
		}
		// CAS failed due to concurrent modification, retry
	}
}

// enqueue adds a waiter to the end of the queue.
// Must be called with lock held.
func (s *FastSemaphore) enqueue(w *waiter) {
	if s.tail == nil {
		s.head = w
		s.tail = w
	} else {
		s.tail.next = w
		s.tail = w
	}
}

// dequeue removes and returns the first waiter from the queue.
// Must be called with lock held.
// Returns nil if the queue is empty.
func (s *FastSemaphore) dequeue() *waiter {
	if s.head == nil {
		return nil
	}
	w := s.head
	s.head = w.next
	if s.head == nil {
		s.tail = nil
	}
	w.next = nil
	return w
}

// notifyOne wakes up the first waiter in the queue if any.
func (s *FastSemaphore) notifyOne() {
	s.lock.Lock()
	w := s.dequeue()
	s.lock.Unlock()

	if w != nil {
		close(w.ready)
	}
}

// Acquire acquires a token, blocking if necessary until one is available or the context is cancelled.
// Returns an error if the context is cancelled or the timeout expires.
// Returns timeoutErr when the timeout expires.
func (s *FastSemaphore) Acquire(ctx context.Context, timeout time.Duration, timeoutErr error) error {
	// Check context first
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Try fast path first
	if s.TryAcquire() {
		return nil
	}

	// Need to wait - create a waiter and add to queue
	w := &waiter{
		ready: make(chan struct{}),
	}

	s.lock.Lock()
	s.enqueue(w)
	s.lock.Unlock()

	// Use timer pool to avoid allocation
	timer := semTimers.Get().(*time.Timer)
	defer semTimers.Put(timer)
	timer.Reset(timeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		// Mark ourselves as cancelled
		w.cancelled.Store(true)
		// Try to remove ourselves from the queue
		s.lock.Lock()
		removed := s.removeWaiter(w)
		s.lock.Unlock()

		if !removed {
			// We were already dequeued and notified
			// Wait for the notification and then release the token
			<-w.ready
			s.releaseToPool()
		}
		return ctx.Err()
	case <-w.ready:
		// We were notified, check if we were cancelled
		if !timer.Stop() {
			<-timer.C
		}
		if w.cancelled.Load() {
			// We were cancelled while being notified, release the token
			s.releaseToPool()
			return ctx.Err()
		}
		return nil
	case <-timer.C:
		// Mark ourselves as cancelled
		w.cancelled.Store(true)
		// Try to remove ourselves from the queue
		s.lock.Lock()
		removed := s.removeWaiter(w)
		s.lock.Unlock()

		if !removed {
			// We were already dequeued and notified
			// Wait for the notification and then release the token
			<-w.ready
			s.releaseToPool()
		}
		return timeoutErr
	}
}

// removeWaiter removes a waiter from the queue.
// Must be called with lock held.
// Returns true if the waiter was found and removed, false otherwise.
func (s *FastSemaphore) removeWaiter(target *waiter) bool {
	if s.head == nil {
		return false
	}

	// Special case: removing head
	if s.head == target {
		s.head = target.next
		if s.head == nil {
			s.tail = nil
		}
		return true
	}

	// Find and remove from middle or tail
	prev := s.head
	for prev.next != nil {
		if prev.next == target {
			prev.next = target.next
			if prev.next == nil {
				s.tail = prev
			}
			return true
		}
		prev = prev.next
	}
	return false
}

// AcquireBlocking acquires a token, blocking indefinitely until one is available.
// This is useful for cases where you don't need timeout or context cancellation.
// Returns immediately if a token is available (fast path).
func (s *FastSemaphore) AcquireBlocking() {
	// Try fast path first
	if s.TryAcquire() {
		return
	}

	// Need to wait - create a waiter and add to queue
	w := &waiter{
		ready: make(chan struct{}),
	}

	s.lock.Lock()
	s.enqueue(w)
	s.lock.Unlock()

	// Wait to be notified
	<-w.ready
}

// releaseToPool releases a token back to the pool.
// This should be called when a waiter was notified but then cancelled/timed out.
// We need to pass the token to another waiter if any, otherwise decrement the counter.
func (s *FastSemaphore) releaseToPool() {
	s.lock.Lock()
	w := s.dequeue()
	s.lock.Unlock()

	if w != nil {
		// Transfer the token to another waiter
		close(w.ready)
	} else {
		// No waiters, decrement the counter to free the slot
		s.count.Add(-1)
	}
}

// Release releases a token back to the semaphore.
// This wakes up the first waiting goroutine if any are blocked.
func (s *FastSemaphore) Release() {
	for {
		s.lock.Lock()
		w := s.dequeue()
		s.lock.Unlock()

		if w == nil {
			// No waiters, decrement the counter to free the slot
			s.count.Add(-1)
			return
		}

		// Check if this waiter was cancelled before we notify them
		if w.cancelled.Load() {
			// This waiter was cancelled, skip them and try the next one
			// We still have the token, so continue the loop
			close(w.ready) // Still need to close to unblock them
			continue
		}

		// Transfer the token directly to the waiter
		// Don't decrement the counter - the waiter takes over this slot
		close(w.ready)
		return
	}
}

// Len returns the current number of acquired tokens.
// Used by tests to check semaphore state.
func (s *FastSemaphore) Len() int32 {
	return s.count.Load()
}
