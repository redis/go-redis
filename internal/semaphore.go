package internal

import (
	"context"
	"sync"
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
	ready chan struct{}
	next  *waiter
}

// FastSemaphore is a counting semaphore implementation using a hybrid approach.
// It's optimized for the fast path (no blocking) while still supporting timeouts and context cancellation.
//
// This implementation uses a buffered channel for the fast path (TryAcquire/Release without waiters)
// and a FIFO queue for waiters to ensure fairness.
//
// Performance characteristics:
// - Fast path (no blocking): Single channel operation (very fast)
// - Slow path (blocking): FIFO queue-based waiting
// - Release: Channel send or wake up first waiter in queue
//
// This is significantly faster than a pure channel-based semaphore because:
// 1. The fast path uses a buffered channel (single atomic operation)
// 2. FIFO ordering prevents starvation for waiters
// 3. Waiters don't compete with TryAcquire callers
type FastSemaphore struct {
	// Buffered channel for fast path (TryAcquire/Release)
	tokens chan struct{}

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
	ch := make(chan struct{}, capacity)
	// Fill the channel with tokens (available slots)
	for i := int32(0); i < capacity; i++ {
		ch <- struct{}{}
	}
	return &FastSemaphore{
		max:    capacity,
		tokens: ch,
	}
}

// TryAcquire attempts to acquire a token without blocking.
// Returns true if successful, false if the semaphore is full.
//
// This is the fast path - just a single channel operation.
func (s *FastSemaphore) TryAcquire() bool {
	select {
	case <-s.tokens:
		return true
	default:
		return false
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

	// Try fast path first (non-blocking channel receive)
	select {
	case <-s.tokens:
		return nil
	default:
		// Channel is empty, need to wait
	}

	// Need to wait - create a waiter and add to queue
	w := &waiter{
		ready: make(chan struct{}),
	}

	s.lock.Lock()
	// After acquiring lock, try the channel again (someone might have released)
	select {
	case <-s.tokens:
		s.lock.Unlock()
		return nil
	default:
		// Still empty, add to queue
		s.enqueue(w)
		s.lock.Unlock()
	}

	// Use timer pool to avoid allocation
	timer := semTimers.Get().(*time.Timer)
	defer semTimers.Put(timer)
	timer.Reset(timeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		// Try to remove ourselves from the queue
		s.lock.Lock()
		removed := s.removeWaiter(w)
		s.lock.Unlock()

		if removed {
			// We successfully removed ourselves, no token given
			return ctx.Err()
		}
		// We were already dequeued and given a token, must return it
		<-w.ready
		s.Release()
		return ctx.Err()
	case <-w.ready:
		// We were notified and got the token
		// Stop the timer and drain it if it already fired
		if !timer.Stop() {
			<-timer.C
		}
		// We have the token, just return
		return nil
	case <-timer.C:
		// Try to remove ourselves from the queue
		s.lock.Lock()
		removed := s.removeWaiter(w)
		s.lock.Unlock()

		if removed {
			// We successfully removed ourselves, no token given
			return timeoutErr
		}
		// We were already dequeued and given a token, must return it
		<-w.ready
		s.Release()
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
	// Try fast path first (non-blocking channel receive)
	select {
	case <-s.tokens:
		return
	default:
		// Channel is empty, need to wait
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

// Release releases a token back to the semaphore.
// This wakes up the first waiting goroutine if any are blocked.
func (s *FastSemaphore) Release() {
	s.lock.Lock()
	w := s.dequeue()
	if w == nil {
		// No waiters, put the token back in the channel
		s.lock.Unlock()
		s.tokens <- struct{}{}
		return
	}
	s.lock.Unlock()

	// We have a waiter, give them the token
	close(w.ready)
}

// Len returns the current number of acquired tokens.
// Used by tests to check semaphore state.
func (s *FastSemaphore) Len() int32 {
	// Number of acquired tokens = max - available tokens in channel
	return s.max - int32(len(s.tokens))
}
