package pool

import (
	"context"
	"sync/atomic"
	"time"
)

// fastSemaphore is a high-performance semaphore implementation using atomic operations.
// It's optimized for the fast path (no blocking) while still supporting timeouts and context cancellation.
//
// Performance characteristics:
// - Fast path (no blocking): Single atomic CAS operation
// - Slow path (blocking): Falls back to channel-based waiting
// - Release: Single atomic decrement + optional channel notification
//
// This is significantly faster than a pure channel-based semaphore because:
// 1. The fast path avoids channel operations entirely (no scheduler involvement)
// 2. Atomic operations are much cheaper than channel send/receive
// 3. Better CPU cache behavior (no channel buffer allocation)
type fastSemaphore struct {
	// Current number of acquired tokens (atomic)
	count atomic.Int32

	// Maximum number of tokens (capacity)
	max int32

	// Channel for blocking waiters
	// Only used when fast path fails (semaphore is full)
	waitCh chan struct{}
}

// newFastSemaphore creates a new fast semaphore with the given capacity.
func newFastSemaphore(capacity int32) *fastSemaphore {
	return &fastSemaphore{
		max:    capacity,
		waitCh: make(chan struct{}, capacity),
	}
}

// tryAcquire attempts to acquire a token without blocking.
// Returns true if successful, false if the semaphore is full.
//
// This is the fast path - just a single CAS operation.
func (s *fastSemaphore) tryAcquire() bool {
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

// acquire acquires a token, blocking if necessary until one is available or the context is cancelled.
// Returns an error if the context is cancelled or the timeout expires.
//
// Performance optimization:
// 1. First try fast path (no blocking)
// 2. If that fails, fall back to channel-based waiting
func (s *fastSemaphore) acquire(ctx context.Context, timeout time.Duration) error {
	// Fast path: try to acquire without blocking
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Try fast acquire first
	if s.tryAcquire() {
		return nil
	}

	// Fast path failed, need to wait
	// Use timer pool to avoid allocation
	timer := timers.Get().(*time.Timer)
	defer timers.Put(timer)
	timer.Reset(timeout)

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()

		case <-s.waitCh:
			// Someone released a token, try to acquire it
			if s.tryAcquire() {
				if !timer.Stop() {
					<-timer.C
				}
				return nil
			}
			// Failed to acquire (race with another goroutine), continue waiting

		case <-timer.C:
			return ErrPoolTimeout
		}

		// Periodically check if we can acquire (handles race conditions)
		if time.Since(start) > timeout {
			return ErrPoolTimeout
		}
	}
}

// release releases a token back to the semaphore.
// This wakes up one waiting goroutine if any are blocked.
func (s *fastSemaphore) release() {
	s.count.Add(-1)

	// Try to wake up a waiter (non-blocking)
	// If no one is waiting, this is a no-op
	select {
	case s.waitCh <- struct{}{}:
		// Successfully notified a waiter
	default:
		// No waiters, that's fine
	}
}

// len returns the current number of acquired tokens.
func (s *fastSemaphore) len() int32 {
	return s.count.Load()
}

// cap returns the maximum capacity of the semaphore.
func (s *fastSemaphore) cap() int32 {
	return s.max
}

