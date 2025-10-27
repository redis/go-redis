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

// FastSemaphore is a counting semaphore implementation using atomic operations.
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
type FastSemaphore struct {
	// Current number of acquired tokens (atomic)
	count atomic.Int32

	// Maximum number of tokens (capacity)
	max int32

	// Channel for blocking waiters
	// Only used when fast path fails (semaphore is full)
	waitCh chan struct{}
}

// NewFastSemaphore creates a new fast semaphore with the given capacity.
func NewFastSemaphore(capacity int32) *FastSemaphore {
	return &FastSemaphore{
		max:    capacity,
		waitCh: make(chan struct{}, capacity),
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

// Acquire acquires a token, blocking if necessary until one is available or the context is cancelled.
// Returns an error if the context is cancelled or the timeout expires.
// Returns timeoutErr when the timeout expires.
//
// Performance optimization:
// 1. First try fast path (no blocking)
// 2. If that fails, fall back to channel-based waiting
func (s *FastSemaphore) Acquire(ctx context.Context, timeout time.Duration, timeoutErr error) error {
	// Fast path: try to acquire without blocking
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Try fast acquire first
	if s.TryAcquire() {
		return nil
	}

	// Fast path failed, need to wait
	// Use timer pool to avoid allocation
	timer := semTimers.Get().(*time.Timer)
	defer semTimers.Put(timer)
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
			if s.TryAcquire() {
				if !timer.Stop() {
					<-timer.C
				}
				return nil
			}
			// Failed to acquire (race with another goroutine), continue waiting

		case <-timer.C:
			return timeoutErr
		}

		// Periodically check if we can acquire (handles race conditions)
		if time.Since(start) > timeout {
			return timeoutErr
		}
	}
}

// AcquireBlocking acquires a token, blocking indefinitely until one is available.
// This is useful for cases where you don't need timeout or context cancellation.
// Returns immediately if a token is available (fast path).
func (s *FastSemaphore) AcquireBlocking() {
	// Try fast path first
	if s.TryAcquire() {
		return
	}

	// Slow path: wait for a token
	for {
		<-s.waitCh
		if s.TryAcquire() {
			return
		}
		// Failed to acquire (race with another goroutine), continue waiting
	}
}

// Release releases a token back to the semaphore.
// This wakes up one waiting goroutine if any are blocked.
func (s *FastSemaphore) Release() {
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

// Len returns the current number of acquired tokens.
// Used by tests to check semaphore state.
func (s *FastSemaphore) Len() int32 {
	return s.count.Load()
}
