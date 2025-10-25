package pool

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ConnState represents the connection state in the state machine.
// States are designed to be lightweight and fast to check.
//
// State Transitions:
//   CREATED → INITIALIZING → IDLE ⇄ IN_USE
//                              ↓
//                          UNUSABLE (handoff/reauth)
//                              ↓
//                           IDLE/CLOSED
type ConnState uint32

const (
	// StateCreated - Connection just created, not yet initialized
	StateCreated ConnState = iota

	// StateInitializing - Connection initialization in progress
	StateInitializing

	// StateIdle - Connection initialized and idle in pool, ready to be acquired
	StateIdle

	// StateInUse - Connection actively processing a command (retrieved from pool)
	StateInUse

	// StateUnusable - Connection temporarily unusable due to background operation
	// (handoff, reauth, etc.). Cannot be acquired from pool.
	StateUnusable

	// StateClosed - Connection closed
	StateClosed
)

// Predefined state slices to avoid allocations in hot paths
var (
	validFromInUse              = []ConnState{StateInUse}
	validFromCreatedOrIdle      = []ConnState{StateCreated, StateIdle}
	validFromCreatedOrInUse     = []ConnState{StateCreated, StateInUse}
	validFromCreatedInUseOrIdle = []ConnState{StateCreated, StateInUse, StateIdle}
)

// String returns a human-readable string representation of the state.
func (s ConnState) String() string {
	switch s {
	case StateCreated:
		return "CREATED"
	case StateInitializing:
		return "INITIALIZING"
	case StateIdle:
		return "IDLE"
	case StateInUse:
		return "IN_USE"
	case StateUnusable:
		return "UNUSABLE"
	case StateClosed:
		return "CLOSED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", s)
	}
}

var (
	// ErrInvalidStateTransition is returned when a state transition is not allowed
	ErrInvalidStateTransition = errors.New("invalid state transition")

	// ErrStateMachineClosed is returned when operating on a closed state machine
	ErrStateMachineClosed = errors.New("state machine is closed")

	// ErrTimeout is returned when a state transition times out
	ErrTimeout = errors.New("state transition timeout")
)

// waiter represents a goroutine waiting for a state transition.
// Designed for minimal allocations and fast processing.
type waiter struct {
	validStates map[ConnState]struct{} // States we're waiting for
	targetState ConnState              // State to transition to
	done        chan error             // Signaled when transition completes or times out
}

// ConnStateMachine manages connection state transitions with FIFO waiting queue.
// Optimized for:
// - Lock-free reads (hot path)
// - Minimal allocations
// - Fast state transitions
// - FIFO fairness for waiters
// Note: Handoff metadata (endpoint, seqID, retries) is managed separately in the Conn struct.
type ConnStateMachine struct {
	// Current state - atomic for lock-free reads
	state atomic.Uint32

	// FIFO queue for waiters - only locked during waiter add/remove/notify
	mu          sync.Mutex
	waiters     *list.List // List of *waiter
	waiterCount atomic.Int32 // Fast lock-free check for waiters (avoids mutex in hot path)
}

// NewConnStateMachine creates a new connection state machine.
// Initial state is StateCreated.
func NewConnStateMachine() *ConnStateMachine {
	sm := &ConnStateMachine{
		waiters: list.New(),
	}
	sm.state.Store(uint32(StateCreated))
	return sm
}

// GetState returns the current state (lock-free read).
// This is the hot path - optimized for zero allocations and minimal overhead.
func (sm *ConnStateMachine) GetState() ConnState {
	return ConnState(sm.state.Load())
}

// TryTransition attempts an immediate state transition without waiting.
// Returns the current state after the transition attempt and an error if the transition failed.
// The returned state is the CURRENT state (after the attempt), not the previous state.
// This is faster than AwaitAndTransition when you don't need to wait.
// Uses compare-and-swap to atomically transition, preventing concurrent transitions.
// This method does NOT wait - it fails immediately if the transition cannot be performed.
//
// Performance: Zero allocations on success path (hot path).
func (sm *ConnStateMachine) TryTransition(validFromStates []ConnState, targetState ConnState) (ConnState, error) {
	// Try each valid from state with CAS
	// This ensures only ONE goroutine can successfully transition at a time
	for _, fromState := range validFromStates {
		// Try to atomically swap from fromState to targetState
		// If successful, we won the race and can proceed
		if sm.state.CompareAndSwap(uint32(fromState), uint32(targetState)) {
			// Success! We transitioned atomically
			// Hot path optimization: only check for waiters if transition succeeded
			// This avoids atomic load on every Get/Put when no waiters exist
			if sm.waiterCount.Load() > 0 {
				sm.notifyWaiters()
			}
			return targetState, nil
		}
	}

	// All CAS attempts failed - state is not valid for this transition
	// Return the current state so caller can decide what to do
	// Note: This error path allocates, but it's the exceptional case
	currentState := sm.GetState()
	return currentState, fmt.Errorf("%w: cannot transition from %s to %s (valid from: %v)",
		ErrInvalidStateTransition, currentState, targetState, validFromStates)
}

// Transition unconditionally transitions to the target state.
// Use with caution - prefer AwaitAndTransition or TryTransition for safety.
// This is useful for error paths or when you know the transition is valid.
func (sm *ConnStateMachine) Transition(targetState ConnState) {
	sm.state.Store(uint32(targetState))
	sm.notifyWaiters()
}

// AwaitAndTransition waits for the connection to reach one of the valid states,
// then atomically transitions to the target state.
// Returns the current state after the transition attempt and an error if the operation failed.
// The returned state is the CURRENT state (after the attempt), not the previous state.
// Returns error if timeout expires or context is cancelled.
//
// This method implements FIFO fairness - the first caller to wait gets priority
// when the state becomes available.
//
// Performance notes:
// - If already in a valid state, this is very fast (no allocation, no waiting)
// - If waiting is required, allocates one waiter struct and one channel
func (sm *ConnStateMachine) AwaitAndTransition(
	ctx context.Context,
	validFromStates []ConnState,
	targetState ConnState,
) (ConnState, error) {
	// Fast path: try immediate transition with CAS to prevent race conditions
	for _, fromState := range validFromStates {
		// Check if we're already in target state
		if fromState == targetState && sm.GetState() == targetState {
			return targetState, nil
		}

		// Try to atomically swap from fromState to targetState
		if sm.state.CompareAndSwap(uint32(fromState), uint32(targetState)) {
			// Success! We transitioned atomically
			sm.notifyWaiters()
			return targetState, nil
		}
	}

	// Fast path failed - check if we should wait or fail
	currentState := sm.GetState()

	// Check if closed
	if currentState == StateClosed {
		return currentState, ErrStateMachineClosed
	}

	// Slow path: need to wait for state change
	// Create waiter with valid states map for fast lookup
	validStatesMap := make(map[ConnState]struct{}, len(validFromStates))
	for _, s := range validFromStates {
		validStatesMap[s] = struct{}{}
	}

	w := &waiter{
		validStates: validStatesMap,
		targetState: targetState,
		done:        make(chan error, 1), // Buffered to avoid goroutine leak
	}

	// Add to FIFO queue
	sm.mu.Lock()
	elem := sm.waiters.PushBack(w)
	sm.waiterCount.Add(1)
	sm.mu.Unlock()

	// Wait for state change or timeout
	select {
	case <-ctx.Done():
		// Timeout or cancellation - remove from queue
		sm.mu.Lock()
		sm.waiters.Remove(elem)
		sm.waiterCount.Add(-1)
		sm.mu.Unlock()
		return sm.GetState(), ctx.Err()
	case err := <-w.done:
		// Transition completed (or failed)
		// Note: waiterCount is decremented in notifyWaiters when waiter is removed
		return sm.GetState(), err
	}
}

// notifyWaiters checks if any waiters can proceed and notifies them in FIFO order.
// This is called after every state transition.
func (sm *ConnStateMachine) notifyWaiters() {
	// Fast path: check atomic counter without acquiring lock
	// This eliminates mutex overhead in the common case (no waiters)
	if sm.waiterCount.Load() == 0 {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Double-check after acquiring lock (waiters might have been processed)
	if sm.waiters.Len() == 0 {
		return
	}

	// Process waiters in FIFO order until no more can be processed
	// We loop instead of recursing to avoid stack overflow and mutex issues
	for {
		processed := false

		// Find the first waiter that can proceed
		for elem := sm.waiters.Front(); elem != nil; elem = elem.Next() {
			w := elem.Value.(*waiter)

			// Read current state inside the loop to get the latest value
			currentState := sm.GetState()

			// Check if current state is valid for this waiter
			if _, valid := w.validStates[currentState]; valid {
				// Remove from queue first
				sm.waiters.Remove(elem)
				sm.waiterCount.Add(-1)

				// Use CAS to ensure state hasn't changed since we checked
				// This prevents race condition where another thread changes state
				// between our check and our transition
				if sm.state.CompareAndSwap(uint32(currentState), uint32(w.targetState)) {
					// Successfully transitioned - notify waiter
					w.done <- nil
					processed = true
					break
				} else {
					// State changed - re-add waiter to front of queue and retry
					sm.waiters.PushFront(w)
					sm.waiterCount.Add(1)
					// Continue to next iteration to re-read state
					processed = true
					break
				}
			}
		}

		// If we didn't process any waiter, we're done
		if !processed {
			break
		}
	}
}

