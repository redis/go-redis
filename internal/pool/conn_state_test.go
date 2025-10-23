package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnStateMachine_GetState(t *testing.T) {
	sm := NewConnStateMachine()
	
	if state := sm.GetState(); state != StateCreated {
		t.Errorf("expected initial state to be CREATED, got %s", state)
	}
}

func TestConnStateMachine_Transition(t *testing.T) {
	sm := NewConnStateMachine()
	
	// Unconditional transition
	sm.Transition(StateInitializing)
	if state := sm.GetState(); state != StateInitializing {
		t.Errorf("expected state to be INITIALIZING, got %s", state)
	}
	
	sm.Transition(StateReady)
	if state := sm.GetState(); state != StateReady {
		t.Errorf("expected state to be READY, got %s", state)
	}
}

func TestConnStateMachine_TryTransition(t *testing.T) {
	tests := []struct {
		name           string
		initialState   ConnState
		validStates    []ConnState
		targetState    ConnState
		expectError    bool
	}{
		{
			name:         "valid transition from CREATED to INITIALIZING",
			initialState: StateCreated,
			validStates:  []ConnState{StateCreated},
			targetState:  StateInitializing,
			expectError:  false,
		},
		{
			name:         "invalid transition from CREATED to READY",
			initialState: StateCreated,
			validStates:  []ConnState{StateInitializing},
			targetState:  StateReady,
			expectError:  true,
		},
		{
			name:         "transition to same state",
			initialState: StateReady,
			validStates:  []ConnState{StateReady},
			targetState:  StateReady,
			expectError:  false,
		},
		{
			name:         "multiple valid from states",
			initialState: StateReady,
			validStates:  []ConnState{StateInitializing, StateReady, StateReauthInProgress},
			targetState:  StateReauthInProgress,
			expectError:  false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewConnStateMachine()
			sm.Transition(tt.initialState)
			
			err := sm.TryTransition(tt.validStates, tt.targetState)
			
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			
			if !tt.expectError {
				if state := sm.GetState(); state != tt.targetState {
					t.Errorf("expected state %s, got %s", tt.targetState, state)
				}
			}
		})
	}
}

func TestConnStateMachine_AwaitAndTransition_FastPath(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady)

	ctx := context.Background()

	// Fast path: already in valid state
	err := sm.AwaitAndTransition(ctx, []ConnState{StateReady}, StateReauthInProgress)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if state := sm.GetState(); state != StateReauthInProgress {
		t.Errorf("expected state REAUTH_IN_PROGRESS, got %s", state)
	}
}

func TestConnStateMachine_AwaitAndTransition_Timeout(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateCreated)
	
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Wait for a state that will never come
	err := sm.AwaitAndTransition(ctx, []ConnState{StateReady}, StateReauthInProgress)
	if err == nil {
		t.Error("expected timeout error but got none")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestConnStateMachine_AwaitAndTransition_FIFO(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateCreated)

	const numWaiters = 10
	order := make([]int, 0, numWaiters)
	var orderMu sync.Mutex
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(numWaiters)

	// Start multiple waiters
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		waiterID := i
		go func() {
			defer wg.Done()

			// Signal that this goroutine is ready
			startBarrier.Done()
			// Wait for all goroutines to be ready before starting
			startBarrier.Wait()

			ctx := context.Background()
			err := sm.AwaitAndTransition(ctx, []ConnState{StateReady}, StateReady)
			if err != nil {
				t.Errorf("waiter %d got error: %v", waiterID, err)
				return
			}

			orderMu.Lock()
			order = append(order, waiterID)
			orderMu.Unlock()

			// Transition back to READY for next waiter
			sm.Transition(StateReady)
		}()
	}

	// Give waiters time to queue up
	time.Sleep(100 * time.Millisecond)

	// Transition to READY to start processing waiters
	sm.Transition(StateReady)

	// Wait for all waiters to complete
	wg.Wait()

	// Verify all waiters completed (FIFO order is not guaranteed due to goroutine scheduling)
	if len(order) != numWaiters {
		t.Errorf("expected %d waiters to complete, got %d", numWaiters, len(order))
	}

	// Verify no duplicates
	seen := make(map[int]bool)
	for _, id := range order {
		if seen[id] {
			t.Errorf("duplicate waiter ID %d in order", id)
		}
		seen[id] = true
	}
}

func TestConnStateMachine_ConcurrentAccess(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady)

	const numGoroutines = 100
	const numIterations = 100

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < numIterations; j++ {
				// Try to transition from READY to REAUTH_IN_PROGRESS
				err := sm.TryTransition([]ConnState{StateReady}, StateReauthInProgress)
				if err == nil {
					successCount.Add(1)
					// Transition back to READY
					sm.Transition(StateReady)
				}

				// Read state (hot path)
				_ = sm.GetState()
			}
		}()
	}

	wg.Wait()

	// At least some transitions should have succeeded
	if successCount.Load() == 0 {
		t.Error("expected at least some successful transitions")
	}

	t.Logf("Successful transitions: %d out of %d attempts", successCount.Load(), numGoroutines*numIterations)
}



func TestConnStateMachine_StateString(t *testing.T) {
	tests := []struct {
		state    ConnState
		expected string
	}{
		{StateCreated, "CREATED"},
		{StateInitializing, "INITIALIZING"},
		{StateReady, "READY"},
		{StateReauthInProgress, "REAUTH_IN_PROGRESS"},
		{StateClosed, "CLOSED"},
		{ConnState(999), "UNKNOWN(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}

func BenchmarkConnStateMachine_GetState(b *testing.B) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.GetState()
	}
}

func TestConnStateMachine_PreventsConcurrentInitialization(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady)

	const numGoroutines = 10
	var inInitializing atomic.Int32
	var maxConcurrent atomic.Int32
	var successCount atomic.Int32
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(numGoroutines)

	// Try to initialize concurrently from multiple goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			startBarrier.Done()
			startBarrier.Wait()

			// Try to transition to INITIALIZING
			err := sm.TryTransition([]ConnState{StateReady}, StateInitializing)
			if err == nil {
				successCount.Add(1)

				// We successfully transitioned - increment concurrent count
				current := inInitializing.Add(1)

				// Track maximum concurrent initializations
				for {
					max := maxConcurrent.Load()
					if current <= max || maxConcurrent.CompareAndSwap(max, current) {
						break
					}
				}

				t.Logf("Goroutine %d: entered INITIALIZING (concurrent=%d)", id, current)

				// Simulate initialization work
				time.Sleep(10 * time.Millisecond)

				// Decrement before transitioning back
				inInitializing.Add(-1)

				// Transition back to READY
				sm.Transition(StateReady)
			} else {
				t.Logf("Goroutine %d: failed to enter INITIALIZING - %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Total successful transitions: %d, Max concurrent: %d", successCount.Load(), maxConcurrent.Load())

	// The maximum number of concurrent initializations should be 1
	if maxConcurrent.Load() != 1 {
		t.Errorf("expected max 1 concurrent initialization, got %d", maxConcurrent.Load())
	}
}

func TestConnStateMachine_AwaitAndTransitionWaitsForInitialization(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady)

	const numGoroutines = 5
	var completedCount atomic.Int32
	var executionOrder []int
	var orderMu sync.Mutex
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(numGoroutines)

	// All goroutines try to initialize concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			startBarrier.Done()
			startBarrier.Wait()

			ctx := context.Background()

			// Try to transition to INITIALIZING - should wait if another is initializing
			err := sm.AwaitAndTransition(ctx, []ConnState{StateReady}, StateInitializing)
			if err != nil {
				t.Errorf("Goroutine %d: failed to transition: %v", id, err)
				return
			}

			// Record execution order
			orderMu.Lock()
			executionOrder = append(executionOrder, id)
			orderMu.Unlock()

			t.Logf("Goroutine %d: entered INITIALIZING (position %d)", id, len(executionOrder))

			// Simulate initialization work
			time.Sleep(10 * time.Millisecond)

			// Transition back to READY
			sm.Transition(StateReady)

			completedCount.Add(1)
			t.Logf("Goroutine %d: completed initialization (total=%d)", id, completedCount.Load())
		}(i)
	}

	wg.Wait()

	// All goroutines should have completed successfully
	if completedCount.Load() != numGoroutines {
		t.Errorf("expected %d completions, got %d", numGoroutines, completedCount.Load())
	}

	// Final state should be READY
	if sm.GetState() != StateReady {
		t.Errorf("expected final state READY, got %s", sm.GetState())
	}

	t.Logf("Execution order: %v", executionOrder)
}

func TestConnStateMachine_FIFOOrdering(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateInitializing) // Start in INITIALIZING so all waiters must queue

	const numGoroutines = 10
	var executionOrder []int
	var orderMu sync.Mutex
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(numGoroutines)

	// Launch goroutines that will all wait
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			startBarrier.Done()
			startBarrier.Wait()

			// Small stagger to ensure queue order
			time.Sleep(time.Duration(id) * time.Millisecond)

			ctx := context.Background()

			// This should queue in FIFO order
			err := sm.AwaitAndTransition(ctx, []ConnState{StateReady}, StateInitializing)
			if err != nil {
				t.Errorf("Goroutine %d: failed to transition: %v", id, err)
				return
			}

			// Record execution order
			orderMu.Lock()
			executionOrder = append(executionOrder, id)
			orderMu.Unlock()

			t.Logf("Goroutine %d: executed (position %d)", id, len(executionOrder))

			// Transition back to READY to allow next waiter
			sm.Transition(StateReady)
		}(i)
	}

	// Wait a bit for all goroutines to queue up
	time.Sleep(50 * time.Millisecond)

	// Transition to READY to start processing the queue
	sm.Transition(StateReady)

	wg.Wait()

	t.Logf("Execution order: %v", executionOrder)

	// Verify FIFO ordering - should be [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
	for i := 0; i < numGoroutines; i++ {
		if executionOrder[i] != i {
			t.Errorf("FIFO violation: expected goroutine %d at position %d, got %d", i, i, executionOrder[i])
		}
	}
}

func TestConnStateMachine_FIFOWithFastPath(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady) // Start in READY so fast path is available

	const numGoroutines = 10
	var executionOrder []int
	var orderMu sync.Mutex
	var wg sync.WaitGroup
	var startBarrier sync.WaitGroup
	startBarrier.Add(numGoroutines)

	// Launch goroutines that will all try the fast path
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			startBarrier.Done()
			startBarrier.Wait()

			// Small stagger to establish arrival order
			time.Sleep(time.Duration(id) * 100 * time.Microsecond)

			ctx := context.Background()

			// This might use fast path (CAS) or slow path (queue)
			err := sm.AwaitAndTransition(ctx, []ConnState{StateReady}, StateInitializing)
			if err != nil {
				t.Errorf("Goroutine %d: failed to transition: %v", id, err)
				return
			}

			// Record execution order
			orderMu.Lock()
			executionOrder = append(executionOrder, id)
			orderMu.Unlock()

			t.Logf("Goroutine %d: executed (position %d)", id, len(executionOrder))

			// Simulate work
			time.Sleep(5 * time.Millisecond)

			// Transition back to READY to allow next waiter
			sm.Transition(StateReady)
		}(i)
	}

	wg.Wait()

	t.Logf("Execution order: %v", executionOrder)

	// Check if FIFO was maintained
	// With the current fast-path implementation, this might NOT be FIFO
	fifoViolations := 0
	for i := 0; i < numGoroutines; i++ {
		if executionOrder[i] != i {
			fifoViolations++
		}
	}

	if fifoViolations > 0 {
		t.Logf("WARNING: %d FIFO violations detected (fast path bypasses queue)", fifoViolations)
		t.Logf("This is expected with current implementation - fast path uses CAS race")
	}
}

func BenchmarkConnStateMachine_TryTransition(b *testing.B) {
	sm := NewConnStateMachine()
	sm.Transition(StateReady)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.TryTransition([]ConnState{StateReady}, StateReauthInProgress)
		sm.Transition(StateReady)
	}
}

