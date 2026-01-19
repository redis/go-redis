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

	sm.Transition(StateIdle)
	if state := sm.GetState(); state != StateIdle {
		t.Errorf("expected state to be IDLE, got %s", state)
	}
}

func TestConnStateMachine_TryTransition(t *testing.T) {
	tests := []struct {
		name         string
		initialState ConnState
		validStates  []ConnState
		targetState  ConnState
		expectError  bool
	}{
		{
			name:         "valid transition from CREATED to INITIALIZING",
			initialState: StateCreated,
			validStates:  []ConnState{StateCreated},
			targetState:  StateInitializing,
			expectError:  false,
		},
		{
			name:         "invalid transition from CREATED to IDLE",
			initialState: StateCreated,
			validStates:  []ConnState{StateInitializing},
			targetState:  StateIdle,
			expectError:  true,
		},
		{
			name:         "transition to same state",
			initialState: StateIdle,
			validStates:  []ConnState{StateIdle},
			targetState:  StateIdle,
			expectError:  false,
		},
		{
			name:         "multiple valid from states",
			initialState: StateIdle,
			validStates:  []ConnState{StateInitializing, StateIdle, StateUnusable},
			targetState:  StateUnusable,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewConnStateMachine()
			sm.Transition(tt.initialState)

			_, err := sm.TryTransition(tt.validStates, tt.targetState)

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
	sm.Transition(StateIdle)

	ctx := context.Background()

	// Fast path: already in valid state
	_, err := sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateUnusable)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if state := sm.GetState(); state != StateUnusable {
		t.Errorf("expected state UNUSABLE, got %s", state)
	}
}

func TestConnStateMachine_AwaitAndTransition_Timeout(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateCreated)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Wait for a state that will never come
	_, err := sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateUnusable)
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
			_, err := sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateIdle)
			if err != nil {
				t.Errorf("waiter %d got error: %v", waiterID, err)
				return
			}

			orderMu.Lock()
			order = append(order, waiterID)
			orderMu.Unlock()

			// Transition back to READY for next waiter
			sm.Transition(StateIdle)
		}()
	}

	// Give waiters time to queue up
	time.Sleep(100 * time.Millisecond)

	// Transition to READY to start processing waiters
	sm.Transition(StateIdle)

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
	sm.Transition(StateIdle)

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
				_, err := sm.TryTransition([]ConnState{StateIdle}, StateUnusable)
				if err == nil {
					successCount.Add(1)
					// Transition back to READY
					sm.Transition(StateIdle)
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
		{StateIdle, "IDLE"},
		{StateInUse, "IN_USE"},
		{StateUnusable, "UNUSABLE"},
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
	sm.Transition(StateIdle)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.GetState()
	}
}

func TestConnStateMachine_PreventsConcurrentInitialization(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)

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
			_, err := sm.TryTransition([]ConnState{StateIdle}, StateInitializing)
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
				sm.Transition(StateIdle)
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
	sm.Transition(StateIdle)

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
			_, err := sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateInitializing)
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
			sm.Transition(StateIdle)

			completedCount.Add(1)
			t.Logf("Goroutine %d: completed initialization (total=%d)", id, completedCount.Load())
		}(i)
	}

	wg.Wait()

	// All goroutines should have completed successfully
	if completedCount.Load() != numGoroutines {
		t.Errorf("expected %d completions, got %d", numGoroutines, completedCount.Load())
	}

	// Final state should be IDLE
	if sm.GetState() != StateIdle {
		t.Errorf("expected final state IDLE, got %s", sm.GetState())
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

	// Launch goroutines one at a time, ensuring each is queued before launching the next
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		expectedWaiters := int32(i + 1)

		go func(id int) {
			defer wg.Done()

			ctx := context.Background()

			// This should queue in FIFO order
			_, err := sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateInitializing)
			if err != nil {
				t.Errorf("Goroutine %d: failed to transition: %v", id, err)
				return
			}

			// Record execution order
			orderMu.Lock()
			executionOrder = append(executionOrder, id)
			orderMu.Unlock()

			t.Logf("Goroutine %d: executed (position %d)", id, len(executionOrder))

			// Transition back to IDLE to allow next waiter
			sm.Transition(StateIdle)
		}(i)

		// Wait until this goroutine has been queued before launching the next
		// Poll the waiter count to ensure the goroutine is actually queued
		timeout := time.After(100 * time.Millisecond)
		for {
			if sm.waiterCount.Load() >= expectedWaiters {
				break
			}
			select {
			case <-timeout:
				t.Fatalf("Timeout waiting for goroutine %d to queue", i)
			case <-time.After(1 * time.Millisecond):
				// Continue polling
			}
		}
	}

	// Give all goroutines time to fully settle in the queue
	time.Sleep(10 * time.Millisecond)

	// Transition to IDLE to start processing the queue
	sm.Transition(StateIdle)

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
	sm.Transition(StateIdle) // Start in READY so fast path is available

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
			_, err := sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateInitializing)
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
			sm.Transition(StateIdle)
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
	sm.Transition(StateIdle)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sm.TryTransition([]ConnState{StateIdle}, StateUnusable)
		sm.Transition(StateIdle)
	}
}

func TestConnStateMachine_IdleInUseTransitions(t *testing.T) {
	sm := NewConnStateMachine()

	// Initialize to IDLE state
	sm.Transition(StateInitializing)
	sm.Transition(StateIdle)

	// Test IDLE → IN_USE transition
	_, err := sm.TryTransition([]ConnState{StateIdle}, StateInUse)
	if err != nil {
		t.Errorf("failed to transition from IDLE to IN_USE: %v", err)
	}
	if state := sm.GetState(); state != StateInUse {
		t.Errorf("expected state IN_USE, got %s", state)
	}

	// Test IN_USE → IDLE transition
	_, err = sm.TryTransition([]ConnState{StateInUse}, StateIdle)
	if err != nil {
		t.Errorf("failed to transition from IN_USE to IDLE: %v", err)
	}
	if state := sm.GetState(); state != StateIdle {
		t.Errorf("expected state IDLE, got %s", state)
	}

	// Test concurrent acquisition (only one should succeed)
	sm.Transition(StateIdle)

	var successCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sm.TryTransition([]ConnState{StateIdle}, StateInUse)
			if err == nil {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if count := successCount.Load(); count != 1 {
		t.Errorf("expected exactly 1 successful transition, got %d", count)
	}

	if state := sm.GetState(); state != StateInUse {
		t.Errorf("expected final state IN_USE, got %s", state)
	}
}

func TestConn_UsedMethods(t *testing.T) {
	cn := NewConn(nil)

	// Initialize connection to IDLE state
	cn.stateMachine.Transition(StateInitializing)
	cn.stateMachine.Transition(StateIdle)

	// Test IsUsed - should be false when IDLE
	if cn.IsUsed() {
		t.Error("expected IsUsed to be false for IDLE connection")
	}

	// Test CompareAndSwapUsed - acquire connection
	if !cn.CompareAndSwapUsed(false, true) {
		t.Error("failed to acquire connection with CompareAndSwapUsed")
	}

	// Test IsUsed - should be true when IN_USE
	if !cn.IsUsed() {
		t.Error("expected IsUsed to be true for IN_USE connection")
	}

	// Test CompareAndSwapUsed - release connection
	if !cn.CompareAndSwapUsed(true, false) {
		t.Error("failed to release connection with CompareAndSwapUsed")
	}

	// Test IsUsed - should be false again
	if cn.IsUsed() {
		t.Error("expected IsUsed to be false after release")
	}

	// Test SetUsed
	cn.SetUsed(true)
	if !cn.IsUsed() {
		t.Error("expected IsUsed to be true after SetUsed(true)")
	}

	cn.SetUsed(false)
	if cn.IsUsed() {
		t.Error("expected IsUsed to be false after SetUsed(false)")
	}
}

func TestConnStateMachine_UnusableState(t *testing.T) {
	sm := NewConnStateMachine()

	// Initialize to IDLE state
	sm.Transition(StateInitializing)
	sm.Transition(StateIdle)

	// Test IDLE → UNUSABLE transition (for background operations)
	_, err := sm.TryTransition([]ConnState{StateIdle}, StateUnusable)
	if err != nil {
		t.Errorf("failed to transition from IDLE to UNUSABLE: %v", err)
	}
	if state := sm.GetState(); state != StateUnusable {
		t.Errorf("expected state UNUSABLE, got %s", state)
	}

	// Test UNUSABLE → IDLE transition (after background operation completes)
	_, err = sm.TryTransition([]ConnState{StateUnusable}, StateIdle)
	if err != nil {
		t.Errorf("failed to transition from UNUSABLE to IDLE: %v", err)
	}
	if state := sm.GetState(); state != StateIdle {
		t.Errorf("expected state IDLE, got %s", state)
	}

	// Test that we can transition from IN_USE to UNUSABLE if needed
	// (e.g., for urgent handoff while connection is in use)
	sm.Transition(StateInUse)
	_, err = sm.TryTransition([]ConnState{StateInUse}, StateUnusable)
	if err != nil {
		t.Errorf("failed to transition from IN_USE to UNUSABLE: %v", err)
	}
	if state := sm.GetState(); state != StateUnusable {
		t.Errorf("expected state UNUSABLE, got %s", state)
	}

	// Test UNUSABLE → INITIALIZING transition (for handoff)
	sm.Transition(StateIdle)
	sm.Transition(StateUnusable)
	_, err = sm.TryTransition([]ConnState{StateUnusable}, StateInitializing)
	if err != nil {
		t.Errorf("failed to transition from UNUSABLE to INITIALIZING: %v", err)
	}
	if state := sm.GetState(); state != StateInitializing {
		t.Errorf("expected state INITIALIZING, got %s", state)
	}
}

func TestConn_UsableUnusable(t *testing.T) {
	cn := NewConn(nil)

	// Initialize connection to IDLE state
	cn.stateMachine.Transition(StateInitializing)
	cn.stateMachine.Transition(StateIdle)

	// Test IsUsable - should be true when IDLE
	if !cn.IsUsable() {
		t.Error("expected IsUsable to be true for IDLE connection")
	}

	// Test CompareAndSwapUsable - make unusable for background operation
	if !cn.CompareAndSwapUsable(true, false) {
		t.Error("failed to make connection unusable with CompareAndSwapUsable")
	}

	// Verify state is UNUSABLE
	if state := cn.stateMachine.GetState(); state != StateUnusable {
		t.Errorf("expected state UNUSABLE, got %s", state)
	}

	// Test IsUsable - should be false when UNUSABLE
	if cn.IsUsable() {
		t.Error("expected IsUsable to be false for UNUSABLE connection")
	}

	// Test CompareAndSwapUsable - make usable again
	if !cn.CompareAndSwapUsable(false, true) {
		t.Error("failed to make connection usable with CompareAndSwapUsable")
	}

	// Verify state is IDLE
	if state := cn.stateMachine.GetState(); state != StateIdle {
		t.Errorf("expected state IDLE, got %s", state)
	}

	// Test SetUsable(false)
	cn.SetUsable(false)
	if state := cn.stateMachine.GetState(); state != StateUnusable {
		t.Errorf("expected state UNUSABLE after SetUsable(false), got %s", state)
	}

	// Test SetUsable(true)
	cn.SetUsable(true)
	if state := cn.stateMachine.GetState(); state != StateIdle {
		t.Errorf("expected state IDLE after SetUsable(true), got %s", state)
	}
}
