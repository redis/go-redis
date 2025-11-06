package pool

import (
	"context"
	"testing"
)

// TestPredefinedSlicesAvoidAllocations verifies that using predefined slices
// avoids allocations in AwaitAndTransition calls
func TestPredefinedSlicesAvoidAllocations(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)
	ctx := context.Background()

	// Test with predefined slice - should have 0 allocations on fast path
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = sm.AwaitAndTransition(ctx, validFromIdle, StateUnusable)
		sm.Transition(StateIdle)
	})

	if allocs > 0 {
		t.Errorf("Expected 0 allocations with predefined slice, got %.2f", allocs)
	}
}

// TestInlineSliceAllocations shows that inline slices cause allocations
func TestInlineSliceAllocations(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)
	ctx := context.Background()

	// Test with inline slice - will allocate
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateUnusable)
		sm.Transition(StateIdle)
	})

	if allocs == 0 {
		t.Logf("Inline slice had 0 allocations (compiler optimization)")
	} else {
		t.Logf("Inline slice caused %.2f allocations per run (expected)", allocs)
	}
}

// BenchmarkAwaitAndTransition_PredefinedSlice benchmarks with predefined slice
func BenchmarkAwaitAndTransition_PredefinedSlice(b *testing.B) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = sm.AwaitAndTransition(ctx, validFromIdle, StateUnusable)
		sm.Transition(StateIdle)
	}
}

// BenchmarkAwaitAndTransition_InlineSlice benchmarks with inline slice
func BenchmarkAwaitAndTransition_InlineSlice(b *testing.B) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = sm.AwaitAndTransition(ctx, []ConnState{StateIdle}, StateUnusable)
		sm.Transition(StateIdle)
	}
}

// BenchmarkAwaitAndTransition_MultipleStates_Predefined benchmarks with predefined multi-state slice
func BenchmarkAwaitAndTransition_MultipleStates_Predefined(b *testing.B) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = sm.AwaitAndTransition(ctx, validFromCreatedIdleOrUnusable, StateInitializing)
		sm.Transition(StateIdle)
	}
}

// BenchmarkAwaitAndTransition_MultipleStates_Inline benchmarks with inline multi-state slice
func BenchmarkAwaitAndTransition_MultipleStates_Inline(b *testing.B) {
	sm := NewConnStateMachine()
	sm.Transition(StateIdle)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = sm.AwaitAndTransition(ctx, []ConnState{StateCreated, StateIdle, StateUnusable}, StateInitializing)
		sm.Transition(StateIdle)
	}
}

// TestPreallocatedErrorsAvoidAllocations verifies that preallocated errors
// avoid allocations in hot paths
func TestPreallocatedErrorsAvoidAllocations(t *testing.T) {
	cn := NewConn(nil)

	// Test MarkForHandoff - first call should succeed
	err := cn.MarkForHandoff("localhost:6379", 123)
	if err != nil {
		t.Fatalf("First MarkForHandoff should succeed: %v", err)
	}

	// Second call should return preallocated error with 0 allocations
	allocs := testing.AllocsPerRun(100, func() {
		_ = cn.MarkForHandoff("localhost:6380", 124)
	})

	if allocs > 0 {
		t.Errorf("Expected 0 allocations for preallocated error, got %.2f", allocs)
	}
}

// BenchmarkHandoffErrors_Preallocated benchmarks handoff errors with preallocated errors
func BenchmarkHandoffErrors_Preallocated(b *testing.B) {
	cn := NewConn(nil)
	cn.MarkForHandoff("localhost:6379", 123)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = cn.MarkForHandoff("localhost:6380", 124)
	}
}

// BenchmarkCompareAndSwapUsable_Preallocated benchmarks with preallocated slices
func BenchmarkCompareAndSwapUsable_Preallocated(b *testing.B) {
	cn := NewConn(nil)
	cn.stateMachine.Transition(StateIdle)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cn.CompareAndSwapUsable(true, false) // IDLE -> UNUSABLE
		cn.CompareAndSwapUsable(false, true) // UNUSABLE -> IDLE
	}
}

// TestAllTryTransitionUsePredefinedSlices verifies all TryTransition calls use predefined slices
func TestAllTryTransitionUsePredefinedSlices(t *testing.T) {
	cn := NewConn(nil)
	cn.stateMachine.Transition(StateIdle)

	// Test CompareAndSwapUsable - should have minimal allocations
	allocs := testing.AllocsPerRun(100, func() {
		cn.CompareAndSwapUsable(true, false) // IDLE -> UNUSABLE
		cn.CompareAndSwapUsable(false, true) // UNUSABLE -> IDLE
	})

	// Allow some allocations for error objects, but should be minimal
	if allocs > 2 {
		t.Errorf("Expected <= 2 allocations with predefined slices, got %.2f", allocs)
	}
}

