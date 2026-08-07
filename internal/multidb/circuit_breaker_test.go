package multidb

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	if cb.State() != CircuitClosed {
		t.Errorf("expected initial state to be Closed, got %v", cb.State())
	}
}

func TestCircuitBreaker_OpenAfterFailures(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		GracePeriod:      100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Record failures
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected state to be Open after %d failures, got %v", 3, cb.State())
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		GracePeriod:      50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("expected state to be Open, got %v", cb.State())
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// CheckState should transition to HalfOpen
	state := cb.CheckState()
	if state != CircuitHalfOpen {
		t.Errorf("expected state to be HalfOpen after timeout, got %v", state)
	}
}

func TestCircuitBreaker_CloseAfterSuccesses(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		GracePeriod:      10 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout and transition to half-open
	time.Sleep(20 * time.Millisecond)
	cb.CheckState()

	// Record successes
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != CircuitClosed {
		t.Errorf("expected state to be Closed after successes, got %v", cb.State())
	}
}

func TestCircuitBreaker_ReopenOnFailureInHalfOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		GracePeriod:      10 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout and transition to half-open
	time.Sleep(20 * time.Millisecond)
	cb.CheckState()

	// Record failure in half-open
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Errorf("expected state to be Open after failure in HalfOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		GracePeriod:      1 * time.Hour,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("expected state to be Open, got %v", cb.State())
	}

	// Reset
	cb.Reset()

	if cb.State() != CircuitClosed {
		t.Errorf("expected state to be Closed after reset, got %v", cb.State())
	}
}

func TestCircuitBreaker_Callbacks(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		GracePeriod:      10 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	var transitions []struct {
		old, new CircuitState
	}
	var mu sync.Mutex

	cb.OnStateChange(func(old, new CircuitState) {
		mu.Lock()
		transitions = append(transitions, struct{ old, new CircuitState }{old, new})
		mu.Unlock()
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(20 * time.Millisecond)
	cb.CheckState()

	// Close the circuit
	cb.RecordSuccess()

	mu.Lock()
	defer mu.Unlock()

	if len(transitions) != 3 {
		t.Errorf("expected 3 transitions, got %d", len(transitions))
	}
}

func TestCircuitBreaker_Concurrent(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 100,
		SuccessThreshold: 10,
		GracePeriod:      1 * time.Hour,
	}
	cb := NewCircuitBreaker(config)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			cb.RecordSuccess()
		}()
		go func() {
			defer wg.Done()
			cb.RecordFailure()
		}()
	}
	wg.Wait()

	// Should not panic and state should be valid
	state := cb.State()
	if state != CircuitClosed && state != CircuitOpen {
		t.Errorf("unexpected state: %v", state)
	}
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state    CircuitState
		expected string
	}{
		{CircuitClosed, "closed"},
		{CircuitOpen, "open"},
		{CircuitHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("CircuitState(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}
