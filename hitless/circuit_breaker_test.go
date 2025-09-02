package hitless

import (
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/logging"
)

func TestCircuitBreaker(t *testing.T) {
	config := &Config{
		LogLevel: logging.LogLevelError, // Reduce noise in tests
	}

	t.Run("InitialState", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		
		if cb.IsOpen() {
			t.Error("Circuit breaker should start in closed state")
		}
		
		if cb.GetState() != CircuitBreakerClosed {
			t.Errorf("Expected state %v, got %v", CircuitBreakerClosed, cb.GetState())
		}
	})

	t.Run("SuccessfulExecution", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		
		err := cb.Execute(func() error {
			return nil // Success
		})
		
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		
		if cb.GetState() != CircuitBreakerClosed {
			t.Errorf("Expected state %v, got %v", CircuitBreakerClosed, cb.GetState())
		}
	})

	t.Run("FailureThreshold", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		testError := errors.New("test error")
		
		// Fail 4 times (below threshold of 5)
		for i := 0; i < 4; i++ {
			err := cb.Execute(func() error {
				return testError
			})
			if err != testError {
				t.Errorf("Expected test error, got %v", err)
			}
			if cb.GetState() != CircuitBreakerClosed {
				t.Errorf("Circuit should still be closed after %d failures", i+1)
			}
		}
		
		// 5th failure should open the circuit
		err := cb.Execute(func() error {
			return testError
		})
		if err != testError {
			t.Errorf("Expected test error, got %v", err)
		}
		
		if cb.GetState() != CircuitBreakerOpen {
			t.Errorf("Expected state %v, got %v", CircuitBreakerOpen, cb.GetState())
		}
	})

	t.Run("OpenCircuitFailsFast", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		testError := errors.New("test error")
		
		// Force circuit to open
		for i := 0; i < 5; i++ {
			cb.Execute(func() error { return testError })
		}
		
		// Now it should fail fast
		err := cb.Execute(func() error {
			t.Error("Function should not be called when circuit is open")
			return nil
		})
		
		if err != ErrCircuitBreakerOpen {
			t.Errorf("Expected ErrCircuitBreakerOpen, got %v", err)
		}
	})

	t.Run("HalfOpenTransition", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		cb.resetTimeout = 100 * time.Millisecond // Short timeout for testing
		testError := errors.New("test error")
		
		// Force circuit to open
		for i := 0; i < 5; i++ {
			cb.Execute(func() error { return testError })
		}
		
		if cb.GetState() != CircuitBreakerOpen {
			t.Error("Circuit should be open")
		}
		
		// Wait for reset timeout
		time.Sleep(150 * time.Millisecond)
		
		// Next call should transition to half-open
		executed := false
		err := cb.Execute(func() error {
			executed = true
			return nil // Success
		})
		
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		
		if !executed {
			t.Error("Function should have been executed in half-open state")
		}
	})

	t.Run("HalfOpenToClosedTransition", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		cb.resetTimeout = 50 * time.Millisecond
		cb.maxRequests = 3
		testError := errors.New("test error")
		
		// Force circuit to open
		for i := 0; i < 5; i++ {
			cb.Execute(func() error { return testError })
		}
		
		// Wait for reset timeout
		time.Sleep(100 * time.Millisecond)
		
		// Execute successful requests in half-open state
		for i := 0; i < 3; i++ {
			err := cb.Execute(func() error {
				return nil // Success
			})
			if err != nil {
				t.Errorf("Expected no error on attempt %d, got %v", i+1, err)
			}
		}
		
		// Circuit should now be closed
		if cb.GetState() != CircuitBreakerClosed {
			t.Errorf("Expected state %v, got %v", CircuitBreakerClosed, cb.GetState())
		}
	})

	t.Run("HalfOpenToOpenOnFailure", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		cb.resetTimeout = 50 * time.Millisecond
		testError := errors.New("test error")
		
		// Force circuit to open
		for i := 0; i < 5; i++ {
			cb.Execute(func() error { return testError })
		}
		
		// Wait for reset timeout
		time.Sleep(100 * time.Millisecond)
		
		// First request in half-open state fails
		err := cb.Execute(func() error {
			return testError
		})
		
		if err != testError {
			t.Errorf("Expected test error, got %v", err)
		}
		
		// Circuit should be open again
		if cb.GetState() != CircuitBreakerOpen {
			t.Errorf("Expected state %v, got %v", CircuitBreakerOpen, cb.GetState())
		}
	})

	t.Run("Stats", func(t *testing.T) {
		cb := newCircuitBreaker("test-endpoint:6379", config)
		testError := errors.New("test error")

		// Execute some operations
		cb.Execute(func() error { return testError })  // Failure
		cb.Execute(func() error { return testError })  // Failure

		stats := cb.GetStats()

		if stats.Endpoint != "test-endpoint:6379" {
			t.Errorf("Expected endpoint 'test-endpoint:6379', got %s", stats.Endpoint)
		}

		if stats.Failures != 2 {
			t.Errorf("Expected 2 failures, got %d", stats.Failures)
		}

		if stats.State != CircuitBreakerClosed {
			t.Errorf("Expected state %v, got %v", CircuitBreakerClosed, stats.State)
		}

		// Test that success resets failure count
		cb.Execute(func() error { return nil }) // Success
		stats = cb.GetStats()

		if stats.Failures != 0 {
			t.Errorf("Expected 0 failures after success, got %d", stats.Failures)
		}
	})
}

func TestCircuitBreakerManager(t *testing.T) {
	config := &Config{
		LogLevel: logging.LogLevelError,
	}

	t.Run("GetCircuitBreaker", func(t *testing.T) {
		manager := newCircuitBreakerManager(config)
		
		cb1 := manager.GetCircuitBreaker("endpoint1:6379")
		cb2 := manager.GetCircuitBreaker("endpoint2:6379")
		cb3 := manager.GetCircuitBreaker("endpoint1:6379") // Same as cb1
		
		if cb1 == cb2 {
			t.Error("Different endpoints should have different circuit breakers")
		}
		
		if cb1 != cb3 {
			t.Error("Same endpoint should return the same circuit breaker")
		}
	})

	t.Run("GetAllStats", func(t *testing.T) {
		manager := newCircuitBreakerManager(config)
		
		// Create circuit breakers for different endpoints
		cb1 := manager.GetCircuitBreaker("endpoint1:6379")
		cb2 := manager.GetCircuitBreaker("endpoint2:6379")
		
		// Execute some operations
		cb1.Execute(func() error { return nil })
		cb2.Execute(func() error { return errors.New("test error") })
		
		stats := manager.GetAllStats()
		
		if len(stats) != 2 {
			t.Errorf("Expected 2 circuit breaker stats, got %d", len(stats))
		}
		
		// Check that we have stats for both endpoints
		endpoints := make(map[string]bool)
		for _, stat := range stats {
			endpoints[stat.Endpoint] = true
		}
		
		if !endpoints["endpoint1:6379"] || !endpoints["endpoint2:6379"] {
			t.Error("Missing stats for expected endpoints")
		}
	})

	t.Run("Reset", func(t *testing.T) {
		manager := newCircuitBreakerManager(config)
		testError := errors.New("test error")
		
		cb := manager.GetCircuitBreaker("test-endpoint:6379")
		
		// Force circuit to open
		for i := 0; i < 5; i++ {
			cb.Execute(func() error { return testError })
		}
		
		if cb.GetState() != CircuitBreakerOpen {
			t.Error("Circuit should be open")
		}
		
		// Reset all circuit breakers
		manager.Reset()
		
		if cb.GetState() != CircuitBreakerClosed {
			t.Error("Circuit should be closed after reset")
		}
		
		if cb.failures.Load() != 0 {
			t.Error("Failure count should be reset to 0")
		}
	})
}
