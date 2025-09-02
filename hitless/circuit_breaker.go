package hitless

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int32

const (
	// CircuitBreakerClosed - normal operation, requests allowed
	CircuitBreakerClosed CircuitBreakerState = iota
	// CircuitBreakerOpen - failing fast, requests rejected
	CircuitBreakerOpen
	// CircuitBreakerHalfOpen - testing if service recovered
	CircuitBreakerHalfOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case CircuitBreakerClosed:
		return "closed"
	case CircuitBreakerOpen:
		return "open"
	case CircuitBreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern for endpoint-specific failure handling
type CircuitBreaker struct {
	// Configuration
	failureThreshold int           // Number of failures before opening
	resetTimeout     time.Duration // How long to stay open before testing
	maxRequests      int           // Max requests allowed in half-open state

	// State tracking (atomic for lock-free access)
	state           atomic.Int32 // CircuitBreakerState
	failures        atomic.Int64 // Current failure count
	successes       atomic.Int64 // Success count in half-open state
	requests        atomic.Int64 // Request count in half-open state
	lastFailureTime atomic.Int64 // Unix timestamp of last failure
	lastSuccessTime atomic.Int64 // Unix timestamp of last success

	// Endpoint identification
	endpoint string
	config   *Config
}

// newCircuitBreaker creates a new circuit breaker for an endpoint
func newCircuitBreaker(endpoint string, config *Config) *CircuitBreaker {
	// Use sensible defaults if not configured
	failureThreshold := 10
	resetTimeout := 500 * time.Millisecond
	maxRequests := 10

	// These could be added to Config in the future without breaking API
	// For now, use internal defaults that work well

	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		maxRequests:      maxRequests,
		endpoint:         endpoint,
		config:           config,
		state:            atomic.Int32{}, // Defaults to CircuitBreakerClosed (0)
	}
}

// IsOpen returns true if the circuit breaker is open (rejecting requests)
func (cb *CircuitBreaker) IsOpen() bool {
	state := CircuitBreakerState(cb.state.Load())

	if state == CircuitBreakerOpen {
		// Check if we should transition to half-open
		if cb.shouldAttemptReset() {
			if cb.state.CompareAndSwap(int32(CircuitBreakerOpen), int32(CircuitBreakerHalfOpen)) {
				cb.requests.Store(0)
				cb.successes.Store(0)
				if cb.config != nil && cb.config.LogLevel.InfoOrAbove() {
					internal.Logger.Printf(context.Background(),
						"hitless: circuit breaker for %s transitioning to half-open", cb.endpoint)
				}
				return false // Now in half-open state, allow requests
			}
		}
		return true // Still open
	}

	return false
}

// shouldAttemptReset checks if enough time has passed to attempt reset
func (cb *CircuitBreaker) shouldAttemptReset() bool {
	lastFailure := time.Unix(cb.lastFailureTime.Load(), 0)
	return time.Since(lastFailure) >= cb.resetTimeout
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	// Fast path: if circuit is open, fail immediately
	if cb.IsOpen() {
		return ErrCircuitBreakerOpen
	}

	state := CircuitBreakerState(cb.state.Load())

	// In half-open state, limit the number of requests
	if state == CircuitBreakerHalfOpen {
		requests := cb.requests.Add(1)
		if requests > int64(cb.maxRequests) {
			cb.requests.Add(-1) // Revert the increment
			return ErrCircuitBreakerOpen
		}
	}

	// Execute the function
	err := fn()

	if err != nil {
		cb.recordFailure()
		return err
	}

	cb.recordSuccess()
	return nil
}

// recordFailure records a failure and potentially opens the circuit
func (cb *CircuitBreaker) recordFailure() {
	cb.lastFailureTime.Store(time.Now().Unix())
	failures := cb.failures.Add(1)

	state := CircuitBreakerState(cb.state.Load())

	switch state {
	case CircuitBreakerClosed:
		if failures >= int64(cb.failureThreshold) {
			if cb.state.CompareAndSwap(int32(CircuitBreakerClosed), int32(CircuitBreakerOpen)) {
				if cb.config != nil && cb.config.LogLevel.WarnOrAbove() {
					internal.Logger.Printf(context.Background(),
						"hitless: circuit breaker opened for endpoint %s after %d failures",
						cb.endpoint, failures)
				}
			}
		}
	case CircuitBreakerHalfOpen:
		// Any failure in half-open state immediately opens the circuit
		if cb.state.CompareAndSwap(int32(CircuitBreakerHalfOpen), int32(CircuitBreakerOpen)) {
			if cb.config != nil && cb.config.LogLevel.WarnOrAbove() {
				internal.Logger.Printf(context.Background(),
					"hitless: circuit breaker reopened for endpoint %s due to failure in half-open state",
					cb.endpoint)
			}
		}
	}
}

// recordSuccess records a success and potentially closes the circuit
func (cb *CircuitBreaker) recordSuccess() {
	cb.lastSuccessTime.Store(time.Now().Unix())

	state := CircuitBreakerState(cb.state.Load())

	if state == CircuitBreakerClosed {
		// Reset failure count on success in closed state
		cb.failures.Store(0)
	} else if state == CircuitBreakerHalfOpen {
		successes := cb.successes.Add(1)

		// If we've had enough successful requests, close the circuit
		if successes >= int64(cb.maxRequests) {
			if cb.state.CompareAndSwap(int32(CircuitBreakerHalfOpen), int32(CircuitBreakerClosed)) {
				cb.failures.Store(0)
				if cb.config != nil && cb.config.LogLevel.InfoOrAbove() {
					internal.Logger.Printf(context.Background(),
						"hitless: circuit breaker closed for endpoint %s after %d successful requests",
						cb.endpoint, successes)
				}
			}
		}
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return CircuitBreakerState(cb.state.Load())
}

// GetStats returns current statistics for monitoring
func (cb *CircuitBreaker) GetStats() CircuitBreakerStats {
	return CircuitBreakerStats{
		Endpoint:        cb.endpoint,
		State:           cb.GetState(),
		Failures:        cb.failures.Load(),
		Successes:       cb.successes.Load(),
		Requests:        cb.requests.Load(),
		LastFailureTime: time.Unix(cb.lastFailureTime.Load(), 0),
		LastSuccessTime: time.Unix(cb.lastSuccessTime.Load(), 0),
	}
}

// CircuitBreakerStats provides statistics about a circuit breaker
type CircuitBreakerStats struct {
	Endpoint        string
	State           CircuitBreakerState
	Failures        int64
	Successes       int64
	Requests        int64
	LastFailureTime time.Time
	LastSuccessTime time.Time
}

// CircuitBreakerManager manages circuit breakers for multiple endpoints
type CircuitBreakerManager struct {
	breakers sync.Map // map[string]*CircuitBreaker
	config   *Config
}

// newCircuitBreakerManager creates a new circuit breaker manager
func newCircuitBreakerManager(config *Config) *CircuitBreakerManager {
	return &CircuitBreakerManager{
		config: config,
	}
}

// GetCircuitBreaker returns the circuit breaker for an endpoint, creating it if necessary
func (cbm *CircuitBreakerManager) GetCircuitBreaker(endpoint string) *CircuitBreaker {
	if breaker, ok := cbm.breakers.Load(endpoint); ok {
		return breaker.(*CircuitBreaker)
	}

	// Create new circuit breaker
	newBreaker := newCircuitBreaker(endpoint, cbm.config)
	actual, _ := cbm.breakers.LoadOrStore(endpoint, newBreaker)
	return actual.(*CircuitBreaker)
}

// GetAllStats returns statistics for all circuit breakers
func (cbm *CircuitBreakerManager) GetAllStats() []CircuitBreakerStats {
	var stats []CircuitBreakerStats
	cbm.breakers.Range(func(key, value interface{}) bool {
		breaker := value.(*CircuitBreaker)
		stats = append(stats, breaker.GetStats())
		return true
	})
	return stats
}

// Reset resets all circuit breakers (useful for testing)
func (cbm *CircuitBreakerManager) Reset() {
	cbm.breakers.Range(func(key, value interface{}) bool {
		breaker := value.(*CircuitBreaker)
		breaker.state.Store(int32(CircuitBreakerClosed))
		breaker.failures.Store(0)
		breaker.successes.Store(0)
		breaker.requests.Store(0)
		breaker.lastFailureTime.Store(0)
		breaker.lastSuccessTime.Store(0)
		return true
	})
}
