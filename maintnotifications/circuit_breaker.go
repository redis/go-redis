package maintnotifications

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/circuitbreaker"
	"github.com/redis/go-redis/v9/internal/maintnotifications/logs"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState = circuitbreaker.State

const (
	// CircuitBreakerClosed - normal operation, requests allowed
	CircuitBreakerClosed = circuitbreaker.StateClosed
	// CircuitBreakerOpen - failing fast, requests rejected
	CircuitBreakerOpen = circuitbreaker.StateOpen
	// CircuitBreakerHalfOpen - testing if service recovered
	CircuitBreakerHalfOpen = circuitbreaker.StateHalfOpen
)

// CircuitBreaker wraps the internal circuit breaker with endpoint-specific
// logging. The inner breaker is held as an unexported field (rather than
// embedded) so its exported methods are not promoted onto this type; that keeps
// callers on the wrapper's API and preserves wrapper invariants such as the
// lastSuccessTime bookkeeping.
type CircuitBreaker struct {
	inner           *circuitbreaker.CircuitBreaker
	endpoint        string
	lastSuccessTime atomic.Int64 // Unix timestamp of last success
}

// newCircuitBreaker creates a new circuit breaker for an endpoint
func newCircuitBreaker(endpoint string, config *Config) *CircuitBreaker {
	// Use configuration values with sensible defaults
	failureThreshold := 5
	resetTimeout := 60 * time.Second
	maxRequests := 3

	if config != nil {
		failureThreshold = config.CircuitBreakerFailureThreshold
		resetTimeout = config.CircuitBreakerResetTimeout
		maxRequests = config.CircuitBreakerMaxRequests
	}

	cb := &CircuitBreaker{
		inner: circuitbreaker.New(circuitbreaker.Config{
			FailureThreshold:    failureThreshold,
			SuccessThreshold:    maxRequests, // Use maxRequests as success threshold
			MaxHalfOpenRequests: maxRequests,
			OpenTimeout:         resetTimeout,
		}),
		endpoint: endpoint,
	}

	// Register logging callback for state changes
	cb.inner.OnStateChange(func(oldState, newState circuitbreaker.State) {
		switch {
		case oldState == circuitbreaker.StateClosed && newState == circuitbreaker.StateOpen:
			if internal.LogLevel.WarnOrAbove() {
				stats := cb.inner.Stats()
				internal.Logger.Printf(context.Background(), logs.CircuitBreakerOpened(endpoint, int64(stats.Failures)))
			}
		case oldState == circuitbreaker.StateOpen && newState == circuitbreaker.StateHalfOpen:
			if internal.LogLevel.InfoOrAbove() {
				internal.Logger.Printf(context.Background(), logs.CircuitBreakerTransitioningToHalfOpen(endpoint))
			}
		case oldState == circuitbreaker.StateHalfOpen && newState == circuitbreaker.StateClosed:
			if internal.LogLevel.InfoOrAbove() {
				stats := cb.inner.Stats()
				internal.Logger.Printf(context.Background(), logs.CircuitBreakerClosed(endpoint, int64(stats.Successes)))
			}
		case oldState == circuitbreaker.StateHalfOpen && newState == circuitbreaker.StateOpen:
			if internal.LogLevel.WarnOrAbove() {
				internal.Logger.Printf(context.Background(), logs.CircuitBreakerReopened(endpoint))
			}
		}
	})

	return cb
}

// IsOpen returns true if the circuit breaker is open (rejecting requests).
// It uses CheckState so the open->half-open transition is honored once
// OpenTimeout has elapsed, rather than reporting a stale open state.
//
// IsOpen does not reserve a half-open request slot; callers that gate work on
// the breaker should use allowRequest so MaxHalfOpenRequests is respected.
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.inner.CheckState() == circuitbreaker.StateOpen
}

// allowRequest reports whether a request should be allowed through, reserving a
// half-open probe slot when in the half-open state so concurrent callers are
// bounded by MaxHalfOpenRequests. A reserved slot must be settled with a
// subsequent recordSuccess/recordFailure, or released via releaseRequest when
// the operation produced neither outcome.
func (cb *CircuitBreaker) allowRequest() bool {
	return cb.inner.IsAllowed()
}

// releaseRequest returns a half-open slot reserved by allowRequest when the
// operation completed without a recordable success or failure.
func (cb *CircuitBreaker) releaseRequest() {
	cb.inner.ReleaseHalfOpen()
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	err := cb.inner.Execute(fn)
	if err == nil {
		cb.lastSuccessTime.Store(time.Now().Unix())
		return nil
	}
	// Convert internal circuit open error to our package's error. Use errors.Is
	// so a future wrapped ErrCircuitOpen is still translated to the public error.
	if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		return ErrCircuitBreakerOpen
	}
	return err
}

// recordFailure records a failure (for external use when not using Execute)
func (cb *CircuitBreaker) recordFailure() {
	cb.inner.RecordFailure()
}

// recordSuccess records a success (for external use when not using Execute)
func (cb *CircuitBreaker) recordSuccess() {
	cb.lastSuccessTime.Store(time.Now().Unix())
	cb.inner.RecordSuccess()
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return cb.inner.State()
}

// GetStats returns current statistics for monitoring
func (cb *CircuitBreaker) GetStats() CircuitBreakerStats {
	stats := cb.inner.Stats()
	return CircuitBreakerStats{
		Endpoint:        cb.endpoint,
		State:           stats.State,
		Failures:        int64(stats.Failures),
		Successes:       int64(stats.Successes),
		Requests:        int64(stats.Requests),
		LastFailureTime: stats.LastFailureTime,
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

// CircuitBreakerEntry wraps a circuit breaker with access tracking
type CircuitBreakerEntry struct {
	breaker    *CircuitBreaker
	lastAccess atomic.Int64 // Unix timestamp
	created    time.Time
}

// CircuitBreakerManager manages circuit breakers for multiple endpoints
type CircuitBreakerManager struct {
	breakers    sync.Map // map[string]*CircuitBreakerEntry
	config      *Config
	cleanupStop chan struct{}
	cleanupMu   sync.Mutex
	lastCleanup atomic.Int64 // Unix timestamp
}

// newCircuitBreakerManager creates a new circuit breaker manager
func newCircuitBreakerManager(config *Config) *CircuitBreakerManager {
	cbm := &CircuitBreakerManager{
		config:      config,
		cleanupStop: make(chan struct{}),
	}
	cbm.lastCleanup.Store(time.Now().Unix())

	// Start background cleanup goroutine
	go cbm.cleanupLoop()

	return cbm
}

// GetCircuitBreaker returns the circuit breaker for an endpoint, creating it if necessary
func (cbm *CircuitBreakerManager) GetCircuitBreaker(endpoint string) *CircuitBreaker {
	now := time.Now().Unix()

	if entry, ok := cbm.breakers.Load(endpoint); ok {
		cbEntry := entry.(*CircuitBreakerEntry)
		cbEntry.lastAccess.Store(now)
		return cbEntry.breaker
	}

	// Create new circuit breaker with metadata
	newBreaker := newCircuitBreaker(endpoint, cbm.config)
	newEntry := &CircuitBreakerEntry{
		breaker: newBreaker,
		created: time.Now(),
	}
	newEntry.lastAccess.Store(now)

	actual, _ := cbm.breakers.LoadOrStore(endpoint, newEntry)
	return actual.(*CircuitBreakerEntry).breaker
}

// GetAllStats returns statistics for all circuit breakers
func (cbm *CircuitBreakerManager) GetAllStats() []CircuitBreakerStats {
	var stats []CircuitBreakerStats
	cbm.breakers.Range(func(key, value interface{}) bool {
		entry := value.(*CircuitBreakerEntry)
		stats = append(stats, entry.breaker.GetStats())
		return true
	})
	return stats
}

// cleanupLoop runs background cleanup of unused circuit breakers
func (cbm *CircuitBreakerManager) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute) // Cleanup every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cbm.cleanup()
		case <-cbm.cleanupStop:
			return
		}
	}
}

// cleanup removes circuit breakers that haven't been accessed recently
func (cbm *CircuitBreakerManager) cleanup() {
	// Prevent concurrent cleanups
	if !cbm.cleanupMu.TryLock() {
		return
	}
	defer cbm.cleanupMu.Unlock()

	now := time.Now()
	cutoff := now.Add(-30 * time.Minute).Unix() // 30 minute TTL

	var toDelete []string
	count := 0

	cbm.breakers.Range(func(key, value interface{}) bool {
		endpoint := key.(string)
		entry := value.(*CircuitBreakerEntry)

		count++

		// Remove if not accessed recently
		if entry.lastAccess.Load() < cutoff {
			toDelete = append(toDelete, endpoint)
		}

		return true
	})

	// Delete expired entries
	for _, endpoint := range toDelete {
		cbm.breakers.Delete(endpoint)
	}

	// Log cleanup results
	if len(toDelete) > 0 && internal.LogLevel.InfoOrAbove() {
		internal.Logger.Printf(context.Background(), logs.CircuitBreakerCleanup(len(toDelete), count))
	}

	cbm.lastCleanup.Store(now.Unix())
}

// Shutdown stops the cleanup goroutine
func (cbm *CircuitBreakerManager) Shutdown() {
	close(cbm.cleanupStop)
}

// Reset resets all circuit breakers (useful for testing)
func (cbm *CircuitBreakerManager) Reset() {
	cbm.breakers.Range(func(key, value interface{}) bool {
		entry := value.(*CircuitBreakerEntry)
		entry.breaker.inner.Reset()
		entry.breaker.lastSuccessTime.Store(0)
		return true
	})
}
