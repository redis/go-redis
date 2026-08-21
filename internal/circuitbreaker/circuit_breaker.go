// Package circuitbreaker provides a circuit breaker implementation for fault tolerance.
package circuitbreaker

import (
	"sync"
	"sync/atomic"
	"time"
)

// State represents the state of a circuit breaker.
type State int32

const (
	// StateClosed indicates the circuit is closed and requests are allowed.
	StateClosed State = iota
	// StateOpen indicates the circuit is open and requests are blocked.
	StateOpen
	// StateHalfOpen indicates the circuit is testing if the service has recovered.
	StateHalfOpen
)

// String returns the string representation of the circuit state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config holds configuration for a circuit breaker.
type Config struct {
	// FailureThreshold is the number of failures before opening the circuit.
	// Default: 5
	FailureThreshold int

	// SuccessThreshold is the number of successes in half-open state before closing.
	// Default: 2
	SuccessThreshold int

	// MaxHalfOpenRequests is the maximum number of requests allowed in half-open state.
	// If 0, uses SuccessThreshold as the limit.
	// Default: 0 (uses SuccessThreshold)
	MaxHalfOpenRequests int

	// OpenTimeout is how long to wait before transitioning from open to half-open.
	// This is the circuit "grace period" that gives a failed database time to
	// self-heal before it is probed again.
	// Default: 60 seconds
	OpenTimeout time.Duration
}

// DefaultConfig returns the default circuit breaker configuration.
func DefaultConfig() Config {
	return Config{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		MaxHalfOpenRequests: 0,
		OpenTimeout:         60 * time.Second,
	}
}

// applyDefaults fills in zero values with defaults.
func (c *Config) applyDefaults() {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 5
	}
	if c.SuccessThreshold <= 0 {
		c.SuccessThreshold = 2
	}
	if c.MaxHalfOpenRequests <= 0 {
		c.MaxHalfOpenRequests = c.SuccessThreshold
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = 60 * time.Second
	}
}

// StateChangeCallback is called when the circuit breaker state changes.
type StateChangeCallback func(oldState, newState State)

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	config Config

	state       atomic.Int32
	failures    atomic.Int32
	successes   atomic.Int32
	requests    atomic.Int32 // Request count in half-open state
	lastFailure atomic.Int64 // Unix nano timestamp

	// transitionMu serializes the open -> half-open transition so the
	// half-open counters can be cleared before the new state is published.
	transitionMu sync.Mutex

	mu        sync.RWMutex
	callbacks []StateChangeCallback
}

// New creates a new circuit breaker with the given configuration.
func New(config Config) *CircuitBreaker {
	config.applyDefaults()
	cb := &CircuitBreaker{
		config: config,
	}
	cb.state.Store(int32(StateClosed))
	return cb
}

// State returns the current state without triggering any transitions.
func (cb *CircuitBreaker) State() State {
	return State(cb.state.Load())
}

// CheckState returns the current state and may trigger state transitions.
// Use this when you need to check if requests should be allowed.
func (cb *CircuitBreaker) CheckState() State {
	state := State(cb.state.Load())

	if state == StateOpen {
		// Check if we should transition to half-open.
		// Guard against a zero timestamp (no failure recorded yet) so we don't
		// treat the Unix epoch as the last failure and transition immediately.
		lastFailure := cb.lastFailure.Load()
		if lastFailure != 0 && time.Now().UnixNano()-lastFailure >= int64(cb.config.OpenTimeout) {
			// Clear the half-open counters BEFORE half-open becomes visible:
			// clearing after a CAS would erase reservations and successes
			// recorded by requests that observe the new state in between.
			// The mutex keeps a second (stale) transition attempt from
			// re-clearing counters that live probes are already using; while
			// the state is still Open no request touches these counters, so
			// clearing here is race-free.
			cb.transitionMu.Lock()
			// Re-read lastFailure under the lock: a failure recorded after
			// the check above (e.g. from a request admitted before the
			// circuit opened) must restart the grace period — transitioning
			// off the stale timestamp would probe the endpoint early.
			lastFailure = cb.lastFailure.Load()
			if State(cb.state.Load()) == StateOpen &&
				lastFailure != 0 && time.Now().UnixNano()-lastFailure >= int64(cb.config.OpenTimeout) {
				cb.successes.Store(0)
				cb.requests.Store(0)
				// CAS, not Store: a concurrent Reset may have just published
				// Closed, and overwriting it with HalfOpen would silently
				// undo the reset.
				if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
					cb.transitionMu.Unlock()
					cb.notifyCallbacks(StateOpen, StateHalfOpen)
					return StateHalfOpen
				}
			}
			cb.transitionMu.Unlock()
		}
	}

	return State(cb.state.Load())
}

// IsAllowed returns true if a request should be allowed through.
// This is a convenience method that combines CheckState with half-open request limiting.
func (cb *CircuitBreaker) IsAllowed() bool {
	allowed, _ := cb.Allow()
	return allowed
}

// Allow reports whether a request may proceed and whether the admission
// reserved a bounded half-open probe slot. Closed-state admissions reserve
// nothing, so callers whose operation may outlive a later open -> half-open
// transition (for example a WATCH transaction) must consult reserved before
// calling ReleaseHalfOpen — an unconditional release would free a slot a
// real recovery probe is holding.
func (cb *CircuitBreaker) Allow() (allowed, reserved bool) {
	state := cb.CheckState()

	switch state {
	case StateClosed:
		return true, false
	case StateOpen:
		return false, false
	case StateHalfOpen:
		// Limit requests in half-open state
		requests := cb.requests.Add(1)
		if int(requests) > cb.config.MaxHalfOpenRequests {
			cb.requests.Add(-1) // Revert
			return false, false
		}
		// Re-check after reserving: a probe failure may have re-opened the
		// circuit in between (zeroing the counter), and admitting here would
		// both send a request to the endpoint that just failed its recovery
		// probe and leave a phantom reservation behind.
		if State(cb.state.Load()) != StateHalfOpen {
			if cb.requests.Add(-1) < 0 {
				cb.requests.Store(0)
			}
			return false, false
		}
		return true, true
	default:
		return false, false
	}
}

// ReleaseHalfOpen returns a half-open request slot previously reserved by a
// successful IsAllowed call when the operation produced neither a recordable
// success nor failure (for example, it was aborted for an unrelated reason).
// Without this, a reserved-but-never-completed probe could permanently starve
// half-open recovery once MaxHalfOpenRequests slots are exhausted. It only has
// an effect while the breaker is half-open.
func (cb *CircuitBreaker) ReleaseHalfOpen() {
	if State(cb.state.Load()) != StateHalfOpen {
		return
	}
	if cb.requests.Add(-1) < 0 {
		cb.requests.Store(0)
	}
}

// RecordSuccess records a successful operation that was admitted through
// IsAllowed. In half-open state the completed probe's admission slot is
// released when the circuit does not close.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.recordSuccess(true)
}

// RecordExternalSuccess records a successful operation that was NOT admitted
// through IsAllowed (e.g. an out-of-band health check). It counts toward
// closing a half-open circuit but never releases an admission slot it did
// not hold — releasing one would let more than MaxHalfOpenRequests requests
// reach a recovering service.
func (cb *CircuitBreaker) RecordExternalSuccess() {
	cb.recordSuccess(false)
}

func (cb *CircuitBreaker) recordSuccess(heldSlot bool) {
	state := State(cb.state.Load())

	switch state {
	case StateHalfOpen:
		successes := cb.successes.Add(1)
		// Re-check state after increment - another goroutine may have changed it
		if State(cb.state.Load()) != StateHalfOpen {
			return
		}
		if int(successes) >= cb.config.SuccessThreshold {
			// Clear the failure counter BEFORE Closed becomes visible: it
			// still holds the count that opened the circuit, and a failure
			// recorded between the state swap and a later reset would
			// immediately re-open the circuit off that stale count.
			cb.failures.Store(0)
			if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateClosed)) {
				// Notify callbacks before resetting the half-open counters so
				// they observe the success count that triggered the transition.
				cb.notifyCallbacks(StateHalfOpen, StateClosed)
				cb.successes.Store(0)
				cb.requests.Store(0)
			}
			return
		}
		if heldSlot {
			// The probe completed but the circuit is still half-open: give
			// its admission slot back, so MaxHalfOpenRequests bounds
			// CONCURRENT probes rather than a lifetime budget. Without this,
			// a MaxHalfOpenRequests lower than SuccessThreshold could never
			// accumulate enough successes to close the circuit.
			cb.ReleaseHalfOpen()
		}
	case StateClosed:
		// Reset failure count on success
		cb.failures.Store(0)
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.lastFailure.Store(time.Now().UnixNano())
	// The loop re-settles when a half-open transition is lost: a closing
	// success can win the race between the state load and the CAS below, and
	// dropping the failure there would leave the circuit closed with a zero
	// failure count right after a recovery probe failed. Each retry observes
	// the freshly published state, so the failure lands exactly once.
	for {
		state := State(cb.state.Load())

		switch state {
		case StateClosed:
			failures := cb.failures.Add(1)
			if int(failures) >= cb.config.FailureThreshold {
				if cb.state.CompareAndSwap(int32(StateClosed), int32(StateOpen)) {
					// A Reset that completed between the timestamp store above and
					// this CAS wiped lastFailure; repair it (CAS so a concurrent
					// newer failure's timestamp is kept), or the zero-timestamp
					// guard in CheckState would wedge the circuit open. A Reset
					// that wipes after this repair also publishes Closed after,
					// so the circuit does not stay Open with a zero timestamp.
					cb.lastFailure.CompareAndSwap(0, time.Now().UnixNano())
					// Notify callbacks before clearing the half-open counters so
					// observers see the failure count that triggered the
					// transition, matching the half-open -> closed/open paths.
					cb.notifyCallbacks(StateClosed, StateOpen)
					// successes and requests should already be 0 in Closed (they
					// are only incremented while half-open, and every half-open
					// exit zeroes them). Reset defensively so the invariant
					// "successes/requests are clean on entry to Open" is upheld
					// consistently across all transitions into Open, even if a
					// future change starts touching those counters in Closed.
					cb.successes.Store(0)
					cb.requests.Store(0)
				}
			}
			return
		case StateHalfOpen:
			// Any failure in half-open state opens the circuit.
			if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateOpen)) {
				// Same timestamp repair as the closed -> open transition above.
				cb.lastFailure.CompareAndSwap(0, time.Now().UnixNano())
				// Notify callbacks before resetting counters so they observe the
				// counts that triggered the transition, matching the half-open ->
				// closed path in RecordSuccess.
				cb.notifyCallbacks(StateHalfOpen, StateOpen)
				cb.successes.Store(0)
				cb.requests.Store(0)
				return
			}
			// Lost the transition race — typically to the closing success of
			// another probe. Re-read and settle in the new state instead of
			// dropping the failure.
			continue
		default:
			// Open: the timestamp store above already restarted the grace
			// period; nothing else to record.
			return
		}
	}
}

// OnStateChange registers a callback to be called when the state changes.
func (cb *CircuitBreaker) OnStateChange(callback StateChangeCallback) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.callbacks = append(cb.callbacks, callback)
}

// notifyCallbacks notifies all registered callbacks of a state change.
func (cb *CircuitBreaker) notifyCallbacks(oldState, newState State) {
	cb.mu.RLock()
	callbacks := make([]StateChangeCallback, len(cb.callbacks))
	copy(callbacks, cb.callbacks)
	cb.mu.RUnlock()

	for _, callback := range callbacks {
		callback(oldState, newState)
	}
}

// Reset resets the circuit breaker to closed state.
// If the circuit was not already closed, callbacks are notified.
func (cb *CircuitBreaker) Reset() {
	// Clear the counters BEFORE Closed becomes visible: a failure recorded
	// right after the swap must count against a fresh counter — off the
	// stale one it could immediately re-open the circuit, and the
	// lastFailure wipe below would then wedge it open past the
	// zero-timestamp guard in CheckState.
	cb.failures.Store(0)
	cb.successes.Store(0)
	cb.requests.Store(0)
	cb.lastFailure.Store(0)
	oldState := State(cb.state.Swap(int32(StateClosed)))
	if oldState != StateClosed {
		cb.notifyCallbacks(oldState, StateClosed)
	}
}

// Stats returns current statistics for monitoring.
type Stats struct {
	State           State
	Failures        int32
	Successes       int32
	Requests        int32
	LastFailureTime time.Time
}

// Stats returns current statistics.
func (cb *CircuitBreaker) Stats() Stats {
	lastFailure := cb.lastFailure.Load()
	var lastFailureTime time.Time
	if lastFailure > 0 {
		lastFailureTime = time.Unix(0, lastFailure)
	}

	return Stats{
		State:           cb.State(),
		Failures:        cb.failures.Load(),
		Successes:       cb.successes.Load(),
		Requests:        cb.requests.Load(),
		LastFailureTime: lastFailureTime,
	}
}

// Execute runs the given function with circuit breaker protection.
// Returns ErrCircuitOpen if the circuit is open and not ready for testing.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	allowed, reserved := cb.Allow()
	if !allowed {
		return ErrCircuitOpen
	}

	err := fn()
	if err != nil {
		cb.RecordFailure()
		return err
	}

	if reserved {
		cb.RecordSuccess()
	} else {
		// Admitted while closed: no slot was reserved, so the success must
		// not release one — fn can outlive a later open -> half-open
		// transition, and RecordSuccess would free a slot a real recovery
		// probe is holding.
		cb.RecordExternalSuccess()
	}
	return nil
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = &CircuitOpenError{}

// CircuitOpenError indicates the circuit breaker is open.
type CircuitOpenError struct{}

func (e *CircuitOpenError) Error() string {
	return "circuit breaker is open"
}
