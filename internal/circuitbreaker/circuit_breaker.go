// Package circuitbreaker provides a circuit breaker implementation for fault tolerance.
package circuitbreaker

import (
	"sync"
	"sync/atomic"
	"time"

	cbq "github.com/redis/go-redis/v9/internal/callbackqueue"
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

// StateChangeCallback is called when the circuit breaker state changes. stats
// is a snapshot taken at the transition, so it reflects the counters that
// triggered the change even though the callback runs asynchronously (see
// cbq): read it instead of calling Stats(), which by delivery time may
// show a later state.
type StateChangeCallback func(oldState, newState State, stats Stats)

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	config Config

	state       atomic.Int32
	failures    atomic.Int32
	successes   atomic.Int32
	requests    atomic.Int32 // Request count in half-open state
	lastFailure atomic.Int64 // Unix nano timestamp
	// generation is bumped on every Open->HalfOpen transition, so a reservation
	// (see AllowReserve) taken in one half-open episode can be recognized as
	// stale if it tries to settle after the circuit has cycled through Open and
	// back to a new half-open episode.
	generation atomic.Uint64

	// transitionMu serializes ALL state transitions and the enqueue of their
	// notifications, so callbacks are delivered in transition (CAS) order. It
	// is held across the counter-clear + CAS + enqueue only, never across the
	// callback itself.
	transitionMu sync.Mutex

	mu        sync.RWMutex
	callbacks []StateChangeCallback

	// cbq delivers state-change callbacks on a single goroutine in FIFO
	// order. Enqueue happens under transitionMu, so the queue order matches the
	// CAS order; the callbacks run OUTSIDE transitionMu, so two concurrent
	// transitions cannot report out of order and a callback may safely re-enter
	// the breaker (RecordFailure/CheckState/Reset) without deadlocking.
	cbq cbq.CallbackQueue
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
		// Guard against a zero timestamp (no failure recorded yet) so we don't
		// treat the Unix epoch as the last failure and transition immediately.
		lastFailure := cb.lastFailure.Load()
		if lastFailure != 0 && time.Now().UnixNano()-lastFailure >= int64(cb.config.OpenTimeout) {
			cb.transitionMu.Lock()
			cb.maybeHalfOpenLocked()
			cb.transitionMu.Unlock()
		}
	}
	return State(cb.state.Load())
}

// maybeHalfOpenLocked performs the Open -> HalfOpen transition when the grace
// period has elapsed. transitionMu MUST be held.
//
// Serializing this transition under the same lock that the half-open
// reservation operations hold (AllowReserve, RecordSuccessFor, ReleaseFor,
// RecordFailureFor) is what makes the generation and the half-open counters
// move atomically with reservation admission and settlement: a reservation
// cannot observe a torn state where the generation has advanced but the counter
// has not (or vice versa), which is the whole class of TOCTOU races the
// lock-free version kept producing.
func (cb *CircuitBreaker) maybeHalfOpenLocked() {
	// Re-read lastFailure under the lock: a failure recorded after the caller's
	// check (e.g. from a request admitted before the circuit opened) must
	// restart the grace period — transitioning off the stale timestamp would
	// probe the endpoint early.
	lastFailure := cb.lastFailure.Load()
	if State(cb.state.Load()) != StateOpen ||
		lastFailure == 0 || time.Now().UnixNano()-lastFailure < int64(cb.config.OpenTimeout) {
		return
	}
	// Clear the half-open counters and advance the generation BEFORE the CAS
	// publishes half-open, so a reservation that observes the new state reads a
	// clean count and the new generation.
	cb.successes.Store(0)
	cb.requests.Store(0)
	cb.generation.Add(1)
	// CAS, not Store: a concurrent Reset may have just published Closed, and
	// overwriting it with HalfOpen would silently undo the reset.
	if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
		// Snapshot + enqueue under the lock so callback order matches CAS order;
		// the callback itself runs on the cbq goroutine.
		stats := cb.Stats()
		cb.cbq.Dispatch(func() { cb.notifyCallbacks(StateOpen, StateHalfOpen, stats) })
	}
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

// Reservation identifies a single admission returned by AllowReserve. It carries
// the half-open episode (generation) the admission belongs to and a once-only
// settle guard, so that:
//   - a reservation taken in one half-open episode cannot settle against a later
//     one (the admitted request outlived an open -> half-open cycle), and
//   - a reservation shared by several outcomes (e.g. a pipeline batch) settles
//     the half-open slot exactly once, however many outcomes report success.
//
// The zero Reservation (held == false) is what a closed-state admission returns:
// it holds no slot, and RecordSuccessFor treats it as an external success.
type Reservation struct {
	gen     uint64
	held    bool
	settled *atomic.Bool
}

// AllowReserve is Allow with an identity-bearing reservation. Prefer it over
// Allow when the admitted operation may outlive a later open -> half-open
// transition, or when several outcomes share one admission: the returned
// Reservation lets RecordSuccessFor / ReleaseFor settle the half-open slot
// exactly once and only within the episode the slot was reserved in.
func (cb *CircuitBreaker) AllowReserve() (allowed bool, r Reservation) {
	state := State(cb.state.Load())
	if state == StateClosed {
		return true, Reservation{}
	}
	// A plain Open whose grace period has not elapsed is the common rejected
	// case — keep it lock-free. Only the half-open lifecycle takes the lock.
	if state == StateOpen {
		lastFailure := cb.lastFailure.Load()
		if lastFailure == 0 || time.Now().UnixNano()-lastFailure < int64(cb.config.OpenTimeout) {
			return false, Reservation{}
		}
	}
	// HalfOpen, or Open with the grace elapsed: under transitionMu, run any
	// pending Open -> HalfOpen transition and admit atomically. Because the
	// generation, the counter and the state cannot move while the lock is held,
	// there is no admit-then-recheck window: the reservation is bound to exactly
	// the episode it was admitted into (no stale generation, no phantom slot).
	cb.transitionMu.Lock()
	cb.maybeHalfOpenLocked()
	if State(cb.state.Load()) != StateHalfOpen {
		st := State(cb.state.Load())
		cb.transitionMu.Unlock()
		return st == StateClosed, Reservation{}
	}
	if requests := cb.requests.Add(1); int(requests) > cb.config.MaxHalfOpenRequests {
		cb.requests.Add(-1)
		cb.transitionMu.Unlock()
		return false, Reservation{}
	}
	gen := cb.generation.Load()
	cb.transitionMu.Unlock()
	return true, Reservation{gen: gen, held: true, settled: new(atomic.Bool)}
}

// RecordSuccessFor settles a successful outcome for a reservation from
// AllowReserve. A half-open reservation releases (or, at SuccessThreshold,
// closes on) its slot at most once, and only while it is still the current
// half-open episode; a stale reservation — the circuit cycled open -> half-open
// since it was taken — records nothing. A closed-state reservation (held ==
// false) clears the failure count while the circuit is still closed and
// records nothing once it has moved on: its successes predate the failures
// that opened the circuit and are no evidence of recovery, and counting them
// would let one in-flight batch close a half-open episode it never probed.
// Out-of-band evidence goes through RecordExternalSuccess, which does count.
func (cb *CircuitBreaker) RecordSuccessFor(r Reservation) {
	if !r.held {
		if State(cb.state.Load()) == StateClosed {
			cb.failures.Store(0)
		}
		return
	}
	if r.settled.Swap(true) {
		return // already settled by an earlier outcome sharing this reservation
	}
	// Under transitionMu the generation check and the settlement are atomic: the
	// episode cannot cycle between them, so this settles exactly the episode the
	// reservation was admitted into (or nothing, if it is already stale).
	cb.transitionMu.Lock()
	if r.gen == cb.generation.Load() {
		cb.recordSuccessHalfOpenLocked(true)
	}
	cb.transitionMu.Unlock()
}

// ReleaseFor returns a half-open reservation's slot when the outcome was neither
// a recordable success nor failure. Like RecordSuccessFor it settles at most
// once and only within the reservation's own half-open episode; a closed-state
// or already-settled reservation is a no-op.
func (cb *CircuitBreaker) ReleaseFor(r Reservation) {
	if !r.held || r.settled.Swap(true) {
		return
	}
	cb.transitionMu.Lock()
	if r.gen == cb.generation.Load() {
		cb.releaseHalfOpenLocked()
	}
	cb.transitionMu.Unlock()
}

// RecordFailureFor records a failure for a reservation from AllowReserve. A
// half-open reservation records only while it is still the CURRENT half-open
// episode: a failure from an admission that outlived an open -> half-open cycle
// must not re-open (and abort the recovery of) the NEW episode — the symmetric
// guard to RecordSuccessFor. A closed-state reservation (held == false) records
// only while the circuit is still closed: it was admitted in the closed state,
// so if the circuit has since moved to open or half-open its failure predates
// that episode and must not re-open it (a stale probe failure aborting a
// recovery it never joined) — symmetric to RecordSuccessFor's held==false
// branch. Unlike success/release this does not consume the once-only settle
// flag — a failure opens the circuit rather than freeing a slot, and the batch
// path records one failure per command.
func (cb *CircuitBreaker) RecordFailureFor(r Reservation) {
	if !r.held {
		// Reading Closed is race-free without the lock: leaving Closed needs a
		// failure to open the circuit first, and Closed -> half-open needs a
		// full grace period, so the state cannot flip to half-open between this
		// read and RecordFailure. (A closed-admission failure that arrives while
		// the circuit is already open does not restart the grace period — it is
		// stale evidence and would only delay recovery.)
		if State(cb.state.Load()) != StateClosed {
			return
		}
		cb.RecordFailure()
		return
	}
	// Same-episode failures re-open under the lock, atomically with the
	// generation check; a stale reservation records nothing.
	cb.transitionMu.Lock()
	if r.gen == cb.generation.Load() {
		cb.lastFailure.Store(time.Now().UnixNano())
		cb.recordFailureHalfOpenLocked()
	}
	cb.transitionMu.Unlock()
}

// ReleaseHalfOpen returns a half-open request slot previously reserved by a
// successful IsAllowed call when the operation produced neither a recordable
// success nor failure (for example, it was aborted for an unrelated reason).
// Without this, a reserved-but-never-completed probe could permanently starve
// half-open recovery once MaxHalfOpenRequests slots are exhausted. It only has
// an effect while the breaker is half-open.
func (cb *CircuitBreaker) ReleaseHalfOpen() {
	cb.transitionMu.Lock()
	cb.releaseHalfOpenLocked()
	cb.transitionMu.Unlock()
}

// releaseHalfOpenLocked frees a half-open slot. transitionMu MUST be held.
func (cb *CircuitBreaker) releaseHalfOpenLocked() {
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
	// Closed is the hot path (every successful command clears the failure
	// count): keep it lock-free. The half-open lifecycle is serialized under
	// transitionMu, the same lock the reservation operations hold.
	if State(cb.state.Load()) == StateClosed {
		cb.failures.Store(0)
		return
	}
	cb.transitionMu.Lock()
	cb.recordSuccessHalfOpenLocked(heldSlot)
	cb.transitionMu.Unlock()
}

// recordSuccessHalfOpenLocked applies a success with transitionMu held. In
// half-open it counts toward SuccessThreshold (closing the circuit) and, for a
// slot-holding admission that does not close it, releases the slot. In closed
// it clears the failure count; Open is a no-op.
func (cb *CircuitBreaker) recordSuccessHalfOpenLocked(heldSlot bool) {
	switch State(cb.state.Load()) {
	case StateHalfOpen:
		successes := cb.successes.Add(1)
		if int(successes) >= cb.config.SuccessThreshold {
			// Clear the failure counter BEFORE Closed becomes visible: it still
			// holds the count that opened the circuit.
			cb.failures.Store(0)
			if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateClosed)) {
				// Snapshot BEFORE clearing the half-open counters so the callback
				// observes the success count that triggered the close.
				stats := cb.Stats()
				cb.cbq.Dispatch(func() { cb.notifyCallbacks(StateHalfOpen, StateClosed, stats) })
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
			cb.releaseHalfOpenLocked()
		}
	case StateClosed:
		// Reset failure count on success
		cb.failures.Store(0)
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.lastFailure.Store(time.Now().UnixNano())
	switch State(cb.state.Load()) {
	case StateClosed:
		// Hot path: count lock-free, take the lock only to open at the threshold.
		if int(cb.failures.Add(1)) >= cb.config.FailureThreshold {
			cb.transitionMu.Lock()
			// Re-read the count under the lock: a Reset (SetActiveDatabase) may
			// have zeroed it since the lock-free Add crossed the threshold, and
			// opening off that stale reading would immediately undo an operator
			// re-selection. openFromClosedLocked's CAS alone cannot tell the
			// difference — Reset already swapped back to Closed.
			if int(cb.failures.Load()) >= cb.config.FailureThreshold {
				cb.openFromClosedLocked()
			}
			cb.transitionMu.Unlock()
		}
	case StateHalfOpen:
		cb.transitionMu.Lock()
		cb.recordFailureHalfOpenLocked()
		cb.transitionMu.Unlock()
	}
	// Open: the timestamp store above already restarted the grace period.
	//
	// The old lost-CAS retry loop is gone: half-open closes (recordSuccessHalfOpenLocked)
	// and opens (recordFailureHalfOpenLocked) now both run under transitionMu, so
	// they are serialized and no CAS can be lost to a concurrent transition.
}

// openFromClosedLocked performs the Closed -> Open transition. transitionMu MUST
// be held.
func (cb *CircuitBreaker) openFromClosedLocked() {
	if cb.state.CompareAndSwap(int32(StateClosed), int32(StateOpen)) {
		// A Reset that completed between the timestamp store in the caller and
		// this CAS wiped lastFailure; repair it (CAS so a concurrent newer
		// failure's timestamp is kept), or the zero-timestamp guard in
		// CheckState would wedge the circuit open.
		cb.lastFailure.CompareAndSwap(0, time.Now().UnixNano())
		stats := cb.Stats()
		cb.cbq.Dispatch(func() { cb.notifyCallbacks(StateClosed, StateOpen, stats) })
		// successes/requests are 0 in Closed already; reset defensively so the
		// invariant "clean on entry to Open" holds across all transitions.
		cb.successes.Store(0)
		cb.requests.Store(0)
	}
}

// recordFailureHalfOpenLocked opens the circuit from half-open (any failure in
// half-open re-opens). transitionMu MUST be held. If the episode already closed
// (a settling success won under the same lock before this call), the failure is
// treated as a fresh closed-state failure.
func (cb *CircuitBreaker) recordFailureHalfOpenLocked() {
	switch State(cb.state.Load()) {
	case StateHalfOpen:
		if cb.state.CompareAndSwap(int32(StateHalfOpen), int32(StateOpen)) {
			cb.lastFailure.CompareAndSwap(0, time.Now().UnixNano())
			stats := cb.Stats()
			cb.cbq.Dispatch(func() { cb.notifyCallbacks(StateHalfOpen, StateOpen, stats) })
			cb.successes.Store(0)
			cb.requests.Store(0)
		}
	case StateClosed:
		if int(cb.failures.Add(1)) >= cb.config.FailureThreshold {
			cb.openFromClosedLocked()
		}
	}
}

// ForceOpen transitions the breaker straight to Open, regardless of the current
// failure count. For callers that have already determined a member is down (for
// example a failed startup or health probe) and want to open the circuit
// without synthesizing FailureThreshold individual RecordFailure calls.
func (cb *CircuitBreaker) ForceOpen() {
	cb.transitionMu.Lock()
	// Store lastFailure UNDER the lock, before the swap: a concurrent Reset
	// (also under the lock) must not be able to clear it between the store and
	// the swap. Otherwise the swap could publish Open with lastFailure == 0,
	// which CheckState refuses to advance to half-open, wedging the circuit open
	// forever past OpenTimeout.
	cb.lastFailure.Store(time.Now().UnixNano())
	// Swap, capturing the old state for the callback; skip the notify (and the
	// counter clear) if it was already Open, so a redundant ForceOpen is a no-op.
	old := State(cb.state.Swap(int32(StateOpen)))
	if old != StateOpen {
		// successes/requests are meaningless in Open; clear them so the next
		// Open -> HalfOpen episode starts clean, matching the other transitions.
		cb.successes.Store(0)
		cb.requests.Store(0)
		stats := cb.Stats()
		cb.cbq.Dispatch(func() { cb.notifyCallbacks(old, StateOpen, stats) })
	}
	cb.transitionMu.Unlock()
}

// OnStateChange registers a callback to be called when the state changes.
func (cb *CircuitBreaker) OnStateChange(callback StateChangeCallback) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.callbacks = append(cb.callbacks, callback)
}

// notifyCallbacks notifies all registered callbacks of a state change. It runs
// on the cbq goroutine (never under transitionMu), so a callback may
// re-enter the breaker without deadlocking.
func (cb *CircuitBreaker) notifyCallbacks(oldState, newState State, stats Stats) {
	cb.mu.RLock()
	callbacks := make([]StateChangeCallback, len(cb.callbacks))
	copy(callbacks, cb.callbacks)
	cb.mu.RUnlock()

	for _, callback := range callbacks {
		// Recover per callback: the queue only recovers around this whole
		// batch, so a panic here would otherwise unwind past the remaining
		// callbacks and skip them for this transition (and every future one,
		// since a deterministic panic recurs on the same callback each time).
		cbq.RunSafely(func() { callback(oldState, newState, stats) })
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
	// Hold transitionMu across the clear + swap + enqueue so Reset serializes
	// with the other transitions and its notification keeps CAS order.
	cb.transitionMu.Lock()
	cb.failures.Store(0)
	cb.successes.Store(0)
	cb.requests.Store(0)
	cb.lastFailure.Store(0)
	// Advance the generation so any in-flight half-open reservation from the
	// episode being reset is now stale: without this, a reservation handed out
	// before the Reset still matches the generation and RecordFailureFor would
	// count its failure against the freshly closed circuit, re-opening the
	// operator-selected member immediately (FailureThreshold == 1).
	cb.generation.Add(1)
	oldState := State(cb.state.Swap(int32(StateClosed)))
	if oldState != StateClosed {
		stats := cb.Stats()
		cb.cbq.Dispatch(func() { cb.notifyCallbacks(oldState, StateClosed, stats) })
	}
	cb.transitionMu.Unlock()
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
