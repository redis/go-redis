package hitless

import (
	"errors"
	"fmt"
	"time"
)

// Configuration errors
var (
	ErrInvalidRelaxedTimeout             = errors.New("hitless: relaxed timeout must be greater than 0")
	ErrInvalidHandoffTimeout             = errors.New("hitless: handoff timeout must be greater than 0")
	ErrInvalidHandoffWorkers             = errors.New("hitless: MaxWorkers must be greater than or equal to 0")
	ErrInvalidHandoffQueueSize           = errors.New("hitless: handoff queue size must be greater than 0")
	ErrInvalidPostHandoffRelaxedDuration = errors.New("hitless: post-handoff relaxed duration must be greater than or equal to 0")
	ErrInvalidLogLevel                   = errors.New("hitless: log level must be LogLevelError (0), LogLevelWarn (1), LogLevelInfo (2), or LogLevelDebug (3)")
	ErrInvalidEndpointType               = errors.New("hitless: invalid endpoint type")
	ErrInvalidMaintNotifications         = errors.New("hitless: invalid maintenance notifications setting (must be 'disabled', 'enabled', or 'auto')")
	ErrMaxHandoffRetriesReached          = errors.New("hitless: max handoff retries reached")

	// Configuration validation errors
	ErrInvalidHandoffRetries = errors.New("hitless: MaxHandoffRetries must be between 1 and 10")
)

// Integration errors
var (
	ErrInvalidClient = errors.New("hitless: invalid client type")
)

// Handoff errors
var (
	ErrHandoffQueueFull = errors.New("hitless: handoff queue is full, cannot queue new handoff requests - consider increasing HandoffQueueSize or MaxWorkers in configuration")
)

// Notification errors
var (
	ErrInvalidNotification = errors.New("hitless: invalid notification format")
)

// connection handoff errors
var (
	// ErrConnectionMarkedForHandoff is returned when a connection is marked for handoff
	// and should not be used until the handoff is complete
	ErrConnectionMarkedForHandoff = errors.New("hitless: connection marked for handoff")
	// ErrConnectionInvalidHandoffState is returned when a connection is in an invalid state for handoff
	ErrConnectionInvalidHandoffState = errors.New("hitless: connection is in invalid state for handoff")
)

// general errors
var (
	ErrShutdown = errors.New("hitless: shutdown")
)

// circuit breaker errors
var (
	ErrCircuitBreakerOpen = errors.New("hitless: circuit breaker is open, failing fast")
)

// CircuitBreakerError provides detailed context for circuit breaker failures
type CircuitBreakerError struct {
	Endpoint     string
	State        string
	Failures     int64
	LastFailure  time.Time
	NextAttempt  time.Time
	Message      string
}

func (e *CircuitBreakerError) Error() string {
	if e.NextAttempt.IsZero() {
		return fmt.Sprintf("hitless: circuit breaker %s for %s (failures: %d, last: %v): %s",
			e.State, e.Endpoint, e.Failures, e.LastFailure, e.Message)
	}
	return fmt.Sprintf("hitless: circuit breaker %s for %s (failures: %d, last: %v, next attempt: %v): %s",
		e.State, e.Endpoint, e.Failures, e.LastFailure, e.NextAttempt, e.Message)
}

// HandoffError provides detailed context for connection handoff failures
type HandoffError struct {
	ConnectionID   uint64
	SourceEndpoint string
	TargetEndpoint string
	Attempt        int
	MaxAttempts    int
	Duration       time.Duration
	FinalError     error
	Message        string
}

func (e *HandoffError) Error() string {
	return fmt.Sprintf("hitless: handoff failed for conn[%d] %s→%s (attempt %d/%d, duration: %v): %s",
		e.ConnectionID, e.SourceEndpoint, e.TargetEndpoint,
		e.Attempt, e.MaxAttempts, e.Duration, e.Message)
}

func (e *HandoffError) Unwrap() error {
	return e.FinalError
}

// circuit breaker configuration errors
var (
	ErrInvalidCircuitBreakerFailureThreshold = errors.New("hitless: circuit breaker failure threshold must be >= 1")
	ErrInvalidCircuitBreakerResetTimeout     = errors.New("hitless: circuit breaker reset timeout must be >= 0")
	ErrInvalidCircuitBreakerMaxRequests      = errors.New("hitless: circuit breaker max requests must be >= 1")
)
