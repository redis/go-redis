package hitless

import (
	"errors"
	"fmt"
)

// Configuration errors
var (
	ErrInvalidRelaxedTimeout             = errors.New("hitless: relaxed timeout must be greater than 0")
	ErrInvalidHandoffTimeout             = errors.New("hitless: handoff timeout must be greater than 0")
	ErrInvalidHandoffWorkers             = errors.New("hitless: MinWorkers must be greater than 0")
	ErrInvalidWorkerRange                = errors.New("hitless: MaxWorkers must be greater than or equal to MinWorkers")
	ErrInvalidHandoffQueueSize           = errors.New("hitless: handoff queue size must be greater than 0")
	ErrInvalidPostHandoffRelaxedDuration = errors.New("hitless: post-handoff relaxed duration must be greater than or equal to 0")
	ErrInvalidLogLevel                   = errors.New("hitless: log level must be between 0 and 3")
	ErrInvalidEndpointType               = errors.New("hitless: invalid endpoint type")
)

// Integration errors
var (
	ErrInvalidClient = errors.New("hitless: invalid client type")
)

// Handoff errors
var (
	ErrHandoffInProgress   = errors.New("hitless: handoff already in progress")
	ErrNoHandoffInProgress = errors.New("hitless: no handoff in progress")
	ErrConnectionFailed    = errors.New("hitless: failed to establish new connection")
)

// Dead error variables removed - unused in simplified architecture

// Notification errors
var (
	ErrInvalidNotification = errors.New("hitless: invalid notification format")
)

// Dead error variables removed - unused in simplified architecture

// HandoffError represents an error that occurred during connection handoff.
type HandoffError struct {
	Operation string
	Endpoint  string
	Cause     error
}

func (e *HandoffError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("hitless: handoff %s failed for endpoint %s: %v", e.Operation, e.Endpoint, e.Cause)
	}
	return fmt.Sprintf("hitless: handoff %s failed for endpoint %s", e.Operation, e.Endpoint)
}

func (e *HandoffError) Unwrap() error {
	return e.Cause
}

// NewHandoffError creates a new HandoffError.
func NewHandoffError(operation, endpoint string, cause error) *HandoffError {
	return &HandoffError{
		Operation: operation,
		Endpoint:  endpoint,
		Cause:     cause,
	}
}
