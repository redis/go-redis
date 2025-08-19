package hitless

import (
	"errors"
	"fmt"
)

// Configuration errors
var (
	ErrInvalidRelaxedTimeout             = errors.New("hitless: relaxed timeout must be greater than 0")
	ErrInvalidHandoffTimeout             = errors.New("hitless: handoff timeout must be greater than 0")
	ErrInvalidHandoffWorkers             = errors.New("hitless: MaxWorkers must be greater than or equal to 0")
	ErrInvalidHandoffQueueSize           = errors.New("hitless: handoff queue size must be greater than 0")
	ErrInvalidPostHandoffRelaxedDuration = errors.New("hitless: post-handoff relaxed duration must be greater than or equal to 0")
	ErrInvalidLogLevel                   = errors.New("hitless: log level must be between 0 and 3")
	ErrInvalidEndpointType               = errors.New("hitless: invalid endpoint type")
	ErrInvalidMaintNotifications         = errors.New("hitless: invalid maintenance notifications setting (must be 'disabled', 'enabled', or 'auto')")
	ErrMaxHandoffRetriesReached          = errors.New("hitless: max handoff retries reached")

	// Configuration validation errors
	ErrInvalidHandoffRetries = errors.New("hitless: MaxHandoffRetries must be between 1 and 10")
	ErrInvalidConnectionValidationTimeout     = errors.New("hitless: ConnectionValidationTimeout must be greater than 0 and less than 30 seconds")
	ErrInvalidConnectionHealthCheckInterval   = errors.New("hitless: ConnectionHealthCheckInterval must be between 0 and 1 hour")
	ErrInvalidOperationCleanupInterval        = errors.New("hitless: OperationCleanupInterval must be greater than 0 and less than 1 hour")
	ErrInvalidMaxActiveOperations             = errors.New("hitless: MaxActiveOperations must be between 100 and 100000")
	ErrInvalidNotificationBufferSize          = errors.New("hitless: NotificationBufferSize must be between 10 and 10000")
	ErrInvalidNotificationTimeout             = errors.New("hitless: NotificationTimeout must be greater than 0 and less than 30 seconds")
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
	ErrHandoffQueueFull    = errors.New("hitless: handoff queue is full, cannot queue new handoff requests - consider increasing HandoffQueueSize or MaxWorkers in configuration")
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
