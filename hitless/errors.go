package hitless

import (
	"errors"
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
	ErrInvalidHandoffState   = errors.New("hitless: Conn is in invalid state for handoff")
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
