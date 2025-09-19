package logs

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/redis/go-redis/v9/internal"
)

// appendJSONIfDebug appends JSON data to a message only if the global log level is Debug
func appendJSONIfDebug(message string, data map[string]interface{}) string {
	if internal.LogLevel.DebugOrAbove() {
		jsonData, _ := json.Marshal(data)
		return fmt.Sprintf("%s %s", message, string(jsonData))
	}
	return message
}

const (
	// ========================================
	// CIRCUIT_BREAKER.GO - Circuit breaker management
	// ========================================
	CircuitBreakerTransitioningToHalfOpenMessage = "circuit breaker transitioning to half-open"
	CircuitBreakerOpenedMessage                  = "circuit breaker opened"
	CircuitBreakerReopenedMessage                = "circuit breaker reopened"
	CircuitBreakerClosedMessage                  = "circuit breaker closed"
	CircuitBreakerCleanupMessage                 = "circuit breaker cleanup"
	CircuitBreakerOpenMessage                    = "circuit breaker is open, failing fast"

	// ========================================
	// CONFIG.GO - Configuration and debug
	// ========================================
	DebugLoggingEnabledMessage = "debug logging enabled"
	ConfigDebugMessage         = "config debug"

	// ========================================
	// ERRORS.GO - Error message constants
	// ========================================
	InvalidRelaxedTimeoutErrorMessage                 = "relaxed timeout must be greater than 0"
	InvalidHandoffTimeoutErrorMessage                 = "handoff timeout must be greater than 0"
	InvalidHandoffWorkersErrorMessage                 = "MaxWorkers must be greater than or equal to 0"
	InvalidHandoffQueueSizeErrorMessage               = "handoff queue size must be greater than 0"
	InvalidPostHandoffRelaxedDurationErrorMessage     = "post-handoff relaxed duration must be greater than or equal to 0"
	InvalidEndpointTypeErrorMessage                   = "invalid endpoint type"
	InvalidMaintNotificationsErrorMessage             = "invalid maintenance notifications setting (must be 'disabled', 'enabled', or 'auto')"
	InvalidHandoffRetriesErrorMessage                 = "MaxHandoffRetries must be between 1 and 10"
	InvalidClientErrorMessage                         = "invalid client type"
	InvalidNotificationErrorMessage                   = "invalid notification format"
	MaxHandoffRetriesReachedErrorMessage              = "max handoff retries reached"
	HandoffQueueFullErrorMessage                      = "handoff queue is full, cannot queue new handoff requests - consider increasing HandoffQueueSize or MaxWorkers in configuration"
	InvalidCircuitBreakerFailureThresholdErrorMessage = "circuit breaker failure threshold must be >= 1"
	InvalidCircuitBreakerResetTimeoutErrorMessage     = "circuit breaker reset timeout must be >= 0"
	InvalidCircuitBreakerMaxRequestsErrorMessage      = "circuit breaker max requests must be >= 1"
	ConnectionMarkedForHandoffErrorMessage            = "connection marked for handoff"
	ConnectionInvalidHandoffStateErrorMessage         = "connection is in invalid state for handoff"
	ShutdownErrorMessage                              = "shutdown"
	CircuitBreakerOpenErrorMessage                    = "circuit breaker is open, failing fast"

	// ========================================
	// EXAMPLE_HOOKS.GO - Example metrics hooks
	// ========================================
	MetricsHookProcessingNotificationMessage = "metrics hook processing"
	MetricsHookRecordedErrorMessage          = "metrics hook recorded error"

	// ========================================
	// HANDOFF_WORKER.GO - Connection handoff processing
	// ========================================
	HandoffStartedMessage                            = "handoff started"
	HandoffFailedMessage                             = "handoff failed"
	ConnectionNotMarkedForHandoffMessage             = "is not marked for handoff and has no retries"
	ConnectionNotMarkedForHandoffErrorMessage        = "is not marked for handoff"
	HandoffRetryAttemptMessage                       = "Performing handoff"
	CannotQueueHandoffForRetryMessage                = "can't queue handoff for retry"
	HandoffQueueFullMessage                          = "handoff queue is full"
	FailedToDialNewEndpointMessage                   = "failed to dial new endpoint"
	ApplyingRelaxedTimeoutDueToPostHandoffMessage    = "applying relaxed timeout due to post-handoff"
	SetNetConnAndInitConnTimingMessage               = "SetNetConnAndInitConn took"
	HandoffSuccessMessage                            = "handoff succeeded"
	RemovingConnectionFromPoolMessage                = "removing connection from pool"
	NoPoolProvidedMessageCannotRemoveMessage         = "no pool provided, cannot remove connection, closing it"
	WorkerExitingDueToShutdownMessage                = "worker exiting due to shutdown"
	WorkerExitingDueToShutdownWhileProcessingMessage = "worker exiting due to shutdown while processing request"
	WorkerPanicRecoveredMessage                      = "worker panic recovered"
	WorkerExitingDueToInactivityTimeoutMessage       = "worker exiting due to inactivity timeout"
	ReachedMaxHandoffRetriesMessage                  = "reached max handoff retries"

	// ========================================
	// MANAGER.GO - Moving operation tracking and handler registration
	// ========================================
	DuplicateMovingOperationMessage  = "duplicate MOVING operation ignored"
	TrackingMovingOperationMessage   = "tracking MOVING operation"
	UntrackingMovingOperationMessage = "untracking MOVING operation"
	OperationNotTrackedMessage       = "operation not tracked"
	FailedToRegisterHandlerMessage   = "failed to register handler"

	// ========================================
	// HOOKS.GO - Notification processing hooks
	// ========================================
	ProcessingNotificationMessage          = "processing notification started"
	ProcessingNotificationFailedMessage    = "proccessing notification failed"
	ProcessingNotificationSucceededMessage = "processing notification succeeded"

	// ========================================
	// POOL_HOOK.GO - Pool connection management
	// ========================================
	FailedToQueueHandoffMessage = "failed to queue handoff"
	MarkedForHandoffMessage     = "connection marked for handoff"

	// ========================================
	// PUSH_NOTIFICATION_HANDLER.GO - Push notification validation and processing
	// ========================================
	InvalidNotificationFormatMessage              = "invalid notification format"
	InvalidNotificationTypeFormatMessage          = "invalid notification type format"
	InvalidNotificationMessage                    = "invalid notification"
	InvalidSeqIDInMovingNotificationMessage       = "invalid seqID in MOVING notification"
	InvalidTimeSInMovingNotificationMessage       = "invalid timeS in MOVING notification"
	InvalidNewEndpointInMovingNotificationMessage = "invalid newEndpoint in MOVING notification"
	NoConnectionInHandlerContextMessage           = "no connection in handler context"
	InvalidConnectionTypeInHandlerContextMessage  = "invalid connection type in handler context"
	SchedulingHandoffToCurrentEndpointMessage     = "scheduling handoff to current endpoint"
	RelaxedTimeoutDueToNotificationMessage        = "applying relaxed timeout due to notification"
	UnrelaxedTimeoutMessage                       = "clearing relaxed timeout"
	ManagerNotInitializedMessage                  = "manager not initialized"
	FailedToMarkForHandoffMessage                 = "failed to mark connection for handoff"

	// ========================================
	// UNUSED CONSTANTS - Not currently used in codebase
	// ========================================
	UnrelaxedTimeoutAfterDeadlineMessage      = "clearing relaxed timeout after deadline"
	FailedToMarkQueuedForHandoffMessage       = "failed to mark connection as queued for handoff"
	ConnectionAlreadyMarkedForHandoffMessage  = "already marked for handoff"
	CircuitBreakerHalfOpenMessage             = "circuit breaker is half-open, testing if endpoint recovered"
	FailingDueToCircuitBreakerMessage         = "failing due to circuit breaker"
	ShuttingDownMessage                       = "shutting down"
	ConnectionInInvalidStateForHandoffMessage = "connection is in invalid state for handoff"
)

func HandoffStarted(connID uint64, newEndpoint string) string {
	message := fmt.Sprintf("conn[%d] %s to %s", connID, HandoffStartedMessage, newEndpoint)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":   connID,
		"endpoint": newEndpoint,
	})
}

func HandoffFailed(connID uint64, newEndpoint string, attempt int, maxAttempts int, err error) string {
	message := fmt.Sprintf("conn[%d] %s to %s (attempt %d/%d): %v", connID, HandoffFailedMessage, newEndpoint, attempt, maxAttempts, err)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":      connID,
		"endpoint":    newEndpoint,
		"attempt":     attempt,
		"maxAttempts": maxAttempts,
		"error":       err.Error(),
	})
}

func HandoffSucceeded(connID uint64, newEndpoint string) string {
	message := fmt.Sprintf("conn[%d] %s to %s", connID, HandoffSuccessMessage, newEndpoint)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":   connID,
		"endpoint": newEndpoint,
	})
}

// Timeout-related log functions
func RelaxedTimeoutDueToNotification(connID uint64, notificationType string, timeout interface{}) string {
	message := fmt.Sprintf("conn[%d] %s %s (%v)", connID, RelaxedTimeoutDueToNotificationMessage, notificationType, timeout)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":           connID,
		"notificationType": notificationType,
		"timeout":          fmt.Sprintf("%v", timeout),
	})
}

func UnrelaxedTimeout(connID uint64) string {
	message := fmt.Sprintf("conn[%d] %s", connID, UnrelaxedTimeoutMessage)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID": connID,
	})
}

func UnrelaxedTimeoutAfterDeadline(connID uint64) string {
	message := fmt.Sprintf("conn[%d] %s", connID, UnrelaxedTimeoutAfterDeadlineMessage)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID": connID,
	})
}

// Handoff queue and marking functions
func HandoffQueueFull(queueLen, queueCap int) string {
	data, _ := json.Marshal(map[string]interface{}{
		"queueLen": queueLen,
		"queueCap": queueCap,
	})
	return fmt.Sprintf("%s (%d/%d), cannot queue new handoff requests - consider increasing HandoffQueueSize or MaxWorkers in configuration %s", HandoffQueueFullMessage, queueLen, queueCap, string(data))
}

func FailedToQueueHandoff(connID uint64, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"error":  err.Error(),
	})
	return fmt.Sprintf("conn[%d] %s: %v %s", connID, FailedToQueueHandoffMessage, err, string(data))
}

func FailedToMarkForHandoff(connID uint64, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"error":  err.Error(),
	})
	return fmt.Sprintf("conn[%d] %s: %v %s", connID, FailedToMarkForHandoffMessage, err, string(data))
}

func FailedToMarkQueuedForHandoff(connID uint64, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"error":  err.Error(),
	})
	return fmt.Sprintf("conn[%d] %s: %v %s", connID, FailedToMarkQueuedForHandoffMessage, err, string(data))
}

func ConnectionAlreadyMarkedForHandoff(connID uint64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
	})
	return fmt.Sprintf("conn[%d] %s %s", connID, ConnectionAlreadyMarkedForHandoffMessage, string(data))
}

func FailedToDialNewEndpoint(connID uint64, endpoint string, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":   connID,
		"endpoint": endpoint,
		"error":    err.Error(),
	})
	return fmt.Sprintf("conn[%d] %s %s: %v %s", connID, FailedToDialNewEndpointMessage, endpoint, err, string(data))
}

func ReachedMaxHandoffRetries(connID uint64, endpoint string, maxRetries int) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":     connID,
		"endpoint":   endpoint,
		"maxRetries": maxRetries,
	})
	return fmt.Sprintf("conn[%d] %s to %s (max retries: %d) %s", connID, ReachedMaxHandoffRetriesMessage, endpoint, maxRetries, string(data))
}

// Notification processing functions
func ProcessingNotification(connID uint64, seqID int64, notificationType string, notification interface{}) string {
	message := fmt.Sprintf("conn[%d] seqID[%d] %s %s: %v", connID, seqID, ProcessingNotificationMessage, notificationType, notification)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":           connID,
		"seqID":            seqID,
		"notificationType": notificationType,
		"notification":     fmt.Sprintf("%v", notification),
	})
}

func ProcessingNotificationFailed(connID uint64, notificationType string, err error, notification interface{}) string {
	message := fmt.Sprintf("conn[%d] %s %s: %v - %v", connID, ProcessingNotificationFailedMessage, notificationType, err, notification)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":           connID,
		"notificationType": notificationType,
		"error":            err.Error(),
		"notification":     fmt.Sprintf("%v", notification),
	})
}

func ProcessingNotificationSucceeded(connID uint64, notificationType string) string {
	message := fmt.Sprintf("conn[%d] %s %s", connID, ProcessingNotificationSucceededMessage, notificationType)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":           connID,
		"notificationType": notificationType,
	})
}

// Moving operation tracking functions
func DuplicateMovingOperation(connID uint64, endpoint string, seqID int64) string {
	message := fmt.Sprintf("conn[%d] %s for %s seqID[%d]", connID, DuplicateMovingOperationMessage, endpoint, seqID)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":   connID,
		"endpoint": endpoint,
		"seqID":    seqID,
	})
}

func TrackingMovingOperation(connID uint64, endpoint string, seqID int64) string {
	message := fmt.Sprintf("conn[%d] %s for %s seqID[%d]", connID, TrackingMovingOperationMessage, endpoint, seqID)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID":   connID,
		"endpoint": endpoint,
		"seqID":    seqID,
	})
}

func UntrackingMovingOperation(connID uint64, seqID int64) string {
	message := fmt.Sprintf("conn[%d] %s seqID[%d]", connID, UntrackingMovingOperationMessage, seqID)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID": connID,
		"seqID":  seqID,
	})
}

func OperationNotTracked(connID uint64, seqID int64) string {
	message := fmt.Sprintf("conn[%d] %s seqID[%d]", connID, OperationNotTrackedMessage, seqID)
	return appendJSONIfDebug(message, map[string]interface{}{
		"connID": connID,
		"seqID":  seqID,
	})
}

// Connection pool functions
func RemovingConnectionFromPool(connID uint64, reason error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"reason": reason.Error(),
	})
	return fmt.Sprintf("conn[%d] %s due to: %v %s", connID, RemovingConnectionFromPoolMessage, reason, string(data))
}

func NoPoolProvidedCannotRemove(connID uint64, reason error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"reason": reason.Error(),
	})
	return fmt.Sprintf("conn[%d] %s due to: %v %s", connID, NoPoolProvidedMessageCannotRemoveMessage, reason, string(data))
}

// Circuit breaker functions
func CircuitBreakerOpen(connID uint64, endpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":   connID,
		"endpoint": endpoint,
	})
	return fmt.Sprintf("conn[%d] %s for %s %s", connID, CircuitBreakerOpenMessage, endpoint, string(data))
}

func CircuitBreakerHalfOpen(endpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"endpoint": endpoint,
	})
	return fmt.Sprintf("%s for %s %s", CircuitBreakerHalfOpenMessage, endpoint, string(data))
}

func FailingDueToCircuitBreaker(connID uint64, endpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":   connID,
		"endpoint": endpoint,
	})
	return fmt.Sprintf("conn[%d] %s for %s %s", connID, FailingDueToCircuitBreakerMessage, endpoint, string(data))
}

// System functions
func ShuttingDown() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", ShuttingDownMessage, string(data))
}

// Connection state functions
func ConnectionInInvalidStateForHandoff(connID uint64, state string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"state":  state,
	})
	return fmt.Sprintf("conn[%d] %s (state: %s) %s", connID, ConnectionInInvalidStateForHandoffMessage, state, string(data))
}

// Additional handoff functions for specific cases

func ConnectionNotMarkedForHandoff(connID uint64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
	})
	return fmt.Sprintf("conn[%d] %s %s", connID, ConnectionNotMarkedForHandoffMessage, string(data))
}

func ConnectionNotMarkedForHandoffError(connID uint64) string {
	return fmt.Sprintf("conn[%d] %s", connID, ConnectionNotMarkedForHandoffErrorMessage)
}

func HandoffRetryAttempt(connID uint64, retries int, newEndpoint string, oldEndpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":      connID,
		"retries":     retries,
		"newEndpoint": newEndpoint,
		"oldEndpoint": oldEndpoint,
	})
	return fmt.Sprintf("conn[%d] Retry %d: %s to %s(was %s) %s", connID, retries, HandoffRetryAttemptMessage, newEndpoint, oldEndpoint, string(data))
}

func CannotQueueHandoffForRetry(err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"error": err.Error(),
	})
	return fmt.Sprintf("%s: %v %s", CannotQueueHandoffForRetryMessage, err, string(data))
}

// Validation and error functions
func InvalidNotificationFormat(notification interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notification": fmt.Sprintf("%v", notification),
	})
	return fmt.Sprintf("%s: %v %s", InvalidNotificationFormatMessage, notification, string(data))
}

func InvalidNotificationTypeFormat(notificationType interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": fmt.Sprintf("%v", notificationType),
	})
	return fmt.Sprintf("%s: %v %s", InvalidNotificationTypeFormatMessage, notificationType, string(data))
}

// InvalidNotification creates a log message for invalid notifications of any type
func InvalidNotification(notificationType string, notification interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": notificationType,
		"notification":     fmt.Sprintf("%v", notification),
	})
	return fmt.Sprintf("invalid %s notification: %v %s", notificationType, notification, string(data))
}

func InvalidSeqIDInMovingNotification(seqID interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"seqID": fmt.Sprintf("%v", seqID),
	})
	return fmt.Sprintf("%s: %v %s", InvalidSeqIDInMovingNotificationMessage, seqID, string(data))
}

func InvalidTimeSInMovingNotification(timeS interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"timeS": fmt.Sprintf("%v", timeS),
	})
	return fmt.Sprintf("%s: %v %s", InvalidTimeSInMovingNotificationMessage, timeS, string(data))
}

func InvalidNewEndpointInMovingNotification(newEndpoint interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"newEndpoint": fmt.Sprintf("%v", newEndpoint),
	})
	return fmt.Sprintf("%s: %v %s", InvalidNewEndpointInMovingNotificationMessage, newEndpoint, string(data))
}

func NoConnectionInHandlerContext(notificationType string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": notificationType,
	})
	return fmt.Sprintf("%s for %s notification %s", NoConnectionInHandlerContextMessage, notificationType, string(data))
}

func InvalidConnectionTypeInHandlerContext(notificationType string, conn interface{}, handlerCtx interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": notificationType,
		"connType":         fmt.Sprintf("%T", conn),
	})
	return fmt.Sprintf("%s for %s notification - %T %#v %s", InvalidConnectionTypeInHandlerContextMessage, notificationType, conn, handlerCtx, string(data))
}

func SchedulingHandoffToCurrentEndpoint(connID uint64, seconds float64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":  connID,
		"seconds": seconds,
	})
	return fmt.Sprintf("conn[%d] %s in %v seconds %s", connID, SchedulingHandoffToCurrentEndpointMessage, seconds, string(data))
}

func ManagerNotInitialized() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", ManagerNotInitializedMessage, string(data))
}

func FailedToRegisterHandler(notificationType string, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": notificationType,
		"error":            err.Error(),
	})
	return fmt.Sprintf("%s for %s: %v %s", FailedToRegisterHandlerMessage, notificationType, err, string(data))
}

func ConnectionMarkedForHandoffError(connID uint64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
	})
	return fmt.Sprintf("%s %s", ConnectionMarkedForHandoffErrorMessage, string(data))
}

func ConnectionInvalidHandoffStateError(connID uint64, state string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
		"state":  state,
	})
	return fmt.Sprintf("%s: %s %s", ConnectionInvalidHandoffStateErrorMessage, state, string(data))
}

func ShutdownError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", ShutdownErrorMessage, string(data))
}

func CircuitBreakerOpenError(endpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"endpoint": endpoint,
	})
	return fmt.Sprintf("%s for %s %s", CircuitBreakerOpenErrorMessage, endpoint, string(data))
}

// Configuration validation error functions
func InvalidRelaxedTimeoutError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidRelaxedTimeoutErrorMessage, string(data))
}

func InvalidHandoffTimeoutError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidHandoffTimeoutErrorMessage, string(data))
}

func InvalidHandoffWorkersError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidHandoffWorkersErrorMessage, string(data))
}

func InvalidHandoffQueueSizeError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidHandoffQueueSizeErrorMessage, string(data))
}

func InvalidPostHandoffRelaxedDurationError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidPostHandoffRelaxedDurationErrorMessage, string(data))
}

func InvalidEndpointTypeError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidEndpointTypeErrorMessage, string(data))
}

func InvalidMaintNotificationsError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidMaintNotificationsErrorMessage, string(data))
}

func InvalidHandoffRetriesError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidHandoffRetriesErrorMessage, string(data))
}

func InvalidClientError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidClientErrorMessage, string(data))
}

func InvalidNotificationError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidNotificationErrorMessage, string(data))
}

func MaxHandoffRetriesReachedError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", MaxHandoffRetriesReachedErrorMessage, string(data))
}

func HandoffQueueFullError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", HandoffQueueFullErrorMessage, string(data))
}

func InvalidCircuitBreakerFailureThresholdError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidCircuitBreakerFailureThresholdErrorMessage, string(data))
}

func InvalidCircuitBreakerResetTimeoutError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidCircuitBreakerResetTimeoutErrorMessage, string(data))
}

func InvalidCircuitBreakerMaxRequestsError() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", InvalidCircuitBreakerMaxRequestsErrorMessage, string(data))
}

// Configuration and debug functions
func DebugLoggingEnabled() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", DebugLoggingEnabledMessage, string(data))
}

func ConfigDebug(config interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"config": fmt.Sprintf("%+v", config),
	})
	return fmt.Sprintf("%s: %+v %s", ConfigDebugMessage, config, string(data))
}

// Handoff worker functions
func WorkerExitingDueToShutdown() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", WorkerExitingDueToShutdownMessage, string(data))
}

func WorkerExitingDueToShutdownWhileProcessing() string {
	data, _ := json.Marshal(map[string]interface{}{})
	return fmt.Sprintf("%s %s", WorkerExitingDueToShutdownWhileProcessingMessage, string(data))
}

func WorkerPanicRecovered(panicValue interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"panic": fmt.Sprintf("%v", panicValue),
	})
	return fmt.Sprintf("%s: %v %s", WorkerPanicRecoveredMessage, panicValue, string(data))
}

func WorkerExitingDueToInactivityTimeout(timeout interface{}) string {
	data, _ := json.Marshal(map[string]interface{}{
		"timeout": fmt.Sprintf("%v", timeout),
	})
	return fmt.Sprintf("%s (%v) %s", WorkerExitingDueToInactivityTimeoutMessage, timeout, string(data))
}

func SetNetConnAndInitConnTiming(connID uint64, duration interface{}, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":   connID,
		"duration": fmt.Sprintf("%v", duration),
		"error":    err.Error(),
	})
	return fmt.Sprintf("conn[%d] %s %v: err: %v %s", connID, SetNetConnAndInitConnTimingMessage, duration, err, string(data))
}

func ApplyingRelaxedTimeoutDueToPostHandoff(connID uint64, timeout interface{}, until string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID":  connID,
		"timeout": fmt.Sprintf("%v", timeout),
		"until":   until,
	})
	return fmt.Sprintf("conn[%d] %s (%v) until %s %s", connID, ApplyingRelaxedTimeoutDueToPostHandoffMessage, timeout, until, string(data))
}

// Example hooks functions
func MetricsHookProcessingNotification(notificationType string, connID uint64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": notificationType,
		"connID":           connID,
	})
	return fmt.Sprintf("%s %s notification on conn[%d] %s", MetricsHookProcessingNotificationMessage, notificationType, connID, string(data))
}

func MetricsHookRecordedError(notificationType string, connID uint64, err error) string {
	data, _ := json.Marshal(map[string]interface{}{
		"notificationType": notificationType,
		"connID":           connID,
		"error":            err.Error(),
	})
	return fmt.Sprintf("%s for %s notification on conn[%d]: %v %s", MetricsHookRecordedErrorMessage, notificationType, connID, err, string(data))
}

// Pool hook functions
func MarkedForHandoff(connID uint64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"connID": connID,
	})
	return fmt.Sprintf("conn[%d] %s %s", connID, MarkedForHandoffMessage, string(data))
}

// Circuit breaker additional functions
func CircuitBreakerTransitioningToHalfOpen(endpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"endpoint": endpoint,
	})
	return fmt.Sprintf("%s for %s %s", CircuitBreakerTransitioningToHalfOpenMessage, endpoint, string(data))
}

func CircuitBreakerOpened(endpoint string, failures int64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"endpoint": endpoint,
		"failures": failures,
	})
	return fmt.Sprintf("%s for endpoint %s after %d failures %s", CircuitBreakerOpenedMessage, endpoint, failures, string(data))
}

func CircuitBreakerReopened(endpoint string) string {
	data, _ := json.Marshal(map[string]interface{}{
		"endpoint": endpoint,
	})
	return fmt.Sprintf("%s for endpoint %s due to failure in half-open state %s", CircuitBreakerReopenedMessage, endpoint, string(data))
}

func CircuitBreakerClosed(endpoint string, successes int64) string {
	data, _ := json.Marshal(map[string]interface{}{
		"endpoint":  endpoint,
		"successes": successes,
	})
	return fmt.Sprintf("%s for endpoint %s after %d successful requests %s", CircuitBreakerClosedMessage, endpoint, successes, string(data))
}

func CircuitBreakerCleanup(removed int, total int) string {
	data, _ := json.Marshal(map[string]interface{}{
		"removed": removed,
		"total":   total,
	})
	return fmt.Sprintf("%s removed %d/%d entries %s", CircuitBreakerCleanupMessage, removed, total, string(data))
}

// ExtractDataFromLogMessage extracts structured data from hitless log messages
// Returns a map containing the parsed key-value pairs from the structured data section
// Example: "conn[123] handoff started to localhost:6379 {"connID":123,"endpoint":"localhost:6379"}"
// Returns: map[string]interface{}{"connID": 123, "endpoint": "localhost:6379"}
func ExtractDataFromLogMessage(logMessage string) map[string]interface{} {
	result := make(map[string]interface{})

	// Find the JSON data section at the end of the message
	re := regexp.MustCompile(`(\{.*\})$`)
	matches := re.FindStringSubmatch(logMessage)
	if len(matches) < 2 {
		return result
	}

	jsonStr := matches[1]
	if jsonStr == "" {
		return result
	}

	// Parse the JSON directly
	var jsonResult map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &jsonResult); err == nil {
		return jsonResult
	}

	// If JSON parsing fails, return empty map
	return result
}
