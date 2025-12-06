package otel

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// Cmder is a minimal interface for command information needed for metrics.
// This avoids circular dependencies with the main redis package.
type Cmder interface {
	Name() string
	FullName() string
	Args() []interface{}
	Err() error
}

// Recorder is the interface for recording metrics.
// Implementations are provided by extra/redisotel-native package.
type Recorder interface {
	// RecordOperationDuration records the total operation duration (including all retries)
	RecordOperationDuration(ctx context.Context, duration time.Duration, cmd Cmder, attempts int, cn *pool.Conn)

	// RecordConnectionStateChange records when a connection changes state
	RecordConnectionStateChange(ctx context.Context, cn *pool.Conn, fromState, toState string)

	// RecordConnectionCreateTime records the time it took to create a new connection
	RecordConnectionCreateTime(ctx context.Context, duration time.Duration, cn *pool.Conn)

	// RecordConnectionRelaxedTimeout records when connection timeout is relaxed/unrelaxed
	// delta: +1 for relaxed, -1 for unrelaxed
	// poolName: name of the connection pool (e.g., "main", "pubsub")
	// notificationType: the notification type that triggered the timeout relaxation (e.g., "MOVING")
	RecordConnectionRelaxedTimeout(ctx context.Context, delta int, cn *pool.Conn, poolName, notificationType string)

	// RecordConnectionHandoff records when a connection is handed off to another node
	// poolName: name of the connection pool (e.g., "main", "pubsub")
	RecordConnectionHandoff(ctx context.Context, cn *pool.Conn, poolName string)

	// RecordError records client errors (ASK, MOVED, handshake failures, etc.)
	// errorType: type of error (e.g., "ASK", "MOVED", "HANDSHAKE_FAILED")
	// statusCode: Redis response status code if available (e.g., "MOVED", "ASK")
	// isInternal: whether this is an internal error
	// retryAttempts: number of retry attempts made
	RecordError(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retryAttempts int)

	// RecordMaintenanceNotification records when a maintenance notification is received
	// notificationType: the type of notification (e.g., "MOVING", "MIGRATING", etc.)
	RecordMaintenanceNotification(ctx context.Context, cn *pool.Conn, notificationType string)
}

// Global recorder instance (initialized by extra/redisotel-native)
var globalRecorder Recorder = noopRecorder{}

// SetGlobalRecorder sets the global recorder (called by Init() in extra/redisotel-native)
func SetGlobalRecorder(r Recorder) {
	if r == nil {
		globalRecorder = noopRecorder{}
		// Unregister pool callbacks
		pool.SetConnectionStateChangeCallback(nil)
		pool.SetConnectionCreateTimeCallback(nil)
		pool.SetConnectionRelaxedTimeoutCallback(nil)
		pool.SetConnectionHandoffCallback(nil)
		pool.SetErrorCallback(nil)
		pool.SetMaintenanceNotificationCallback(nil)
		return
	}
	globalRecorder = r

	// Register pool callback to forward state changes to recorder
	pool.SetConnectionStateChangeCallback(func(ctx context.Context, cn *pool.Conn, fromState, toState string) {
		globalRecorder.RecordConnectionStateChange(ctx, cn, fromState, toState)
	})

	// Register pool callback to forward connection creation time to recorder
	pool.SetConnectionCreateTimeCallback(func(ctx context.Context, duration time.Duration, cn *pool.Conn) {
		globalRecorder.RecordConnectionCreateTime(ctx, duration, cn)
	})

	// Register pool callback to forward connection relaxed timeout changes to recorder
	pool.SetConnectionRelaxedTimeoutCallback(func(ctx context.Context, delta int, cn *pool.Conn, poolName, notificationType string) {
		globalRecorder.RecordConnectionRelaxedTimeout(ctx, delta, cn, poolName, notificationType)
	})

	// Register pool callback to forward connection handoffs to recorder
	pool.SetConnectionHandoffCallback(func(ctx context.Context, cn *pool.Conn, poolName string) {
		globalRecorder.RecordConnectionHandoff(ctx, cn, poolName)
	})

	// Register pool callback to forward errors to recorder
	pool.SetErrorCallback(func(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retryAttempts int) {
		globalRecorder.RecordError(ctx, errorType, cn, statusCode, isInternal, retryAttempts)
	})

	// Register pool callback to forward maintenance notifications to recorder
	pool.SetMaintenanceNotificationCallback(func(ctx context.Context, cn *pool.Conn, notificationType string) {
		globalRecorder.RecordMaintenanceNotification(ctx, cn, notificationType)
	})
}

// RecordOperationDuration records the total operation duration.
// This is called from redis.go after command execution completes.
func RecordOperationDuration(ctx context.Context, duration time.Duration, cmd Cmder, attempts int, cn *pool.Conn) {
	globalRecorder.RecordOperationDuration(ctx, duration, cmd, attempts, cn)
}

// RecordConnectionStateChange records when a connection changes state.
// This is called from pool.go when connections transition between states.
func RecordConnectionStateChange(ctx context.Context, cn *pool.Conn, fromState, toState string) {
	globalRecorder.RecordConnectionStateChange(ctx, cn, fromState, toState)
}

// RecordConnectionCreateTime records the time it took to create a new connection.
// This is called from pool.go when a new connection is successfully created.
func RecordConnectionCreateTime(ctx context.Context, duration time.Duration, cn *pool.Conn) {
	globalRecorder.RecordConnectionCreateTime(ctx, duration, cn)
}

// noopRecorder is a no-op implementation (zero overhead when metrics disabled)
type noopRecorder struct{}

func (noopRecorder) RecordOperationDuration(context.Context, time.Duration, Cmder, int, *pool.Conn) {}
func (noopRecorder) RecordConnectionStateChange(context.Context, *pool.Conn, string, string)        {}
func (noopRecorder) RecordConnectionCreateTime(context.Context, time.Duration, *pool.Conn)          {}
func (noopRecorder) RecordConnectionRelaxedTimeout(context.Context, int, *pool.Conn, string, string) {
}
func (noopRecorder) RecordConnectionHandoff(context.Context, *pool.Conn, string)        {}
func (noopRecorder) RecordError(context.Context, string, *pool.Conn, string, bool, int) {}
func (noopRecorder) RecordMaintenanceNotification(context.Context, *pool.Conn, string)  {}
