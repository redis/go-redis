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

	// RecordPipelineOperationDuration records the total pipeline/transaction duration.
	// operationName should be "PIPELINE" for regular pipelines or "MULTI" for transactions.
	// cmdCount is the number of commands in the pipeline.
	// err is the error from the pipeline execution (can be nil).
	RecordPipelineOperationDuration(ctx context.Context, duration time.Duration, operationName string, cmdCount int, attempts int, err error, cn *pool.Conn)

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

	// RecordConnectionWaitTime records the time spent waiting for a connection from the pool
	RecordConnectionWaitTime(ctx context.Context, duration time.Duration, cn *pool.Conn)

	// RecordConnectionUseTime records the time a connection was checked out from the pool
	RecordConnectionUseTime(ctx context.Context, duration time.Duration, cn *pool.Conn)

	// RecordConnectionClosed records when a connection is closed
	// reason: reason for closing (e.g., "idle", "max_lifetime", "error", "pool_closed")
	// err: the error that caused the close (nil for non-error closures)
	RecordConnectionClosed(ctx context.Context, cn *pool.Conn, reason string, err error)

	// RecordPubSubMessage records a Pub/Sub message
	// direction: "sent" or "received"
	// channel: channel name (may be hidden for cardinality reduction)
	// sharded: true for sharded pub/sub (SPUBLISH/SSUBSCRIBE)
	RecordPubSubMessage(ctx context.Context, cn *pool.Conn, direction, channel string, sharded bool)

	// RecordStreamLag records the lag for stream consumer group processing
	// lag: time difference between message creation and consumption
	// streamName: name of the stream (may be hidden for cardinality reduction)
	// consumerGroup: name of the consumer group
	// consumerName: name of the consumer
	RecordStreamLag(ctx context.Context, lag time.Duration, cn *pool.Conn, streamName, consumerGroup, consumerName string)
}

// Global recorder instance (initialized by extra/redisotel-native)
var globalRecorder Recorder = noopRecorder{}

// SetGlobalRecorder sets the global recorder (called by Init() in extra/redisotel-native)
func SetGlobalRecorder(r Recorder) {
	if r == nil {
		globalRecorder = noopRecorder{}
		// Unregister pool callbacks
		pool.SetConnectionCreateTimeCallback(nil)
		pool.SetConnectionRelaxedTimeoutCallback(nil)
		pool.SetConnectionHandoffCallback(nil)
		pool.SetErrorCallback(nil)
		pool.SetMaintenanceNotificationCallback(nil)
		pool.SetConnectionWaitTimeCallback(nil)
		pool.SetConnectionUseTimeCallback(nil)
		pool.SetConnectionClosedCallback(nil)
		return
	}
	globalRecorder = r

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

	// Register pool callback to forward connection wait time to recorder
	pool.SetConnectionWaitTimeCallback(func(ctx context.Context, duration time.Duration, cn *pool.Conn) {
		globalRecorder.RecordConnectionWaitTime(ctx, duration, cn)
	})

	// Register pool callback to forward connection use time to recorder
	pool.SetConnectionUseTimeCallback(func(ctx context.Context, duration time.Duration, cn *pool.Conn) {
		globalRecorder.RecordConnectionUseTime(ctx, duration, cn)
	})

	// Register pool callback to forward connection closed to recorder
	pool.SetConnectionClosedCallback(func(ctx context.Context, cn *pool.Conn, reason string, err error) {
		globalRecorder.RecordConnectionClosed(ctx, cn, reason, err)
	})
}

// RecordOperationDuration records the total operation duration.
// This is called from redis.go after command execution completes.
func RecordOperationDuration(ctx context.Context, duration time.Duration, cmd Cmder, attempts int, cn *pool.Conn) {
	globalRecorder.RecordOperationDuration(ctx, duration, cmd, attempts, cn)
}

// RecordPipelineOperationDuration records the total pipeline/transaction duration.
// This is called from redis.go after pipeline/transaction execution completes.
// operationName should be "PIPELINE" for regular pipelines or "MULTI" for transactions.
// err is the error from the pipeline execution (can be nil).
func RecordPipelineOperationDuration(ctx context.Context, duration time.Duration, operationName string, cmdCount int, attempts int, err error, cn *pool.Conn) {
	globalRecorder.RecordPipelineOperationDuration(ctx, duration, operationName, cmdCount, attempts, err, cn)
}

// RecordConnectionCreateTime records the time it took to create a new connection.
// This is called from pool.go when a new connection is successfully created.
func RecordConnectionCreateTime(ctx context.Context, duration time.Duration, cn *pool.Conn) {
	globalRecorder.RecordConnectionCreateTime(ctx, duration, cn)
}

// RecordPubSubMessage records a Pub/Sub message sent or received.
// This is called from pubsub.go when messages are sent or received.
func RecordPubSubMessage(ctx context.Context, cn *pool.Conn, direction, channel string, sharded bool) {
	globalRecorder.RecordPubSubMessage(ctx, cn, direction, channel, sharded)
}

// RecordStreamLag records the lag between message creation and consumption in a stream.
// This is called from stream_commands.go when processing stream messages.
func RecordStreamLag(ctx context.Context, lag time.Duration, cn *pool.Conn, streamName, consumerGroup, consumerName string) {
	globalRecorder.RecordStreamLag(ctx, lag, cn, streamName, consumerGroup, consumerName)
}

// noopRecorder is a no-op implementation (zero overhead when metrics disabled)
type noopRecorder struct{}

func (noopRecorder) RecordOperationDuration(context.Context, time.Duration, Cmder, int, *pool.Conn) {}
func (noopRecorder) RecordPipelineOperationDuration(context.Context, time.Duration, string, int, int, error, *pool.Conn) {
}
func (noopRecorder) RecordConnectionCreateTime(context.Context, time.Duration, *pool.Conn) {}
func (noopRecorder) RecordConnectionRelaxedTimeout(context.Context, int, *pool.Conn, string, string) {
}
func (noopRecorder) RecordConnectionHandoff(context.Context, *pool.Conn, string)        {}
func (noopRecorder) RecordError(context.Context, string, *pool.Conn, string, bool, int) {}
func (noopRecorder) RecordMaintenanceNotification(context.Context, *pool.Conn, string)  {}

func (noopRecorder) RecordConnectionWaitTime(context.Context, time.Duration, *pool.Conn) {}
func (noopRecorder) RecordConnectionUseTime(context.Context, time.Duration, *pool.Conn)  {}
func (noopRecorder) RecordConnectionClosed(context.Context, *pool.Conn, string, error)   {}

func (noopRecorder) RecordPubSubMessage(context.Context, *pool.Conn, string, string, bool) {}

func (noopRecorder) RecordStreamLag(context.Context, time.Duration, *pool.Conn, string, string, string) {
}
