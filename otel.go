package redis

import (
	"context"
	"net"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
)

// ConnInfo provides information about a Redis connection for metrics.
// This is a public interface to avoid exposing internal types.
type ConnInfo interface {
	// RemoteAddr returns the remote network address
	RemoteAddr() net.Addr
}

// OTelRecorder is the interface for recording OpenTelemetry metrics.
// Implementations are provided by extra/redisotel-native package.
//
// This interface is exported to allow external packages to implement
// custom recorders without depending on internal packages.
type OTelRecorder interface {
	// RecordOperationDuration records the total operation duration (including all retries)
	RecordOperationDuration(ctx context.Context, duration time.Duration, cmd Cmder, attempts int, cn ConnInfo)

	// RecordConnectionStateChange records when a connection changes state (e.g., idle -> used)
	RecordConnectionStateChange(ctx context.Context, cn ConnInfo, fromState, toState string)

	// RecordConnectionCreateTime records the time it took to create a new connection
	RecordConnectionCreateTime(ctx context.Context, duration time.Duration, cn ConnInfo)

	// RecordConnectionRelaxedTimeout records when connection timeout is relaxed/unrelaxed
	// delta: +1 for relaxed, -1 for unrelaxed
	// poolName: name of the connection pool (e.g., "main", "pubsub")
	// notificationType: the notification type that triggered the timeout relaxation (e.g., "MOVING", "HANDOFF")
	RecordConnectionRelaxedTimeout(ctx context.Context, delta int, cn ConnInfo, poolName, notificationType string)

	// RecordConnectionHandoff records when a connection is handed off to another node
	// poolName: name of the connection pool (e.g., "main", "pubsub")
	RecordConnectionHandoff(ctx context.Context, cn ConnInfo, poolName string)

	// RecordError records client errors (ASK, MOVED, handshake failures, etc.)
	// errorType: type of error (e.g., "ASK", "MOVED", "HANDSHAKE_FAILED")
	// statusCode: Redis response status code if available (e.g., "MOVED", "ASK")
	// isInternal: whether this is an internal error
	// retryAttempts: number of retry attempts made
	RecordError(ctx context.Context, errorType string, cn ConnInfo, statusCode string, isInternal bool, retryAttempts int)

	// RecordMaintenanceNotification records when a maintenance notification is received
	// notificationType: the type of notification (e.g., "MOVING", "MIGRATING", etc.)
	RecordMaintenanceNotification(ctx context.Context, cn ConnInfo, notificationType string)

	// RecordConnectionWaitTime records the time spent waiting for a connection from the pool
	RecordConnectionWaitTime(ctx context.Context, duration time.Duration, cn ConnInfo)

	// RecordConnectionUseTime records the time a connection was checked out from the pool
	RecordConnectionUseTime(ctx context.Context, duration time.Duration, cn ConnInfo)

	// RecordConnectionTimeout records when a connection timeout occurs
	// timeoutType: "pool" for pool timeout, "read" for read timeout, "write" for write timeout
	RecordConnectionTimeout(ctx context.Context, cn ConnInfo, timeoutType string)

	// RecordConnectionClosed records when a connection is closed
	// reason: reason for closing (e.g., "idle", "max_lifetime", "error", "pool_closed")
	RecordConnectionClosed(ctx context.Context, cn ConnInfo, reason string)

	// RecordConnectionPendingRequests records changes in pending requests count
	// delta: +1 when request starts, -1 when request completes
	RecordConnectionPendingRequests(ctx context.Context, delta int, cn ConnInfo)

	// RecordPubSubMessage records a Pub/Sub message
	// direction: "sent" or "received"
	// channel: channel name (may be hidden for cardinality reduction)
	// sharded: true for sharded pub/sub (SPUBLISH/SSUBSCRIBE)
	RecordPubSubMessage(ctx context.Context, cn ConnInfo, direction, channel string, sharded bool)

	// RecordStreamLag records the lag for stream consumer group processing
	// lag: time difference between message creation and consumption
	// streamName: name of the stream (may be hidden for cardinality reduction)
	// consumerGroup: name of the consumer group
	// consumerName: name of the consumer
	RecordStreamLag(ctx context.Context, lag time.Duration, cn ConnInfo, streamName, consumerGroup, consumerName string)
}

// SetOTelRecorder sets the global OpenTelemetry recorder.
// This is typically called by Init() in extra/redisotel-native package.
//
// Setting a nil recorder disables metrics collection.
func SetOTelRecorder(r OTelRecorder) {
	if r == nil {
		otel.SetGlobalRecorder(nil)
		return
	}
	otel.SetGlobalRecorder(&otelRecorderAdapter{r})
}

// otelRecorderAdapter adapts the public OTelRecorder interface to the internal otel.Recorder interface
type otelRecorderAdapter struct {
	recorder OTelRecorder
}

// toConnInfo converts internal pool.Conn to public ConnInfo interface
// Returns nil if cn is nil, otherwise returns cn (which implements ConnInfo)
func toConnInfo(cn *pool.Conn) ConnInfo {
	if cn != nil {
		return cn
	}
	return nil
}

func (a *otelRecorderAdapter) RecordOperationDuration(ctx context.Context, duration time.Duration, cmd otel.Cmder, attempts int, cn *pool.Conn) {
	// Convert internal Cmder to public Cmder
	if publicCmd, ok := cmd.(Cmder); ok {
		a.recorder.RecordOperationDuration(ctx, duration, publicCmd, attempts, toConnInfo(cn))
	}
}

func (a *otelRecorderAdapter) RecordConnectionStateChange(ctx context.Context, cn *pool.Conn, fromState, toState string) {
	a.recorder.RecordConnectionStateChange(ctx, toConnInfo(cn), fromState, toState)
}

func (a *otelRecorderAdapter) RecordConnectionCreateTime(ctx context.Context, duration time.Duration, cn *pool.Conn) {
	a.recorder.RecordConnectionCreateTime(ctx, duration, toConnInfo(cn))
}

func (a *otelRecorderAdapter) RecordConnectionRelaxedTimeout(ctx context.Context, delta int, cn *pool.Conn, poolName, notificationType string) {
	a.recorder.RecordConnectionRelaxedTimeout(ctx, delta, toConnInfo(cn), poolName, notificationType)
}

func (a *otelRecorderAdapter) RecordConnectionHandoff(ctx context.Context, cn *pool.Conn, poolName string) {
	a.recorder.RecordConnectionHandoff(ctx, toConnInfo(cn), poolName)
}

func (a *otelRecorderAdapter) RecordError(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retryAttempts int) {
	a.recorder.RecordError(ctx, errorType, toConnInfo(cn), statusCode, isInternal, retryAttempts)
}

func (a *otelRecorderAdapter) RecordMaintenanceNotification(ctx context.Context, cn *pool.Conn, notificationType string) {
	a.recorder.RecordMaintenanceNotification(ctx, toConnInfo(cn), notificationType)
}

func (a *otelRecorderAdapter) RecordConnectionWaitTime(ctx context.Context, duration time.Duration, cn *pool.Conn) {
	a.recorder.RecordConnectionWaitTime(ctx, duration, toConnInfo(cn))
}

func (a *otelRecorderAdapter) RecordConnectionUseTime(ctx context.Context, duration time.Duration, cn *pool.Conn) {
	a.recorder.RecordConnectionUseTime(ctx, duration, toConnInfo(cn))
}

func (a *otelRecorderAdapter) RecordConnectionTimeout(ctx context.Context, cn *pool.Conn, timeoutType string) {
	a.recorder.RecordConnectionTimeout(ctx, toConnInfo(cn), timeoutType)
}

func (a *otelRecorderAdapter) RecordConnectionClosed(ctx context.Context, cn *pool.Conn, reason string) {
	a.recorder.RecordConnectionClosed(ctx, toConnInfo(cn), reason)
}

func (a *otelRecorderAdapter) RecordConnectionPendingRequests(ctx context.Context, delta int, cn *pool.Conn) {
	a.recorder.RecordConnectionPendingRequests(ctx, delta, toConnInfo(cn))
}

func (a *otelRecorderAdapter) RecordPubSubMessage(ctx context.Context, cn *pool.Conn, direction, channel string, sharded bool) {
	a.recorder.RecordPubSubMessage(ctx, toConnInfo(cn), direction, channel, sharded)
}

func (a *otelRecorderAdapter) RecordStreamLag(ctx context.Context, lag time.Duration, cn *pool.Conn, streamName, consumerGroup, consumerName string) {
	a.recorder.RecordStreamLag(ctx, lag, toConnInfo(cn), streamName, consumerGroup, consumerName)
}
