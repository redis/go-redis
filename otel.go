package redis

import (
	"context"
	"fmt"
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

func (a *otelRecorderAdapter) RecordOperationDuration(ctx context.Context, duration time.Duration, cmd otel.Cmder, attempts int, cn *pool.Conn) {
	// Convert internal Cmder to public Cmder
	if publicCmd, ok := cmd.(Cmder); ok {
		// Convert internal pool.Conn to public ConnInfo
		var connInfo ConnInfo
		if cn != nil {
			connInfo = cn
		}
		a.recorder.RecordOperationDuration(ctx, duration, publicCmd, attempts, connInfo)
	}
}

func (a *otelRecorderAdapter) RecordConnectionStateChange(ctx context.Context, cn *pool.Conn, fromState, toState string) {
	// Convert internal pool.Conn to public ConnInfo
	var connInfo ConnInfo
	if cn != nil {
		connInfo = cn
	}
	a.recorder.RecordConnectionStateChange(ctx, connInfo, fromState, toState)
}

func (a *otelRecorderAdapter) RecordConnectionCreateTime(ctx context.Context, duration time.Duration, cn *pool.Conn) {

	fmt.Println("RecordConnectionCreateTime---")

	// Convert internal pool.Conn to public ConnInfo
	var connInfo ConnInfo
	if cn != nil {
		connInfo = cn
	}
	a.recorder.RecordConnectionCreateTime(ctx, duration, connInfo)
}

func (a *otelRecorderAdapter) RecordConnectionRelaxedTimeout(ctx context.Context, delta int, cn *pool.Conn, poolName, notificationType string) {
	// Convert internal pool.Conn to public ConnInfo
	var connInfo ConnInfo
	if cn != nil {
		connInfo = cn
	}
	a.recorder.RecordConnectionRelaxedTimeout(ctx, delta, connInfo, poolName, notificationType)
}

func (a *otelRecorderAdapter) RecordConnectionHandoff(ctx context.Context, cn *pool.Conn, poolName string) {
	// Convert internal pool.Conn to public ConnInfo
	var connInfo ConnInfo
	if cn != nil {
		connInfo = cn
	}
	a.recorder.RecordConnectionHandoff(ctx, connInfo, poolName)
}

func (a *otelRecorderAdapter) RecordError(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retryAttempts int) {
	// Convert internal pool.Conn to public ConnInfo
	var connInfo ConnInfo
	if cn != nil {
		connInfo = cn
	}
	a.recorder.RecordError(ctx, errorType, connInfo, statusCode, isInternal, retryAttempts)
}

func (a *otelRecorderAdapter) RecordMaintenanceNotification(ctx context.Context, cn *pool.Conn, notificationType string) {
	// Convert internal pool.Conn to public ConnInfo
	var connInfo ConnInfo
	if cn != nil {
		connInfo = cn
	}
	a.recorder.RecordMaintenanceNotification(ctx, connInfo, notificationType)
}
