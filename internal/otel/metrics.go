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
func (noopRecorder) RecordConnectionStateChange(context.Context, *pool.Conn, string, string)       {}
func (noopRecorder) RecordConnectionCreateTime(context.Context, time.Duration, *pool.Conn)         {}
