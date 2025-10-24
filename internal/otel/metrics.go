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
}

// Global recorder instance (initialized by extra/redisotel-native)
var globalRecorder Recorder = noopRecorder{}

// SetGlobalRecorder sets the global recorder (called by Init() in extra/redisotel-native)
func SetGlobalRecorder(r Recorder) {
	if r == nil {
		globalRecorder = noopRecorder{}
		return
	}
	globalRecorder = r
}

// RecordOperationDuration records the total operation duration.
// This is called from redis.go after command execution completes.
func RecordOperationDuration(ctx context.Context, duration time.Duration, cmd Cmder, attempts int, cn *pool.Conn) {
	globalRecorder.RecordOperationDuration(ctx, duration, cmd, attempts, cn)
}

// noopRecorder is a no-op implementation (zero overhead when metrics disabled)
type noopRecorder struct{}

func (noopRecorder) RecordOperationDuration(context.Context, time.Duration, Cmder, int, *pool.Conn) {}
