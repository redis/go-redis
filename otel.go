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

