package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
)

// fdOtelRecorder counts RecordOperationDuration to prove the full-duplex reader
// emits the native per-command OTel metric (it previously bypassed process, so
// no metric was recorded — #3964). All other Recorder methods are no-ops.
type fdOtelRecorder struct{ opDurations atomic.Int64 }

func (r *fdOtelRecorder) RecordOperationDuration(context.Context, time.Duration, otel.Cmder, int, error, *pool.Conn, int) {
	r.opDurations.Add(1)
}
func (r *fdOtelRecorder) RecordPipelineOperationDuration(context.Context, time.Duration, string, int, int, error, *pool.Conn, int) {
}
func (r *fdOtelRecorder) RecordConnectionCreateTime(context.Context, time.Duration, *pool.Conn) {}
func (r *fdOtelRecorder) RecordConnectionRelaxedTimeout(context.Context, int, *pool.Conn, string, string) {
}
func (r *fdOtelRecorder) RecordConnectionHandoff(context.Context, *pool.Conn, string)         {}
func (r *fdOtelRecorder) RecordError(context.Context, string, *pool.Conn, string, bool, int)  {}
func (r *fdOtelRecorder) RecordMaintenanceNotification(context.Context, *pool.Conn, string)   {}
func (r *fdOtelRecorder) RecordConnectionWaitTime(context.Context, time.Duration, *pool.Conn) {}
func (r *fdOtelRecorder) RecordConnectionClosed(context.Context, *pool.Conn, string, error)   {}
func (r *fdOtelRecorder) RecordPubSubMessage(context.Context, *pool.Conn, string, string, bool) {
}
func (r *fdOtelRecorder) RecordStreamLag(context.Context, time.Duration, *pool.Conn, string, string, string) {
}
func (r *fdOtelRecorder) RecordConnectionCount(context.Context, int, *pool.Conn, string, bool) {}
func (r *fdOtelRecorder) RecordPendingRequests(context.Context, int, *pool.Conn, string)       {}

// TestFullDuplexRecordsOTelOperationDuration verifies the FD reader records the
// native per-command OTel duration metric (redisotel-native), which it did not
// before — the reader completes commands without going through process().
func TestFullDuplexRecordsOTelOperationDuration(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	rec := &fdOtelRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	// Runs through the FD pipe (writer -> reader), which is where the metric is
	// emitted; Result() blocks until the reader completed it (emit is before
	// complete()).
	if err := ap.Set(ctx, "fd:otel:k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if n := rec.opDurations.Load(); n < 1 {
		t.Fatalf("FullDuplex recorded no OTel RecordOperationDuration (n=%d) — native metric bypassed", n)
	}
}
