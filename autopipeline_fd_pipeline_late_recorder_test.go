package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
)

// fdDurationRecorder records the longest pipeline duration it was given.
type fdDurationRecorder struct {
	fdOtelRecorder
	calls atomic.Int64
	max   atomic.Int64
}

func (r *fdDurationRecorder) RecordPipelineOperationDuration(_ context.Context, d time.Duration, _ string, _, _ int, _ error, _ *pool.Conn, _ int) {
	r.calls.Add(1)
	if int64(d) > r.max.Load() {
		r.max.Store(int64(d))
	}
}

// A recorder installed while an FD pipeline runs finds the operation without
// a start time: the start is read only when a duration callback exists.
// fdPipelineMetrics used to report time.Since(time.Time{}), a duration of
// centuries. It now records no duration for that operation, as an ordinary
// pipeline does (it reads the callback once, at the start).
func TestFDPipelineMetricsSkipsDurationWithoutStart(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	rec := &fdDurationRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	cmds := []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	ap.fdPipelineMetrics(ctx, time.Time{}, cmds, 1, nil)
	if n := rec.calls.Load(); n != 0 {
		t.Fatalf("recorded %d duration(s) for an operation with no start (max %v)", n, time.Duration(rec.max.Load()))
	}
	ap.fdPipelineMetrics(ctx, time.Now(), cmds, 1, nil)
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("recorded %d duration(s) for an operation with a start, want 1", n)
	}
}

// The pooled re-run continues an FD operation. If that operation began with
// no duration callback (zero start) and a recorder was installed before the
// re-run, generalProcessPipelineFrom started the clock at the re-run: the
// duration covered only the pooled tail while the count included the FD
// attempt. It now records no duration, as the FD path does.
func TestPipelineRetriesAfterSkipsDurationWithoutStart(t *testing.T) {
	srv := newFDStateServer(t)
	c := NewClient(&Options{Addr: srv.addr(), Protocol: 2, DisableIdentity: true})
	defer c.Close()
	rec := &fdDurationRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	cmds := []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	if err := c.processPipelineRetriesAfter(ctx, cmds, 0, time.Time{}, 1); err != nil {
		t.Fatalf("processPipelineRetriesAfter: %v", err)
	}
	if n := rec.calls.Load(); n != 0 {
		t.Fatalf("recorded %d duration(s) for a continued operation with no start", n)
	}
	cmds = []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	if err := c.processPipelineRetriesAfter(ctx, cmds, 0, time.Now(), 1); err != nil {
		t.Fatalf("processPipelineRetriesAfter: %v", err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("recorded %d duration(s) for a continued operation with a start, want 1", n)
	}
}
