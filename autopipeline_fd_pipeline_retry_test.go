package redis

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// The whole-batch retry and its interaction with the engine's own replays.

// After a connection-error replay the replay already was the batch's retry, so
// a stale retryable reply on the first command must not trigger a pooled
// re-run on top. Here SET gets LOADING, the connection drops before GET's
// reply, and the engine replays GET. The re-run used to run GET a third time,
// where an ordinary pipeline runs it twice (once, then once in its retry).
func TestFDPipelineNoRerunAfterReplay(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		if conn == 0 {
			if name == "set" {
				return "-LOADING Redis is loading the dataset in memory\r\n", false
			}
			return "", true // GET executed, then the connection drops
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if n := srv.count("get"); n != 2 {
		t.Fatalf("GET executed %d times, want 2 (once, then the engine's replay)", n)
	}
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec err=%v, want the first command's LOADING", err)
	}
}

// The Close-time flush runs a pipelined batch through the pooled pipeline with
// its own retry loop, so the batch must count as already retried: otherwise
// fdPipelineExec saw one issue and a stale LOADING on the first command, and
// ran the batch again after Close.
func TestFDShutdownFlushMarksPipelinedBatchesRetried(t *testing.T) {
	ctx := context.Background()
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		runPipeline: func(_ context.Context, cmds []Cmder, _ int) error {
			cmds[0].SetErr(proto.RedisError("LOADING Redis is loading the dataset in memory"))
			return nil
		},
	}
	set := NewStatusCmd(ctx, "set", "k", "v")
	b := newAPBatch()
	b.fdAttempts = 1
	fd.shutdownFlush(ctx, []fdReq{{cmd: set, batch: b, attempts: 1, pipelined: true}})

	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the flushed command never completed")
	}
	if b.fdAttempts < 2 {
		t.Fatalf("flushed pipelined batch reports %d issue(s); fdPipelineExec would re-run it after Close", b.fdAttempts)
	}
}

// fdRetryMetricRecorder captures pipeline-duration records.
type fdRetryMetricRecorder struct {
	fdOtelRecorder
	calls    atomic.Int64
	attempts atomic.Int64
	duration atomic.Int64
	lastErr  atomic.Value
}

func (r *fdRetryMetricRecorder) RecordPipelineOperationDuration(_ context.Context, d time.Duration, _ string, _ int, attempts int, err error, _ *pool.Conn, _ int) {
	r.calls.Add(1)
	r.attempts.Store(int64(attempts))
	r.duration.Store(int64(d))
	if err != nil {
		r.lastErr.Store(err)
	}
}

// A whole-batch re-run continues the FD attempt's measurement: one pipeline
// metric, counting the FD attempt and covering its time. The pooled re-run
// used to open its own measurement, recording one attempt and only its own
// duration.
func TestFDPipelineRetryMetricCoversTheWholeOperation(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	srv.delay = 30 * time.Millisecond
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	rec := &fdRetryMetricRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d pipeline metrics for one operation, want 1", n)
	}
	if a := rec.attempts.Load(); a != 2 {
		t.Fatalf("attempts=%d, want 2 (the FD attempt and the re-run)", a)
	}
	// Both attempts wait srv.delay, so one operation spans at least two of them.
	if d := time.Duration(rec.duration.Load()); d < 2*srv.delay {
		t.Fatalf("duration %v excludes the FD attempt (each attempt waits %v)", d, srv.delay)
	}
}

// A ctx that ends during the backoff before the re-run still records the
// operation, with the ctx error.
func TestFDPipelineRetryMetricOnBackoffCancel(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3,
		MinRetryBackoff: time.Second, MaxRetryBackoff: time.Second})
	rec := &fdRetryMetricRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec err=%v, want the ctx deadline", err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d pipeline metrics, want 1 even though ctx ended in the backoff", n)
	}
	if e, _ := rec.lastErr.Load().(error); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("metric error %v, want the ctx deadline", e)
	}
}
