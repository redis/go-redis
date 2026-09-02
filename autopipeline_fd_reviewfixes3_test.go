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
)

// TestFDFailReqsRecoversMetricCallbackPanic pins that a panicking user MetricError
// callback on the engine-goroutine failure path (failReqs, reached on lease
// failure / retry exhaustion / Close) cannot abort settlement: every req must get
// its error set and its batch closed (caller woken), and the panic must not
// escape into fd.run. The guard is PER-req, not around the loop — three reqs with
// a callback that always panics prove reqs after the first still settle. Pure, no
// server.
func TestFDFailReqsRecoversMetricCallbackPanic(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
			panic("boom from a user metric callback")
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}

	const n = 3
	reqs := make([]fdReq, n)
	batches := make([]*apBatch, n)
	for i := range reqs {
		cmd := NewStatusCmd(ctx, "set", "k", "v")
		b := newAPBatch()
		cmd.setReady(b) // cmd.Err()/await() would now block until b closes
		batches[i] = b
		reqs[i] = fdReq{cmd: cmd, batch: b, ctx: ctx, attempts: 1}
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("failReqs propagated a callback panic: %v", r)
			}
		}()
		fd.failReqs(reqs, ErrClosed)
	}()

	if got := calls.Load(); got != n {
		t.Fatalf("error callback invoked %d times, want %d (every req must reach it)", got, n)
	}
	for i := range reqs {
		if err := reqs[i].cmd.rawErr(); !errors.Is(err, ErrClosed) {
			t.Fatalf("req %d cmd err = %v, want ErrClosed", i, err)
		}
		select {
		case <-batches[i].done:
		default:
			t.Fatalf("req %d batch not closed: a panicking callback left the caller wedged", i)
		}
	}
}

// TestFDFailQueueRecoversMetricCallbackPanic is the failQueue twin of the above:
// draining the accepted backlog on an fdLeaseErr must settle every buffered req
// even when the MetricError callback panics, and must not propagate. Pure, no
// server.
func TestFDFailQueueRecoversMetricCallbackPanic(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
			panic("boom from a user metric callback")
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	const n = 3
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}, ch: make(chan fdReq, n)}
	batches := make([]*apBatch, n)
	for i := 0; i < n; i++ {
		cmd := NewStatusCmd(ctx, "set", "k", "v")
		b := newAPBatch()
		cmd.setReady(b)
		batches[i] = b
		fd.ch <- fdReq{cmd: cmd, batch: b, ctx: ctx, attempts: 1}
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("failQueue propagated a callback panic: %v", r)
			}
		}()
		fd.failQueue(ErrClosed)
	}()

	if got := calls.Load(); got != n {
		t.Fatalf("error callback invoked %d times, want %d (every buffered req must reach it)", got, n)
	}
	for i := range batches {
		select {
		case <-batches[i].done:
		default:
			t.Fatalf("req %d batch not closed: a panicking callback left the caller wedged", i)
		}
	}
}

// fdBlockingDurationRecorder is a custom OTel recorder whose duration callback
// reads its OWN async command (cmd.Err()/cmd.String()). Both accessors await the
// command's batch.done, which the FD reader closes only AFTER the duration
// callback returns — so without the executor-guard escape in reportReplyMetrics
// this call blocks forever and wedges the reader. Embeds fdOtelRecorder for the
// no-op remainder of the Recorder interface.
type fdBlockingDurationRecorder struct {
	fdOtelRecorder
	called atomic.Int64
	gotErr atomic.Value // string: cmd.Err() text ("" for nil)
	gotStr atomic.Value // string: cmd.String()
}

func (r *fdBlockingDurationRecorder) RecordOperationDuration(_ context.Context, _ time.Duration, cmd otel.Cmder, _ int, _ error, _ *pool.Conn, _ int) {
	r.called.Add(1)
	acc, ok := cmd.(interface {
		Err() error
		String() string
	})
	if !ok {
		return
	}
	if e := acc.Err(); e != nil {
		r.gotErr.Store(e.Error())
	} else {
		r.gotErr.Store("")
	}
	r.gotStr.Store(acc.String())
}

// TestFDReportReplyMetricsDurationCallbackNoDeadlock pins F3: a duration callback
// that reads its own command must not wedge the reader. reportReplyMetrics runs
// BEFORE req.complete() and the reader is not otherwise the batch's executor, so a
// naive cmd.Err()/cmd.String() would block on the batch.done the reader has not
// closed yet. The fix registers the reader as the batch's executor for the call,
// so the accessor guard returns the just-set view without blocking. Asserting the
// returned value (not just "did not hang") separates a working guard from one that
// short-circuits to a garbage view. Pure, no server.
func TestFDReportReplyMetricsDurationCallbackNoDeadlock(t *testing.T) {
	rec := &fdBlockingDurationRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	cmd := NewStatusCmd(ctx, "get", "k")
	cmd.SetVal("OK") // the "final result" the reader set before reporting
	b := newAPBatch()
	cmd.setReady(b) // cmd.Err()/cmd.String() now await b.done until it closes
	req := fdReq{cmd: cmd, batch: b, attempts: 1, writtenAt: time.Now()}

	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// e == nil exercises the duration callback (the blocking-view path). The
		// batch is deliberately left open, matching the real order where
		// reportReplyMetrics runs before req.complete().
		fd.reportReplyMetrics(ctx, req, nil, nil)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reportReplyMetrics wedged: a duration callback read its own command and blocked on batch.done the reader has not yet closed")
	}

	if got := rec.called.Load(); got != 1 {
		t.Fatalf("duration callback invoked %d times, want 1", got)
	}
	if v, _ := rec.gotErr.Load().(string); v != "" {
		t.Fatalf("callback saw cmd.Err() = %q, want \"\" (the just-set view)", v)
	}
	if v, _ := rec.gotStr.Load().(string); !strings.Contains(v, "OK") {
		t.Fatalf("callback saw cmd.String() = %q, want it to contain the set value OK", v)
	}
	// The guard must not have completed the batch; that stays req.complete()'s job.
	select {
	case <-b.done:
		t.Fatal("reportReplyMetrics closed the batch; completion is req.complete()'s job")
	default:
	}
}
