package redis

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
)

// Pipeline hooks and pipeline metrics on the full-duplex path. A pipeline
// batch runs inside the client's ProcessPipelineHook chain, exactly once, and
// records one pipeline duration, as an ordinary pipeline does, while staying
// on the held connection.

// fdRodePipe reports whether cmd ran as a full-duplex pipeline batch: only
// that path installs a batch the reader stamps with an attempt count.
func fdRodePipe(cmd Cmder) bool {
	var b *apBatch
	switch c := cmd.(type) {
	case *StatusCmd:
		b = c.ready.Load()
	case *StringCmd:
		b = c.ready.Load()
	}
	return b != nil && b.fdAttempts > 0
}

// fdWarm runs one pipeline so the held connection is leased before a test
// measures: the lease's handshake (HELLO) goes through the client's process
// path once, which is connection setup, not a pipelined command.
func fdWarm(t *testing.T, ap *AutoPipeliner) {
	t.Helper()
	pipe := ap.Pipeline()
	pipe.Get(context.Background(), "warm")
	if _, err := pipe.Exec(context.Background()); err != nil && !errors.Is(err, Nil) {
		t.Fatalf("warm-up Exec: %v", err)
	}
}

// fdPipeHooksCounter counts pipeline and per-command hook calls, and records what a
// pipeline hook sees in the last command after next() returns.
type fdPipeHooksCounter struct {
	pipelines, processes atomic.Int64
	afterNext            atomic.Value // string
}

func (h *fdPipeHooksCounter) DialHook(next DialHook) DialHook { return next }
func (h *fdPipeHooksCounter) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		h.processes.Add(1)
		return next(ctx, cmd)
	}
}
func (h *fdPipeHooksCounter) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.pipelines.Add(1)
		err := next(ctx, cmds)
		if s, ok := cmds[len(cmds)-1].(*StringCmd); ok {
			h.afterNext.Store(s.Val())
		}
		return err
	}
}

func TestFDPipelineHooksWrapTheFDBatch(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	fdWarm(t, ap)
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !fdRodePipe(set) || !fdRodePipe(get) {
		t.Fatal("a hook took the pipeline off the FD path")
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times, want 1", n)
	}
	if n := h.processes.Load(); n != 0 {
		t.Fatalf("ProcessHook ran %d times for a pipeline, want 0 (as an ordinary pipeline)", n)
	}
	if v, _ := h.afterNext.Load().(string); v != "v" {
		t.Fatalf("the hook saw %q after next(), want the GET result", v)
	}
}

func TestFDPipelinedRunsPipelineHooks(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	fdWarm(t, ap)
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	get := NewStringCmd(ctx, "get", "k")
	if err := ap.FDPipelined(ctx, []Cmder{NewStatusCmd(ctx, "set", "k", "v"), get}); err != nil {
		t.Fatalf("FDPipelined: %v", err)
	}
	if !fdRodePipe(get) {
		t.Fatal("FDPipelined did not run on the FD path")
	}
	if h.pipelines.Load() != 1 || h.processes.Load() != 0 {
		t.Fatalf("hooks: pipeline=%d process=%d, want 1 and 0", h.pipelines.Load(), h.processes.Load())
	}
}

// A batch that cannot ride the pipe runs the ordinary pipeline INSIDE the same
// hook chain, so the hooks still run once, not once around the refusal and
// again around the fallback.
func TestFDPipelineHooksRunOnceOnFallback(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	var buf strings.Builder
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	_ = pipe.Process(ctx, NewRawWriteToCmd(ctx, &buf, "get", "k")) // NoRetry: ordinary path
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if fdRodePipe(set) {
		t.Fatal("a batch with a NoRetry command rode the FD path")
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times across the fallback, want 1", n)
	}
}

// The whole-batch retry runs inside the hook chain, as generalProcessPipeline's
// retries do, so the hooks see one pipeline.
func TestFDPipelineHooksRunOnceAcrossRetry(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v" {
		t.Fatalf("get: v=%q err=%v", v, err)
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times across the retry, want 1", n)
	}
}

// An OTel recorder gets one pipeline duration for an FD batch and no
// per-command durations, as for an ordinary pipeline; the batch stays on the
// FD path. Both entry points.
func TestFDPipelineRecordsPipelineMetricOnFD(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	fdWarm(t, ap)
	rec := &fdPipelineRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !fdRodePipe(get) {
		t.Fatal("an OTel recorder took the pipeline off the FD path")
	}
	if rec.pipelines.Load() != 1 || rec.cmdCount.Load() != 2 {
		t.Fatalf("pipeline metric: count=%d cmdCount=%d, want 1 and 2", rec.pipelines.Load(), rec.cmdCount.Load())
	}
	if n := rec.opDurations.Load(); n != 0 {
		t.Fatalf("%d per-command durations recorded for a pipeline, want 0", n)
	}

	get2 := NewStringCmd(ctx, "get", "k")
	if err := ap.FDPipelined(ctx, []Cmder{get2}); err != nil {
		t.Fatalf("FDPipelined: %v", err)
	}
	if rec.pipelines.Load() != 2 || rec.opDurations.Load() != 0 {
		t.Fatalf("after FDPipelined: pipelines=%d per-command=%d, want 2 and 0", rec.pipelines.Load(), rec.opDurations.Load())
	}
}
