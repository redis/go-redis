package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
)

// processPipelineRetriesAfter continues an operation that already ran, so its
// retries continue the backoff too. It restarted at the first step, so after
// an FD attempt the pooled re-run's later retries slept like early ones. With
// 20 prior attempts the next backoff is at MaxRetryBackoff; the first step is
// at most 3*MinRetryBackoff.
func TestPipelineRetriesAfterContinueBackoff(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1) // the first run gets LOADING, the retry succeeds
	c := NewClient(&Options{
		Addr: srv.addr(), Protocol: 2, DisableIdentity: true,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: 200 * time.Millisecond,
	})
	defer c.Close()

	ctx := context.Background()
	cmd := NewStatusCmd(ctx, "set", "k", "v")
	start := time.Now()
	if err := c.processPipelineRetriesAfter(ctx, []Cmder{cmd}, 1, start, 20); err != nil {
		t.Fatalf("processPipelineRetriesAfter: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("the retry slept %v; after 20 prior attempts the backoff should be at its 200ms cap", elapsed)
	}
}

// A pipeline rejected before admission (its ctx already done) records one
// pipeline metric, as an ordinary pipeline does when withPipelineConn fails.
// The FD path recorded nothing.
func TestFDPipelineRecordsRejectedPipelineMetric(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	rec := &fdPipelineRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Exec err=%v, want context.Canceled", err)
	}
	if n := rec.pipelines.Load(); n != 1 {
		t.Fatalf("Pipeline().Exec: pipeline duration recorded %d times, want 1", n)
	}

	cmds := []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	if err := ap.FDPipelined(ctx, cmds); !errors.Is(err, context.Canceled) {
		t.Fatalf("FDPipelined err=%v, want context.Canceled", err)
	}
	if n := rec.pipelines.Load(); n != 2 {
		t.Fatalf("FDPipelined: pipeline duration recorded %d times in total, want 2", n)
	}
}

// A batch the Close-time flush never runs keeps its own metric: the flush
// marked every batch before running any, so when the carry hit a dead
// endpoint and the fresh queue was failed, fdPipelineExec skipped the failed
// batch's metric and nothing recorded it.
func TestFDShutdownFlushMarksOnlyBatchesItRuns(t *testing.T) {
	ctx := context.Background()
	dead := errors.New("dial tcp: connection refused")
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		q:        newFDQueue(8),
		runPipeline: func(_ context.Context, cmds []Cmder, _ int) error {
			setCmdsErr(cmds, dead)
			return dead
		},
	}
	carry := fdPipeGroupReqs(ctx, 1)
	fresh := fdPipeGroupReqs(ctx, 1)
	carry[0].batch.fdAttempts, fresh[0].batch.fdAttempts = 1, 1 // as submitBatch stamps them
	fd.q.push(fresh[0])
	fd.shutdownFlush(ctx, carry)

	if !carry[0].batch.fdFlushed {
		t.Fatal("the carried batch ran in the flush but is not marked flushed")
	}
	if carry[0].batch.fdAttempts < 2 {
		t.Fatalf("the carried batch ran again in the flush but reports %d issue(s)", carry[0].batch.fdAttempts)
	}
	if fresh[0].batch.fdFlushed {
		t.Fatal("the fresh batch never ran (the carry hit a dead endpoint) but is marked flushed; its failure would record no metric")
	}
	// Its attempt count must not count an issue that never happened.
	if a := fresh[0].batch.fdAttempts; a != 1 {
		t.Fatalf("the fresh batch never ran but reports %d issues, want 1", a)
	}
}

// Only a pipeline the flush ran in full was measured by the flush. When it ran
// only the unread tail, the FD metric still covers the whole operation.
func TestFDPipelineAllFlushed(t *testing.T) {
	a, b := newAPBatch(), newAPBatch()
	b.fdFlushed = true
	if fdPipelineAllFlushed([]*apBatch{a, b}) {
		t.Fatal("a pipeline with only its tail flushed reported as flushed in full")
	}
	a.fdFlushed = true
	if !fdPipelineAllFlushed([]*apBatch{a, b}) {
		t.Fatal("a pipeline flushed in full reported as not flushed")
	}
	if fdPipelineAllFlushed(nil) {
		t.Fatal("an empty pipeline reported as flushed")
	}
}
