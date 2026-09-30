package redis

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestFDFailQueueSkipsPipelinedErrorMetric pins that failQueue, like
// failReqs, reports no per-command error for a pipelined command.
// fdPipelineExec reports the batch once, as an ordinary pipeline does, so a
// per-command report here would count the same failure twice. A plain
// command in the same backlog is still reported.
func TestFDFailQueueSkipsPipelinedErrorMetric(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}, q: newFDQueue(4)}
	group := newAPBatch()
	group.fdGroup = group
	var batches []*apBatch
	for i := 0; i < 3; i++ {
		b := group
		if i > 0 {
			b = newAPBatch()
			b.fdGroup = group
		}
		batches = append(batches, b)
		fd.q.push(fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v"), batch: b, ctx: ctx, attempts: 1, pipelined: true})
	}
	plain := newAPBatch()
	batches = append(batches, plain)
	fd.q.push(fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v"), batch: plain, ctx: ctx, attempts: 1})

	fd.failQueue(ErrClosed)

	if got := calls.Load(); got != 1 {
		t.Fatalf("error callback invoked %d times, want 1 (the plain command only)", got)
	}
	for i, b := range batches {
		select {
		case <-b.done:
		default:
			t.Fatalf("req %d not completed", i)
		}
	}
}
