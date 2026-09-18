package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestRecordCommandErrorSkipsNilReply pins that a Nil reply (GET on a missing key)
// never reaches the native error callback. Nil matches no classification case, so
// before the guard every cache miss was counted as an UNKNOWN internal client error.
func TestRecordCommandErrorSkipsNilReply(t *testing.T) {
	var calls atomic.Int64
	var gotType atomic.Value
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(_ context.Context, errorType string, _ *pool.Conn, _ string, _ bool, _ int) {
			calls.Add(1)
			gotType.Store(errorType)
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()

	recordCommandError(ctx, Nil, nil, 0)
	if got := calls.Load(); got != 0 {
		t.Fatalf("error callback invoked %d times for Nil, want 0 (an empty reply is not a failure)", got)
	}

	// A hook may wrap the command error, so the guard unwraps rather than comparing.
	recordCommandError(ctx, fmt.Errorf("hook: %w", Nil), nil, 0)
	if got := calls.Load(); got != 0 {
		t.Fatalf("error callback invoked %d times for a wrapped Nil, want 0", got)
	}

	recordCommandError(ctx, proto.RedisError("WRONGTYPE Operation"), nil, 0)
	if got := calls.Load(); got != 1 {
		t.Fatalf("error callback invoked %d times, want 1: a real Redis error must still be recorded", got)
	}
	if got := gotType.Load(); got != "WRONGTYPE" {
		t.Fatalf("errorType = %v, want WRONGTYPE", got)
	}
}

// TestPipelineNilReplySkipsErrorMetric is the pipeline twin: generalProcessPipeline
// reports the first command's error, so a batch led by a miss emitted one UNKNOWN
// error per Exec. Needs a server — the error surfaces only from a real reply.
func TestPipelineNilReplySkipsErrorMetric(t *testing.T) {
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}

	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()

	if err := client.Del(ctx, "nil-reply-metric").Err(); err != nil {
		t.Fatalf("del: %v", err)
	}
	calls.Store(0)

	pipe := client.Pipeline()
	pipe.Get(ctx, "nil-reply-metric")
	pipe.Ping(ctx)
	if _, err := pipe.Exec(ctx); err != Nil {
		t.Fatalf("Exec err = %v, want Nil", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("error callback invoked %d times for a pipelined miss, want 0", got)
	}
}
