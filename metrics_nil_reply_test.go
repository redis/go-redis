package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestClassifyCommandErrorNilReply pins that a Nil reply (GET on a missing key)
// gets its own NIL type, so the recorder can drop it. Before, Nil matched no case
// and every cache miss was counted as an UNKNOWN internal client error.
func TestClassifyCommandErrorNilReply(t *testing.T) {
	for _, err := range []error{Nil, fmt.Errorf("hook: %w", Nil)} {
		errorType, statusCode, isInternal := classifyCommandError(err)
		if errorType != "NIL" || statusCode != "NIL" || isInternal {
			t.Fatalf("classifyCommandError(%v) = %q, %q, %v, want NIL, NIL, false", err, errorType, statusCode, isInternal)
		}
	}

	errorType, _, _ := classifyCommandError(proto.RedisError("WRONGTYPE Operation"))
	if errorType != "WRONGTYPE" {
		t.Fatalf("errorType = %q, want WRONGTYPE", errorType)
	}
}

// TestPipelineNilReplyErrorType is the pipeline twin: generalProcessPipeline
// reports the first command's error, so a batch led by a miss emitted one UNKNOWN
// error per Exec. Needs a server — the error surfaces only from a real reply.
func TestPipelineNilReplyErrorType(t *testing.T) {
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}

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
	if got := calls.Load(); got != 1 {
		t.Fatalf("error callback invoked %d times for a pipelined miss, want 1", got)
	}
	if got := gotType.Load(); got != "NIL" {
		t.Fatalf("errorType = %v, want NIL", got)
	}
}
