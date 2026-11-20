package redis

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/otel"
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

// TestClassifyCommandErrorNonServerError pins that only a server error reply
// yields a Redis error prefix. io.EOF reads "EOF", which looks like a prefix,
// but it is a dropped connection and must be reported as internal.
func TestClassifyCommandErrorNonServerError(t *testing.T) {
	for _, err := range []error{io.EOF, fmt.Errorf("read: %w", io.EOF), io.ErrUnexpectedEOF} {
		errorType, statusCode, isInternal := classifyCommandError(err)
		if errorType != "UNKNOWN" || statusCode != "UNKNOWN" || !isInternal {
			t.Errorf("classifyCommandError(%v) = %q, %q, %v, want UNKNOWN, UNKNOWN, true", err, errorType, statusCode, isInternal)
		}
	}

	for _, err := range []error{
		proto.RedisError("ERR unknown command"),
		fmt.Errorf("hook: %w", proto.RedisError("ERR unknown command")),
	} {
		errorType, statusCode, isInternal := classifyCommandError(err)
		if errorType != "ERR" || statusCode != "ERR" || isInternal {
			t.Errorf("classifyCommandError(%v) = %q, %q, %v, want ERR, ERR, false", err, errorType, statusCode, isInternal)
		}
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
	setTestErrorRecorder(func(_ context.Context, errorType string, _ *pool.Conn, _ string, _ bool, _ int) {
		calls.Add(1)
		gotType.Store(errorType)
	})
	defer otel.SetGlobalRecorder(nil)

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

// testErrorRecorder is a global recorder whose RecordError calls fn and that
// discards everything else, for tests pinning the client's error metric.
type testErrorRecorder struct {
	otel.Recorder
	fn func(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retries int)
}

func (r testErrorRecorder) RecordError(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retries int) {
	r.fn(ctx, errorType, cn, statusCode, isInternal, retries)
}

// setTestErrorRecorder installs a testErrorRecorder; undo with otel.SetGlobalRecorder(nil).
func setTestErrorRecorder(fn func(ctx context.Context, errorType string, cn *pool.Conn, statusCode string, isInternal bool, retries int)) {
	otel.SetGlobalRecorder(testErrorRecorder{Recorder: otel.NoopRecorder(), fn: fn})
}
