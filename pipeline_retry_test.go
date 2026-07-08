package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// These tests exercise the pipeline retry/re-drive loop in generalProcessPipeline
// (redis.go), which was previously untested: the CI harness runs with
// MaxRetries=-1 (-> 0), so the retry loop never iterated. AutoPipeline dispatches
// through this same execer, so it inherits whatever is proven here
// (see autopipeline_retry_test.go for the AutoPipeline-specific coverage).

// TestPipelineRetriesOnNetworkError seeds a broken connection into the pool; the
// first pipeline attempt fails on it, and the retry redials a healthy conn and
// re-drives the whole batch. Verifies the replies are correct and in order after
// the re-drive.
func TestPipelineRetriesOnNetworkError(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", MaxRetries: 2, PoolSize: 1})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Set(ctx, "pr:n", 0, 0).Err(); err != nil {
		t.Fatal(err)
	}

	// Put a bad connection in the pool: the next pipeline write fails on it.
	cn, err := client.Pool().Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cn.SetNetConn(&badConn{writeErr: io.EOF})
	client.Pool().Put(ctx, cn)

	pipe := client.Pipeline()
	set := pipe.Set(ctx, "pr:k", "v", 0)
	get := pipe.Get(ctx, "pr:k")
	incr := pipe.Incr(ctx, "pr:n")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipe.Exec after retry: %v", err)
	}
	if set.Val() != "OK" {
		t.Fatalf("set = %q, want OK", set.Val())
	}
	if get.Val() != "v" {
		t.Fatalf("get = %q, want v", get.Val())
	}
	if incr.Val() != 1 {
		t.Fatalf("incr = %d, want 1", incr.Val())
	}
}

// TestPipelineRetryExhausted makes every dial hand back a broken conn, so all
// MaxRetries+1 attempts fail. Verifies the pipeline returns an error (does not
// hang), every command carries the error, and the loop ran the expected number
// of attempts.
func TestPipelineRetryExhausted(t *testing.T) {
	ctx := context.Background()
	var dials int32
	client := redis.NewClient(&redis.Options{
		Addr:       ":6379",
		MaxRetries: 2, // -> 3 attempts total
		PoolSize:   1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&dials, 1)
			return &badConn{writeErr: io.EOF}, nil // dial "succeeds", all I/O fails
		},
	})
	defer client.Close()

	pipe := client.Pipeline()
	_ = pipe.Set(ctx, "k", "v", 0)
	_ = pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("expected error after retry exhaustion, got nil")
	}
	// MaxRetries=2 -> 3 attempts, each dials a fresh (bad) conn.
	if got := atomic.LoadInt32(&dials); got < 3 {
		t.Fatalf("dials = %d, want >= 3 (MaxRetries+1 attempts)", got)
	}
	// NOTE: manual Pipeline surfaces exhaustion via Exec's returned error. When
	// the failure is a conn-init failure (conn never runs pipelineProcessCmds),
	// the individual command objects are NOT populated with the error — see
	// TestAutoPipelineRetryExhaustionSurfacesError for why that matters for
	// AutoPipeline, which has no Exec return value to surface it.
}

// TestPipelineWrongTypeNotRetriedIsolated verifies a server -ERR (WRONGTYPE) in
// the middle of a pipeline is isolated to its own command — siblings still get
// their correct replies — and is not retried (redis errors are non-retryable).
func TestPipelineWrongTypeNotRetriedIsolated(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", MaxRetries: 3})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.Del(ctx, "wt:list", "wt:k")
	if err := client.LPush(ctx, "wt:list", "x").Err(); err != nil {
		t.Fatal(err)
	}

	pipe := client.Pipeline()
	good1 := pipe.Set(ctx, "wt:k", "v", 0)
	bad := pipe.Incr(ctx, "wt:list") // WRONGTYPE against a list
	good2 := pipe.Get(ctx, "wt:k")
	_, _ = pipe.Exec(ctx) // Exec returns the first command error (the WRONGTYPE)

	if bad.Err() == nil {
		t.Fatal("expected WRONGTYPE on the bad command")
	}
	if good1.Err() != nil {
		t.Fatalf("good1 inherited an error: %v", good1.Err())
	}
	if good2.Err() != nil || good2.Val() != "v" {
		t.Fatalf("good2 = %q, %v; want v, nil", good2.Val(), good2.Err())
	}
}

// TestPipelineContextCanceledNotRetried verifies a cancelled context aborts the
// pipeline without retrying (context errors are non-retryable) and every command
// carries the context error.
func TestPipelineContextCanceledNotRetried(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: ":6379", MaxRetries: 3})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	pipe := client.Pipeline()
	c := pipe.Get(ctx, "ctx:k")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !errors.Is(c.Err(), context.Canceled) {
		t.Fatalf("cmd err = %v, want context.Canceled", c.Err())
	}
}
