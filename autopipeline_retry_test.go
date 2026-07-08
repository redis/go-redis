package redis_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineRetriesOnNetworkError verifies AutoPipeline inherits the
// pipeline retry/re-drive loop (it dispatches through the same execer as manual
// Pipeline). A broken conn is seeded; the batch's first attempt fails on it and
// the retry redials a healthy conn. Results are read through the futures, and
// must be correct after the re-drive.
//
// MaxFlushDelay forces the two commands into one real (>=2) batch so they run
// through the pipeline execer; a lone command would take the single-command
// fast path (Process) instead.
func TestAutoPipelineRetriesOnNetworkError(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", MaxRetries: 2, PoolSize: 1})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Set(ctx, "apr:n", 0, 0).Err(); err != nil {
		t.Fatal(err)
	}

	cn, err := client.Pool().Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cn.SetNetConn(&badConn{writeErr: io.EOF})
	client.Pool().Put(ctx, cn)

	ap, err := client.AsyncAutoPipeline(&redis.AutoPipelineConfig{
		MaxBatchSize:  300,
		MaxFlushDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	s := ap.Set(ctx, "apr:k", "v", 0)
	n := ap.Incr(ctx, "apr:n")
	if err := s.Err(); err != nil {
		t.Fatalf("set after retry: %v", err)
	}
	if err := n.Err(); err != nil {
		t.Fatalf("incr after retry: %v", err)
	}
	if s.Val() != "OK" {
		t.Fatalf("set = %q, want OK", s.Val())
	}
	if n.Val() != 1 {
		t.Fatalf("incr = %d, want 1", n.Val())
	}
}

// TestAutoPipelineRetryExhaustionSurfacesError checks that when the pipeline
// retry loop is EXHAUSTED (every attempt fails on a retryable error), the
// AutoPipeline commands surface the error to their futures.
//
// This matters more for AutoPipeline than for manual Pipeline: manual Pipeline
// returns the error from Exec, but AutoPipeline discards the pipeline error
// (dispatchCmds: `_ = ...processPipelineHook(...)`) and results are observable
// ONLY via each command's future. If generalProcessPipeline does not call
// setCmdsErr on exhaustion (it only does so on the early-exit branch), a caller
// sees a nil error and a zero value — silent loss.
func TestAutoPipelineRetryExhaustionSurfacesError(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:       ":6379",
		MaxRetries: 2,
		PoolSize:   1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &badConn{writeErr: io.EOF}, nil // every conn fails all I/O
		},
	})
	defer client.Close()

	ap, err := client.AsyncAutoPipeline(&redis.AutoPipelineConfig{
		MaxBatchSize:  300,
		MaxFlushDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	s := ap.Set(ctx, "k", "v", 0)
	n := ap.Incr(ctx, "n")
	if s.Err() == nil || n.Err() == nil {
		t.Fatalf("SILENT LOSS: AutoPipeline commands returned nil error on retry exhaustion (set err=%v, incr err=%v) — every attempt failed but the futures report success",
			s.Err(), n.Err())
	}
}
