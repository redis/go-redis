package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// AutoPipeline context contract (previously untested; WaitContext had zero
// coverage). The documented contract:
//   - a caller's context is NOT honored once a command is enqueued (execution
//     runs on context.Background); it only gates the wait via WaitContext.
//   - WaitContext abandons the wait on ctx done, but the command still executes
//     and its result becomes readable via a later Wait.
//   - blocking commands (readTimeout != nil) run directly with the caller ctx
//     and DO honor it.

// AP-C1: a caller ctx cancelled after enqueue does not cancel the command; the
// result is still produced.
func TestAPContextCancelDoesNotCancelExecution(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	cctx, cancel := context.WithCancel(ctx)
	f := ap.Set(cctx, "apctx:k", "v", 0)
	cancel() // cancel after submit, before the batch flushes/completes

	if err := f.Err(); err != nil {
		t.Fatalf("command should still execute after caller ctx cancel, got %v", err)
	}
	if f.Val() != "OK" {
		t.Fatalf("val = %q, want OK", f.Val())
	}
	if v, _ := client.Get(ctx, "apctx:k").Result(); v != "v" {
		t.Fatalf("value not persisted: %q", v)
	}
	client.Del(ctx, "apctx:k")
}

// AP-C2: WaitContext returns ctx.Err while the batch is in flight, and a later
// Wait returns the real result.
func TestAPWaitContextAbandonsWaitOnly(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Hold the batch open long enough that the WaitContext deadline fires first.
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 300, MaxFlushDelay: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	f := ap.Submit(ctx, redis.NewCmd(ctx, "set", "apctx:wc", "v"))

	wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := f.WaitContext(wctx); err != context.DeadlineExceeded {
		t.Fatalf("WaitContext err = %v, want DeadlineExceeded (batch held open)", err)
	}
	// The command still completes; a plain Wait returns the real result.
	if err := f.Wait(); err != nil {
		t.Fatalf("Wait after WaitContext timeout: %v", err)
	}
	client.Del(ctx, "apctx:wc")
}

// AP-C3: WaitContext returns the real result when the batch completes first.
func TestAPWaitContextReturnsResult(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	f := ap.Submit(ctx, redis.NewCmd(ctx, "set", "apctx:ok", "v"))
	if err := f.WaitContext(ctx); err != nil {
		t.Fatalf("WaitContext: %v", err)
	}
	client.Del(ctx, "apctx:ok")
}

// AP-C4: a blocking command runs directly with the caller ctx (not batched on
// Background). With ContextTimeoutEnabled the ctx deadline reaches the read, so a
// BLPOP is cut short by the ctx — proving the caller ctx flows to the blocking
// path (unlike batched commands, which execute on Background).
func TestAPBlockingCommandHonorsContext(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", ContextTimeoutEnabled: true})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AutoPipeline() // blocking face
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	client.Del(ctx, "apctx:blk")

	bctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	// BLPOP on an empty list blocks; the ctx deadline must cut it short.
	err = ap.BLPop(bctx, 5*time.Second, "apctx:blk").Err()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected BLPOP to fail on ctx deadline")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("BLPOP did not honor ctx deadline (took %v)", elapsed)
	}
}

// AP-C5: submitting after Close returns ErrClosed.
func TestAPSubmitAfterCloseErrClosed(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f := ap.Set(ctx, "apctx:closed", "v", 0)
	if err := f.Err(); err != redis.ErrClosed {
		t.Fatalf("submit after close err = %v, want ErrClosed", err)
	}
}

// AP-C6: WaitContext on a zero AutoFuture returns an error, not a panic.
func TestAPZeroFutureWaitContext(t *testing.T) {
	var f redis.AutoFuture
	if err := f.WaitContext(context.Background()); err == nil {
		t.Fatal("zero AutoFuture WaitContext should return an error")
	}
}
