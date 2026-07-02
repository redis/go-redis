package redis

import (
	"context"
	"testing"
	"time"
)

// TestAutoPipelineStopsGrowingDefaultWindow verifies that with the implicit
// default window (no MaxFlushDelay / AdaptiveDelay configured) a lone caller is
// flushed as soon as the queue stops growing, rather than waiting the whole
// defaultAccumulateWindow. The window is temporarily enlarged so the difference
// is seconds vs microseconds — no dependency on sub-millisecond wall-clock timing.
//
// The config is passed as an argument to AutoPipeline (the honored path;
// Options.AutoPipelineConfig is not consulted by the blocking face).
func TestAutoPipelineStopsGrowingDefaultWindow(t *testing.T) {
	saved := defaultAccumulateWindow
	defaultAccumulateWindow = 1 * time.Second
	defer func() { defaultAccumulateWindow = saved }()

	ctx := context.Background()
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	ap, err := client.AutoPipeline(&AutoPipelineConfig{
		MaxBatchSize:         300,
		MaxFlushDelay:        0, // default window -> stops-growing applies
		AdaptiveDelay:        false,
		MaxConcurrentBatches: 5,
		Unordered:            true,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()

	start := time.Now()
	// Typed call: goes through the engine (Do bypasses the pipeline entirely).
	cmd := ap.Set(ctx, "sg-key", "v", 0)
	if err := cmd.Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	elapsed := time.Since(start)

	// The 1s window is the upper bound; stops-growing must return in ~one gap
	// plus a round-trip. A generous 200ms ceiling makes this non-flaky while
	// still failing loudly if the full window is (re)introduced.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("single command took %v; stops-growing early exit not working "+
			"(would be ~%v if it waited the whole default window)", elapsed, defaultAccumulateWindow)
	}

	if v, err := client.Get(ctx, "sg-key").Result(); err != nil || v != "v" {
		t.Fatalf("get sg-key = %q, %v; want \"v\", nil", v, err)
	}
}

// TestAutoPipelineExplicitDelayWaitsFullWindow guards the gating: an explicit
// MaxFlushDelay is an intentional accumulation window and must still be waited
// in full (stops-growing must NOT short-circuit it), so a later change can't
// silently turn every explicit-delay command into an immediate flush.
func TestAutoPipelineExplicitDelayWaitsFullWindow(t *testing.T) {
	const delay = 100 * time.Millisecond

	ctx := context.Background()
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	ap, err := client.AutoPipeline(&AutoPipelineConfig{
		MaxBatchSize:         10000, // large: only the timer flushes
		MaxFlushDelay:        delay,
		AdaptiveDelay:        false,
		MaxConcurrentBatches: 5,
		Unordered:            true,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()

	start := time.Now()
	// Typed call: goes through the engine (Do bypasses the pipeline entirely).
	cmd := ap.Set(ctx, "ed-key", "v", 0)
	if err := cmd.Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	elapsed := time.Since(start)

	// It should wait roughly the full delay (not flush early on a stalled queue).
	if elapsed < delay/2 {
		t.Fatalf("single command flushed in %v; explicit MaxFlushDelay=%v was not "+
			"honored (stops-growing must not apply to an explicit window)", elapsed, delay)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("single command took %v; timer flush appears broken", elapsed)
	}

	if v, err := client.Get(ctx, "ed-key").Result(); err != nil || v != "v" {
		t.Fatalf("get ed-key = %q, %v; want \"v\", nil", v, err)
	}
}
