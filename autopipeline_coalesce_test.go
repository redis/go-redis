package redis

import (
	"context"
	"testing"
	"time"
)

// TestAutoPipelineLoneCallerFlushesImmediately verifies that with the default
// config (no MaxFlushDelay / AdaptiveDelay) a lone caller's command flushes
// without any coalescing wait: an idle autopipeliner (no batches in flight, no
// expected arrivals) must dispatch in a single round trip. This pins the fix
// for the low-concurrency latency tax — the engine used to arm a ~20µs
// debounce timer per flush, which fires ~1ms late on an idle host and made a
// lone caller ~5x slower than a plain client.
func TestAutoPipelineLoneCallerFlushesImmediately(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	ap, err := newAutoPipeliner(client, &AutoPipelineConfig{
		MaxBatchSize:         300,
		MaxFlushDelay:        0, // default: no coalescing wait when idle
		AdaptiveDelay:        false,
		MaxConcurrentBatches: 5,
		Unordered:            true,
	}, true)
	if err != nil {
		t.Fatalf("newAutoPipeliner: %v", err)
	}
	defer ap.Close()

	// Warm the connection, then time a lone command. Generous ceiling so CI
	// jitter cannot flake it while a reintroduced per-flush wait (millisecond
	// scale on idle hosts, per command) would still fail loudly across the
	// sample of commands below.
	if err := ap.Set(ctx, "lc-warm", "v", 0).Err(); err != nil {
		t.Fatalf("warm set: %v", err)
	}
	const n = 20
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := ap.Set(ctx, "lc-key", "v", 0).Err(); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	perCmd := time.Since(start) / n

	if perCmd > 50*time.Millisecond {
		t.Fatalf("lone caller averaged %v per command; the idle path must flush "+
			"immediately (no coalescing timer)", perCmd)
	}

	if v, err := client.Get(ctx, "lc-key").Result(); err != nil || v != "v" {
		t.Fatalf("get lc-key = %q, %v; want \"v\", nil", v, err)
	}
}

// TestAutoPipelineExplicitDelayWaitsFullWindow guards the gating: an explicit
// MaxFlushDelay is an intentional accumulation window and must still be waited
// in full (the idle fast path must NOT short-circuit it), so a later change
// can't silently turn every explicit-delay command into an immediate flush.
func TestAutoPipelineExplicitDelayWaitsFullWindow(t *testing.T) {
	const delay = 100 * time.Millisecond

	ctx := context.Background()
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	ap, err := newAutoPipeliner(client, &AutoPipelineConfig{
		MaxBatchSize:         10000, // large: only the timer flushes
		MaxFlushDelay:        delay,
		AdaptiveDelay:        false,
		MaxConcurrentBatches: 5,
		Unordered:            true,
	}, true)
	if err != nil {
		t.Fatalf("newAutoPipeliner: %v", err)
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
			"honored (the idle fast path must not apply to an explicit window)", elapsed, delay)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("single command took %v; timer flush appears broken", elapsed)
	}

	if v, err := client.Get(ctx, "ed-key").Result(); err != nil || v != "v" {
		t.Fatalf("get ed-key = %q, %v; want \"v\", nil", v, err)
	}
}

// TestObserveBatchExecEWMA pins the round-trip smoothing behavior.
func TestObserveBatchExecEWMA(t *testing.T) {
	ap := &AutoPipeliner{}
	ap.observeBatchExec(0) // ignored
	if got := ap.execEWMA.Load(); got != 0 {
		t.Fatalf("zero sample stored: %d", got)
	}
	ap.observeBatchExec(80 * time.Millisecond) // first sample: stored as-is
	if got := ap.execEWMA.Load(); got != int64(80*time.Millisecond) {
		t.Fatalf("first sample = %d", got)
	}
	ap.observeBatchExec(160 * time.Millisecond) // ewma += (sample-ewma)/8
	want := int64(80*time.Millisecond) + int64(80*time.Millisecond)/8
	if got := ap.execEWMA.Load(); got != want {
		t.Fatalf("ewma after second sample = %d, want %d", got, want)
	}
}

// TestSilenceGap pins the silence-fallback derivation: loopback round trips
// clamp to the floor, slow links scale as exec/8, and the ceiling bounds how
// long a stale expectation can delay a flush.
func TestSilenceGap(t *testing.T) {
	cases := []struct {
		name string
		ewma time.Duration
		want time.Duration
	}{
		{"no sample yet", 0, silenceGapFloor},
		{"loopback 100µs", 100 * time.Microsecond, silenceGapFloor},
		{"fast lan 1ms", time.Millisecond, silenceGapFloor},
		{"lan 4ms", 4 * time.Millisecond, 500 * time.Microsecond},
		{"wan 12ms", 12 * time.Millisecond, 1500 * time.Microsecond},
		{"slow wan 200ms (ceiling)", 200 * time.Millisecond, silenceGapCeil},
	}
	for _, c := range cases {
		ap := &AutoPipeliner{}
		if c.ewma > 0 {
			ap.execEWMA.Store(int64(c.ewma))
		}
		if got := ap.silenceGap(); got != c.want {
			t.Errorf("%s: silenceGap() with ewma %v = %v, want %v", c.name, c.ewma, got, c.want)
		}
	}
}

// TestAutoPipelineWaveCoalesces verifies the load path: concurrent blocking
// callers cycling against the pipeliner make sustained progress and their
// commands all execute. (The depth of the batches is a performance property
// covered by benchmarks; this guards liveness of the expected-arrivals /
// in-flight wait machinery under a closed-loop wave.)
func TestAutoPipelineWaveCoalesces(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	ap, err := newAutoPipeliner(client, nil, true) // defaults: ordered, no delay
	if err != nil {
		t.Fatalf("newAutoPipeliner: %v", err)
	}
	defer ap.Close()

	const workers, iters = 16, 50
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func() {
			var err error
			for i := 0; i < iters; i++ {
				if e := ap.Incr(ctx, "wave-ctr").Err(); e != nil {
					err = e
					break
				}
			}
			errCh <- err
		}()
	}
	deadline := time.After(30 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("worker error: %v", err)
			}
		case <-deadline:
			t.Fatalf("wave stalled: coalescing wait is not making progress")
		}
	}
	n, err := client.Get(ctx, "wave-ctr").Int()
	if err != nil || n != workers*iters {
		t.Fatalf("herd-ctr = %d, %v; want %d", n, err, workers*iters)
	}
}
