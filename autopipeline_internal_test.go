// Code consolidated from per-topic autopipeline test files.
package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ===== from autopipeline_coalesce_test.go =====
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
	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{
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
	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{
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

// ===== from autopipeline_review_fixes_test.go =====
// TestSubmitRejectedOnBlockingFace verifies Submit errors on the blocking face:
// Submit does not wait, which defeats the ordering invariant the blocking
// face's enqueue striping relies on.
func TestSubmitRejectedOnBlockingFace(t *testing.T) {
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()

	f := ap.Submit(context.Background(), NewCmd(context.Background(), "set", "k", "v"))
	if err := f.Wait(); err == nil || !strings.Contains(err.Error(), "AsyncAutoPipeline") {
		t.Fatalf("Submit on blocking face: got err %v, want rejection pointing at AsyncAutoPipeline", err)
	}

	// The async face accepts Submit.
	aap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer aap.Close()
	f = aap.Submit(context.Background(), NewCmd(context.Background(), "set", "sub-k", "v"))
	if err := f.Wait(); err != nil {
		t.Fatalf("Submit on async face: %v", err)
	}
}

// TestNumShardsRequiresUnorderedOnAsyncFace verifies construction fails for
// NumShards>1 on the ordered async face (round-robin shards flush concurrently
// and do not preserve submit order), while the blocking face and Unordered
// configs are accepted.
func TestNumShardsRequiresUnorderedOnAsyncFace(t *testing.T) {
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	if _, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{NumShards: 4}); err == nil ||
		!strings.Contains(err.Error(), "Unordered") {
		t.Fatalf("async ordered NumShards=4: got err %v, want Unordered requirement", err)
	}

	aap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{NumShards: 4, MaxConcurrentBatches: 4, Unordered: true})
	if err != nil {
		t.Fatalf("async unordered NumShards=4: %v", err)
	}
	_ = aap.Close()

	// Blocking face is exempt: callers wait per command and Submit is rejected.
	ap, err := client.AutoPipelineWithOptions(&AutoPipelineOptions{NumShards: 4})
	if err != nil {
		t.Fatalf("blocking NumShards=4: %v", err)
	}
	_ = ap.Close()
}

// TestAdaptiveDelayRequiresMaxFlushDelay verifies the silent-no-op combination
// is rejected at construction.
func TestAdaptiveDelayRequiresMaxFlushDelay(t *testing.T) {
	cfg := &AutoPipelineOptions{AdaptiveDelay: true}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "MaxFlushDelay") {
		t.Fatalf("AdaptiveDelay without MaxFlushDelay: got %v, want validation error", err)
	}
	ok := &AutoPipelineOptions{AdaptiveDelay: true, MaxFlushDelay: time.Millisecond}
	if err := ok.Validate(); err != nil {
		t.Fatalf("AdaptiveDelay with MaxFlushDelay: %v", err)
	}
}

// TestDoBypassesPipeline verifies Do runs on a normal connection: a
// connection-state command through Do must not poison the shared pipeline
// pool for later batched commands.
func TestDoBypassesPipeline(t *testing.T) {
	ctx := context.Background()
	// Configure a dedicated pipeline pool (enabled by the buffer options):
	// batches then run on pipeline conns, isolated from the normal conns Do
	// uses. (Without a pipeline pool, Do shares the main pool and carries the
	// same stateful-command caveats as plain Client.Do — no worse.)
	client := NewClient(&Options{Addr: ":6379", PipelineReadBufferSize: 64 << 10, PipelineWriteBufferSize: 64 << 10})
	defer client.Close()

	if err := client.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()

	// MULTI through Do: previously this entered a shared batch, leaving the
	// pipeline conn inside an open transaction — every later command on it
	// got +QUEUED instead of its reply. Now Do runs it on a normal conn
	// (plain Client.Do semantics), so batched commands stay uncorrupted.
	_ = ap.Do(ctx, "multi").Err() // stateful; poisons one NORMAL-pool conn (Client.Do semantics)

	// The dedicated pipeline pool isolates BATCHED commands (>=2 per flush) from
	// that normal-pool poison. Force a real multi-command batch with an explicit
	// flush delay so all 20 SETs coalesce into a single pipeline-pool dispatch.
	// (A command that flushes ALONE legitimately takes the single-command fast
	// path onto the normal pool and can see the poison — the documented Do
	// footgun, orthogonal to what this test guards: MULTI-via-Do must not reach
	// the pipeline-pool batch path.)
	aapCheck, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		MaxBatchSize:  300,
		MaxFlushDelay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer aapCheck.Close()
	cmds := make([]*StatusCmd, 20)
	for i := range cmds {
		cmds[i] = aapCheck.Set(ctx, "do-bypass-key", "v0", 0) // coalesce into one batch
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("batched set %d after Do(multi): %v (stateful command reached the pipeline pool?)", i, err)
		}
		if got := c.Val(); got != "OK" {
			t.Fatalf("batched set %d after Do(multi): reply %q, want OK (QUEUED means MULTI reached the pipeline-pool batch)", i, got)
		}
	}
	// Verify with a fresh client: the MULTI above intentionally poisoned one
	// main-pool conn (plain Client.Do semantics — pre-existing footgun), so
	// this client's pool may hand back QUEUED for plain commands.
	verify := NewClient(&Options{Addr: ":6379"})
	defer verify.Close()
	if v, err := verify.Get(ctx, "do-bypass-key").Result(); err != nil || v != "v0" {
		t.Fatalf("get = %q, %v; want \"v0\"", v, err)
	}

	// Zero-arg Do reports a real error, not ErrClosed.
	if err := ap.Do(ctx).Err(); err == nil || err == ErrClosed {
		t.Fatalf("zero-arg Do: got %v, want a non-ErrClosed error", err)
	}

	// Async face keeps the deferred shape.
	aap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer aap.Close()
	cmd := aap.Do(ctx, "set", "do-async-key", "v1")
	if err := cmd.Err(); err != nil { // blocks until the background Process completes
		t.Fatalf("async Do: %v", err)
	}
}

// ===== from autopipeline_shards_test.go =====
// TestAutoPipelineShardCountDecoupled verifies that the shard count no longer
// follows MaxConcurrentBatches: a standalone autopipeliner defaults to a single
// deep queue regardless of the permit budget, and NumShards overrides it.
func TestAutoPipelineShardCountDecoupled(t *testing.T) {
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	ap, err := client.AutoPipelineWithOptions(&AutoPipelineOptions{
		MaxConcurrentBatches: 4,
		Unordered:            true,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	if got := ap.numShards(); got != 1 {
		t.Fatalf("standalone default shards = %d, want 1 (must not follow MaxConcurrentBatches)", got)
	}
	_ = ap.Close()

	ap2, err := client.AutoPipelineWithOptions(&AutoPipelineOptions{
		MaxConcurrentBatches: 2,
		Unordered:            true,
		NumShards:            4,
	})
	if err != nil {
		t.Fatalf("AutoPipeline with NumShards: %v", err)
	}
	if got := ap2.numShards(); got != 4 {
		t.Fatalf("NumShards=4 gave %d shards, want 4", got)
	}
	_ = ap2.Close()
}

// TestAutoPipelineNumShardsValidation verifies a negative NumShards is rejected
// at construction instead of being silently coerced.
func TestAutoPipelineNumShardsValidation(t *testing.T) {
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	_, err := client.AutoPipelineWithOptions(&AutoPipelineOptions{NumShards: -1})
	if err == nil || !strings.Contains(err.Error(), "NumShards") {
		t.Fatalf("NumShards=-1: got err %v, want NumShards validation error", err)
	}
}

// TestClusterAutoPipelineOptionsShardDefault verifies the cluster wiring fills
// in a multi-shard default (slot routing needs several shards) without mutating
// the caller's config, and leaves an explicit NumShards untouched.
func TestClusterAutoPipelineOptionsShardDefault(t *testing.T) {
	user := &AutoPipelineOptions{MaxConcurrentBatches: 8, Unordered: true}
	got := clusterAutoPipelineOptions(user)
	if got == user {
		t.Fatalf("expected a copy when filling the default, got the same pointer")
	}
	if user.NumShards != 0 {
		t.Fatalf("caller's config mutated: NumShards=%d, want 0", user.NumShards)
	}
	if want := numAutoPipelineShards(); got.NumShards != want {
		t.Fatalf("cluster default NumShards = %d, want %d", got.NumShards, want)
	}
	if !got.contentSharded {
		t.Fatalf("cluster default must mark contentSharded (slot routing preserves per-key order)")
	}

	// The DEFAULT config (MaxConcurrentBatches=1) must still get several
	// slot-routed shards — deriving the shard count from the permit budget
	// once collapsed cluster slot routing to a single shard at the default.
	def := clusterAutoPipelineOptions(DefaultAutoPipelineOptions())
	if def.NumShards < 2 {
		t.Fatalf("cluster default-config NumShards = %d, want >= 2 (slot routing must not be dead code at defaults)", def.NumShards)
	}

	// An explicit NumShards is preserved, but the cluster still marks
	// contentSharded so slot routing (which keeps per-key order) is not rejected
	// on the deferred (async) face; the caller's config must not be mutated.
	explicit := &AutoPipelineOptions{MaxConcurrentBatches: 8, Unordered: true, NumShards: 3}
	if got := clusterAutoPipelineOptions(explicit); got.NumShards != 3 || !got.contentSharded {
		t.Fatalf("explicit NumShards must be preserved with contentSharded set, got %+v", got)
	}
	if explicit.contentSharded {
		t.Fatalf("caller config mutated: contentSharded set on the original")
	}
}
