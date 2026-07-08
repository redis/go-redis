package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

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

	if _, err := client.AsyncAutoPipeline(&AutoPipelineConfig{NumShards: 4}); err == nil ||
		!strings.Contains(err.Error(), "Unordered") {
		t.Fatalf("async ordered NumShards=4: got err %v, want Unordered requirement", err)
	}

	aap, err := client.AsyncAutoPipeline(&AutoPipelineConfig{NumShards: 4, MaxConcurrentBatches: 4, Unordered: true})
	if err != nil {
		t.Fatalf("async unordered NumShards=4: %v", err)
	}
	_ = aap.Close()

	// Blocking face is exempt: callers wait per command and Submit is rejected.
	ap, err := client.AutoPipeline(&AutoPipelineConfig{NumShards: 4})
	if err != nil {
		t.Fatalf("blocking NumShards=4: %v", err)
	}
	_ = ap.Close()
}

// TestAdaptiveDelayRequiresMaxFlushDelay verifies the silent-no-op combination
// is rejected at construction.
func TestAdaptiveDelayRequiresMaxFlushDelay(t *testing.T) {
	cfg := &AutoPipelineConfig{AdaptiveDelay: true}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "MaxFlushDelay") {
		t.Fatalf("AdaptiveDelay without MaxFlushDelay: got %v, want validation error", err)
	}
	ok := &AutoPipelineConfig{AdaptiveDelay: true, MaxFlushDelay: time.Millisecond}
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
	aapCheck, err := client.AsyncAutoPipeline(&AutoPipelineConfig{
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
