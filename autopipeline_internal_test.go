// Code consolidated from per-topic autopipeline test files.
package redis

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// internalTestRedisAddr mirrors main_test.go's redisAddr for the internal
// (package redis) tests, which cannot see the redis_test harness variables.
func internalTestRedisAddr() string {
	if p := os.Getenv("REDIS_PORT"); p != "" {
		return ":" + p
	}
	return ":6379"
}

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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr(), PipelineReadBufferSize: 64 << 10, PipelineWriteBufferSize: 64 << 10})
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
	verify := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
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
	if want := numAutoPipelineShards(); def.NumShards != want {
		t.Fatalf("cluster default-config NumShards = %d, want %d (slot routing must not be collapsed by the permit budget)", def.NumShards, want)
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

// pushInjectionScript builds a RESP3 stream: a push frame wedged between two
// bulk-string replies. A push-blind reader consumes the push frame AS the
// first command's reply and shifts every subsequent reply by one.
func pushInjectionScript() []byte {
	return []byte(">2\r\n$8\r\nTESTPUSH\r\n$4\r\ndata\r\n" + // push frame (no handler: consumed+ignored)
		"$2\r\nv1\r\n" + // reply for cmd 1
		"$2\r\nv2\r\n") // reply for cmd 2
}

func pushInjectionConn(t *testing.T) *pool.Conn {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	return pool.NewConn(client)
}

// TestStandalonePipelineReadDrainsPushMidBatch pins the standalone pipeline
// read loop: a RESP3 push notification arriving mid-batch must be drained
// before each reply read, not consumed as a command's reply (which would
// silently misassociate every following reply).
func TestStandalonePipelineReadDrainsPushMidBatch(t *testing.T) {
	opt := &Options{Addr: "127.0.0.1:1", Protocol: 3}
	client := NewClient(opt)
	defer client.Close()

	ctx := context.Background()
	cn := pushInjectionConn(t)
	rd := proto.NewReader(bytes.NewReader(pushInjectionScript()))

	cmd1 := NewStringCmd(ctx, "get", "k1")
	cmd2 := NewStringCmd(ctx, "get", "k2")
	if err := client.baseClient.pipelineReadCmds(ctx, cn, rd, []Cmder{cmd1, cmd2}); err != nil {
		t.Fatalf("pipelineReadCmds: %v", err)
	}
	if cmd1.Val() != "v1" || cmd2.Val() != "v2" {
		t.Fatalf("replies misassociated: cmd1=%q cmd2=%q, want v1/v2 (push frame consumed as a reply?)",
			cmd1.Val(), cmd2.Val())
	}
}

// TestClusterPipelineReadDrainsPushMidBatch pins the CLUSTER pipeline read
// loop — the one the autopipeliner routes all cluster traffic through, and
// the only reader that was push-blind before this PR: without the drain, a
// maintnotifications MOVING frame mid-batch shifted every subsequent reply
// by one for existing ClusterClient.Pipeline() users too.
func TestClusterPipelineReadDrainsPushMidBatch(t *testing.T) {
	copt := &ClusterOptions{Addrs: []string{"127.0.0.1:1"}, Protocol: 3}
	cc := NewClusterClient(copt)
	defer cc.Close()

	nodeClient := NewClient(&Options{Addr: "127.0.0.1:1", Protocol: 3})
	defer nodeClient.Close()
	node := &clusterNode{Client: nodeClient}

	ctx := context.Background()
	cn := pushInjectionConn(t)
	rd := proto.NewReader(bytes.NewReader(pushInjectionScript()))

	cmd1 := NewStringCmd(ctx, "get", "k1")
	cmd2 := NewStringCmd(ctx, "get", "k2")
	failed := newCmdsMap()
	if err := cc.pipelineReadCmds(ctx, node, cn, rd, []Cmder{cmd1, cmd2}, failed); err != nil {
		t.Fatalf("pipelineReadCmds: %v", err)
	}
	if cmd1.Val() != "v1" || cmd2.Val() != "v2" {
		t.Fatalf("replies misassociated: cmd1=%q cmd2=%q, want v1/v2 (push frame consumed as a reply?)",
			cmd1.Val(), cmd2.Val())
	}
	if len(failed.m) != 0 {
		t.Fatalf("unexpected remapped commands: %d", len(failed.m))
	}
}

// TestDispatchChunkedAbortsAfterFailedPrefix pins chunked dispatch's
// ordered-stream contract at the unit level: when an earlier chunk dies on a
// transport-class failure (here a hook abort), later chunks are NOT
// dispatched — the unchunked path fails the batch as a unit, and later
// commands must not overtake a failed prefix. All commands carry the failure.
// (Unit-level on purpose: through the public API the byte cap wakes the
// flusher mid-submission, so one logical batch may split into several drains
// and the "single dispatch" assertion races. Direct dispatch pins one drain.)
func TestDispatchChunkedAbortsAfterFailedPrefix(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr(), MaxRetries: -1})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	var dispatches atomic.Int32
	client.AddHook(chunkBreakerHook{dispatches: &dispatches})

	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		MaxBatchSize:  100,
		MaxBatchBytes: 150 * 1024, // ~2 x 64KiB per chunk -> 5 chunks of 2
	})
	if err != nil {
		t.Fatal(err)
	}

	value := strings.Repeat("x", 64*1024)
	cmds := make([]Cmder, 10)
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "cab:"+strconv.Itoa(i), value)
	}
	ap.dispatchCmdsMaybeChunked(ctx, [][]Cmder{cmds}, len(cmds))

	for i, cmd := range cmds {
		if err := cmd.Err(); err == nil || !strings.Contains(err.Error(), "chunk breaker") {
			t.Fatalf("cmd %d: err = %v, want the chunk breaker error on every command", i, err)
		}
	}
	if n := dispatches.Load(); n != 1 {
		t.Fatalf("hook saw %d chunk dispatches, want exactly 1 (later chunks must not run after a failed prefix)", n)
	}
}

// chunkBreakerHook aborts every pipeline dispatch it sees (without calling
// next) and counts them.
type chunkBreakerHook struct {
	dispatches *atomic.Int32
}

func (h chunkBreakerHook) DialHook(next DialHook) DialHook          { return next }
func (h chunkBreakerHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (h chunkBreakerHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.dispatches.Add(1)
		return errors.New("chunk breaker")
	}
}

// TestEnqueueStampsReadyUnderStripeLock pins the setReady ordering the
// cluster node-executor registration depends on: on the deferred face the
// gating batch is published on the command BEFORE it becomes visible to a
// drain (both sides hold the stripe lock), so a flush racing the submitter
// can never take a command whose readyBatch is still nil — which would skip
// its batch during node-executor registration and let a node hook
// self-deadlock (codex on #3942).
func TestEnqueueStampsReadyUnderStripeLock(t *testing.T) {
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		MaxBatchSize:  100,
		MaxFlushDelay: time.Hour, // hold the drain so the queue state is inspectable
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := NewStatusCmd(context.Background(), "set", "readystamp", "v")
	batch := ap.enqueue(cmd)
	if got := cmd.readyBatch(); got != batch {
		t.Fatalf("readyBatch right after enqueue = %p, want the enqueued batch %p (must be stamped under the stripe lock)", got, batch)
	}
	if err := ap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Err(); err != nil {
		t.Fatalf("drained command: %v", err)
	}
}

// TestConnStateClusterCommandsRunOutsidePipeline pins the divert set for the
// connection-scoped cluster state commands: queued onto a shared pipeline
// conn they would leak replica-reads (or a pending redirect) to every later
// batch on that conn (codex on #3942).
func TestConnStateClusterCommandsRunOutsidePipeline(t *testing.T) {
	for _, name := range []string{"readonly", "readwrite", "asking"} {
		if !runsOutsidePipeline(name) {
			t.Errorf("runsOutsidePipeline(%q) = false, want true", name)
		}
	}
}

// TestAPDelegatesClusterWideOverrides is the guard for the recurring
// "AutoPipeliner shadows a ClusterClient override" class: every EXPORTED
// method that ClusterClient defines itself AND cmdable also provides is a
// cluster-wide override (DBSize, the SCRIPT admin trio, the HIMPORT admin
// trio) — reaching it through the AutoPipeliner's embedded cmdable would run
// it on one routed node instead of fanning out. Each must therefore have an
// explicit delegate on AutoPipeliner. Found by review twice; this fails on
// the third occurrence instead.
func TestAPDelegatesClusterWideOverrides(t *testing.T) {
	ccT := reflect.TypeOf(&ClusterClient{})
	apT := reflect.TypeOf(&AutoPipeliner{})
	cmdableT := reflect.TypeOf(cmdable(nil))

	for i := 0; i < ccT.NumMethod(); i++ {
		name := ccT.Method(i).Name
		if _, isCmdable := cmdableT.MethodByName(name); !isCmdable {
			continue // not a shadowing override, just a ClusterClient API
		}
		if _, ok := apT.MethodByName(name); !ok {
			t.Errorf("ClusterClient.%s overrides the generic cmdable implementation "+
				"(cluster-wide fan-out) but AutoPipeliner has no delegate, so the "+
				"embedded cmdable would run it on a single routed node; add "+
				"func (ap *AutoPipeliner) %s(...) { return ap.pipeliner.%s(...) }",
				name, name, name)
		}
	}
}

// TestRawBlockingCommandsDivert pins that RAW blocking Cmders — which carry no
// per-command read timeout, unlike the typed helpers — are recognized as
// blocking and therefore diverted off the shared pipeline connection.
func TestRawBlockingCommandsDivert(t *testing.T) {
	ctx := context.Background()
	blocking := [][]interface{}{
		{"blpop", "k", 0},
		{"brpop", "k", 0},
		{"brpoplpush", "a", "b", 0},
		{"blmove", "a", "b", "LEFT", "RIGHT", 0},
		{"blmpop", 0, 1, "k", "LEFT"},
		{"bzpopmin", "k", 0},
		{"bzpopmax", "k", 0},
		{"bzmpop", 0, 1, "k", "MIN"},
		{"wait", 1, 0},
		{"waitaof", 1, 0, 0},
		{"xread", "BLOCK", 0, "STREAMS", "s", "$"},
		{"xreadgroup", "GROUP", "g", "c", "BLOCK", 0, "STREAMS", "s", ">"},
	}
	for _, args := range blocking {
		if cmd := NewCmd(ctx, args...); !isBlockingCmd(cmd) {
			t.Errorf("isBlockingCmd(%v) = false, want true (would ride a shared pipeline conn)", args[0])
		}
	}
	// The non-blocking forms must stay batched: diverting them would drop the
	// common case out of pipelining for nothing.
	nonBlocking := [][]interface{}{
		{"xread", "COUNT", 10, "STREAMS", "s", "0"},
		{"xreadgroup", "GROUP", "g", "c", "STREAMS", "s", ">"},
		{"lpop", "k"},
		{"get", "k"},
	}
	for _, args := range nonBlocking {
		if cmd := NewCmd(ctx, args...); isBlockingCmd(cmd) {
			t.Errorf("isBlockingCmd(%v) = true, want false (must stay batched)", args)
		}
	}
}

// TestCloseBoundedByDispatchBackstop pins the bound added for the
// unkillable-dispatch case: an accepted command is never cancelled by Close
// (the flush contract), so with read timeouts disabled a stalled dispatch or a
// zero-timeout blocking command would hang Close forever. waitForDispatches
// must give up and report what is outstanding instead.
func TestCloseBoundedByDispatchBackstop(t *testing.T) {
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Simulate a diverted blocking command that never returns.
	release := make(chan struct{})
	ap.divertWg.Add(1)
	go func() { defer ap.divertWg.Done(); <-release }()

	start := time.Now()
	err = ap.waitForDispatches(150 * time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("waitForDispatches returned nil with a stuck diverted command, want a timeout error")
	}
	if !strings.Contains(err.Error(), "diverted") {
		t.Fatalf("error %q does not name the outstanding work", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("waitForDispatches took %v, want ~150ms (the bound must not be ignored)", elapsed)
	}
	close(release)
	if err := ap.waitForDispatches(5 * time.Second); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

// TestCloseWaitsForDivertedCommand pins the other half: Close must NOT return
// while a diverted command is still executing (its pooled connection is still
// in use). Before the divert goroutines were tracked, Close raced past them.
func TestCloseWaitsForDivertedCommand(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	// BLPOP with a short timeout: diverted, still running when Close is called.
	fut := ap.Submit(ctx, NewCmd(ctx, "blpop", "closewait:missing", 1))
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 500*time.Millisecond {
		t.Fatalf("Close returned after %v, before the ~1s diverted BLPOP finished — its pooled connection was still in flight", elapsed)
	}
	_ = fut.Wait()
}
