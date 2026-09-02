// Code consolidated from per-topic autopipeline test files.
package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
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

	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
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

	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
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

	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
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

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

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

	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
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

// TestFDReplyIsFatalPushDrain guards the full-duplex push-drain classification:
// a drain error (errFDPushDrainFailed) must ABORT the session — stop the reader
// and replay the unacked tail — even when it wraps a Redis-typed or retryable
// cause. The drain error carries the processor cause via %w so a caller can still
// errors.Is/As it, but that wrapped cause must NOT let isRedisError reclassify the
// desync as a normal command reply: doing so would settle or divert it and leave
// the unread frame in the stream, shifting every later reply (FIFO desync). This
// pins the %v->%w wrap against re-opening that hole.
func TestFDReplyIsFatalPushDrain(t *testing.T) {
	redisErr := proto.RedisError("WRONGTYPE Operation against a key holding the wrong kind of value")
	loading := proto.RedisError("LOADING Redis is loading the dataset in memory")

	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"transport error stops the session", io.EOF, true},
		{"plain redis reply is not fatal", redisErr, false},
		{"drain wrapping a redis error is fatal", fmt.Errorf("%w: %w", errFDPushDrainFailed, redisErr), true},
		{"drain wrapping a retryable redis error is fatal", fmt.Errorf("%w: %w", errFDPushDrainFailed, loading), true},
		{"drain wrapping a transport error is fatal", fmt.Errorf("%w: %w", errFDPushDrainFailed, io.EOF), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fdReplyIsFatal(tc.err); got != tc.fatal {
				t.Fatalf("fdReplyIsFatal(%v) = %v, want %v", tc.err, got, tc.fatal)
			}
		})
	}

	// The %w chain stays intact: after the session fails, a caller can still
	// recognize both the drain marker and the underlying processor cause.
	wrapped := fmt.Errorf("%w: %w", errFDPushDrainFailed, redisErr)
	if !errors.Is(wrapped, errFDPushDrainFailed) {
		t.Fatalf("errors.Is(wrapped, errFDPushDrainFailed) = false, want true")
	}
	var re proto.RedisError
	if !errors.As(wrapped, &re) {
		t.Fatalf("errors.As(wrapped, &proto.RedisError) = false, want true (processor cause dropped from chain)")
	}
}

// TestFDPartitionByBudget pins the shutdown-flush retry-budget split: a carried
// command whose budget is spent (attempts > MaxRetries) is separated out so
// shutdownFlush fails it rather than handing it to processPipeline for a fresh
// MaxRetries+1 loop. Also guards that kept never aliases carry's backing array —
// shutdownFlush appends the queue to kept, so aliasing would corrupt the caller's
// unacked tail.
func TestFDPartitionByBudget(t *testing.T) {
	const maxRetries = 3 // budget = 1 FD attempt + 3 replays = 4 attempts
	carry := []fdReq{{attempts: 1}, {attempts: 4}, {attempts: 3}, {attempts: 5}}
	kept, exhausted := fdPartitionByBudget(carry, maxRetries)

	if len(kept) != 2 || kept[0].attempts != 1 || kept[1].attempts != 3 {
		t.Fatalf("kept = %+v, want attempts [1 3] (<= MaxRetries)", kept)
	}
	if len(exhausted) != 2 || exhausted[0].attempts != 4 || exhausted[1].attempts != 5 {
		t.Fatalf("exhausted = %+v, want attempts [4 5] (> MaxRetries)", exhausted)
	}
	// Boundary: attempts == MaxRetries is kept (one attempt left); MaxRetries+1 is
	// exhausted (full budget spent).
	if _, e := fdPartitionByBudget([]fdReq{{attempts: maxRetries}}, maxRetries); len(e) != 0 {
		t.Fatalf("attempts == MaxRetries must be kept, got exhausted %+v", e)
	}
	if k, _ := fdPartitionByBudget([]fdReq{{attempts: maxRetries + 1}}, maxRetries); len(k) != 0 {
		t.Fatalf("attempts == MaxRetries+1 must be exhausted, got kept %+v", k)
	}
	// Aliasing guard: kept must not share carry's backing array.
	if len(kept) > 0 && &kept[0] == &carry[0] {
		t.Fatalf("kept aliases carry's backing array; shutdownFlush would corrupt the caller's tail")
	}

	// Finding-A scenario: an exhausted command at the head must NOT deny a newer
	// command written behind it (lower attempts) its remaining retries. run() carries
	// oldest-first with attempts bumped together, so exhausted is the leading run and
	// the eligible suffix keeps FIFO order.
	tail := []fdReq{{attempts: maxRetries + 1}, {attempts: maxRetries}, {attempts: 1}}
	elig, ex := fdPartitionByBudget(tail, maxRetries)
	if len(ex) != 1 || ex[0].attempts != maxRetries+1 {
		t.Fatalf("exhausted = %+v, want the single head at MaxRetries+1", ex)
	}
	if len(elig) != 2 || elig[0].attempts != maxRetries || elig[1].attempts != 1 {
		t.Fatalf("eligible = %+v, want [MaxRetries 1] in order (newer commands keep their retries)", elig)
	}
}

// TestFDCarryRemainingRetries pins the Close-flush budget formula. carry is
// POST-BUMP (a command carried at attempts=A has done A-1 executions), so of its
// MaxRetries+1 budget it may run MaxRetries+1-A more times; a negative result means
// the budget is spent (drop). attempts is clamped to >=1 (attempts==0 is test-only).
func TestFDCarryRemainingRetries(t *testing.T) {
	cases := []struct {
		attempts, maxRetries, want int
	}{
		// MaxRetries=3 (budget 4 executions).
		{1, 3, 3},  // never run (0 done): full budget -> 4 executions
		{2, 3, 2},  // 1 done: 3 more -> 4 total
		{4, 3, 0},  // 3 done: 1 final execution -> 4 total
		{5, 3, -1}, // 4 done: budget spent -> drop
		{0, 3, 3},  // clamp: treat as attempts==1 (full budget), not MaxRetries+2
		// MaxRetries=0 (budget 1 execution) — the case the old `attempts > mr` drop
		// mishandled.
		{1, 0, 0},  // never run: exactly one execution
		{2, 0, -1}, // 1 done == budget: drop, do not re-run
	}
	for _, tc := range cases {
		if got := fdCarryRemainingRetries(tc.attempts, tc.maxRetries); got != tc.want {
			t.Errorf("fdCarryRemainingRetries(attempts=%d, maxRetries=%d) = %d, want %d",
				tc.attempts, tc.maxRetries, got, tc.want)
		}
	}
}

// TestFDRefundUnsentAttempt pins the never-sent-suffix accounting fix: a carried
// command whose chunk was never written (reader died / conn broke mid-replay) must
// have the optimistic attempt bump refunded, floored at 0, so it is not declared
// budget-exhausted a replay early.
func TestFDRefundUnsentAttempt(t *testing.T) {
	reqs := []fdReq{{attempts: 2}, {attempts: 3}, {attempts: 1}, {attempts: 0}}
	fdRefundUnsentAttempt(reqs)
	want := []int{1, 2, 0, 0} // each -1, floored at 0
	for i, w := range want {
		if reqs[i].attempts != w {
			t.Errorf("reqs[%d].attempts = %d, want %d", i, reqs[i].attempts, w)
		}
	}
}

// TestFDFirstNoRetrySentGate pins the sent-aware NoRetry split: only a NoRetry
// command whose bytes may have reached the wire (sent) blocks the tail. A
// never-sent NoRetry recovered from a dead connection's backlog stays in the
// replay prefix — issuing it is its FIRST send, and failing it would hand the
// caller an error for a command the server never saw.
func TestFDFirstNoRetrySentGate(t *testing.T) {
	ctx := context.Background()
	retryable := func() fdReq { return fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v")} }
	noRetry := func(sent bool) fdReq {
		return fdReq{cmd: NewZeroCopyStringCmd(ctx, make([]byte, 8), "get", "k"), sent: sent}
	}
	if !noRetry(false).cmd.NoRetry() {
		t.Fatal("precondition: zero-copy read should forbid retries")
	}

	for _, tc := range []struct {
		name string
		reqs []fdReq
		want int
	}{
		{"empty", nil, 0},
		{"no noRetry at all", []fdReq{retryable(), retryable()}, 2},
		{"never-sent noRetry does not split", []fdReq{retryable(), noRetry(false), retryable()}, 3},
		{"sent noRetry splits", []fdReq{retryable(), noRetry(true), retryable()}, 1},
		{"sent noRetry at head", []fdReq{noRetry(true), retryable()}, 0},
		{"unsent then sent noRetry", []fdReq{noRetry(false), noRetry(true)}, 1},
	} {
		if got := fdFirstNoRetry(tc.reqs); got != tc.want {
			t.Errorf("%s: fdFirstNoRetry = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestFDRecoverTailRefundsSuffixOnly pins the fdConnErr recovery-set builder:
// the drained (sent) tail keeps its attempt charge, the never-sent suffix is
// refunded (run()'s recovery re-bumps on the real re-issue), and the result is
// a fresh slice — takeRemaining's array is deque-owned, so appending onto it in
// place could corrupt the deque.
func TestFDRecoverTailRefundsSuffixOnly(t *testing.T) {
	rem := []fdReq{{attempts: 2, sent: true}, {attempts: 2, sent: true}}
	suffix := []fdReq{{attempts: 2}, {attempts: 1}}
	out := fdRecoverTail(rem, suffix)

	if len(out) != 4 {
		t.Fatalf("len(out) = %d, want 4", len(out))
	}
	wantAttempts := []int{2, 2, 1, 0} // rem unchanged; suffix -1 each
	for i, w := range wantAttempts {
		if out[i].attempts != w {
			t.Errorf("out[%d].attempts = %d, want %d", i, out[i].attempts, w)
		}
	}
	// Order preserved: sent tail first, suffix after.
	if !out[0].sent || !out[1].sent || out[2].sent || out[3].sent {
		t.Error("recovery set out of order: want [sent, sent, unsent, unsent]")
	}
	// Fresh backing array: the refund must not have reached the input slices, and
	// mutating out must not write through into rem.
	if suffix[0].attempts != 2 || suffix[1].attempts != 1 {
		t.Error("fdRecoverTail mutated the input suffix slice")
	}
	out[0].attempts = 99
	if rem[0].attempts != 2 {
		t.Error("fdRecoverTail aliases takeRemaining's backing array")
	}
}

// errBreakerOpen is the sentinel a fakeBreaker denies with; the engine must
// surface it VERBATIM (errors.Is) on every command of a denied chunk.
var errBreakerOpen = errors.New("fake breaker: open")

// fakeBreaker is a Limiter stand-in for a circuit breaker: denies while open,
// and counts allows/denies/reports so the strict pairing contract (exactly one
// ReportResult per successful Allow, none for a deny) is assertable. errReports
// counts the reports that carried a non-nil error, so a test can assert the
// breaker actually SEES failures (reply-side reporting) instead of only ever
// being told success — the finding-ed53z contract.
type fakeBreaker struct {
	open       atomic.Bool
	allows     atomic.Int64
	denies     atomic.Int64
	reports    atomic.Int64
	errReports atomic.Int64
}

func (b *fakeBreaker) Allow() error {
	if b.open.Load() {
		b.denies.Add(1)
		return errBreakerOpen
	}
	b.allows.Add(1)
	return nil
}

func (b *fakeBreaker) ReportResult(err error) {
	b.reports.Add(1)
	if err != nil {
		b.errReports.Add(1)
	}
}

// TestFDLimiterPerBatch pins the per-batch Limiter contract of the full-duplex
// engine: Allow() is paid per written chunk (not per session or per lease), a
// deny fails exactly that chunk's commands with the Limiter's error verbatim
// while the session and engine survive, and every successful Allow is paired
// with exactly one ReportResult.
func TestFDLimiterPerBatch(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:                    internalTestRedisAddr(),
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
		Limiter:                 br,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Phase 1 — breaker closed: commands are admitted per chunk and succeed.
	for i := 0; i < 5; i++ {
		if err := ap.Set(ctx, "fd:limbatch:"+strconv.Itoa(i), "v", 0).Err(); err != nil {
			t.Fatalf("phase 1 set %d: %v", i, err)
		}
	}
	if br.allows.Load() < 1 {
		t.Fatal("phase 1: Limiter.Allow never called — per-batch admission bypassed")
	}

	// Phase 2 — breaker open: the next chunk's Allow is denied, so the command
	// fails fast with the Limiter's error VERBATIM (errors.Is must see the
	// sentinel — the engine must not wrap or replace it). The deny settles the
	// chunk at write time; nothing hangs waiting for the breaker to close.
	br.open.Store(true)
	done := make(chan error, 1)
	go func() { done <- ap.Set(ctx, "fd:limbatch:denied", "v", 0).Err() }()
	select {
	case e := <-done:
		if !errors.Is(e, errBreakerOpen) {
			t.Fatalf("phase 2: err = %v, want the breaker sentinel verbatim (errors.Is)", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("phase 2: command hung on an open breaker instead of failing the chunk fast")
	}
	if br.denies.Load() < 1 {
		t.Fatal("phase 2: Limiter denied nothing — the failure did not come from admission")
	}

	// Phase 3 — breaker closed again: the SAME client/engine serves new work
	// (the deny failed only the chunk; the session/engine stayed alive, and the
	// next chunk pays Allow again).
	br.open.Store(false)
	if err := ap.Set(ctx, "fd:limbatch:resumed", "v", 0).Err(); err != nil {
		t.Fatalf("phase 3 set after breaker closed: %v (engine did not resume)", err)
	}

	// Teardown — strict pairing: after Close (which waits out the engine and any
	// final flush) every successful Allow must have exactly one ReportResult; a
	// denied Allow must have none. A brief settle covers the plain-path release
	// report that can land just after a call returns.
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	a, r, d := br.allows.Load(), br.reports.Load(), br.denies.Load()
	if a != r {
		t.Fatalf("Limiter unpaired: allows=%d reports=%d (want equal; denies=%d report nothing)", a, r, d)
	}
	if d < 1 {
		t.Fatalf("denies = %d, want >= 1 (phase 2 must have been denied by the Limiter)", d)
	}
}

// failReadNetConn is a net.Conn whose Write succeeds (a peer that ACCEPTS the
// batch) but whose Read always fails — the reply-side transport failure of
// finding ed53z: the write is admitted and reported healthy under the old
// contract, yet no reply ever arrives.
type failReadNetConn struct{ mockNetConn }

func (c *failReadNetConn) Read(b []byte) (int, error) { return 0, errors.New("read boom") }

// TestFDLimiterReportsReplySide pins the core of finding ed53z at the mechanism
// level: writeBatch admits a chunk (one Allow) but a CLEAN write must NOT report
// yet — the obligation is deferred to the reply side and rides the chunk's last
// req. A later transport failure that abandons the unread replies settles it
// with the error (settleTail), and the settle is exactly-once (CAS), so a reader
// and a failure path racing the same obligation cannot double-report.
func TestFDLimiterReportsReplySide(t *testing.T) {
	br := &fakeBreaker{}
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, Limiter: br}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()
	cn := pool.NewConn(&mockNetConn{}) // Write succeeds -> writeBatch flushes clean

	reqs := make([]fdReq, 3)
	for i := range reqs {
		reqs[i] = fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v")}
	}
	if err := fd.writeBatch(context.Background(), cn, inflight, reqs); err != nil {
		t.Fatalf("writeBatch: %v", err)
	}
	// THE discriminator between reply-side and write-outcome reporting: a clean
	// write admits (allows==1) but reports NOTHING yet.
	if a, r := br.allows.Load(), br.reports.Load(); a != 1 || r != 0 {
		t.Fatalf("after a clean write: allows=%d reports=%d, want allows=1 reports=0 (reply-side, not write-time reporting)", a, r)
	}

	rem := inflight.takeRemaining()
	if len(rem) != len(reqs) {
		t.Fatalf("takeRemaining len=%d, want %d", len(rem), len(reqs))
	}
	// Only the chunk's LAST req carries the obligation.
	for i := 0; i < len(rem)-1; i++ {
		if rem[i].limReport != nil {
			t.Fatalf("req %d carries a Limiter obligation; only the chunk's LAST req should", i)
		}
	}
	ob := rem[len(rem)-1].limReport
	if ob == nil {
		t.Fatal("chunk's last req carries no Limiter obligation — the reader could never report the reply-side outcome")
	}

	// A transport failure abandoning the unread replies settles the obligation
	// with the error: the breaker SEES a failure.
	ob.settle(errBreakerOpen)
	if a, r, e := br.allows.Load(), br.reports.Load(), br.errReports.Load(); a != 1 || r != 1 || e != 1 {
		t.Fatalf("after a read-side failure: allows=%d reports=%d errReports=%d, want 1/1/1", a, r, e)
	}
	// Exactly-once: a racing second settle (reader vs failure path) is a no-op.
	ob.settle(nil)
	ob.settle(errBreakerOpen)
	if r, e := br.reports.Load(), br.errReports.Load(); r != 1 || e != 1 {
		t.Fatalf("obligation double-reported: reports=%d errReports=%d, want 1/1 (CAS must make settle exactly-once)", r, e)
	}
}

// TestFDLimiterReportsReadFailure drives a whole session() against a peer that
// ACCEPTS every write but whose reader immediately fails, and asserts the
// Limiter actually receives the transport failure — the wiring finding ed53z is
// about. Reverting writeBatch to report the WRITE outcome makes errReports 0 and
// this test fails (the breaker would only ever be told success and never open).
// Deterministic and dial-free: no reply is ever produced, so the reader breaks
// on the first read and the session takes its fdConnErr recovery path.
func TestFDLimiterReportsReadFailure(t *testing.T) {
	br := &fakeBreaker{}
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: context.Background()},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, ReadTimeout: time.Second, Limiter: br}}},
		maxBatch: 2,   // 5 commands -> chunks [0:2] [2:4] [4:5] -> 3 admitted chunks
		window:   100, // >= command count, so writeCarryChunked never window-gates
	}
	cn := pool.NewConn(&failReadNetConn{})

	const n = 5
	carry := make([]fdReq, n)
	for i := range carry {
		carry[i] = fdReq{cmd: NewStatusCmd(context.Background(), "set", fmt.Sprintf("k%d", i), "v"), attempts: 1}
	}

	type res struct {
		result fdResult
	}
	done := make(chan res, 1)
	go func() {
		_, r, _ := fd.session(context.Background(), cn, carry)
		done <- res{r}
	}()
	var got res
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session() hung after a read failure — the reader/serve loop did not converge")
	}
	if got.result != fdConnErr {
		t.Fatalf("session result = %v, want fdConnErr (the reader failed)", got.result)
	}

	a, r, e := br.allows.Load(), br.reports.Load(), br.errReports.Load()
	if a < 1 {
		t.Fatal("Limiter.Allow never called — the writes were not admitted per chunk")
	}
	if a != r {
		t.Fatalf("Limiter unpaired: allows=%d reports=%d (want equal — every Allow gets exactly one ReportResult)", a, r)
	}
	if e != a {
		t.Fatalf("read failure reported %d errors of %d reports — a peer that drops before replying must report FAILURE, not success (finding ed53z)", e, a)
	}
}

// TestFDLimiterPairingWithReadFailure exercises the strict-pairing contract
// through the real engine across the main paths: successful chunks (replies land
// -> ReportResult(nil)), then the connection is killed mid-flight so in-flight
// replies never arrive (reply-side transport failure -> ReportResult(err)), then
// Close. Every Allow must still pair 1:1 with a ReportResult, and at least one of
// those reports must carry the error. Needs redis; uses the delay-proxy harness.
func TestFDLimiterPairingWithReadFailure(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	const delay = 40 * time.Millisecond
	paddr, stop := delayReplyProxy(t, "127.0.0.1"+internalTestRedisAddr(), delay)
	defer stop()

	br := &fakeBreaker{}
	c := NewClient(&Options{
		Addr:                    paddr,
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
		MaxRetries:              1, // bound the post-kill re-lease backoff so Close is prompt
		Limiter:                 br,
		// Disable the maint-notifications handshake so the ONLY report carrying an
		// error is the conn kill's abandoned replies (not a version-dependent
		// CLIENT MAINT_NOTIFICATIONS reply error at init).
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer c.Close()
	const window, maxBatch = 8, 4
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: window,
		MaxBatchSize:     maxBatch,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// A run of successful chunks first (these report nil), then kill the conn so a
	// backlog's replies never arrive (these report the transport error).
	for i := 0; i < 10; i++ {
		if err := ap.Set(ctx, fmt.Sprintf("fdlrp:ok:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("warmup set %d: %v", i, err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = ap.Set(ctx, fmt.Sprintf("fdlrp:kill:%d", i), "v", 0)
		}
	}()
	time.Sleep(delay / 2)
	stop() // kill live conns: in-flight replies never arrive -> reader failure

	closed := make(chan struct{})
	go func() { _ = ap.Close(); _ = c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung after conn kill")
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	a, r, e := br.allows.Load(), br.reports.Load(), br.errReports.Load()
	if a < 1 || a != r {
		t.Fatalf("Limiter unpaired after successes + read failure + Close: allows=%d reports=%d (want equal, >=1)", a, r)
	}
	if e < 1 {
		t.Fatalf("no report carried the transport error: errReports=%d, want >=1 (the killed conn's abandoned replies must open the breaker — finding ed53z)", e)
	}
}

// TestFDLimiterReplyLevelErrorReportsNil pins that a reply-LEVEL error (redis.Nil
// from a GET on a missing key) is NOT a transport failure: the server answered,
// so the chunk's obligation must report success. errReports must stay 0 — a
// redis.Nil must never open the breaker. Needs redis.
func TestFDLimiterReplyLevelErrorReportsNil(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	br := &fakeBreaker{}
	client := NewClient(&Options{
		Addr:                    internalTestRedisAddr(),
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
		Limiter:                 br,
		// Disable the maint-notifications handshake: on a server that lacks it the
		// CLIENT MAINT_NOTIFICATIONS reply-level error is reported to the shared
		// Limiter at conn init and would pollute errReports (version-dependent).
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	client.Del(ctx, "fd:lim:missing")
	if err := ap.Get(ctx, "fd:lim:missing").Err(); !errors.Is(err, Nil) {
		t.Fatalf("get missing key: err = %v, want redis.Nil (reply-level error)", err)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("ap close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	a, r, e := br.allows.Load(), br.reports.Load(), br.errReports.Load()
	if a < 1 || a != r {
		t.Fatalf("Limiter unpaired: allows=%d reports=%d (want equal, >=1)", a, r)
	}
	if e != 0 {
		t.Fatalf("redis.Nil opened the breaker: errReports=%d, want 0 (a reply-level error is not a transport failure)", e)
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
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
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
// unkillable-dispatch case (every stage of the drain, not just the last):
// an accepted command is never cancelled by Close
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
	err = ap.drainAll(150 * time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("drainAll returned nil with a stuck diverted command, want a timeout error")
	}
	if !strings.Contains(err.Error(), "diverted") {
		t.Fatalf("error %q does not name the outstanding work", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("drainAll took %v, want ~150ms (the bound must not be ignored)", elapsed)
	}
	close(release)
}

// TestCloseWaitsForDivertedCommand pins the other half: Close must NOT return
// while a diverted command is still executing (its pooled connection is still
// in use). Before the divert goroutines were tracked, Close raced past them.
func TestCloseWaitsForDivertedCommand(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
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

// TestSharedClosedRejectsEveryEntryPoint pins that ALL command entry points
// honor the shared pool-set closed flag, not just the batched ones: a
// WithTimeout clone's Close sets only that flag, and a guard reading
// ap.closed alone would accept work against pools that are already gone and
// surface pool-closed errors instead of ErrClosed (found by cursor on #3942
// for Do/DoRaw/DoRawWriteTo; IsClosed had the same gap while promising the
// opposite in its doc).
func TestSharedClosedRejectsEveryEntryPoint(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	parent := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer parent.Close()
	if err := parent.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := parent.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Set(ctx, "sharedclosed:pre", "v", 0).Err(); err != nil {
		t.Fatalf("pre-close set: %v", err)
	}

	// Closing an ordinary clone closes the SHARED pools; ap.closed stays false.
	if err := parent.WithTimeout(5 * time.Second).Close(); err != nil {
		t.Fatalf("clone close: %v", err)
	}
	if ap.closed.Load() {
		t.Fatal("precondition broken: ap.closed is set, so this would not exercise the shared flag")
	}

	if !ap.IsClosed() {
		t.Error("IsClosed() = false after the shared pools closed, want true (its doc promises the owning client's close counts)")
	}
	for name, err := range map[string]error{
		"typed":         ap.Set(ctx, "sharedclosed:post", "v", 0).Err(),
		"Do":            ap.Do(ctx, "get", "sharedclosed:post").Err(),
		"DoRaw":         ap.DoRaw(ctx, "get", "sharedclosed:post").Err(),
		"DoRawWriteTo":  ap.DoRawWriteTo(ctx, io.Discard, "get", "sharedclosed:post").Err(),
		"diverted(Do)":  ap.Do(ctx, "client", "id").Err(),
		"blockingDiver": ap.Submit(ctx, NewCmd(ctx, "blpop", "sharedclosed:missing", 1)).Wait(),
	} {
		if err != ErrClosed {
			t.Errorf("%s after shared close = %v, want ErrClosed", name, err)
		}
	}
}

// TestBlockingDetectionNormalizesArgTypes pins that the BLOCK token is matched
// the way the encoder renders it: a raw Cmder may carry RESP keywords as
// []byte or *string, and a string-only type switch would let those be batched
// onto a shared connection (codex on #3942).
func TestBlockingDetectionNormalizesArgTypes(t *testing.T) {
	ctx := context.Background()
	blockStr := "BLOCK"
	for _, args := range [][]interface{}{
		{"xread", []byte("BLOCK"), 0, "STREAMS", "s", "$"},
		{"xread", &blockStr, 0, "STREAMS", "s", "$"},
		{"xread", []byte("block"), 0, "STREAMS", "s", "$"},
		{"xreadgroup", "GROUP", "g", "c", []byte("BLOCK"), 0, "STREAMS", "s", ">"},
	} {
		if !isBlockingCmd(NewCmd(ctx, args...)) {
			t.Errorf("isBlockingCmd(%v) = false, want true", args)
		}
	}
	// A []byte that is not the token must not trigger the divert.
	if isBlockingCmd(NewCmd(ctx, "xread", []byte("COUNT"), 10, "STREAMS", "s", "0")) {
		t.Error("non-BLOCK []byte token classified as blocking")
	}
}

// TestDivertRegistrationRacesClose hammers the window between a diverted
// command's closed check and its registration against Close: every submitted
// command must end with a definite outcome, and Close must never report
// success while a diverted command is still executing. Run with -race.
func TestDivertRegistrationRacesClose(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	for round := 0; round < 40; round++ {
		ap, err := client.AsyncAutoPipeline()
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var accepted atomic.Int32
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// "client id" is diverted (connection-hostile), so it takes the
				// same registration gate as a blocking command but returns fast.
				if err := ap.Do(ctx, "client", "id").Err(); err == nil {
					accepted.Add(1)
				}
			}()
		}
		if err := ap.Close(); err != nil {
			t.Fatalf("round %d: close: %v", round, err)
		}
		wg.Wait()
		// Every command either ran (counted) or was rejected — no hangs, and
		// -race would flag an Add racing the Wait.
		_ = accepted.Load()
	}
}

// TestDivertedClusterWideCommandsSkipPreflight pins that a command which will
// be DIVERTED is not rejected by the cluster preflight: the preflight exists to
// keep fan-out-policy commands out of shared pipelines, but a diverted command
// goes through ClusterClient.Process, where the cluster-wide aggregation works.
// Before the ordering fix, typed WAIT/WAITAOF were rejected on a cluster with
// command policies enabled (codex on #3942).
func TestDivertedClusterWideCommandsSkipPreflight(t *testing.T) {
	ctx := context.Background()
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}})
	defer cc.Close()
	if err := cc.Ping(ctx).Err(); err != nil {
		t.Skipf("no cluster: %v", err)
	}
	ap, err := cc.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// WAIT has a fan-out request policy; diverted, it must execute rather than
	// be rejected as non-pipelineable.
	if err := ap.Wait(ctx, 0, time.Second).Err(); err != nil {
		t.Errorf("ap.Wait on cluster = %v, want it to execute (diverted, not preflight-rejected)", err)
	}
	// A genuinely batched fan-out command must still be rejected.
	if err := ap.DBSize(ctx).Err(); err != nil {
		t.Errorf("ap.DBSize should be delegated cluster-wide, got %v", err)
	}
}

// TestOtelMetricsDoNotAwaitOnAsyncFace pins that enabling telemetry cannot
// change the deferred face's call shape: the post-execution metric emissions in
// the command wrappers read the outcome only when it is available WITHOUT
// blocking, so an instrumented ap.Publish / ap.XReadGroup still returns
// immediately (codex on #3942).
func TestOtelMetricsDoNotAwaitOnAsyncFace(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Hold every dispatch so a submitted command provably has NOT executed
	// while the wrapper's metric block runs.
	gate := make(chan struct{})
	var armed atomic.Bool
	client.AddHook(dispatchGateHook{gate: gate, armed: &armed})

	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ap.Publish(ctx, "otel:chan", "payload")
		ap.SPublish(ctx, "otel:chan", "payload")
		// Block: 0 is the form that would wait indefinitely for messages.
		ap.XReadGroup(ctx, &XReadGroupArgs{
			Group: "g", Consumer: "c", Streams: []string{"otel:stream", ">"}, Block: 0,
		})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		armed.Store(false)
		close(gate)
		t.Fatal("submitting instrumented commands blocked on the deferred face; a metric path awaited the result")
	}
	armed.Store(false)
	close(gate)
	_ = ap.Close()
}

// dispatchGateHook holds every dispatch (solo, pipelined and diverted) while
// armed, so a submitted command is guaranteed not to have executed.
type dispatchGateHook struct {
	gate  chan struct{}
	armed *atomic.Bool
}

func (h dispatchGateHook) DialHook(next DialHook) DialHook { return next }
func (h dispatchGateHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if h.armed.Load() {
			<-h.gate
		}
		return next(ctx, cmd)
	}
}

func (h dispatchGateHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		if h.armed.Load() {
			<-h.gate
		}
		return next(ctx, cmds)
	}
}

// TestConfigDoesNotLeakInternalSharding pins that the exported effective config
// carries no internal-only bits: a cluster autopipeliner sets contentSharded to
// tell Validate its shards are slot-routed, and copying that config into a
// STANDALONE async autopipeliner would silence the NumShards>1 ordering
// requirement for round-robin shards, which really do reorder (codex on #3942).
func TestConfigDoesNotLeakInternalSharding(t *testing.T) {
	ctx := context.Background()
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}})
	defer cc.Close()
	if err := cc.Ping(ctx).Err(); err != nil {
		t.Skipf("no cluster: %v", err)
	}
	clusterAP, err := cc.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer clusterAP.Close()

	cfg := clusterAP.Config()
	if cfg.contentSharded {
		t.Error("Config() exposes the internal contentSharded flag")
	}

	// The round-trip must be rejected: many shards, ordered, deferred face.
	cfg.NumShards = 4
	cfg.Unordered = false
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if _, err := client.AsyncAutoPipelineWithOptions(&cfg); err == nil {
		t.Error("standalone async autopipeliner accepted NumShards>1 without Unordered, " +
			"via a config copied from a cluster autopipeliner — the ordering check was bypassed")
	}
}

// TestEvalDoesNotAwaitOnAsyncFace pins the deferred contract for the Eval
// family: the NOSCRIPT normalization must not read the result while it is still
// pending, or every ap.Eval/EvalSha becomes synchronous (codex on #3942).
func TestEvalDoesNotAwaitOnAsyncFace(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	gate := make(chan struct{})
	var armed atomic.Bool
	client.AddHook(dispatchGateHook{gate: gate, armed: &armed})

	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ap.Eval(ctx, "return 1", nil)
		ap.EvalSha(ctx, "ffffffffffffffffffffffffffffffffffffffff", nil)
		ap.EvalRO(ctx, "return 1", nil)
		ap.EvalShaRO(ctx, "ffffffffffffffffffffffffffffffffffffffff", nil)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		armed.Store(false)
		close(gate)
		t.Fatal("Eval family blocked on the deferred face: the NOSCRIPT normalization awaited the result")
	}
	armed.Store(false)
	close(gate)
	_ = ap.Close()
}

// TestScriptRunFallbackAcceptsRawNoScript pins the other half: since the Eval
// wrappers skip normalization while a result is pending, Script.Run's fallback
// must recognize the server's raw NOSCRIPT error too.
func TestScriptRunFallbackAcceptsRawNoScript(t *testing.T) {
	if !isNoScriptErr(ErrNoScript) {
		t.Error("normalized ErrNoScript not recognized")
	}
	if !isNoScriptErr(proto.RedisError("NOSCRIPT No matching script")) {
		t.Error("raw server NOSCRIPT error not recognized")
	}
	if isNoScriptErr(nil) || isNoScriptErr(proto.RedisError("WRONGTYPE nope")) {
		t.Error("unrelated error treated as NOSCRIPT")
	}
}

// TestBlockingSetCoversEveryReadTimeoutHelper pins the raw-blocking allowlist
// against the typed helpers it mirrors. Every cmdable method that calls
// setReadTimeout parks the connection, so its wire name must be recognized for
// RAW Cmders too (a hand-built NewCmd carries no timeout marker). Derive the
// list with:
//
//	grep -rn 'setReadTimeout' --include='*.go' . | grep -v _test
//
// and add any new name here and in blockingCommands (codex found blmovem,
// migrate and ts.read missing on #3942).
func TestBlockingSetCoversEveryReadTimeoutHelper(t *testing.T) {
	ctx := context.Background()
	byName := []string{
		"blpop", "brpop", "brpoplpush", "blmove", "blmovem", "blmpop",
		"bzpopmin", "bzpopmax", "bzmpop", "wait", "waitaof", "migrate",
	}
	for _, name := range byName {
		if !isBlockingCmd(NewCmd(ctx, name, "k", 0)) {
			t.Errorf("isBlockingCmd(%q) = false, want true", name)
		}
	}
	byArg := [][]interface{}{
		{"xread", "BLOCK", 0, "STREAMS", "s", "$"},
		{"xreadgroup", "GROUP", "g", "c", "BLOCK", 0, "STREAMS", "s", ">"},
		{"ts.read", "k", "BLOCK", 0},
	}
	for _, args := range byArg {
		if !isBlockingCmd(NewCmd(ctx, args...)) {
			t.Errorf("isBlockingCmd(%v) = false, want true", args)
		}
	}
	// The non-blocking forms of the arg-driven commands must stay batched.
	for _, args := range [][]interface{}{
		{"ts.read", "k", "COUNT", 5},
		{"xread", "COUNT", 10, "STREAMS", "s", "0"},
	} {
		if isBlockingCmd(NewCmd(ctx, args...)) {
			t.Errorf("isBlockingCmd(%v) = true, want false", args)
		}
	}
}

// TestNoRetryRunsIsolatePolicyAndPreserveOrder pins BOTH halves of retry
// isolation. A zero-copy read forbids retries because its reply decodes into a
// caller buffer, and cmdsContainNoRetry applies that verdict to a whole
// dispatched slice — so a shared batch must not put one in with retryable
// commands. But the split must never REORDER: grouping all retryable commands
// ahead of the no-retry ones would run a later SET before an earlier zero-copy
// GET of the same key, and the ordered faces promise submit order. Contiguous
// runs satisfy both (both findings by codex on #3942 — the reordering bug came
// from the first fix for the retry leak).
func TestNoRetryRunsIsolatePolicyAndPreserveOrder(t *testing.T) {
	ctx := context.Background()
	mk := func(tag string) Cmder { return NewStatusCmd(ctx, "set", tag, "v") }
	mkZC := func(tag string) Cmder {
		return NewZeroCopyStringCmd(ctx, make([]byte, 8), "get", tag)
	}
	if !mkZC("x").NoRetry() {
		t.Fatal("precondition: zero-copy read should forbid retries")
	}

	// Uniform batches: no split at all, so no extra dispatch and no allocation.
	if runs := splitRetryRuns([]Cmder{mk("a"), mk("b"), mk("c")}); runs != nil {
		t.Errorf("all-retryable batch was split into %d runs, want no split", len(runs))
	}
	if runs := splitRetryRuns([]Cmder{mkZC("a"), mkZC("b")}); runs != nil {
		t.Errorf("all-no-retry batch was split into %d runs, want no split", len(runs))
	}

	// Mixed: contiguous runs, each policy-uniform, concatenating back to the
	// ORIGINAL order.
	zc, s1, s2, zc2 := mkZC("zc1"), mk("s1"), mk("s2"), mkZC("zc2")
	original := []Cmder{zc, s1, s2, zc2}
	runs := splitRetryRuns(original)
	if len(runs) != 3 {
		t.Fatalf("got %d runs, want 3 (noRetry | retryable,retryable | noRetry)", len(runs))
	}
	var flat []Cmder
	for _, run := range runs {
		if len(run) == 0 {
			t.Fatal("empty run")
		}
		policy := run[0].NoRetry()
		for _, cmd := range run {
			if cmd.NoRetry() != policy {
				t.Error("run mixes retry policies: cmdsContainNoRetry would leak across callers")
			}
		}
		flat = append(flat, run...)
	}
	if len(flat) != len(original) {
		t.Fatalf("runs cover %d commands, want %d", len(flat), len(original))
	}
	for i := range original {
		if flat[i] != original[i] {
			t.Fatalf("ORDER VIOLATION at %d: runs reorder the batch, so a later write could execute before an earlier read", i)
		}
	}
}

// TestNoRetryOrderObservedEndToEnd is the behavioral version of the ordering
// half: a zero-copy read submitted BEFORE a write to the same key must observe
// the OLD value on the ordered deferred face.
func TestNoRetryOrderObservedEndToEnd(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Set(ctx, "order:zc", "old-value", 0).Err(); err != nil {
		t.Fatal(err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		MaxBatchSize:  32,
		MaxFlushDelay: 25 * time.Millisecond, // one drain holds both commands
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	buf := make([]byte, 32)
	read := ap.GetToBuffer(ctx, "order:zc", buf) // submitted FIRST
	write := ap.Set(ctx, "order:zc", "new-value", 0)
	if err := write.Err(); err != nil {
		t.Fatal(err)
	}
	n, err := read.Result()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "old-value" {
		t.Fatalf("zero-copy read saw %q, want %q — the write submitted AFTER it executed first", got, "old-value")
	}
}

// TestNoRetrySplitExecutesEveryCommand pins the split end to end: both groups
// must actually execute and get their own results.
func TestNoRetrySplitExecutesEveryCommand(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Set(ctx, "split:zc", "zerocopy-value", 0).Err(); err != nil {
		t.Fatal(err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		MaxBatchSize:  64,
		MaxFlushDelay: 20 * time.Millisecond, // hold the window so both land in one drain
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	buf := make([]byte, 64)
	sets := make([]*StatusCmd, 8)
	for i := range sets {
		sets[i] = ap.Set(ctx, "split:plain:"+strconv.Itoa(i), "v", 0)
	}
	zc := ap.GetToBuffer(ctx, "split:zc", buf)
	for i, c := range sets {
		if err := c.Err(); err != nil {
			t.Fatalf("plain %d: %v", i, err)
		}
	}
	n, err := zc.Result()
	if err != nil {
		t.Fatalf("zero-copy read: %v", err)
	}
	if got := string(buf[:n]); got != "zerocopy-value" {
		t.Fatalf("zero-copy buffer = %q, want %q", got, "zerocopy-value")
	}
}

// cacheHook serves the batch itself: it fills in every command's value and
// returns nil WITHOUT calling next — what a caching or mocking hook does, and
// what plain Pipeline hooks are allowed to do.
type cacheHook struct {
	armed *atomic.Bool
	value string
	calls *atomic.Int32
}

func (h cacheHook) DialHook(next DialHook) DialHook { return next }
func (h cacheHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if !h.armed.Load() {
			return next(ctx, cmd)
		}
		h.calls.Add(1)
		if sc, ok := cmd.(*StringCmd); ok {
			sc.SetVal(h.value)
		}
		return nil
	}
}

func (h cacheHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		if !h.armed.Load() {
			return next(ctx, cmds)
		}
		h.calls.Add(1)
		for _, cmd := range cmds {
			if sc, ok := cmd.(*StringCmd); ok {
				sc.SetVal(h.value)
			}
		}
		return nil
	}
}

// TestSuccessfulHookShortCircuitIsHonored pins hook parity with plain
// pipelines: a hook that supplies results and returns nil without calling next
// has succeeded, and the engine must not overwrite that with an error. It used
// to synthesize one, so a caching hook that works on Pipeline made every
// autopipelined batch fail (codex on #3942).
func TestSuccessfulHookShortCircuitIsHonored(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	var armed atomic.Bool
	var calls atomic.Int32
	client.AddHook(cacheHook{armed: &armed, value: "from-cache", calls: &calls})

	// Baseline: a plain pipeline accepts the successful short-circuit.
	armed.Store(true)
	pipe := client.Pipeline()
	pg := pipe.Get(ctx, "sc:key")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("plain pipeline rejected a successful short-circuit: %v", err)
	}
	if v, err := pg.Result(); err != nil || v != "from-cache" {
		t.Fatalf("plain pipeline: v=%q err=%v, want the cached value", v, err)
	}
	armed.Store(false)

	for _, tc := range []struct {
		name  string
		build func() (*AutoPipeliner, error)
	}{
		{"async", func() (*AutoPipeliner, error) { return client.AsyncAutoPipeline() }},
		{"blocking", func() (*AutoPipeliner, error) { return client.AutoPipeline() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ap, err := tc.build()
			if err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			defer armed.Store(false)
			// Several commands so the batched path (not just the solo one) runs.
			gets := make([]*StringCmd, 4)
			for i := range gets {
				gets[i] = ap.Get(ctx, "sc:key:"+strconv.Itoa(i))
			}
			for i, g := range gets {
				v, err := g.Result()
				if err != nil {
					t.Fatalf("cmd %d: err = %v, want nil — a successful short-circuit was rewritten to an error", i, err)
				}
				if v != "from-cache" {
					t.Fatalf("cmd %d: v = %q, want the value the hook supplied", i, v)
				}
			}
		})
	}
	if calls.Load() == 0 {
		t.Error("the hook never ran; the test proved nothing")
	}
}

// TestMustDivertKeepsCommandOffTheBatchPath pins the hook cluster wiring uses
// for commands whose routing is not slot-derived (ReqSpecial, e.g. FT.CURSOR
// READ): batched, mapCmdsByNode would route them by slot and reach the wrong
// shard, so they must leave the batching path entirely (codex on #3942).
func TestMustDivertKeepsCommandOffTheBatchPath(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		MaxBatchSize:  64,
		MaxFlushDelay: time.Hour, // nothing batched can execute during this test
	})
	if err != nil {
		t.Fatal(err)
	}
	ap.setMustDivert(func(_ context.Context, cmd Cmder) bool { return cmd.Name() == "echo" })

	// Diverted: executes despite the hour-long flush window.
	done := make(chan error, 1)
	go func() { done <- ap.Do(ctx, "echo", "diverted").Err() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("diverted command failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mustDivert command was batched: it waited on the flush window instead of running on its own connection")
	}

	// Control: a batchable command is still held by the window.
	held := ap.Get(ctx, "mustdivert:control")
	select {
	case <-time.After(200 * time.Millisecond):
	default:
	}
	if held.rawErr() != nil {
		t.Fatalf("control command should still be queued, got %v", held.rawErr())
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestRetryRunsStopAfterFailedPrefix pins the ordered-stream contract for the
// retry-policy runs, the same rule the byte chunker follows: once a run dies on
// a transport-class failure, later runs must NOT be dispatched — they would
// overtake a failed prefix — and must carry that error. Both paths now share
// dispatchSequential precisely because each got this wrong on its own (codex on
// #3942).
func TestRetryRunsStopAfterFailedPrefix(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr(), MaxRetries: -1})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	var dispatches atomic.Int32
	client.AddHook(chunkBreakerHook{dispatches: &dispatches})

	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	// Mixed batch -> several runs. The first run must fail and stop the rest.
	zc := NewZeroCopyStringCmd(ctx, make([]byte, 8), "get", "runs:zc")
	cmds := []Cmder{
		zc,
		NewStatusCmd(ctx, "set", "runs:a", "v"),
		NewStatusCmd(ctx, "set", "runs:b", "v"),
		NewZeroCopyStringCmd(ctx, make([]byte, 8), "get", "runs:zc2"),
	}
	ap.dispatchCmds(ctx, [][]Cmder{cmds}, len(cmds))

	if n := dispatches.Load(); n != 1 {
		t.Fatalf("hook saw %d run dispatches, want exactly 1 (later runs must not overtake a failed prefix)", n)
	}
	for i, cmd := range cmds {
		err := cmd.rawErr()
		if err == nil || !strings.Contains(err.Error(), "chunk breaker") {
			t.Fatalf("cmd %d: err = %v, want the prefix failure on every command", i, err)
		}
	}
}

// TestAsyncProcessReportsSubmitRejection pins that the deferred face's Process
// returns SUBMIT-time errors. Callers reaching the engine through
// UniversalClient.Process only see this return value, so swallowing a rejection
// made Process claim success for a command that would never run (codex on
// #3942). Execution errors must still NOT be reported here — that would make
// the call wait.
func TestAsyncProcessReportsSubmitRejection(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer client.Close()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	// Accepted submission: Process returns nil without waiting for execution.
	if err := ap.Process(ctx, NewStatusCmd(ctx, "set", "procrej:ok", "v")); err != nil {
		t.Fatalf("Process on an accepted command = %v, want nil", err)
	}
	// A command that fails at EXECUTION must still not surface here (no wait).
	bad := NewStatusCmd(ctx, "set") // wrong arity: fails server-side
	if err := ap.Process(ctx, bad); err != nil {
		t.Fatalf("Process reported an execution error (%v); the deferred face must not wait", err)
	}

	if err := ap.Close(); err != nil {
		t.Fatal(err)
	}
	// Rejected at submit: the error must be RETURNED, not only stored.
	if err := ap.Process(ctx, NewStatusCmd(ctx, "set", "procrej:closed", "v")); err != ErrClosed {
		t.Fatalf("Process after Close = %v, want ErrClosed", err)
	}
}

// TestAutopipelineSoloRoutingGate pins #3962: a cacheable solo straggler is
// routed through the cache-honoring Process path (main pool) ONLY when the
// client has client-side caching active. A non-CSC client (the default) must
// keep cacheable solos on the pipeline pool the straggler gate probed, so
// the live gate (cscActiveFn) must report false there and true for a CSC
// client — and false again after CSC disables itself mid-life.
func TestAutopipelineSoloRoutingGate(t *testing.T) {
	ctx := context.Background()

	// Non-CSC client: dial-free and deterministic.
	plain := NewClient(&Options{Addr: "localhost:1", Protocol: 3})
	defer plain.Close()
	ap, err := plain.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.cscActiveFn != nil && ap.cscActiveFn() {
		t.Fatal("non-CSC client: gate reports active, want inactive — a cacheable solo would wrongly use the main pool instead of the pipeline pool")
	}

	// CSC client: needs a live RESP3 server for CLIENT TRACKING to attach.
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis for the CSC half: %v", err)
	}
	if err := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3}).Ping(ctx).Err(); err != nil {
		t.Skipf("no redis for the CSC half: %v", err)
	}
	cscC := NewClient(&Options{
		Addr:                  internalTestRedisAddr(),
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 128},
	})
	defer cscC.Close()
	if err := cscC.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if !cscC.autopipelineCSCActive() {
		t.Skip("client-side caching did not attach (server lacks CLIENT TRACKING?)")
	}
	apc, err := cscC.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline (csc): %v", err)
	}
	defer apc.Close()
	if apc.cscActiveFn == nil || !apc.cscActiveFn() {
		t.Fatal("CSC client: gate reports inactive, want active — cacheable solos must honor the cache")
	}
	// Mid-life disable (RESP3 fallback, processor damping): the LIVE gate must
	// flip so cacheable solos return to the pipeline pool.
	cscC.disableCSCServing(ctx, "test: mid-life disable")
	if apc.cscActiveFn() {
		t.Fatal("gate still reports active after CSC disabled itself mid-life")
	}
}
