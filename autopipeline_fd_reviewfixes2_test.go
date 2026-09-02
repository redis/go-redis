package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// fdEncoderPanicArg is a command argument whose BinaryMarshaler panics, so
// writeCmd (proto WriteArgs) panics mid-serialization - simulating a user
// BinaryMarshaler that blows up on the FD writer goroutine.
type fdEncoderPanicArg struct{}

func (fdEncoderPanicArg) MarshalBinary() ([]byte, error) { panic("boom: encoder panic") }

// TestFDWriteBatchStampsSentPerCommand pins finding eTWPc: writeBatch must stamp
// sent/writtenAt per-command INSIDE the serialize loop, so a command the
// serializer never reaches (behind an earlier encoder panic) keeps sent=false and
// its optimistic attempt refunded - otherwise a never-executed NoRetry behind the
// panic is failed unsent (acute at MaxRetries==0).
func TestFDWriteBatchStampsSentPerCommand(t *testing.T) {
	ctx := context.Background()
	cn := pool.NewConn(&mockNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	// index 1's encoder panics; indices 2 and 3 must never be serialized.
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), attempts: 1},
		{cmd: NewStatusCmd(ctx, "set", "k1", fdEncoderPanicArg{}), attempts: 1},
		{cmd: NewRawWriteToCmd(ctx, nil, "get", "k2"), attempts: 1}, // NoRetry() == true
		{cmd: NewStatusCmd(ctx, "set", "k3", "v"), attempts: 1},
	}

	err := fd.writeBatch(ctx, cn, inflight, reqs)
	if err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Fatalf("writeBatch err = %v, want errFDPanicRecovered", err)
	}

	if !reqs[0].sent {
		t.Error("reqs[0] not marked sent (it was serialized)")
	}
	if !reqs[1].sent {
		t.Error("reqs[1] not marked sent (the serializer reached it; it panicked)")
	}
	if reqs[2].sent {
		t.Error("reqs[2] marked sent but was never serialized (behind the panic)")
	}
	if reqs[3].sent {
		t.Error("reqs[3] marked sent but was never serialized (behind the panic)")
	}

	if reqs[0].attempts != 1 || reqs[1].attempts != 1 {
		t.Errorf("serialized-prefix attempts = %d,%d, want 1,1", reqs[0].attempts, reqs[1].attempts)
	}
	if reqs[2].attempts != 0 || reqs[3].attempts != 0 {
		t.Errorf("never-serialized-suffix attempts = %d,%d, want 0,0 (refunded)", reqs[2].attempts, reqs[3].attempts)
	}

	if inflight.len() != 4 {
		t.Fatalf("inflight.len() = %d, want 4 (whole batch recovered for settlement)", inflight.len())
	}

	// The never-serialized NoRetry (reqs[2]) must REPLAY, not fail: sent=false so it
	// is not a split point, and refunded so it is not budget-exhausted at MaxRetries=0.
	if n := fdFirstNoRetry(reqs[2:]); n != len(reqs[2:]) {
		t.Errorf("fdFirstNoRetry flagged a never-sent NoRetry at %d - it would be failed unsent", n)
	}
	kept, exhausted := fdPartitionByBudget(reqs[2:], 0)
	if len(kept) != 2 || len(exhausted) != 0 {
		t.Errorf("fdPartitionByBudget(suffix, 0) kept=%d exhausted=%d, want 2,0 - never-serialized suffix would be failed at MaxRetries=0", len(kept), len(exhausted))
	}
}

// fdFakeRedisErr implements the redis.Error interface (a custom
// PushNotificationProcessor could return one).
type fdFakeRedisErr struct{}

func (fdFakeRedisErr) Error() string { return "FAKE custom redis error" }
func (fdFakeRedisErr) RedisError()   {}

// TestFDPipelineErrConnUnusableStampsCmds pins finding eR1Nh: when a push-drain
// returns errConnUnusable WRAPPING a redis.Error, isRedisError classifies it as a
// reply, so the old guard skipped setCmdsErr and every command kept Err()==nil
// while Exec returned an error (the FD shutdown flush could then complete them as
// successes). The errConnUnusable marker must take precedence and stamp all cmds.
func TestFDPipelineErrConnUnusableStampsCmds(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	wrapped := fmt.Errorf("%w: pipeline push drain: %w", errConnUnusable, fdFakeRedisErr{})
	// Preconditions that make the bug possible: isRedisError sees through the wrap,
	// yet the marker is present.
	if !isRedisError(wrapped) {
		t.Fatal("precondition: isRedisError(wrapped) should be true (it unwraps to a redis.Error)")
	}
	if !errors.Is(wrapped, errConnUnusable) {
		t.Fatal("precondition: errors.Is(wrapped, errConnUnusable) should be true")
	}

	cmds := []Cmder{
		NewStatusCmd(ctx, "ping"),
		NewStatusCmd(ctx, "ping"),
	}
	stub := func(context.Context, *pool.Conn, []Cmder) (bool, error) {
		return false, wrapped // no retry; behaves like a fatal push-drain error
	}

	err := c.generalProcessPipeline(ctx, cmds, stub, "test", 0)
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("generalProcessPipeline err = %v, want errConnUnusable-wrapped", err)
	}
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			t.Errorf("cmd[%d].Err() == nil - a transport-desync error was left unstamped", i)
		}
	}
}

// fdCountingHook counts ProcessHook invocations.
type fdCountingHook struct{ n *int32 }

func (h fdCountingHook) DialHook(next DialHook) DialHook { return next }
func (h fdCountingHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		atomic.AddInt32(h.n, 1)
		return next(ctx, cmd)
	}
}

func (h fdCountingHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFDHookSnapshotExcludesLateHook pins finding eTWPh: the FD hook host must run
// the hook chain captured at SUBMIT time (hookSnapshot), so a hook added after the
// command was accepted does not retroactively wrap it. This is the mechanism unit
// test the finding sanctions; hostHook's use of the snapshot is covered by
// inspection (it passes the submit-captured *hooksState to withProcessHookSnapshot).
func TestFDHookSnapshotExcludesLateHook(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{Addr: ":6379"}) // no dial: only hook methods are exercised
	defer c.Close()

	var h1, h2 int32
	c.AddHook(fdCountingHook{&h1})
	snap := c.hookSnapshot()       // captured at "submit time"
	c.AddHook(fdCountingHook{&h2}) // installed AFTER the snapshot

	cmd := NewStatusCmd(ctx, "ping")
	base := func(context.Context, Cmder) error { return nil }

	if err := c.withProcessHookSnapshot(snap, ctx, cmd, base); err != nil {
		t.Fatalf("withProcessHookSnapshot: %v", err)
	}
	if got := atomic.LoadInt32(&h1); got != 1 {
		t.Fatalf("snapshot hook ran %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&h2); got != 0 {
		t.Fatalf("hook added after the snapshot ran %d times, want 0 - a late hook retroactively wrapped an in-flight command", got)
	}

	// Control: the LIVE chain (the pre-fix behavior) DOES observe the late hook,
	// proving the snapshot is what excludes it.
	if err := c.withProcessHook(ctx, cmd, base); err != nil {
		t.Fatalf("withProcessHook: %v", err)
	}
	if got := atomic.LoadInt32(&h2); got != 1 {
		t.Fatalf("control: live withProcessHook did not observe the late hook (%d); test cannot distinguish the fix", got)
	}
}

// TestFDWriteBatchRefundsReplaySuffixByReached pins finding F1: writeBatch's
// recovery-push must refund the optimistic attempt by how far the serialize loop
// REACHED this call (index >= written), NOT by the lifetime `sent` flag. `sent` is
// sticky across replays, so on a SECOND-session replay every command already carries
// sent==true; keying the refund on !sent then skips the suffix that this call never
// serialized (an earlier command's encoder panicked), leaving it over-charged and
// budget-exhausted one replay early. At MaxRetries==1 that FAILS a command that was
// never actually re-issued.
func TestFDWriteBatchRefundsReplaySuffixByReached(t *testing.T) {
	ctx := context.Background()
	cn := pool.NewConn(&mockNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	// SECOND-session replay: every command was already sent in session 1 (sent==true,
	// sticky) and re-charged for this replay (attempts==2: submit + one replay bump,
	// like run()'s carry[i].attempts++). index 1's encoder panics, so indices 2 and 3
	// are never reached this call.
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "k1", fdEncoderPanicArg{}), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "k2", "v"), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "k3", "v"), attempts: 2, sent: true},
	}

	err := fd.writeBatch(ctx, cn, inflight, reqs)
	if err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Fatalf("writeBatch err = %v, want errFDPanicRecovered", err)
	}

	// The prefix REACHED this call keeps its charge: reqs[0] serialized cleanly,
	// reqs[1] panicked mid-serialize (its bytes may have reached the wire).
	if reqs[0].attempts != 2 || reqs[1].attempts != 2 {
		t.Errorf("reached-prefix attempts = %d,%d, want 2,2 (kept)", reqs[0].attempts, reqs[1].attempts)
	}
	// The suffix NOT reached this call must be refunded DESPITE sent==true — this is
	// the fix. A refund keyed on !sent would skip these (they carry the sticky flag).
	if reqs[2].attempts != 1 || reqs[3].attempts != 1 {
		t.Errorf("unreached-suffix attempts = %d,%d, want 1,1 (refunded despite sent==true)", reqs[2].attempts, reqs[3].attempts)
	}
	// Consequence: at MaxRetries==1 the refunded suffix stays eligible for one more
	// replay; over-charged (attempts 2 > 1) it would be declared budget-exhausted and
	// FAILED without ever being re-issued.
	kept, exhausted := fdPartitionByBudget(reqs[2:], 1)
	if len(kept) != 2 || len(exhausted) != 0 {
		t.Errorf("fdPartitionByBudget(suffix, 1) kept=%d exhausted=%d, want 2,0 (suffix loses its retry without the reached-based refund)", len(kept), len(exhausted))
	}
	if inflight.len() != 4 {
		t.Fatalf("inflight.len() = %d, want 4 (whole batch recovered for settlement)", inflight.len())
	}
}

// fdPanicAllowLimiter is a user Limiter whose Allow panics (user code that blows up
// on the engine's writer goroutine). ReportResult counts calls so the test can pin
// that a panicking Allow — like a deny — grants no permit and so reports nothing.
type fdPanicAllowLimiter struct{ reports atomic.Int64 }

func (l *fdPanicAllowLimiter) Allow() error         { panic("boom: limiter Allow panic") }
func (l *fdPanicAllowLimiter) ReportResult(_ error) { l.reports.Add(1) }

// TestFDWriteBatchRecoversPanickingLimiterAllow pins finding F4: Limiter.Allow runs
// on the FD writer goroutine BEFORE writeBatch arms its serialize-panic defer, so a
// panicking Allow (user code) would escape fd.run and crash the process with the
// accepted chunk unsettled. writeBatch must recover it, fail the chunk (deny
// semantics), and — because no permit was granted — never call ReportResult.
func TestFDWriteBatchRecoversPanickingLimiterAllow(t *testing.T) {
	ctx := context.Background()
	cn := pool.NewConn(&mockNetConn{})
	lim := &fdPanicAllowLimiter{}
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, Limiter: lim}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), batch: newAPBatch(), attempts: 1},
		{cmd: NewStatusCmd(ctx, "set", "k1", "v"), batch: newAPBatch(), attempts: 1},
	}

	// The panic must be recovered inside writeBatch, not propagated to the caller.
	err := func() (e error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("writeBatch propagated a limiter Allow panic instead of recovering it: %v", r)
			}
		}()
		return fd.writeBatch(ctx, cn, inflight, reqs)
	}()
	// Deny semantics: writeBatch returns nil (only THIS chunk failed), each command
	// carries the wrapped panic error, and the connection is untouched.
	if err != nil {
		t.Fatalf("writeBatch err = %v, want nil (a deny fails the chunk, not writeBatch)", err)
	}
	for i := range reqs {
		got := reqs[i].cmd.rawErr()
		if got == nil {
			t.Errorf("reqs[%d].Err() == nil, want the recovered panic error", i)
		} else if !errors.Is(got, errFDPanicRecovered) {
			t.Errorf("reqs[%d].Err() = %v, want errFDPanicRecovered", i, got)
		}
	}
	if reqs[0].sent || reqs[1].sent {
		t.Errorf("a command was marked sent after a denied (panicking) Allow — the connection must be untouched")
	}
	if inflight.len() != 0 {
		t.Errorf("inflight.len() = %d, want 0 — a denied chunk must never enter the in-flight deque", inflight.len())
	}
	// Strict pairing: a panicking Allow grants no permit, so there is NO ReportResult.
	if n := lim.reports.Load(); n != 0 {
		t.Errorf("ReportResult called %d times after a panicking Allow, want 0 (no permit -> no report)", n)
	}
}

// TestAutoPipelineConcurrentSharedDrainRunsOnce pins finding F3: the two close entry
// points for one engine — the explicit AutoPipeliner.Close and the shared-pool close
// hook (which calls cancelAndDrain WITHOUT setting ap.closed) — can run concurrently
// when a pool-sharing clone closes the shared pools while the owner calls Close. The
// drain body must run exactly ONCE (two concurrent drains would interleave drainAll's
// flushers->sweep->batchWg.Wait ordering and panic "WaitGroup misuse: Add called
// concurrently with Wait"), and both callers must get the SAME close error.
//
// No -race red-check is available: there is no shared-memory data race here (the
// hazard is a WaitGroup-misuse runtime panic, and each caller returns its own value).
// The red-check is the drainRuns counter: reverting cancelAndDrain to call drainBody
// directly (no drainOnce) makes both callers run the body, so drainRuns == 2.
func TestAutoPipelineConcurrentSharedDrainRunsOnce(t *testing.T) {
	c := NewClient(&Options{Addr: ":6379"}) // no dial: nothing is dispatched
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{Unordered: true, NumShards: 4})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)
	start := make(chan struct{})
	// Path A: the explicit Close (CAS on ap.closed, unregister hook, then drain).
	go func() { defer wg.Done(); <-start; errs[0] = ap.Close() }()
	// Path B: the shared-pool close hook path (cancelAndDrain, ap.closed left false).
	go func() { defer wg.Done(); <-start; errs[1] = ap.cancelAndDrain() }()
	close(start)
	wg.Wait()

	if got := ap.drainRuns.Load(); got != 1 {
		t.Fatalf("drain body ran %d times, want 1 — concurrent closers double-drained the engine", got)
	}
	if errs[0] != errs[1] {
		t.Fatalf("concurrent close paths returned different results: Close=%v hook=%v", errs[0], errs[1])
	}
	if errs[0] != nil {
		t.Fatalf("healthy concurrent close returned %v, want nil", errs[0])
	}
}
