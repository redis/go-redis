package redis

import (
	"context"
	"errors"
	"fmt"
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
