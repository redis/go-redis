package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// fdPanicNoRetryCmd is a Cmder whose NoRetry() panics. flushReqs reads
// reqs[i].cmd.NoRetry() at the top of each chunk (before any I/O), so this
// triggers the shutdown-flush recover defer at the exact `i` the defer expects,
// without needing a live server.
type fdPanicNoRetryCmd struct{ *StatusCmd }

func (fdPanicNoRetryCmd) NoRetry() bool { panic("boom: NoRetry panic in shutdown flush") }

// TestFDShutdownFlushAbortsAfterRecoveredPanic pins finding F1: flushReqs
// recovers an encoder/serialize panic in a carried-tail flush and fails that
// group, but with an UNNAMED return it returned nil after recovery, so
// flushCarryBudgeted treated the failed group as success and ran later
// attempt-count groups (and, via shutdownFlush, the fresh queue) even though an
// earlier ordered command never completed. The named return must propagate the
// failure so the ordered flush aborts like a transport failure.
//
// Red-check: revert flushReqs to an unnamed/nil return -> flushCarryBudgeted
// runs the later group (runPipeline call count becomes 1).
func TestFDShutdownFlushAbortsAfterRecoveredPanic(t *testing.T) {
	ctx := context.Background()
	var runCalls atomic.Int32
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 5}}},
		maxBatch: 8,
		runPipeline: func(_ context.Context, _ []Cmder, _ int) error {
			runCalls.Add(1)
			return nil
		},
	}

	// carry is attempts-descending, so [attempts=2] and [attempts=1] are two
	// contiguous groups. group1's first command panics in NoRetry(); group2 is a
	// normal command that must NOT run once group1 aborts. MaxRetries=5 keeps both
	// groups' budgets positive (rem = MaxRetries+1-attempts), so neither is
	// dropped as budget-exhausted before it is flushed.
	g1 := fdPanicNoRetryCmd{NewStatusCmd(ctx, "set", "k0", "v")}
	g2 := NewStatusCmd(ctx, "get", "k1")
	carry := []fdReq{
		{cmd: g1, batch: newAPBatch(), attempts: 2},
		{cmd: g2, batch: newAPBatch(), attempts: 1},
	}

	err := fd.flushCarryBudgeted(ctx, carry)
	if err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Fatalf("flushCarryBudgeted err = %v, want errFDPanicRecovered", err)
	}
	if got := runCalls.Load(); got != 0 {
		t.Fatalf("runPipeline called %d times, want 0 - a later group ran after a recovered shutdown-flush panic", got)
	}
	// Both the panicking group and the aborted later group must be failed (not left
	// hanging) with the panic error.
	if e := g1.rawErr(); e == nil || !errors.Is(e, errFDPanicRecovered) {
		t.Fatalf("group1 cmd Err() = %v, want errFDPanicRecovered", e)
	}
	if e := g2.rawErr(); e == nil || !errors.Is(e, errFDPanicRecovered) {
		t.Fatalf("group2 cmd Err() = %v, want errFDPanicRecovered (failed by the abort)", e)
	}
}

// TestFDFlushReqsAbortsChunksOnErrConnUnusable pins finding F2: when a
// shutdown-flush chunk returns an errConnUnusable wrapper (e.g. a custom push
// processor returned a redis.Error during the close-time drain, so the chunk was
// never written), the chunk loop classified it as an ordinary Redis reply
// (isRedisError unwraps the marker) and CONTINUED to later chunks - flushing an
// ordered shutdown out of order. The errConnUnusable precedence
// (pipelineErrShouldStamp) must abort and fail the remaining reqs.
//
// Red-check: revert the guard to `!isRedisError(err)` -> the loop continues and
// runPipeline is called for every chunk (call count becomes 3).
func TestFDFlushReqsAbortsChunksOnErrConnUnusable(t *testing.T) {
	ctx := context.Background()

	// The exact shape withPipelineConn produces on a drain failure: errConnUnusable
	// wrapping a redis.Error. isRedisError sees through the wrap, yet the marker is
	// present - the bug's precondition.
	wrapped := fmt.Errorf("%w: pipeline push drain: %w", errConnUnusable, fdFakeRedisErr{})
	if !isRedisError(wrapped) {
		t.Fatal("precondition: isRedisError(wrapped) should be true (it unwraps to a redis.Error)")
	}
	if !errors.Is(wrapped, errConnUnusable) {
		t.Fatal("precondition: errors.Is(wrapped, errConnUnusable) should be true")
	}

	var calls atomic.Int32
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{}}},
		maxBatch: 1, // one command per chunk, so the three reqs span three chunks
		runPipeline: func(_ context.Context, _ []Cmder, _ int) error {
			if calls.Add(1) == 1 {
				return wrapped // chunk 1: desynced, never written
			}
			return nil
		},
	}
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "ping"), batch: newAPBatch(), attempts: 1},
		{cmd: NewStatusCmd(ctx, "ping"), batch: newAPBatch(), attempts: 1},
		{cmd: NewStatusCmd(ctx, "ping"), batch: newAPBatch(), attempts: 1},
	}

	err := fd.flushReqs(ctx, reqs, 0)
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("flushReqs err = %v, want errConnUnusable-wrapped", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("runPipeline called %d times, want 1 - later chunks ran after an errConnUnusable chunk", got)
	}
	// The un-run chunks must be failed with the desync error, not completed as
	// successes.
	for i := 1; i < len(reqs); i++ {
		if e := reqs[i].cmd.rawErr(); e == nil || !errors.Is(e, errConnUnusable) {
			t.Errorf("reqs[%d].Err() = %v, want errConnUnusable (failed by the abort)", i, e)
		}
	}
}

// fdDrainErrProcessor is a custom (non-*push.Processor) PushNotificationProcessor
// whose drain returns a caller-supplied error.
type fdDrainErrProcessor struct{ err error }

func (fdDrainErrProcessor) GetHandler(string) push.NotificationHandler { return nil }
func (fdDrainErrProcessor) RegisterHandler(string, push.NotificationHandler, bool) error {
	return nil
}
func (fdDrainErrProcessor) UnregisterHandler(string) error { return nil }
func (p fdDrainErrProcessor) ProcessPendingNotifications(context.Context, push.NotificationHandlerContext, *proto.Reader) error {
	return p.err
}

// TestReleaseConnRemovesConnAfterCustomDrainError pins finding F3: on the release
// path, a custom PushNotificationProcessor that returns a redis.Error during the
// release-time push drain left the possibly-partially-drained conn returning to
// the pool - releaseConnToPool only Removed it when isBadConn recognized the
// error, and isBadConn returns false for a (non-readonly, non-moved) redis.Error.
// A drain error must make the conn unusable so it is Removed regardless.
//
// Red-check: restore the `if isBadConn(err, ...) { Remove; return }` guard -> the
// redis.Error drain error is not isBadConn, so the conn is Put (puts=1, removes=0).
func TestReleaseConnRemovesConnAfterCustomDrainError(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	// A push frame so MaybeHasData() and PeekReplyType() see a RespPush, routing to
	// the custom processor (redis.go peeks the type, then calls it).
	if _, err := server.Write([]byte(">1\r\n$3\r\nfoo\r\n")); err != nil {
		t.Fatalf("write push frame: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("push frame never became readable")
	}

	cp := &releaseRecordingPool{}
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		connPool:      cp,
		pushProcessor: fdDrainErrProcessor{err: fdFakeRedisErr{}},
	}
	// err=nil so the first guard (isBadConn/errConnUnusable on the op error) does
	// not fire; the drain error alone must drive removal.
	c.releaseConn(context.Background(), cn, nil)

	if cp.removes != 1 || cp.puts != 0 {
		t.Fatalf("a custom release-drain error must remove, not re-pool, the conn: removes=%d puts=%d",
			cp.removes, cp.puts)
	}
}

// TestFDConfigReportsResolvedWindow pins finding F4: newFDEngine resolves a zero
// FullDuplexWindow to fdDefaultWindow in a local, so Config() (documented to
// report the effective config) reported 0. The resolved window must be written
// back so Config() reports the value actually enforced.
//
// Red-check: drop `ap.config.FullDuplexWindow = w` in newFDEngine -> Config()
// reports 0.
func TestFDConfigReportsResolvedWindow(t *testing.T) {
	c := NewClient(&Options{
		Addr:                    internalTestRedisAddr(),
		Protocol:                3,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
	})
	defer c.Close()

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: 0, // 0 => default
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active; cannot exercise the window write-back")
	}

	if got := ap.Config().FullDuplexWindow; got != fdDefaultWindow {
		t.Fatalf("Config().FullDuplexWindow = %d, want %d (resolved default not published)", got, fdDefaultWindow)
	}
}
