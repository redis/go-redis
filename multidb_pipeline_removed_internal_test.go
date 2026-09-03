package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// markThenCloseHook marks the batch executed, then returns ErrClosed without
// calling next — simulating a cluster fan-out where one shard applied its
// commands before another shard's connection acquisition failed against a
// member that was switched away and removed.
type markThenCloseHook struct{ calls *int32 }

func (markThenCloseHook) DialHook(next DialHook) DialHook          { return next }
func (markThenCloseHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (h markThenCloseHook) ProcessPipelineHook(ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		atomic.AddInt32(h.calls, 1)
		markPipelineExecuted(ctx, cmds)
		return ErrClosed
	}
}

func newRemovedActiveCore(t *testing.T) *multidbCore {
	t.Helper()
	core := newMultidbCore(&MultiDBOptions{})
	// A closed client's pool returns ErrClosed from its hooks without dialing.
	a := NewClient(&Options{Addr: "127.0.0.1:6379", MaxRetries: -1})
	t.Cleanup(func() { _ = a.Close() })
	_ = a.Close()
	db := &multidbDatabase{id: 0, weight: 1, c: a, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	})}
	db.removed.Store(true)
	core.dbs[0] = db
	core.active.Store(0)
	return core
}

// TestProcessPipelineRemovedActiveDoesNotSurfaceErrClosed is the batch analogue
// of the single-command path: a pipeline whose snapshotted member was removed
// mid-flight must not surface the terminal ErrClosed while the client is open.
// It reports the retryable ErrTemporarilyNotAvailable instead, so the caller
// retries; nothing is replayed by the client itself.
func TestProcessPipelineRemovedActiveDoesNotSurfaceErrClosed(t *testing.T) {
	core := newRemovedActiveCore(t)
	err := core.processPipeline(context.Background(), []Cmder{NewStatusCmd(context.Background(), "ping")})
	if errors.Is(err, ErrClosed) {
		t.Fatalf("processPipeline surfaced terminal ErrClosed for a removed former active: %v", err)
	}
	if !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("got %v, want ErrTemporarilyNotAvailable after the bounded re-gate", err)
	}
}

// TestProcessTxPipelineRemovedActiveDoesNotSurfaceErrClosed is the same for the
// transaction path: the retryable error is surfaced, never a replay — EXEC may
// have committed, so at-most-once forbids re-running it.
func TestProcessTxPipelineRemovedActiveDoesNotSurfaceErrClosed(t *testing.T) {
	core := newRemovedActiveCore(t)
	wrapped := wrapMultiExec(context.Background(), []Cmder{NewStatusCmd(context.Background(), "ping")})
	err := core.processTxPipeline(context.Background(), wrapped)
	if errors.Is(err, ErrClosed) {
		t.Fatalf("processTxPipeline surfaced terminal ErrClosed for a removed former active: %v", err)
	}
	if !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("got %v, want ErrTemporarilyNotAvailable after the bounded re-gate", err)
	}
}

// TestProcessPipelinePartiallyExecutedRemovedMemberNotReplayed pins that a batch
// which PARTIALLY executed (execution marker set) before hitting ErrClosed on a
// removed member is NOT replayed — replaying would duplicate the applied writes
// from the completed shard. It surfaces the retryable error exactly once and
// keeps the executed commands' results (they are not rewritten).
func TestProcessPipelinePartiallyExecutedRemovedMemberNotReplayed(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	var calls int32
	a := NewClient(&Options{Addr: "127.0.0.1:6379", MaxRetries: -1})
	t.Cleanup(func() { _ = a.Close() })
	a.AddHook(markThenCloseHook{calls: &calls})
	db := &multidbDatabase{id: 0, weight: 1, c: a, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	})}
	db.removed.Store(true)
	core.dbs[0] = db
	core.active.Store(0)

	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k", "v"),
		NewStatusCmd(context.Background(), "get", "k"),
	}
	err := core.processPipeline(context.Background(), cmds)
	// The batch executed: the retryable error is surfaced (never a replay), the
	// batch runs exactly once, and the executed commands keep their results —
	// they are not rewritten with the aggregate error.
	if !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("partially-executed removed-member batch: got %v, want ErrTemporarilyNotAvailable", err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("batch executed %d times, want 1 — it was replayed (duplicating applied writes)", n)
	}
	for i, cmd := range cmds {
		if cmd.rawErr() != nil {
			t.Fatalf("executed command %d had its result rewritten to %v", i, cmd.rawErr())
		}
	}
}
