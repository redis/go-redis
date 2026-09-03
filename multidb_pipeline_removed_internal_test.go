package redis

import (
	"context"
	"errors"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

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
// of the single-command re-gate: a pipeline whose snapshotted member was removed
// mid-flight must not surface the terminal ErrClosed while the client is open.
// It re-enters the gate; with no live member the re-gates are bounded and it
// reports the retryable ErrTemporarilyNotAvailable.
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
// transaction path. Re-gating is at-most-once-safe because the execution marker
// is empty (the removed member's pool returned ErrClosed before anything ran).
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
