package redis

import (
	"context"
	"errors"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// TestRemovedFormerActiveErrClosedDoesNotSurface pins that a command whose
// snapshotted active member was removed mid-flight (its client closed) does not
// surface the terminal ErrClosed to the caller while the MultiDBClient is still
// open. Instead the command re-enters the gate; with no live member to serve it
// the re-gates are bounded and it reports the retryable ErrTemporarilyNotAvailable,
// never ErrClosed.
func TestRemovedFormerActiveErrClosedDoesNotSurface(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	// A closed client's pool returns ErrClosed from Process without dialing.
	a := NewClient(&Options{Addr: "127.0.0.1:6379"})
	_ = a.Close()
	db := &multidbDatabase{id: 0, weight: 1, c: a, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	db.removed.Store(true)
	core.dbs[0] = db
	core.active.Store(0)

	err := core.process(context.Background(), NewStatusCmd(context.Background(), "ping"))
	if errors.Is(err, ErrClosed) {
		t.Fatalf("removed former-active surfaced terminal ErrClosed instead of re-gating: %v", err)
	}
	if !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("got %v, want ErrTemporarilyNotAvailable after the bounded re-gate", err)
	}
}
