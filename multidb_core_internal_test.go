package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/proto"
)

// classifyOutcome must treat typed server replies (the proto reader parses
// recognized prefixes into *proto.AuthError, *proto.MovedError, ... rather
// than the concrete proto.RedisError string) exactly like their string
// forms: application-level replies prove the database served the request,
// while availability replies and surfaced redirects are failures.
func TestClassifyOutcomeTypedReplies(t *testing.T) {
	for _, err := range []error{
		proto.NewAuthError("NOAUTH Authentication required"),
		proto.NewPermissionError("NOPERM this user has no permissions"),
		proto.NewExecAbortError("EXECABORT Transaction discarded because of previous errors"),
	} {
		if got := classifyOutcome(err, true); got != outcomeSuccess {
			t.Errorf("classifyOutcome(%q) = %v, want outcomeSuccess", err.Error(), got)
		}
	}
	for _, err := range []error{
		proto.NewLoadingError("LOADING Redis is loading the dataset in memory"),
		proto.NewClusterDownError("CLUSTERDOWN The cluster is down"),
		// A MOVED/ASK that surfaces to this layer means the cluster client
		// exhausted its redirect budget: an availability failure, not a
		// healthy reply (see classifyOutcome's isRedirectReply case).
		proto.NewMovedError("MOVED 3999 127.0.0.1:6381", "127.0.0.1:6381"),
		proto.NewAskError("ASK 3999 127.0.0.1:6381", "127.0.0.1:6381"),
	} {
		if got := classifyOutcome(err, true); got != outcomeFailure {
			t.Errorf("classifyOutcome(%q) = %v, want outcomeFailure", err.Error(), got)
		}
	}
	if got := classifyOutcome(proto.RedisError("WRONGTYPE Operation against a key"), true); got != outcomeSuccess {
		t.Errorf("classifyOutcome(WRONGTYPE) = %v, want outcomeSuccess", got)
	}
}

// A cluster member whose client has no known nodes cannot route any command:
// an availability failure that must drive failover, not a neutral no-op.
func TestClassifyOutcomeClusterNoNodes(t *testing.T) {
	if got := classifyOutcome(errClusterNoNodes, true); got != outcomeFailure {
		t.Errorf("classifyOutcome(errClusterNoNodes) = %v, want outcomeFailure", got)
	}
	if got := classifyOutcome(fmt.Errorf("wrapped: %w", errClusterNoNodes), true); got != outcomeFailure {
		t.Errorf("classifyOutcome(wrapped errClusterNoNodes) = %v, want outcomeFailure", got)
	}
}

// TestTryFallbackYieldsWhenFailoverLocked pins that the background fallback
// yields to a real failover instead of blocking it: with failoverMu already
// held (as the command-path tryFailover would hold it), tryFallbackToPrimary
// must return promptly via TryLock rather than block on the lock, and must not
// change the active member. Under the old Lock() this goroutine would block
// forever and the test would time out.
func TestTryFallbackYieldsWhenFailoverLocked(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	active := &multidbDatabase{id: 0, weight: 1, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	cand := &multidbDatabase{id: 1, weight: 2, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	core.dbs[0] = active
	core.dbs[1] = cand
	core.active.Store(0)

	// Simulate a concurrent failover holding the lock across a slow probe.
	core.failoverMu.Lock()
	defer core.failoverMu.Unlock()

	done := make(chan struct{})
	go func() {
		core.tryFallbackToPrimary(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("tryFallbackToPrimary blocked on a held failoverMu instead of yielding")
	}
	if got := int(core.active.Load()); got != 0 {
		t.Errorf("active changed to %d while a failover held the lock", got)
	}
}
