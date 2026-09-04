package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/routing"
)

// This file consolidates the MultiDB internal (package redis) tests: core
// failover, probe and breaker gating, membership control paths, and the
// pipeline / transaction batch paths.

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

// TestNewPubSubClusterActiveRetryableAfterStandaloneLoss pins that a PubSub
// created while a standalone member existed treats a later "cluster active, no
// standalone" state as retryable, not terminal: membership is runtime-mutable
// (a passive standalone can be removed after failover and another added), so
// the channel loop must keep polling instead of closing permanently.
func TestNewPubSubClusterActiveRetryableAfterStandaloneLoss(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	// A passive standalone (id 0) exists at creation, and a cluster member
	// (id 1) is active — so staticAllCluster is false. (newPubSub adopts the
	// standalone's options, so give it a real client, not a zero &Client{}.)
	standalone := NewClient(&Options{Addr: "127.0.0.1:1"})
	defer standalone.Close()
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: standalone, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	core.dbs[1] = &multidbDatabase{id: 1, weight: 1, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})} // c == nil -> cluster
	core.active.Store(1)

	ps := core.newPubSub() // created while a standalone member (id 0) exists

	// The standalone is removed; the cluster member stays active.
	delete(core.dbs, 0)

	_, err := ps.newConn(context.Background(), "", nil)
	if errors.Is(err, ErrClosed) {
		t.Fatalf("newConn returned terminal ErrClosed for a config that had a standalone at creation")
	}
	if !errors.Is(err, errPubSubRequiresStandalone) {
		t.Fatalf("newConn err = %v, want errPubSubRequiresStandalone (retryable)", err)
	}
}

// TestNewPubSubAdoptsStandaloneOptionsWhenClusterActive pins that a PubSub
// created while a cluster member is active adopts a standalone member's options
// (write timeout, protocol) rather than the zero Options — otherwise subscribe
// frames would use a zero WriteTimeout and the wrong Protocol gate after the
// subscription fails over to the standalone.
func TestNewPubSubAdoptsStandaloneOptionsWhenClusterActive(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	standalone := NewClient(&Options{Addr: "127.0.0.1:1", WriteTimeout: 4321 * time.Millisecond, Protocol: 3})
	defer standalone.Close()
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: standalone, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	core.dbs[1] = &multidbDatabase{id: 1, weight: 1, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})} // c == nil -> cluster
	core.active.Store(1)                                                                                              // cluster active at creation

	ps := core.newPubSub()
	if ps.opt == nil {
		t.Fatal("PubSub opt is nil")
	}
	if ps.opt.WriteTimeout != standalone.opt.WriteTimeout {
		t.Errorf("PubSub WriteTimeout = %v, want the standalone's %v (zero-Options bug)", ps.opt.WriteTimeout, standalone.opt.WriteTimeout)
	}
	if ps.opt.Protocol != 3 {
		t.Errorf("PubSub Protocol = %d, want 3 (standalone's)", ps.opt.Protocol)
	}
}

// TestNewPubSubAllClusterAtCreationTerminal pins the preserved fail-fast path: a
// PubSub created on an all-cluster config that still has no standalone member
// returns the terminal ErrClosed so the channel loop exits instead of spinning.
func TestNewPubSubAllClusterAtCreationTerminal(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})} // c == nil -> cluster
	core.active.Store(0)

	ps := core.newPubSub() // all-cluster at creation

	_, err := ps.newConn(context.Background(), "", nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("all-cluster newConn err = %v, want terminal ErrClosed", err)
	}
}

// resettingHealthCheck reports unhealthy and runs onCheck while it executes,
// simulating an operator reselect (breaker Reset) landing mid-probe.
type resettingHealthCheck struct{ onCheck func() }

func (c *resettingHealthCheck) CheckHealth(context.Context, *Client) (bool, error) {
	c.onCheck()
	return false, nil
}

func (c *resettingHealthCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	c.onCheck()
	return false, nil
}

// TestProbeVerdictVoidedByResetDuringProbe pins that the probe records through
// the reset-generation gate: an operator reselect (breaker Reset) that lands
// while the probe's checks run voids the probe's failure verdict, so a stale
// probe cannot re-open the member the operator just selected.
func TestProbeVerdictVoidedByResetDuringProbe(t *testing.T) {
	cb := imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	db := &multidbDatabase{
		id:     0,
		cb:     cb,
		policy: defaultMultiDBPolicy{},
		c:      NewClient(&Options{Addr: "127.0.0.1:6379"}),
	}
	defer db.c.Close()

	chk := &resettingHealthCheck{onCheck: func() { cb.Reset() }}
	db.probeWith(context.Background(), time.Second, []MultiDBHealthCheck{chk})

	if cb.State() != imultidb.CircuitClosed {
		t.Fatalf("breaker %v, want closed: a probe failure overtaken by an operator Reset must not re-open the member", cb.State())
	}
}

// okLivenessCheck is an always-healthy, non-fail-back-only check: it stands in
// for the default PING liveness floor on members built without a client.
type okLivenessCheck struct{}

func (okLivenessCheck) CheckHealth(context.Context, *Client) (bool, error) { return true, nil }
func (okLivenessCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return true, nil
}

// failbackLagCheck is a fail-back-only check that reports unhealthy and, as a
// side effect, flips the active member mid-probe — simulating a failover that
// lands while a passive member is being probed with the full check set.
type failbackLagCheck struct{ onCheck func() }

func (c *failbackLagCheck) CheckHealth(context.Context, *Client) (bool, error) {
	c.onCheck()
	return false, nil
}

func (c *failbackLagCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	c.onCheck()
	return false, nil
}

func (c *failbackLagCheck) FailbackOnly() bool { return true }

// TestBackgroundProbeDoesNotEvictMemberGoneActive pins the split-verdict: the
// background pass runs a passive member's full checks without recording, then
// revalidates the active. If the member became active mid-probe, only the
// active-safe subset is recorded — so a fail-back-only failure (lag) cannot open
// the breaker of the member now serving traffic.
func TestBackgroundProbeDoesNotEvictMemberGoneActive(t *testing.T) {
	// A real probe timeout: the active-safe probe now always runs (liveness
	// floor), and an already-expired probe context reads as unhealthy.
	core := newMultidbCore(&MultiDBOptions{HealthCheckTimeout: time.Second})
	mkCB := func() *imultidb.CircuitBreaker {
		return imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	}
	// Both members carry a passing liveness check standing in for the default
	// PING floor (neither has a client).
	a := &multidbDatabase{id: 0, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{okLivenessCheck{}}}
	b := &multidbDatabase{id: 1, cb: mkCB(), policy: defaultMultiDBPolicy{}}
	core.dbs[0] = a
	core.dbs[1] = b
	core.active.Store(0) // A active at the start of the pass

	// The fail-back-only check is the one whose failure must not evict b once
	// it is the active.
	b.checks = []MultiDBHealthCheck{okLivenessCheck{}, &failbackLagCheck{onCheck: func() { core.active.Store(1) }}}

	core.runHealthChecksOnce(context.Background())

	if b.cb.State() != imultidb.CircuitClosed {
		t.Fatalf("member that became active mid-probe was evicted by a fail-back-only check: breaker=%v", b.cb.State())
	}
}

// lockProbeCheck records whether failoverMu was UNheld at the moment the health
// check ran — i.e. whether the initial probe runs off the failover lock.
type lockProbeCheck struct {
	core    *multidbCore
	offLock *bool
}

func (c *lockProbeCheck) CheckHealth(context.Context, *Client) (bool, error) {
	if c.core.failoverMu.TryLock() {
		c.core.failoverMu.Unlock()
		*c.offLock = true
	}
	return true, nil
}

func (c *lockProbeCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return c.CheckHealth(nil, nil)
}

// TestAddDatabaseProbesOffFailoverLock pins that AddDatabase runs the initial
// health probe WITHOUT holding failoverMu. Holding it across the probe (up to
// HealthCheckTimeout, or unbounded for an uncooperative custom check) would
// block an urgent tryFailover, which needs the same lock, and let short command
// contexts expire.
func TestAddDatabaseProbesOffFailoverLock(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{
		HealthCheckTimeout:   time.Second,
		HealthCheckPolicy:    defaultMultiDBPolicy{},
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Second},
	})
	offLock := false
	chk := &lockProbeCheck{core: core, offLock: &offLock}

	_, _ = core.addDatabase(context.Background(), MultiDBClientConfig{
		Options:      &Options{Addr: "127.0.0.1:6379"},
		HealthChecks: []MultiDBHealthCheck{chk},
		Weight:       1,
	})

	if !offLock {
		t.Fatal("initial health probe ran while holding failoverMu — a slow check would block failover")
	}
}

// AddDatabase must refuse a cluster member once Close has begun tearing the
// client down. Close clears the cached autopipeliner pointers and flips
// autopipelinerClosed under autopipelinerMu before it drains the accepted
// batches. Without the closed check the liveness guard sees no live
// autopipeliner and would admit the cluster member while the drain is still
// flushing, letting a failing-over batch reach it through the unsharded
// autopipeline path the cluster member does not support.
func TestAddClusterDatabaseRejectedAfterCloseBegan(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	c := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex), autopipelinerClosed: true}

	before := len(core.dbs)
	id, err := c.AddDatabase(context.Background(), MultiDBClientConfig{
		ClusterOptions:         &ClusterOptions{Addrs: []string{"127.0.0.1:6379"}},
		SkipInitialHealthCheck: true,
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("cluster add during shutdown: got id=%d err=%v, want ErrClosed", id, err)
	}
	if got := len(core.dbs); got != before {
		t.Fatalf("cluster add during shutdown mutated membership: %d -> %d", before, got)
	}
}

// AddDatabase must refuse a STANDALONE member too once Close has begun: the
// shutdown guard is not cluster-specific. Otherwise a standalone add racing
// Close could publish a member that the completing Close immediately tears down,
// handing the caller an id for a dead member.
func TestAddStandaloneDatabaseRejectedAfterCloseBegan(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	c := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex), autopipelinerClosed: true}

	before := len(core.dbs)
	id, err := c.AddDatabase(context.Background(), MultiDBClientConfig{
		Options:                &Options{Addr: "127.0.0.1:6379"},
		SkipInitialHealthCheck: true,
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("standalone add during shutdown: got id=%d err=%v, want ErrClosed", id, err)
	}
	if got := len(core.dbs); got != before {
		t.Fatalf("standalone add during shutdown mutated membership: %d -> %d", before, got)
	}
}

// apCallingCheck is a health check that calls the MultiDBClient's own
// AutoPipeline() from inside the probe — the re-entrant pattern that would
// deadlock a cluster AddDatabase holding autopipelinerMu across the probe.
type apCallingCheck struct {
	mdb *MultiDBClient
	got chan error
}

func (c *apCallingCheck) CheckHealth(context.Context, *Client) (bool, error) {
	_, err := c.mdb.AutoPipeline()
	c.got <- err
	return true, nil
}

func (c *apCallingCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	_, err := c.mdb.AutoPipeline()
	c.got <- err
	return true, nil
}

// TestAddClusterDatabaseProbeMayCallAutoPipeline pins that a cluster AddDatabase
// does not hold autopipelinerMu across the member's initial health probe: a
// custom check that calls AutoPipeline() must neither deadlock nor be refused.
// Cluster members and autopipeliners coexist — commands that cannot ride a
// pipeline on the member are kept out of merged batches instead.
func TestAddClusterDatabaseProbeMayCallAutoPipeline(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{
		HealthCheckTimeout:   time.Second,
		HealthCheckPolicy:    defaultMultiDBPolicy{},
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Second},
	})
	t.Cleanup(func() { _ = core.close() })
	mdb := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}
	got := make(chan error, 1)
	chk := &apCallingCheck{mdb: mdb, got: got}

	done := make(chan error, 1)
	go func() {
		_, err := mdb.AddDatabase(context.Background(), MultiDBClientConfig{
			ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:6379"}},
			HealthChecks:   []MultiDBHealthCheck{chk},
			Weight:         1,
		})
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("AddDatabase deadlocked: the probe's AutoPipeline() call blocked on autopipelinerMu held across the probe")
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("AutoPipeline() from inside a cluster member's probe: %v, want an instance", err)
		}
	default:
		t.Fatal("health check never ran")
	}
}

// TestNaNWeightRejected pins that a NaN member weight is rejected on both the
// config path (validate) and the runtime API (setWeight). A stored NaN makes
// every ordered comparison in selection/auto-fallback false, degenerating
// priority to iteration order.
func TestNaNWeightRejected(t *testing.T) {
	cfg := MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:6379"}, Weight: math.NaN()}
	if err := cfg.validate(); err == nil {
		t.Fatal("validate accepted a NaN Weight")
	}

	core := newMultidbCore(&MultiDBOptions{})
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	if err := core.setWeight(0, math.NaN()); err == nil {
		t.Fatal("setWeight accepted a NaN weight")
	}
	if core.dbs[0].weight != 1 {
		t.Fatalf("NaN weight was stored (weight=%v), member selection is now poisoned", core.dbs[0].weight)
	}
}

// TestRemovedFormerActiveErrClosedDoesNotSurface pins that a command whose
// snapshotted active member was removed mid-flight (its client closed) does not
// surface the terminal ErrClosed to the caller while the MultiDBClient is still
// open. It surfaces the retryable ErrTemporarilyNotAvailable instead, so the
// caller retries like it would for a transport failure; nothing is replayed by
// the client itself.
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

// execAll marks every command as executed — the per-command marker a fully
// executed batch produces (see executedCmds / markPipelineExecuted).
func execAll(cmds []Cmder) *executedCmds {
	ec := newExecutedCmds(len(cmds))
	ec.mark(cmds)
	return ec
}

// batchTimeoutErr is a pointer-typed local read timeout: shouldRetry treats it
// as retryable only when retryTimeout is set, so it is neutral for a blocking
// command and a transport failure for an ordinary one — the asymmetry the
// propagation rule below has to reconcile. Pointer identity mirrors the real
// *net.OpError the reader stamps onto unread followers; the padding byte keeps
// distinct allocations at distinct addresses (zero-size values may alias).
type batchTimeoutErr struct{ _ byte }

func (*batchTimeoutErr) Error() string { return "i/o timeout" }
func (*batchTimeoutErr) Timeout() bool { return true }

// TestRecordBatchOutcomesPropagatedBlockingTimeoutIsNeutral pins the reader
// stamping rule: pipelineReadCmds stops at the first transport error and stamps
// that same error onto every unread follower. When the originator is a blocking
// command whose local deadline is neutral, the followers are propagations of
// that one event — not N transport failures that would charge the breaker and
// replay the batch (blocking command included).
func TestRecordBatchOutcomesPropagatedBlockingTimeoutIsNeutral(t *testing.T) {
	newDB := func() *multidbDatabase {
		return &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
		})}
	}
	blocking := func() *StatusCmd {
		b := NewStatusCmd(context.Background(), "blpop", "k", "5")
		b.setReadTimeout(5 * time.Second)
		return b
	}
	ordinary := func(name string) *StatusCmd {
		return NewStatusCmd(context.Background(), name, "k", "v")
	}
	stamp := func(err error, cmds ...Cmder) {
		for _, c := range cmds {
			c.SetErr(err)
		}
	}

	t.Run("blocking origin: followers are neutral, batch not replayed", func(t *testing.T) {
		core := newMultidbCore(&MultiDBOptions{})
		db := newDB()
		e := &batchTimeoutErr{}
		cmds := []Cmder{blocking(), ordinary("set"), ordinary("get")}
		stamp(e, cmds...) // reader: origin + identical instance on followers

		got := core.recordBatchOutcomes(db, cmds, e, execAll(cmds), imultidb.Reservation{}, 0)
		if got != 0 {
			t.Errorf("transportFailures = %d, want 0: the followers carry the blocker's neutral deadline", got)
		}
		if st := db.cb.State(); st != imultidb.CircuitClosed {
			t.Errorf("breaker %v, want closed: one neutral event must not be charged N times", st)
		}
		for i, c := range cmds {
			if c.rawErr() != e {
				t.Errorf("cmd %d error %v, want the propagated timeout surfaced to the caller", i, c.rawErr())
			}
		}
	})

	t.Run("ordinary origin: propagation stays a transport failure", func(t *testing.T) {
		core := newMultidbCore(&MultiDBOptions{})
		db := newDB()
		e := &batchTimeoutErr{}
		cmds := []Cmder{ordinary("set"), ordinary("get"), ordinary("incr")}
		stamp(e, cmds...)

		if got := core.recordBatchOutcomes(db, cmds, e, execAll(cmds), imultidb.Reservation{}, 0); got != 3 {
			t.Errorf("transportFailures = %d, want 3: an ordinary command's timeout is a real transport failure", got)
		}
		if st := db.cb.State(); st != imultidb.CircuitOpen {
			t.Errorf("breaker %v, want open", st)
		}
	})

	t.Run("blocking origin but followers carry a different error", func(t *testing.T) {
		core := newMultidbCore(&MultiDBOptions{})
		db := newDB()
		origin, other := &batchTimeoutErr{}, &batchTimeoutErr{}
		cmds := []Cmder{blocking(), ordinary("set"), ordinary("get")}
		stamp(origin, cmds[0])
		stamp(other, cmds[1], cmds[2]) // distinct instance: not a propagation

		if got := core.recordBatchOutcomes(db, cmds, origin, execAll(cmds), imultidb.Reservation{}, 0); got != 2 {
			t.Errorf("transportFailures = %d, want 2: only an identical stamped error is a propagation", got)
		}
		if st := db.cb.State(); st != imultidb.CircuitOpen {
			t.Errorf("breaker %v, want open", st)
		}
	})

	t.Run("ordinary origin stamps a later blocking command", func(t *testing.T) {
		// The first carrier in slice order is the originator. A blocking
		// command that merely RECEIVED an ordinary command's timeout must not
		// retroactively neutralize it.
		core := newMultidbCore(&MultiDBOptions{})
		db := newDB()
		e := &batchTimeoutErr{}
		cmds := []Cmder{ordinary("set"), blocking(), ordinary("get")}
		stamp(e, cmds...)

		// set: failure (origin); blpop: neutral (its own rule); get: failure.
		if got := core.recordBatchOutcomes(db, cmds, e, execAll(cmds), imultidb.Reservation{}, 0); got != 2 {
			t.Errorf("transportFailures = %d, want 2: the ordinary originator's timeout is real", got)
		}
		if st := db.cb.State(); st != imultidb.CircuitOpen {
			t.Errorf("breaker %v, want open", st)
		}
	})
}

// TestRecordBatchOutcomesStaleReservationFailureDoesNotReopen pins the failure
// path's reservation binding: a half-open batch that outlives its recovery
// episode must not apply its failure to the NEW half-open episode another
// request has since opened (that would abort a recovery it was never admitted
// to). A current admission still re-opens, and a closed admission still counts
// toward opening.
func TestRecordBatchOutcomesStaleReservationFailureDoesNotReopen(t *testing.T) {
	newHalfOpen := func(t *testing.T) *multidbDatabase {
		t.Helper()
		db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			GracePeriod:      20 * time.Millisecond,
		})}
		db.cb.RecordFailure() // open
		time.Sleep(30 * time.Millisecond)
		if st := db.cb.CheckState(); st != imultidb.CircuitHalfOpen {
			t.Fatalf("state after grace = %v, want half-open", st)
		}
		return db
	}
	failedBatch := func() []Cmder {
		cmd := NewStatusCmd(context.Background(), "set", "k", "v")
		cmd.SetErr(io.EOF)
		return []Cmder{cmd}
	}

	t.Run("stale half-open admission records nothing", func(t *testing.T) {
		core := newMultidbCore(&MultiDBOptions{})
		db := newHalfOpen(t)
		ok, stale := db.cb.AllowReserve() // episode 1 probe slot
		if !ok {
			t.Fatal("AllowReserve on a half-open breaker with a free slot was rejected")
		}
		// The recovery fails elsewhere and a NEW half-open episode begins.
		db.cb.ForceOpen()
		time.Sleep(30 * time.Millisecond)
		if st := db.cb.CheckState(); st != imultidb.CircuitHalfOpen {
			t.Fatalf("state after second grace = %v, want half-open", st)
		}

		cmds := failedBatch()
		got := core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), stale, 0)
		if got != 1 {
			t.Errorf("transportFailures = %d, want 1: the caller still sees a transport failure", got)
		}
		if st := db.cb.State(); st != imultidb.CircuitHalfOpen {
			t.Errorf("breaker %v, want half-open: a stale batch must not re-open the new episode", st)
		}
	})

	t.Run("current half-open admission re-opens", func(t *testing.T) {
		core := newMultidbCore(&MultiDBOptions{})
		db := newHalfOpen(t)
		ok, cur := db.cb.AllowReserve()
		if !ok {
			t.Fatal("AllowReserve on a half-open breaker with a free slot was rejected")
		}
		cmds := failedBatch()
		core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), cur, 0)
		if st := db.cb.State(); st != imultidb.CircuitOpen {
			t.Errorf("breaker %v, want open: a live probe's failure aborts the recovery", st)
		}
	})

	t.Run("closed admission counts toward opening", func(t *testing.T) {
		core := newMultidbCore(&MultiDBOptions{})
		db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
		})}
		cmds := failedBatch()
		core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{}, 0)
		if st := db.cb.State(); st != imultidb.CircuitOpen {
			t.Errorf("breaker %v, want open", st)
		}
	})
}

// TestProcessTxPipelineHImportPreservesFirstError pins that a rejected HIMPORT
// transaction reports the positionally-first error (Pipeline.Exec semantics):
// a command the caller queued with a pre-existing error, before the HIMPORT,
// must win over errMultiDBHImport.
func TestProcessTxPipelineHImportPreservesFirstError(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	prior := errors.New("prior error")
	preErr := NewStatusCmd(context.Background(), "set", "k", "v")
	preErr.SetErr(prior)
	him := NewStatusCmd(context.Background(), "himport", "fs")

	// Wrap like TxPipeline().Exec does before dispatch. The synthetic MULTI at
	// index 0 must not win the first-error ordering over a user command the
	// caller pre-stamped.
	wrapped := wrapMultiExec(context.Background(), []Cmder{preErr, him})
	err := core.processTxPipeline(context.Background(), wrapped)
	if !errors.Is(err, prior) {
		t.Fatalf("processTxPipeline = %v, want the positionally-first pre-existing error %v", err, prior)
	}
	// The HIMPORT command itself is stamped — proving the envelope was stripped
	// and the user slice stamped, not that stamping was skipped altogether.
	if !errors.Is(him.Err(), errMultiDBHImport) {
		t.Fatalf("HIMPORT command err = %v, want errMultiDBHImport", him.Err())
	}
}

func TestRecordBatchOutcomesPostExecHookError(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}

	cmds := []Cmder{NewStatusCmd(context.Background(), "set", "k", "v")}

	// Executed batch, every reply read fine, then a post-exec hook injected
	// a retryable error without stamping the commands: the commands are
	// authoritative — no phantom failures, no stamping, no replay signal.
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{}, 0); got != 0 {
		t.Errorf("transportFailures = %d for an executed all-success batch, want 0", got)
	}
	if err := cmds[0].Err(); err != nil {
		t.Errorf("executed batch had its successful command stamped with %v", err)
	}

	// Not executed (hook aborted before next): the batch error stands in
	// for the commands and is stamped so callers see it.
	resetCmds(cmds)
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, newExecutedCmds(0), imultidb.Reservation{}, 0); got != 1 {
		t.Errorf("transportFailures = %d for an unexecuted batch, want 1", got)
	}
	if err := cmds[0].Err(); err == nil {
		t.Error("unexecuted batch left the command unstamped")
	}
}

func TestMarkPipelineExecuted(t *testing.T) {
	cmd := NewStatusCmd(context.Background(), "ping")
	ec := newExecutedCmds(1)
	markPipelineExecuted(context.WithValue(context.Background(), pipelineExecutedKey{}, ec), []Cmder{cmd})
	if !ec.has(cmd) {
		t.Error("marker did not record the executed command")
	}
	if !ec.any() {
		t.Error("marker did not report any executed command")
	}
	// Without a marker in the context it must be a no-op, not a panic.
	markPipelineExecuted(context.Background(), []Cmder{cmd})
}

func TestRecordBatchOutcomesExecutedBatchKeepsSuccessfulPrefix(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}

	// Executed batch: the first command's nil error is a successfully-read
	// reply, the second carries a retryable server reply that is also the
	// batch error. Exactly one failure may be recorded, and the successful
	// prefix must stay unstamped — otherwise the batch would be replayed.
	loading := proto.RedisError("LOADING Redis is loading the dataset in memory")
	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k1", "v"),
		NewStatusCmd(context.Background(), "set", "k2", "v"),
	}
	cmds[1].SetErr(loading)

	if got := core.recordBatchOutcomes(db, cmds, loading, execAll(cmds), imultidb.Reservation{}, 0); got != 1 {
		t.Errorf("transportFailures = %d, want 1 (prefix must not count)", got)
	}
	if err := cmds[0].Err(); err != nil {
		t.Errorf("successful prefix was stamped with %v", err)
	}
}

func TestRecordBatchOutcomesFailuresBeforeSuccesses(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Nanosecond,
	})}
	db.cb.RecordFailure() // -> open; 1ns grace has already elapsed
	if db.cb.CheckState() != imultidb.CircuitHalfOpen {
		t.Fatal("setup: expected a half-open breaker")
	}

	// Executed mixed batch on a half-open breaker: the failure must be
	// recorded before the success, so a failed recovery batch re-opens the
	// circuit instead of its own successful prefix closing it.
	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k1", "v"),
		NewStatusCmd(context.Background(), "set", "k2", "v"),
	}
	cmds[1].SetErr(io.EOF)
	_, res := db.cb.AllowReserve() // authentic half-open admission for this batch
	core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), res, 0)

	if got := db.cb.State(); got != imultidb.CircuitOpen {
		t.Errorf("breaker state = %v after a failed recovery batch, want open", got)
	}
}

func TestRecordBatchOutcomesClosedStateKeepsArrivalOrder(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}
	db.cb.RecordFailure() // one stale failure below the threshold

	// Closed breaker: the batch's successful reply arrived BEFORE its EOF,
	// exactly like sequential single commands, whose ordering would reset
	// the stale failure count. Failure-first recording here would combine
	// the stale failure with the batch failure and open a healthy member's
	// circuit; that ordering is only for half-open recovery probes.
	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k1", "v"),
		NewStatusCmd(context.Background(), "set", "k2", "v"),
	}
	cmds[1].SetErr(io.EOF)
	core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{}, 0)

	if got := db.cb.State(); got != imultidb.CircuitClosed {
		t.Errorf("breaker state = %v, want closed (stale failure must be reset by the earlier success)", got)
	}
}

func TestRecordBatchOutcomesSuccessSinceFailover(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}
	// Make db the active member: recordBatchOutcomes marks recovery traffic
	// (and feeds the detector) only while the batch's member is still the
	// active, mirroring the single-command path.
	core.dbs[0] = db
	core.active.Store(0)

	// An executed batch success is recovery traffic: it breaks the
	// consecutive-failed-failover escalation chain.
	cmds := []Cmder{NewStatusCmd(context.Background(), "set", "k", "v")}
	core.recordBatchOutcomes(db, cmds, nil, execAll(cmds), imultidb.Reservation{}, 0)
	if !core.successSinceFailover.Load() {
		t.Error("executed batch success did not mark recovery traffic")
	}

	// A hook-served batch (nil without execution) is not.
	core.successSinceFailover.Store(false)
	resetCmds(cmds)
	core.recordBatchOutcomes(db, cmds, nil, newExecutedCmds(0), imultidb.Reservation{}, 0)
	if core.successSinceFailover.Load() {
		t.Error("hook-served batch counted as recovery traffic")
	}
}

// countingFD counts detector outcomes for the recordBatchOutcomes tests.
type countingFD struct {
	successes int
	failures  int
}

func (d *countingFD) RecordSuccess()       { d.successes++ }
func (d *countingFD) RecordFailure(error)  { d.failures++ }
func (d *countingFD) ShouldFailover() bool { return false }
func (d *countingFD) Reset()               {}

// TestRecordBatchOutcomesPartialExecutionDoesNotCountUntouched pins the
// per-command execution marker: in a cluster fan-out one node can execute while
// another short-circuits, leaving its commands untouched (nil error). Only the
// commands that actually executed may be recorded — an untouched nil-error
// command must not be counted as a database success.
func TestRecordBatchOutcomesPartialExecutionDoesNotCountUntouched(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}
	core.dbs[0] = db
	core.active.Store(0)

	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k1", "v"),
		NewStatusCmd(context.Background(), "set", "k2", "v"),
	}
	// Both commands look successful (nil error), but only the first actually
	// executed — the second's node short-circuited.
	ec := newExecutedCmds(len(cmds))
	ec.mark(cmds[:1])

	core.recordBatchOutcomes(db, cmds, nil, ec, imultidb.Reservation{}, 0)

	if det.successes != 1 {
		t.Errorf("detector successes = %d, want 1 (an untouched command must not count as a success)", det.successes)
	}
	if det.failures != 0 {
		t.Errorf("detector failures = %d, want 0 (an untouched command must not count at all)", det.failures)
	}
}

// unhashableCmd is a value-type Cmder (methods promoted from the embedded
// *StatusCmd) made non-comparable by a slice field — a legal Cmder that would
// panic as a map key.
type unhashableCmd struct {
	*StatusCmd
	extra []int
}

// TestExecutedCmdsTracksNonComparableCommand pins that executedCmds tracks a
// non-comparable command (Cmder imposes no comparability contract) by the
// identity of the command it embeds, instead of skipping it: a batch of such
// commands that executed must not look never-executed after a transport
// error, or a retry could replay writes that had applied.
func TestExecutedCmdsTracksNonComparableCommand(t *testing.T) {
	e := newExecutedCmds(2)
	inner := NewStatusCmd(context.Background(), "ping")
	cmd := unhashableCmd{StatusCmd: inner, extra: []int{1}}

	e.mark([]Cmder{cmd}) // must not panic ("hash of unhashable type")
	if !e.has(cmd) {
		t.Error("non-comparable command not reported executed after mark")
	}
	if !e.any() {
		t.Error("a batch of one non-comparable command looks never-executed")
	}
	// Identity is the embedded command: another decorator around it, and the
	// bare command, are the same execution.
	if !e.has(unhashableCmd{StatusCmd: inner, extra: []int{2}}) || !e.has(inner) {
		t.Error("the embedded command's identity is not what is tracked")
	}
	if e.has(NewStatusCmd(context.Background(), "ping")) {
		t.Error("a different command reported executed")
	}

	// A normal pointer command is still tracked.
	normal := NewStatusCmd(context.Background(), "get", "k")
	e.mark([]Cmder{normal})
	if !e.has(normal) {
		t.Error("normal command not tracked")
	}
}

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

// TestProcessPipelineReusedCmdErrorsDoNotMaskTransportFailure pins the reset
// before execution. A caller may hand the batch a command that still carries a
// Redis error from a prior use. When connection acquisition then fails before
// anything executes, the stale per-command errors must not mask the transport
// failure: setCmdsErr fills only empty slots, so without the reset it never
// stamps the batch error, every command classifies as an unexecuted success,
// transportFailures stays 0, and the outage is neither recorded on the detector
// nor failed over. The reset clears the stale errors so the connection-acquire
// failure is recorded.
func TestProcessPipelineReusedCmdErrorsDoNotMaskTransportFailure(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det, CommandRetries: 0})
	dead := NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer dead.Close()
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: dead, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	})}
	core.active.Store(0)

	cmds := []Cmder{
		NewStatusCmd(context.Background(), "set", "k", "v"),
		NewStatusCmd(context.Background(), "get", "k"),
	}
	// Reused commands still carrying a Redis error from a prior batch.
	for _, cmd := range cmds {
		cmd.SetErr(proto.RedisError("ERR from a prior use"))
	}

	_ = core.processPipeline(context.Background(), cmds)

	if det.failures == 0 {
		t.Fatalf("transport failure not recorded: stale per-command errors masked the connection-acquire failure, so no failover would trigger")
	}
}

// TestProcessTxPipelineReusedCmdErrorsDoNotMaskTransportFailure is the tx
// analogue: processTxPipeline classifies only the user slice inside the
// MULTI/EXEC envelope, and a reused user command carrying a stale Redis error
// would otherwise mask a connection-acquire failure exactly as in the plain
// pipeline path. The reset targets the user slice; the envelope is clean.
func TestProcessTxPipelineReusedCmdErrorsDoNotMaskTransportFailure(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det, CommandRetries: 0})
	dead := NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer dead.Close()
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: dead, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	})}
	core.active.Store(0)

	// [MULTI, set, get, EXEC]: the user slice is the middle two commands, and
	// they are the reused ones carrying a stale Redis error.
	multi := NewStatusCmd(context.Background(), "multi")
	exec := NewSliceCmd(context.Background(), "exec")
	set := NewStatusCmd(context.Background(), "set", "k", "v")
	get := NewStatusCmd(context.Background(), "get", "k")
	set.SetErr(proto.RedisError("ERR from a prior use"))
	get.SetErr(proto.RedisError("ERR from a prior use"))

	_ = core.processTxPipeline(context.Background(), []Cmder{multi, set, get, exec})

	if det.failures == 0 {
		t.Fatalf("transport failure not recorded: stale user-command errors masked the connection-acquire failure inside the transaction")
	}
}

// TestTxPipelineRestoresNoRetryForDuplicateCommand pins that the temporary
// NoRetry marker TxPipeline sets on the wrapped batch is restored correctly even
// when the SAME Cmder is queued twice. The second occurrence snapshots the
// already-set marker, so a forward-order restore would end on prev=true and
// leave the shared command permanently non-retryable for later ordinary use.
func TestTxPipelineRestoresNoRetryForDuplicateCommand(t *testing.T) {
	core := newRemovedActiveCore(t) // exec returns fast; the deferred restore still runs
	mdb := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}
	mdb.initHooks(hooks{
		process:    core.process,
		pipeline:   core.processPipeline,
		txPipeline: core.processTxPipeline,
	})

	cmd := NewStatusCmd(context.Background(), "set", "k", "v")
	if cmd.NoRetry() {
		t.Fatal("precondition: fresh command must be retryable")
	}
	pipe := mdb.TxPipeline()
	_ = pipe.Process(context.Background(), cmd)
	_ = pipe.Process(context.Background(), cmd) // same object queued twice
	_, _ = pipe.Exec(context.Background())      // error expected; restore must still run

	if cmd.NoRetry() {
		t.Fatal("NoRetry left set on a command queued twice in a TxPipeline — a later ordinary request would silently lose retries")
	}
}

// TestRecordBatchOutcomesSkipsDetectorAfterReset pins the detector window
// guard: a batch that sampled its detector generation before a reset (an
// operator reselect of the current member, an automatic failover, a fallback)
// must not record its outcome into the fresh window, while a batch in the
// current window records normally.
func TestRecordBatchOutcomesSkipsDetectorAfterReset(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det})
	db := &multidbDatabase{id: 0, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
	})}
	core.dbs[0] = db
	core.active.Store(0)
	cmds := []Cmder{NewStatusCmd(context.Background(), "set", "k", "v")}
	setCmdsErr(cmds, io.EOF)

	dg := core.detectorGen.Load()
	core.resetDetectorSafely() // a reset lands after the batch sampled its window
	core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{}, dg)
	if det.failures != 0 {
		t.Fatalf("pre-reset failure recorded into the fresh detector window: %d", det.failures)
	}
	core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{}, core.detectorGen.Load())
	if det.failures != 1 {
		t.Fatalf("current-window failure not recorded: %d", det.failures)
	}
}

// reselectHook force-reselects the current active member from inside the
// command (resetting breaker and detector) and then completes the command with
// err — the in-flight-command-vs-same-member-reselect race, on either outcome.
type reselectHook struct {
	core *multidbCore
	err  error
}

func (reselectHook) DialHook(next DialHook) DialHook { return next }
func (h reselectHook) ProcessHook(ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		_ = h.core.setActiveDatabase(ctx, 0, false)
		return h.err
	}
}

func (reselectHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestProcessSkipsDetectorAfterSameMemberReselect pins the single-command
// path: an operator reselect of the CURRENT member while a command is in flight
// resets the detector; the command's failure must not land in that fresh window
// even though the member identity is unchanged.
func TestProcessSkipsDetectorAfterSameMemberReselect(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det})
	a := NewClient(&Options{Addr: "127.0.0.1:6379", MaxRetries: -1})
	t.Cleanup(func() { _ = a.Close() })
	a.AddHook(reselectHook{core: core, err: io.EOF})
	db := &multidbDatabase{id: 0, weight: 1, c: a, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
	})}
	core.dbs[0] = db
	core.active.Store(0)

	_ = core.process(context.Background(), NewStatusCmd(context.Background(), "ping"))

	if det.failures != 0 {
		t.Fatalf("failure from before the same-member reselect polluted the fresh detector window: %d", det.failures)
	}
}

// TestProcessSameMemberReselectStillBreaksEscalation pins the success side of
// the same race: the escalation flag means "any success on the active" and has
// no window semantics, so it must be set even when the detector write is
// skipped for being outside the sampled window.
func TestProcessSameMemberReselectStillBreaksEscalation(t *testing.T) {
	det := &countingFD{}
	core := newMultidbCore(&MultiDBOptions{FailureDetector: det})
	a := NewClient(&Options{Addr: "127.0.0.1:6379", MaxRetries: -1})
	t.Cleanup(func() { _ = a.Close() })
	a.AddHook(reselectHook{core: core, err: nil})
	db := &multidbDatabase{id: 0, weight: 1, c: a, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
	})}
	core.dbs[0] = db
	core.active.Store(0)
	core.successSinceFailover.Store(false)

	if err := core.process(context.Background(), NewStatusCmd(context.Background(), "ping")); err != nil {
		t.Fatalf("process: %v", err)
	}
	if !core.successSinceFailover.Load() {
		t.Fatal("a success on the active must break the failed-failover chain even when its detector write is outside the sampled window")
	}
	if det.successes != 0 {
		t.Fatalf("success from before the same-member reselect polluted the fresh detector window: %d", det.successes)
	}
}

// scriptedStrategy returns a fixed sequence of ids, one per Select call, and
// records how many candidates each call was offered.
type scriptedStrategy struct {
	picks   []int
	offered []int
	before  func(call int) // optional side effect run before each pick
}

func (s *scriptedStrategy) Select(cands []MultiDBDatabaseState) int {
	if s.before != nil {
		s.before(len(s.offered))
	}
	s.offered = append(s.offered, len(cands))
	i := len(s.offered) - 1
	if i >= len(s.picks) {
		return -1
	}
	return s.picks[i]
}

// TestSelectCandidateDropsDisallowedPick pins the strategy contract: an id
// whose candidate is not Allowed is never seated — the candidate is dropped and
// Select is asked again with the rest, so one bad pick does not abort a
// failover round that still has admissible candidates. The caller's slice must
// come back untouched (removeCandidate compacts in place).
func TestSelectCandidateDropsDisallowedPick(t *testing.T) {
	s := &scriptedStrategy{picks: []int{0, 1}}
	core := newMultidbCore(&MultiDBOptions{FailoverStrategy: s})
	cands := []MultiDBDatabaseState{
		{ID: 0, Weight: 1, Allowed: false},
		{ID: 1, Weight: 1, Allowed: true},
	}
	if got := core.selectCandidate(cands); got != 1 {
		t.Fatalf("selectCandidate = %d, want 1 (the allowed candidate after dropping the disallowed pick)", got)
	}
	if len(s.offered) != 2 || s.offered[1] != 1 {
		t.Fatalf("Select offered sizes = %v, want a second call offered only the remaining candidate", s.offered)
	}
	if cands[0].ID != 0 || cands[1].ID != 1 {
		t.Fatalf("caller's candidate slice was mutated: %+v", cands)
	}
}

// TestSelectCandidateGivesUpWhenOnlyDisallowedPicked: a strategy that keeps
// naming ids outside the shrinking offered set ends with no target, never with
// a disallowed member.
func TestSelectCandidateGivesUpWhenOnlyDisallowedPicked(t *testing.T) {
	s := &scriptedStrategy{picks: []int{0, 0}}
	core := newMultidbCore(&MultiDBOptions{FailoverStrategy: s})
	cands := []MultiDBDatabaseState{
		{ID: 0, Weight: 1, Allowed: false},
		{ID: 1, Weight: 1, Allowed: true},
	}
	if got := core.selectCandidate(cands); got != -1 {
		t.Fatalf("selectCandidate = %d, want -1", got)
	}
}

// TestPubSubDialErrRewritesRemovedMemberClosed pins the PubSub dial
// classification: a removed member's closed pool yields ErrClosed, which the
// channel loops treat as terminal; it must come back as the retryable
// ErrTemporarilyNotAvailable so the loop re-dials the live active. A live
// member's ErrClosed and any other error pass through untouched.
func TestPubSubDialErrRewritesRemovedMemberClosed(t *testing.T) {
	db := &multidbDatabase{}
	if err := pubSubDialErr(db, ErrClosed, false); !errors.Is(err, ErrClosed) {
		t.Fatalf("live member: got %v, want ErrClosed passthrough", err)
	}
	db.removed.Store(true)
	if err := pubSubDialErr(db, ErrClosed, false); !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("removed member: got %v, want ErrTemporarilyNotAvailable", err)
	}
	if err := pubSubDialErr(db, io.EOF, false); !errors.Is(err, io.EOF) {
		t.Fatalf("removed member, other error: got %v, want io.EOF passthrough", err)
	}
	// Close marks every member removed: with the client itself closed the
	// terminal error must survive, or the channel loop would keep re-dialing.
	if err := pubSubDialErr(db, ErrClosed, true); !errors.Is(err, ErrClosed) {
		t.Fatalf("removed member on a closed client: got %v, want ErrClosed kept", err)
	}
}

// TestRewriteRemovedMemberErr pins the cluster-override rewrite: ErrClosed
// from a member removed mid-call becomes the retryable
// ErrTemporarilyNotAvailable; a live member's ErrClosed and other errors stay.
func TestRewriteRemovedMemberErr(t *testing.T) {
	ctx := context.Background()
	db := &multidbDatabase{}
	cmd := NewIntCmd(ctx, "dbsize")
	cmd.SetErr(ErrClosed)
	rewriteRemovedMemberErr(db, cmd, false)
	if !errors.Is(cmd.Err(), ErrClosed) {
		t.Fatalf("live member: got %v, want ErrClosed untouched", cmd.Err())
	}
	db.removed.Store(true)
	rewriteRemovedMemberErr(db, cmd, false)
	if !errors.Is(cmd.Err(), ErrTemporarilyNotAvailable) {
		t.Fatalf("removed member: got %v, want ErrTemporarilyNotAvailable", cmd.Err())
	}
	other := NewIntCmd(ctx, "dbsize")
	other.SetErr(io.EOF)
	rewriteRemovedMemberErr(db, other, false)
	if !errors.Is(other.Err(), io.EOF) {
		t.Fatalf("removed member, other error: got %v, want io.EOF untouched", other.Err())
	}
	// Close marks every member removed: with the client itself closed the
	// terminal error must survive, as it does on the command and batch paths.
	closedCmd := NewIntCmd(ctx, "dbsize")
	closedCmd.SetErr(ErrClosed)
	rewriteRemovedMemberErr(db, closedCmd, true)
	if !errors.Is(closedCmd.Err(), ErrClosed) {
		t.Fatalf("removed member on a closed client: got %v, want ErrClosed kept", closedCmd.Err())
	}
}

// shouldFailoverPanicFD is a custom detector whose ShouldFailover panics.
type shouldFailoverPanicFD struct{}

func (shouldFailoverPanicFD) RecordSuccess()       {}
func (shouldFailoverPanicFD) RecordFailure(error)  {}
func (shouldFailoverPanicFD) ShouldFailover() bool { panic("custom detector bug") }
func (shouldFailoverPanicFD) Reset()               {}

// TestShouldFailoverSafelyRecoversPanic pins the gate's panic safety: a
// panicking custom detector must not escape (on the background loop it would
// end health checking for good) and reads as "not tripped", leaving failover
// to the breaker.
func TestShouldFailoverSafelyRecoversPanic(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{FailureDetector: shouldFailoverPanicFD{}})
	if core.shouldFailoverSafely() {
		t.Fatal("a panicking detector must read as not tripped")
	}
}

// latchedFD is a custom detector that stays tripped until Reset.
type latchedFD struct{ tripped bool }

func (d *latchedFD) RecordSuccess()       {}
func (d *latchedFD) RecordFailure(error)  {}
func (d *latchedFD) ShouldFailover() bool { return d.tripped }
func (d *latchedFD) Reset()               { d.tripped = false }

// newLazyMember builds a member around a never-dialed client with a closed
// breaker, for control-path tests that never send a command.
func newLazyMember(id int) *multidbDatabase {
	return &multidbDatabase{
		id:     id,
		weight: 1,
		c:      NewClient(&Options{Addr: "127.0.0.1:6379", MaxRetries: -1}),
		cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 1,
		}),
	}
}

// TestBackgroundFailoverOnceHonorsTrippedDetector pins the background twin of
// the gate: a tripped detector alone (breaker closed) must move the active off
// the member with no command traffic, and the switch resets the detector so a
// second pass does not ping-pong.
func TestBackgroundFailoverOnceHonorsTrippedDetector(t *testing.T) {
	det := &latchedFD{tripped: true}
	core := newMultidbCore(&MultiDBOptions{
		FailureDetector:  det,
		FailoverStrategy: &scriptedStrategy{picks: []int{1, 0}},
	})
	a, b := newLazyMember(0), newLazyMember(1)
	t.Cleanup(func() { _ = a.c.Close(); _ = b.c.Close() })
	core.dbs[0], core.dbs[1] = a, b
	core.active.Store(0)

	core.backgroundFailoverOnce(context.Background())
	if got := core.active.Load(); got != 1 {
		t.Fatalf("active = %d after a pass with a tripped detector and a closed breaker, want 1", got)
	}
	if det.tripped {
		t.Fatal("the switch must reset the detector")
	}
	core.backgroundFailoverOnce(context.Background())
	if got := core.active.Load(); got != 1 {
		t.Fatalf("active = %d after a second pass with a clear detector, want 1 (no ping-pong)", got)
	}
}

// TestTryFailoverRevalidatesCandidateAtCommit pins commit-time revalidation:
// a candidate whose breaker opened after candidates() snapshotted it must not
// be published; it is dropped and the next pick is taken.
func TestTryFailoverRevalidatesCandidateAtCommit(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{FailureDetector: &countingFD{}})
	a, b, cc := newLazyMember(0), newLazyMember(1), newLazyMember(2)
	t.Cleanup(func() { _ = a.c.Close(); _ = b.c.Close(); _ = cc.c.Close() })
	core.dbs[0], core.dbs[1], core.dbs[2] = a, b, cc
	core.active.Store(0)
	a.cb.ForceOpen()
	core.strategy = &scriptedStrategy{
		picks: []int{1, 2},
		before: func(call int) {
			if call == 0 {
				b.cb.ForceOpen() // opens between the snapshot and the commit
			}
		},
	}

	if err := core.tryFailover(context.Background(), 0); err != nil {
		t.Fatalf("tryFailover: %v", err)
	}
	if got := core.active.Load(); got != 2 {
		t.Fatalf("active = %d, want 2 (the stale pick 1 opened before commit)", got)
	}
}

// TestWatchRewritesRemovedMemberClosed pins the Watch removed-member race: a
// member removed mid-call answers ErrClosed from its closed client although the
// MultiDBClient is open; the caller must see the retryable error, and the
// transaction is not replayed.
func TestWatchRewritesRemovedMemberClosed(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{FailureDetector: &countingFD{}})
	a := newLazyMember(0)
	core.dbs[0] = a
	core.active.Store(0)
	_ = a.c.Close()
	a.removed.Store(true)
	mc := &MultiDBClient{core: core}

	calls := 0
	err := mc.Watch(context.Background(), func(*Tx) error { calls++; return nil }, "k")
	if !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("Watch on a removed member: got %v, want ErrTemporarilyNotAvailable", err)
	}
	if calls != 0 {
		t.Fatalf("fn ran %d times, want 0 (WATCH failed before fn; no replay)", calls)
	}
}

// blockingCheck signals when it starts and then blocks until its context ends.
type blockingCheck struct {
	started chan struct{}
	once    sync.Once
}

func (b *blockingCheck) CheckHealth(ctx context.Context, _ *Client) (bool, error) {
	b.once.Do(func() { close(b.started) })
	<-ctx.Done()
	return false, ctx.Err()
}

func (b *blockingCheck) CheckClusterHealth(ctx context.Context, _ *ClusterClient) (bool, error) {
	return b.CheckHealth(ctx, nil)
}

// TestCloseCancelsInFlightAddDatabaseProbe pins shutdown vs AddDatabase: a
// member whose startup probe is still running when Close lands is not in
// c.dbs yet, so closeAll cannot drain it. Close must cancel the probe so the
// add unwinds promptly (instead of waiting out HealthCheckTimeout) and closes
// the built client itself.
func TestCloseCancelsInFlightAddDatabaseProbe(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{
		FailureDetector:      &countingFD{},
		HealthCheckTimeout:   30 * time.Second,
		HealthCheckPolicy:    defaultMultiDBPolicy{},
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Second},
	})
	check := &blockingCheck{started: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := core.addDatabase(context.Background(), MultiDBClientConfig{
			Options:      &Options{Addr: "127.0.0.1:6379", MaxRetries: -1},
			Weight:       1,
			HealthChecks: []MultiDBHealthCheck{check},
		})
		done <- err
	}()
	select {
	case <-check.started:
	case <-time.After(5 * time.Second):
		t.Fatal("startup probe never started")
	}
	if err := core.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("addDatabase after Close: got %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("addDatabase did not unwind after Close canceled its probe (would wait out HealthCheckTimeout)")
	}
}

// lagOnlyStub is a fail-back-only check (like the lag-aware REST check) that
// always passes, to exercise a member with no liveness check of its own.
type lagOnlyStub struct{}

func (lagOnlyStub) CheckHealth(context.Context, *Client) (bool, error) { return true, nil }
func (lagOnlyStub) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return true, nil
}
func (lagOnlyStub) FailbackOnly() bool { return true }

// TestNonFailbackChecksKeepsLivenessFloor pins the liveness floor: a member
// configured with only fail-back-only checks still gets the default PING as
// its active-safe probe, so a dead active endpoint is detected on an idle
// client instead of the active probe becoming a no-op.
func TestNonFailbackChecksKeepsLivenessFloor(t *testing.T) {
	db := &multidbDatabase{checks: []MultiDBHealthCheck{lagOnlyStub{}}}
	got := db.nonFailbackChecks()
	if len(got) != 1 {
		t.Fatalf("nonFailbackChecks = %d checks, want 1 (the default PING)", len(got))
	}
	if _, ok := got[0].(defaultPingHealthCheck); !ok {
		t.Fatalf("nonFailbackChecks = %T, want defaultPingHealthCheck", got[0])
	}
	// The floor is a real probe: against a refusing endpoint the active-safe
	// check set reports unhealthy instead of vacuously healthy.
	dead := &multidbDatabase{
		checks: []MultiDBHealthCheck{lagOnlyStub{}},
		c:      NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond}),
		policy: defaultMultiDBPolicy{},
	}
	t.Cleanup(func() { _ = dead.c.Close() })
	if dead.checkHealthyNoRecord(context.Background(), time.Second, dead.nonFailbackChecks()) {
		t.Fatal("a member with only fail-back-only checks and a dead endpoint must not probe healthy")
	}
}

// pongHook answers every command with PONG without dialing, counting calls.
type pongHook struct{ pings *int }

func (pongHook) DialHook(next DialHook) DialHook { return next }
func (h pongHook) ProcessHook(ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		*h.pings++
		if sc, ok := cmd.(*StatusCmd); ok {
			sc.SetVal("PONG")
		}
		return nil
	}
}

func (pongHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestBackgroundProbeLagOnlyMemberGoneActiveUsesPingFloor is the split-verdict
// scenario for a member configured with ONLY a fail-back-only check: when it
// becomes active mid-probe, the active-safe pass must run the PING floor (a
// real liveness verdict) and must not record the lag failure — the member
// stays closed because PONG came back, not because nothing was probed.
func TestBackgroundProbeLagOnlyMemberGoneActiveUsesPingFloor(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{HealthCheckTimeout: time.Second})
	mkCB := func() *imultidb.CircuitBreaker {
		return imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	}
	a := &multidbDatabase{id: 0, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{okLivenessCheck{}}}
	pings := 0
	b := &multidbDatabase{
		id:     1,
		weight: 1,
		c:      NewClient(&Options{Addr: "127.0.0.1:6379", MaxRetries: -1}),
		cb:     mkCB(),
		policy: defaultMultiDBPolicy{},
	}
	t.Cleanup(func() { _ = b.c.Close() })
	b.c.AddHook(pongHook{pings: &pings})
	core.dbs[0], core.dbs[1] = a, b
	core.active.Store(0)
	b.checks = []MultiDBHealthCheck{&failbackLagCheck{onCheck: func() { core.active.Store(1) }}} // lag-only

	core.runHealthChecksOnce(context.Background())

	if b.cb.State() != imultidb.CircuitClosed {
		t.Fatalf("lag-only member that went active mid-probe was evicted: breaker=%v (PING answered PONG, so only the lag failure could have opened it)", b.cb.State())
	}
	if pings == 0 {
		t.Fatal("the active-safe pass ran no liveness probe: the PING floor did not execute")
	}
}

// policyByName resolves a fixed request policy for one command name and
// nothing for the rest, standing in for a server's reported command tips.
func policyByName(name string, req routing.RequestPolicy) *commandInfoResolver {
	return NewCommandInfoResolver(func(_ context.Context, cmd Cmder) *routing.CommandPolicy {
		if cmd.Name() != name {
			return nil
		}
		return &routing.CommandPolicy{Request: req}
	})
}

// newClusterMemberClient builds a MultiDBClient holding one cluster member
// whose command policies come from resolver. The member never dials: these
// tests only ask what may be pipelined, which is answered from policy alone.
func newClusterMemberClient(t *testing.T, resolver *commandInfoResolver) *MultiDBClient {
	t.Helper()
	core := newMultidbCore(&MultiDBOptions{FailureDetector: &countingFD{}})
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = cc.Close() })
	cc.SetCommandInfoResolver(resolver)
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, cc: cc, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
	})}
	core.active.Store(0)
	return &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}
}

// TestCanPipelineAsksClusterMembers pins the per-command query that replaced
// the blanket cluster refusal. A command whose request policy needs several
// nodes cannot ride a pipeline on a cluster member, so it must be kept out of
// merged batches; ordinary commands still may.
func TestCanPipelineAsksClusterMembers(t *testing.T) {
	ctx := context.Background()
	fanOut := []routing.RequestPolicy{routing.ReqAllNodes, routing.ReqAllShards, routing.ReqMultiShard}
	for _, req := range fanOut {
		mdb := newClusterMemberClient(t, policyByName("dbsize", req))
		if mdb.canPipeline(ctx, NewIntCmd(ctx, "dbsize")) {
			t.Errorf("canPipeline(dbsize) with request policy %v = true, want false", req)
		}
		if !mdb.canPipeline(ctx, NewStatusCmd(ctx, "set", "k", "v")) {
			t.Errorf("canPipeline(set) with a %v dbsize policy = false, want true", req)
		}
	}

	// Routing that is not derived from the slot cannot ride a batch either: the
	// batch router would send it to the wrong shard.
	mdb := newClusterMemberClient(t, policyByName("ft.cursor", routing.ReqSpecial))
	if mdb.canPipeline(ctx, NewStatusCmd(ctx, "ft.cursor", "read", "idx", "1")) {
		t.Error("canPipeline(ft.cursor) with ReqSpecial = true, want false")
	}
}

// TestCanPipelineWithoutClusterMembers pins the standalone case: with no
// cluster member configured nothing is withheld from batching, and no policy
// is consulted at all.
func TestCanPipelineWithoutClusterMembers(t *testing.T) {
	ctx := context.Background()
	core := newMultidbCore(&MultiDBOptions{FailureDetector: &countingFD{}})
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, c: NewClient(&Options{Addr: "127.0.0.1:1"}), cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
	})}
	t.Cleanup(func() { _ = core.dbs[0].c.Close() })
	core.active.Store(0)
	mdb := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}

	if !mdb.canPipeline(ctx, NewIntCmd(ctx, "dbsize")) {
		t.Error("canPipeline(dbsize) with only a standalone member = false, want true")
	}
}

// TestCanPipelineAsksEveryClusterMember pins the conservative reach: a command
// is withheld when ANY cluster member refuses it, not only the active one. The
// active member can change between the submit that batched the command and the
// flush that dispatches it, and by then the batch is already formed.
func TestCanPipelineAsksEveryClusterMember(t *testing.T) {
	ctx := context.Background()
	mdb := newClusterMemberClient(t, NewCommandInfoResolver(
		func(_ context.Context, _ Cmder) *routing.CommandPolicy { return nil },
	))

	// A second cluster member, NOT the active one, refuses dbsize.
	strict := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:2"}})
	t.Cleanup(func() { _ = strict.Close() })
	strict.SetCommandInfoResolver(policyByName("dbsize", routing.ReqAllShards))
	mdb.core.dbs[1] = &multidbDatabase{id: 1, weight: 1, cc: strict, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 1,
	})}

	if mdb.canPipeline(ctx, NewIntCmd(ctx, "dbsize")) {
		t.Error("canPipeline(dbsize) = true, want false: a passive cluster member refuses it")
	}
}

// countingDispatchHook records how commands reached the member: one at a time,
// or inside a pipeline batch. That is what distinguishes a diverted command
// from a batched one.
type countingDispatchHook struct {
	single  atomic.Int32
	batched atomic.Int32
}

func (*countingDispatchHook) DialHook(next DialHook) DialHook { return next }

func (h *countingDispatchHook) ProcessHook(ProcessHook) ProcessHook {
	return func(_ context.Context, cmd Cmder) error {
		h.single.Add(1)
		return nil
	}
}

func (h *countingDispatchHook) ProcessPipelineHook(ProcessPipelineHook) ProcessPipelineHook {
	return func(_ context.Context, cmds []Cmder) error {
		h.batched.Add(int32(len(cmds)))
		return nil
	}
}

// TestMultiDBAutoPipelineDivertsUnpipelineableOnClusterMember is the end-to-end
// counterpart of the canPipeline unit tests. With a cluster member configured,
// a command that member cannot pipeline must reach the active member on its own
// connection, while ordinary commands still ride a batch. Before the
// per-command check existed, an autopipeliner could not be created at all here.
func TestMultiDBAutoPipelineDivertsUnpipelineableOnClusterMember(t *testing.T) {
	ctx := context.Background()
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		InitialDBState:       InitialDBStateOneAvailable,
		Clients: []MultiDBClientConfig{
			// Standalone, highest weight, so it is the active member and its
			// dispatches are observable through the hook below.
			{
				Options:      &Options{Addr: "127.0.0.1:1"},
				Weight:       2,
				HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}},
			},
			// Cluster member, passive. It never serves a command here; it only
			// has to be present for canPipeline to consult it.
			{
				ClusterOptions:         &ClusterOptions{Addrs: []string{"127.0.0.1:2"}},
				Weight:                 1,
				HealthChecks:           []MultiDBHealthCheck{okLivenessCheck{}},
				SkipInitialHealthCheck: true,
			},
		},
	}
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	hook := &countingDispatchHook{}
	if err := mdb.AddDatabaseHook(0, hook); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}
	// MGET, not DBSIZE: the autopipeliner delegates typed fan-out admin
	// commands (DBSize, Script*, HImport*) straight to the client, so they
	// never reach a batch on any client type. The commands that DO reach one
	// and that a cluster member refuses are the multi-key data commands, whose
	// request policy is multi_shard.
	mdb.core.dbs[1].cc.SetCommandInfoResolver(policyByName("mget", routing.ReqMultiShard))
	if got := mdb.core.activeDatabaseID(); got != 0 {
		t.Fatalf("active member = %d, want 0 (the standalone one)", got)
	}

	ap, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline with a cluster member configured: %v", err)
	}

	// The cluster member cannot pipeline MGET, so it must be diverted.
	_ = ap.MGet(ctx, "a", "b").Err()
	if got := hook.single.Load(); got != 1 {
		t.Errorf("MGet reached the member as a single command %d times, want 1 (it must be diverted)", got)
	}
	if got := hook.batched.Load(); got != 0 {
		t.Errorf("MGet was batched (%d commands in batches), want 0", got)
	}

	// An ordinary command still rides a batch.
	_ = ap.Set(ctx, "k", "v", 0).Err()
	if got := hook.batched.Load(); got != 1 {
		t.Errorf("Set arrived in a batch %d times, want 1", got)
	}
	if got := hook.single.Load(); got != 1 {
		t.Errorf("Set was diverted too: single dispatches = %d, want still 1", got)
	}
}

// TestInitializeSucceedsAtDeadlineWhenPolicyMet pins startup under the
// partial outage OneAvailable and MajorityAvailable exist to tolerate. One
// member's probe blocks until the constructor's deadline; the others answer
// at once. A serial pass that waited for every member let the blocked probe
// spend the deadline and then failed startup. Now the deadline that arrives
// with the policy already met succeeds on the verdicts gathered before it:
// the initial active is the highest-weight member that PASSED (even when the
// blocked member outranks it), and the blocked member — which did not answer
// within the whole deadline — is recorded unhealthy and forced open, exactly
// like a probe that hit HealthCheckTimeout, so failover cannot select it.
func TestInitializeSucceedsAtDeadlineWhenPolicyMet(t *testing.T) {
	const deadline = time.Second
	for _, tc := range []struct {
		name       string
		state      InitialDBState
		weights    [3]float64 // members 0, 1, 2; member 2 is the blocked one
		wantActive int
	}{
		{"one_available", InitialDBStateOneAvailable, [3]float64{3, 2, 1}, 0},
		{"majority", InitialDBStateMajorityAvailable, [3]float64{3, 2, 1}, 0},
		{"majority_blocked_outranks", InitialDBStateMajorityAvailable, [3]float64{2, 1, 3}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stuck := &blockingCheck{started: make(chan struct{})}
			opts := &MultiDBOptions{
				HealthCheckInterval:  time.Hour,
				AutoFallbackInterval: -1,
				HealthCheckTimeout:   5 * time.Second, // longer than the deadline
				InitialDBState:       tc.state,
				Clients: []MultiDBClientConfig{
					{Options: &Options{Addr: "127.0.0.1:1"}, Weight: tc.weights[0], HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
					{Options: &Options{Addr: "127.0.0.1:2"}, Weight: tc.weights[1], HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
					{Options: &Options{Addr: "127.0.0.1:3"}, Weight: tc.weights[2], HealthChecks: []MultiDBHealthCheck{stuck}},
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()

			start := time.Now()
			mdb, err := NewMultiDBClient(ctx, opts)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("NewMultiDBClient: %v (after %v) — two members were healthy at once", err, elapsed)
			}
			t.Cleanup(func() { _ = mdb.Close() })
			// The pass waits for every verdict it can get, so with one blocked
			// member it runs to the deadline — and succeeds there instead of
			// failing.
			if elapsed < deadline/2 || elapsed > deadline+time.Second {
				t.Fatalf("startup took %v, want about the %v deadline", elapsed, deadline)
			}
			if got := mdb.core.activeDatabaseID(); got != tc.wantActive {
				t.Errorf("active = %d, want %d (highest-weight member that PASSED)", got, tc.wantActive)
			}
			// The blocked member did not answer within the deadline: recorded
			// unhealthy and forced open, so failover cannot select it before
			// the background loop has probed it.
			if got := mdb.core.dbs[2].cb.CheckState(); got != imultidb.CircuitOpen {
				t.Errorf("blocked member's breaker = %v, want open (no answer within the deadline)", got)
			}
		})
	}
}

// seqCheck answers one scripted verdict per call and repeats the last one.
type seqCheck struct {
	mu       sync.Mutex
	verdicts []bool
}

func (s *seqCheck) CheckHealth(context.Context, *Client) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.verdicts[0]
	if len(s.verdicts) > 1 {
		s.verdicts = s.verdicts[1:]
	}
	return v, nil
}

func (s *seqCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return s.CheckHealth(context.Background(), nil)
}

// Startup probes must not touch the breakers. A member that fails the first
// pass and passes the retry comes out of construction with a breaker that
// never moved: closed, and no state-change callback fired. (With recording
// probes the first verdict opened it and the reconciliation reset it, two
// callbacks, and a slow recording probe could land after the final pass and
// open the breaker of the member just selected.)
func TestInitializeProbesDoNotRecordOnBreakers(t *testing.T) {
	var transitions atomic.Int32
	opts := &MultiDBOptions{
		HealthCheckInterval:   time.Hour,
		AutoFallbackInterval:  -1,
		HealthCheckTimeout:    time.Second,
		InitialDBState:        InitialDBStateOneAvailable,
		CircuitBreakerConfig:  &MultiDBCircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1},
		OnCircuitStateChanged: func(int, string, string) { transitions.Add(1) },
		Clients: []MultiDBClientConfig{{
			Options:      &Options{Addr: "127.0.0.1:1"},
			Weight:       1,
			HealthChecks: []MultiDBHealthCheck{&seqCheck{verdicts: []bool{false, true}}},
		}},
	}
	// A deadline is what enables the retry pass.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v (the retry pass should have passed)", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	if got := mdb.core.dbs[0].cb.State(); got != imultidb.CircuitClosed {
		t.Fatalf("breaker = %v after startup, want closed", got)
	}
	// Callbacks are delivered from a queue; give a wrongly recorded
	// transition time to surface.
	time.Sleep(200 * time.Millisecond)
	if n := transitions.Load(); n != 0 {
		t.Fatalf("%d circuit-state callbacks during startup, want 0: a startup probe recorded on the breaker", n)
	}
}

// reentrantRegistrar is an OTel pool registrar whose unregister callbacks
// call back into MultiDB, as a metrics integration reacting to a pool going
// away might.
type reentrantRegistrar struct {
	otel.Recorder
	onUnregister func()
}

func (*reentrantRegistrar) RegisterPool(string, pool.Pooler)             {}
func (r *reentrantRegistrar) UnregisterPool(pool.Pooler)                 { r.onUnregister() }
func (*reentrantRegistrar) RegisterPubSubPool(string, otel.PubSubPooler) {}
func (r *reentrantRegistrar) UnregisterPubSubPool(otel.PubSubPooler)     { r.onUnregister() }

// Close must not hold the membership lock while it closes member clients:
// closing a client reaches code outside the package (here the OTel pool
// registrar), and that code may call back into MultiDB. With the lock held,
// such a call deadlocked Close.
func TestMultiDBCloseReleasesMembershipLockBeforeClosingClients(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{{
			Options:      &Options{Addr: "127.0.0.1:1"},
			Weight:       1,
			HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	reentered := make(chan int, 8)
	otel.SetGlobalRecorder(&reentrantRegistrar{
		Recorder:     otel.NoopRecorder(),
		onUnregister: func() { reentered <- mdb.core.memberCount() },
	})
	t.Cleanup(func() { otel.SetGlobalRecorder(otel.NoopRecorder()) })

	done := make(chan error, 1)
	go func() { done <- mdb.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return: closing a member client re-entered MultiDB while the membership lock was held")
	}
	select {
	case <-reentered:
	default:
		t.Fatal("the registrar's unregister callback never ran: the re-entrant path was not exercised")
	}
}

// activeObservingCheck records which member was active when it ran.
type activeObservingCheck struct {
	core    *multidbCore
	healthy bool
	saw     chan int64
}

func (c *activeObservingCheck) CheckHealth(context.Context, *Client) (bool, error) {
	c.saw <- c.core.active.Load()
	return c.healthy, nil
}

func (c *activeObservingCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return c.CheckHealth(context.Background(), nil)
}

// The background pass probes the active first and decides failover right
// after it: by the time any passive is probed, an active found unhealthy has
// already been replaced. Probing the passives first made an idle client wait
// for every passive's probe (up to HealthCheckTimeout each) before it could
// switch.
func TestBackgroundPassFailsOverBeforeProbingPassives(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{HealthCheckTimeout: time.Second, FailoverStrategy: WeightBasedFailoverStrategy{}})
	mkCB := func() *imultidb.CircuitBreaker {
		return imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	}
	saw := make(chan int64, 4)
	activeCheck := &activeObservingCheck{core: core, healthy: false, saw: make(chan int64, 4)}
	core.dbs[0] = &multidbDatabase{id: 0, weight: 3, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{activeCheck}}
	core.dbs[1] = &multidbDatabase{id: 1, weight: 2, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{&activeObservingCheck{core: core, healthy: true, saw: saw}}}
	core.dbs[2] = &multidbDatabase{id: 2, weight: 1, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{&activeObservingCheck{core: core, healthy: true, saw: saw}}}
	core.active.Store(0)

	core.runHealthChecksOnce(context.Background())

	if got := core.activeDatabaseID(); got != 1 {
		t.Fatalf("active = %d after the pass, want 1 (highest-weight healthy passive)", got)
	}
	for i := 0; i < 2; i++ {
		select {
		case id := <-saw:
			if id == 0 {
				t.Fatal("a passive was probed while member 0 was still active: failover waited for the passives' probes")
			}
		default:
			t.Fatalf("only %d passive probes ran, want 2", i)
		}
	}
}

// intReplyHook answers every command with a fixed integer and never dials.
type intReplyHook struct {
	val   int64
	calls *atomic.Int32
}

func (intReplyHook) DialHook(next DialHook) DialHook { return next }
func (h intReplyHook) ProcessHook(ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		h.calls.Add(1)
		if ic, ok := cmd.(*IntCmd); ok {
			ic.SetVal(h.val)
		}
		return nil
	}
}

func (intReplyHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// Cluster control commands (DBSize, Script*) bypass the hook chain, but not
// the admission gate: an active cluster member whose breaker is open must be
// failed over before the command is sent, like any other command.
func TestMultiDBClusterControlCommandsFailOverOpenActive(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{
			{ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:1"}}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	if got := mdb.core.activeDatabaseID(); got != 0 {
		t.Fatalf("active = %d, want the cluster member (0)", got)
	}
	var calls atomic.Int32
	mdb.core.dbs[1].c.AddHook(intReplyHook{val: 42, calls: &calls})

	// The cluster member is now known-unhealthy.
	mdb.core.dbs[0].cb.ForceOpen()

	res := mdb.DBSize(ctx)
	if err := res.Err(); err != nil {
		t.Fatalf("DBSize: %v (sent to the open cluster member instead of failing over)", err)
	}
	if res.Val() != 42 || calls.Load() == 0 {
		t.Fatalf("DBSize = %d (standalone hook calls %d), want 42 from the failed-over member", res.Val(), calls.Load())
	}
	if got := mdb.core.activeDatabaseID(); got != 1 {
		t.Errorf("active = %d after the control command, want 1", got)
	}
}

// decoratedCmd is a caller's decorator around a Cmder: legal, since embedding
// promotes the whole interface, but it is not a *baseCmd.
type decoratedCmd struct{ Cmder }

// batchInspectHook hands every batch to fn and answers it locally.
type batchInspectHook struct{ fn func([]Cmder) }

func (batchInspectHook) DialHook(next DialHook) DialHook          { return next }
func (batchInspectHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (h batchInspectHook) ProcessPipelineHook(ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.fn(cmds)
		return nil
	}
}

// A MultiDB transaction marks every command NoRetry so the member's own
// retry loop cannot replay a possibly committed EXEC. A decorated command
// must be marked too: a cluster member trims the synthetic MULTI/EXEC before
// its retry check, so the decorated command is the only marker left.
func TestMultiDBTxMarksDecoratedCommandsNoRetry(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{
			{ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:1"}}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	var seen, marked atomic.Bool
	mdb.core.dbs[0].cc.AddHook(batchInspectHook{fn: func(cmds []Cmder) {
		for _, cmd := range cmds {
			if d, ok := cmd.(decoratedCmd); ok {
				seen.Store(true)
				marked.Store(d.NoRetry())
			}
		}
	}})

	inner := NewStatusCmd(ctx, "set", "k", "v")
	_, _ = mdb.TxPipelined(ctx, func(p Pipeliner) error {
		return p.Process(ctx, decoratedCmd{inner})
	})
	if !seen.Load() {
		t.Fatal("the decorated command never reached the member")
	}
	if !marked.Load() {
		t.Fatal("decorated command reached the member without NoRetry: a cluster member could replay the transaction")
	}
	if inner.NoRetry() {
		t.Error("NoRetry was not restored on the caller's command after the transaction")
	}
}

// A sharded subscription is pinned to the member it binds to, so it must
// bind only after the same admission commands get: an active whose breaker
// is open is failed over first.
func TestMultiDBSSubscribeFailsOverOpenActive(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	if got := mdb.core.activeDatabaseID(); got != 0 {
		t.Fatalf("active = %d, want 0", got)
	}

	mdb.core.dbs[0].cb.ForceOpen()

	ps := mdb.SSubscribe(ctx, "ch")
	t.Cleanup(func() { _ = ps.Close() })
	if got := mdb.core.activeDatabaseID(); got != 1 {
		t.Fatalf("active = %d after SSubscribe, want 1: the subscription was pinned to the open member", got)
	}
}

// Fallback suppression must run on the core's monotonic clock: a wall-clock
// step must not stretch or shrink it. Driven through the injectable clock.
func TestFallbackSuppressionUsesMonotonicClock(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1)
	prev := multidbNowNano
	multidbNowNano = clock.Load
	t.Cleanup(func() { multidbNowNano = prev })

	core := newMultidbCore(&MultiDBOptions{HealthCheckTimeout: time.Second, FailoverStrategy: WeightBasedFailoverStrategy{}})
	mkCB := func() *imultidb.CircuitBreaker {
		return imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	}
	core.dbs[0] = &multidbDatabase{id: 0, weight: 2, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{okLivenessCheck{}}}
	core.dbs[1] = &multidbDatabase{id: 1, weight: 1, cb: mkCB(), policy: defaultMultiDBPolicy{}, checks: []MultiDBHealthCheck{okLivenessCheck{}}}
	core.active.Store(1)
	// Member 0 was vacated by an automatic failover: suppressed for an hour
	// on the core's clock.
	core.dbs[0].noFallbackBefore.Store(clock.Load() + int64(time.Hour))

	core.tryFallbackToPrimary(context.Background())
	if got := core.activeDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1: fallback ignored the suppression window (compared against the wall clock)", got)
	}
	// One hour on the core's clock, no wall-clock time at all.
	clock.Add(int64(time.Hour) + 1)
	core.tryFallbackToPrimary(context.Background())
	if got := core.activeDatabaseID(); got != 0 {
		t.Fatalf("active = %d, want 0: the suppression window did not end on the core's clock", got)
	}
}

// RemoveDatabase must close the removed member's client with failoverMu
// released: closing a client reaches code outside the package (here the OTel
// pool registrar), and that code may call a control operation that takes
// failoverMu. With the lock held, such a call deadlocked RemoveDatabase.
func TestMultiDBRemoveDatabaseClosesClientOutsideFailoverLock(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	reentered := make(chan error, 8)
	otel.SetGlobalRecorder(&reentrantRegistrar{
		Recorder: otel.NoopRecorder(),
		// A control operation that takes failoverMu, from inside the close.
		onUnregister: func() { reentered <- mdb.RemoveDatabase(context.Background(), 12345) },
	})
	t.Cleanup(func() { otel.SetGlobalRecorder(otel.NoopRecorder()) })

	done := make(chan error, 1)
	go func() { done <- mdb.RemoveDatabase(ctx, 1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RemoveDatabase: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RemoveDatabase did not return: closing the member re-entered a control operation while failoverMu was held")
	}
	select {
	case err := <-reentered:
		if !errors.Is(err, ErrDatabaseNotFound) {
			t.Fatalf("re-entrant RemoveDatabase(12345) = %v, want ErrDatabaseNotFound", err)
		}
	default:
		t.Fatal("the registrar's unregister callback never ran: the re-entrant path was not exercised")
	}
}

// A PubSub handshake (initConn) that fails with the pool's ErrClosed on a
// member removed meanwhile must be translated like the dial before it: the
// channel loops treat ErrClosed as terminal, and the subscription must re-dial
// on the retryable error instead. The trigger is synthetic: the member's
// credentials provider answers ErrClosed, which initConn wraps and returns
// before any network I/O, and the member is marked removed as a concurrent
// RemoveDatabase would.
func TestMultiDBPubSubInitConnErrTranslatedForRemovedMember(t *testing.T) {
	pipeDialer := func(context.Context, string, string) (net.Conn, error) {
		c1, c2 := net.Pipe()
		go func() { _, _ = io.Copy(io.Discard, c2) }()
		return c1, nil
	}
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{{
			Options: &Options{
				Addr:   "127.0.0.1:1",
				Dialer: pipeDialer,
				CredentialsProviderContext: func(context.Context) (string, string, error) {
					return "", "", pool.ErrClosed
				},
			},
			Weight:       1,
			HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	mdb.core.dbs[0].removed.Store(true)

	ps := mdb.Subscribe(ctx)
	t.Cleanup(func() { _ = ps.Close() })
	err = ps.Subscribe(ctx, "ch")
	if err == nil {
		t.Fatal("Subscribe succeeded; the handshake was expected to fail")
	}
	if !errors.Is(err, ErrTemporarilyNotAvailable) {
		t.Fatalf("Subscribe error = %v, want ErrTemporarilyNotAvailable: the handshake error of a removed member was not translated", err)
	}
	if errors.Is(err, pool.ErrClosed) {
		t.Fatalf("Subscribe error still carries the pool's ErrClosed, which the channel loops treat as terminal: %v", err)
	}
}

// The tracked-autopipeliner list is compacted in place when a new instance
// is built. The slots behind the new length must be cleared, or drained
// instances stay reachable through the backing array.
func TestMultiDBTrackedAutopipelinersDropDrainedReferences(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{{
			Options:      &Options{Addr: "127.0.0.1:1"},
			Weight:       1,
			HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	// Two live instances, then both drained: the next build compacts a
	// two-slot array down to one entry.
	a, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	b, err := mdb.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	_ = a.Close()
	_ = a.WaitClosed()
	_ = b.Close()
	_ = b.WaitClosed()
	c, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline after drain: %v", err)
	}

	mdb.autopipelinerMu.Lock()
	s := mdb.builtAutopipeliners
	full := s[:cap(s)]
	mdb.autopipelinerMu.Unlock()
	if len(s) != 1 || s[0] != c {
		t.Fatalf("tracked = %d entries (first == new: %v), want exactly the new instance", len(s), len(s) > 0 && s[0] == c)
	}
	for i := len(s); i < len(full); i++ {
		if full[i] != nil {
			t.Fatalf("slot %d behind the slice length still references an autopipeliner: drained instances are retained", i)
		}
	}
}
