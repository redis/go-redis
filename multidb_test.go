package redis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/multidb"
)

// The multidb package's policies must satisfy the root policy interface
// structurally, so MultiDBOptions.HealthCheckPolicy can take them directly.
var (
	_ redis.MultiDBHealthCheckPolicy = (*multidb.HealthyAllPolicy)(nil)
	_ redis.MultiDBHealthCheckPolicy = (*multidb.HealthyMajorityPolicy)(nil)
	_ redis.MultiDBHealthCheckPolicy = (*multidb.HealthyAnyPolicy)(nil)
	_ redis.MultiDBHealthCheck       = (*multidb.PingHealthCheck)(nil)
	_ redis.MultiDBHealthCheck       = (*multidb.LagAwareHealthCheck)(nil)
)

// fakeHealthCheck reports the health stored in an atomic flag, ignoring the
// client argument entirely so no server is needed.
type fakeHealthCheck struct {
	healthy atomic.Bool
	calls   atomic.Int64
}

func newFakeHealthCheck(healthy bool) *fakeHealthCheck {
	hc := &fakeHealthCheck{}
	hc.healthy.Store(healthy)
	return hc
}

func (hc *fakeHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) (bool, error) {
	hc.calls.Add(1)
	if hc.healthy.Load() {
		return true, nil
	}
	return false, errors.New("fake: unhealthy")
}

func (hc *fakeHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) (bool, error) {
	hc.calls.Add(1)
	return hc.healthy.Load(), nil
}

// customErr lets a test inject an arbitrary error from the hook.
type customErr struct{ err error }

// hookedDB is a process hook that short-circuits every command (never dials),
// recording the commands it saw and failing while `fail` is set.
type hookedDB struct {
	name     string
	fail     atomic.Bool
	custom   atomic.Pointer[customErr]
	commands atomic.Int64
}

func (h *hookedDB) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *hookedDB) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		if ce := h.custom.Load(); ce != nil {
			cmd.SetErr(ce.err)
			return ce.err
		}
		if h.fail.Load() {
			// Wrap io.EOF so the failure classifies as a transport error
			// (the kind that records on the breaker/detector and retries).
			err := fmt.Errorf("hooked: %s down: %w", h.name, io.EOF)
			cmd.SetErr(err)
			return err
		}
		return nil
	}
}

func (h *hookedDB) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// testDB bundles one fake database: config + hook + health check.
type testDB struct {
	hook  *hookedDB
	check *fakeHealthCheck
	cfg   redis.MultiDBClientConfig
}

func newTestDB(name, addr string, weight float64, healthy bool) *testDB {
	db := &testDB{
		hook:  &hookedDB{name: name},
		check: newFakeHealthCheck(healthy),
	}
	db.cfg = redis.MultiDBClientConfig{
		Options:      &redis.Options{Addr: addr},
		Weight:       weight,
		HealthChecks: []redis.MultiDBHealthCheck{db.check},
	}
	return db
}

// hookInstaller lets NewMultiDBClient install the per-DB hook after the
// underlying clients are built.
func installHooks(t *testing.T, mdb *redis.MultiDBClient, dbs ...*testDB) {
	t.Helper()
	for i, db := range dbs {
		if err := mdb.AddDatabaseHook(i, db.hook); err != nil {
			t.Fatalf("AddDatabaseHook(%d): %v", i, err)
		}
	}
}

func newTestMultiDB(t *testing.T, opts *redis.MultiDBOptions, dbs ...*testDB) *redis.MultiDBClient {
	t.Helper()
	for _, db := range dbs {
		opts.Clients = append(opts.Clients, db.cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	installHooks(t, mdb, dbs...)
	return mdb
}

func baseOptions() *redis.MultiDBOptions {
	return &redis.MultiDBOptions{
		// Long interval: background loop stays quiet unless a test shortens it.
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
	}
}

func TestMultiDBInitialDBStatePolicies(t *testing.T) {
	mk := func(h1, h2, h3 bool, state redis.InitialDBState, ctx context.Context) error {
		opts := baseOptions()
		opts.InitialDBState = state
		for i, h := range []bool{h1, h2, h3} {
			db := newTestDB(string(rune('a'+i)), "127.0.0.1:1", 1.0, h)
			opts.Clients = append(opts.Clients, db.cfg)
		}
		mdb, err := redis.NewMultiDBClient(ctx, opts)
		if err == nil {
			_ = mdb.Close()
		}
		return err
	}

	ctx := context.Background()

	if err := mk(true, true, false, redis.InitialDBStateMajorityAvailable, ctx); err != nil {
		t.Errorf("majority with 2/3 healthy should succeed, got %v", err)
	}
	if err := mk(true, false, false, redis.InitialDBStateMajorityAvailable, ctx); !errors.Is(err, redis.ErrInsufficientHealthyDatabases) {
		t.Errorf("majority with 1/3 healthy should fail, got %v", err)
	}
	if err := mk(true, false, false, redis.InitialDBStateOneAvailable, ctx); err != nil {
		t.Errorf("one_available with 1/3 healthy should succeed, got %v", err)
	}
	if err := mk(true, true, false, redis.InitialDBStateAllAvailable, ctx); !errors.Is(err, redis.ErrInsufficientHealthyDatabases) {
		t.Errorf("all_available with 2/3 healthy should fail, got %v", err)
	}

	// With a deadline the init blocks and retries until the deadline elapses.
	dctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := mk(true, false, false, redis.InitialDBStateAllAvailable, dctx)
	if !errors.Is(err, redis.ErrInsufficientHealthyDatabases) {
		t.Errorf("blocking init should fail with ErrInsufficientHealthyDatabases, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("blocking init returned too early: %v", elapsed)
	}
}

func TestMultiDBActiveIsHighestWeight(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 0.5, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 2.0, true)
	db3 := newTestDB("db3", "127.0.0.1:3", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1, db2, db3)

	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index = %d, want 1 (highest weight)", got)
	}
}

func TestMultiDBProcessRoutesToActive(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1, db2)

	ctx := context.Background()
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if db1.hook.commands.Load() == 0 {
		t.Error("active db1 saw no commands")
	}
	if db2.hook.commands.Load() != 0 {
		t.Error("passive db2 saw commands")
	}
}

func TestMultiDBFailoverOnCommandFailures(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour, // stay open for the whole test
	}
	var failoverFrom, failoverTo atomic.Int32
	failoverFrom.Store(-1)
	failoverTo.Store(-1)
	opts.OnFailover = func(ctx context.Context, from, to int) {
		failoverFrom.Store(int32(from))
		failoverTo.Store(int32(to))
	}

	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)

	// The command should fail over to db2 and succeed there.
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after active failure: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index after failover = %d, want 1", got)
	}
	if db2.hook.commands.Load() == 0 {
		t.Error("db2 did not receive the retried command")
	}
	if failoverFrom.Load() != 0 || failoverTo.Load() != 1 {
		t.Errorf("OnFailover(from=%d,to=%d), want (0,1)", failoverFrom.Load(), failoverTo.Load())
	}
}

func TestMultiDBEscalation(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 1
	opts.MaxFailoverAttempts = 2
	opts.FailoverAttemptDelay = 10 * time.Millisecond
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}

	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)
	db2.hook.fail.Store(true)

	// Open both breakers organically.
	_ = mdb.Set(ctx, "k", "v", 0).Err()
	_ = mdb.Set(ctx, "k", "v", 0).Err()

	// Now no healthy target exists: expect temporary, then permanent.
	sawTemporary := false
	sawPermanent := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := mdb.Set(ctx, "k", "v", 0).Err()
		if errors.Is(err, redis.ErrTemporarilyNotAvailable) {
			sawTemporary = true
		}
		if errors.Is(err, redis.ErrPermanentlyNotAvailable) {
			sawPermanent = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawTemporary {
		t.Error("never saw ErrTemporarilyNotAvailable")
	}
	if !sawPermanent {
		t.Error("never saw ErrPermanentlyNotAvailable")
	}
}

func TestMultiDBManualFailover(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, false) // unhealthy target
	opts := baseOptions()
	// db2 is intentionally unhealthy; majority (2 of 2) would fail init.
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	// SetActiveIndex probes the target and must refuse the unhealthy one.
	if err := mdb.SetActiveIndex(ctx, 1); !errors.Is(err, redis.ErrTargetUnhealthy) {
		t.Fatalf("SetActiveIndex to unhealthy target: err = %v, want ErrTargetUnhealthy", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("active changed to %d after refused switch", got)
	}

	// ForceActiveIndex switches unconditionally.
	if err := mdb.ForceActiveIndex(ctx, 1); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index = %d after force, want 1", got)
	}

	// Once the target is healthy, SetActiveIndex succeeds.
	db1.check.healthy.Store(true)
	if err := mdb.SetActiveIndex(ctx, 0); err != nil {
		t.Fatalf("SetActiveIndex to healthy target: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("active index = %d, want 0", got)
	}

	// Out-of-range index errors on both methods.
	if err := mdb.SetActiveIndex(ctx, 9); err == nil {
		t.Error("SetActiveIndex(9) should fail")
	}
	if err := mdb.ForceActiveIndex(ctx, -1); err == nil {
		t.Error("ForceActiveIndex(-1) should fail")
	}
}

func TestMultiDBRuntimeMembership(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1, db2)
	ctx := context.Background()

	db3 := newTestDB("db3", "127.0.0.1:3", 3.0, true)
	db3.cfg.SkipInitialHealthCheck = true
	idx, err := mdb.AddDatabase(ctx, db3.cfg)
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if idx != 2 {
		t.Fatalf("AddDatabase index = %d, want 2", idx)
	}

	if err := mdb.SetWeight(idx, 5.0); err != nil {
		t.Fatalf("SetWeight: %v", err)
	}
	if err := mdb.SetWeight(42, 1.0); err == nil {
		t.Error("SetWeight out of range should fail")
	}

	// Cannot remove the active database.
	if err := mdb.RemoveDatabase(ctx, mdb.ActiveIndex()); err == nil {
		t.Error("RemoveDatabase(active) should fail")
	}
	if err := mdb.RemoveDatabase(ctx, 1); err != nil {
		t.Fatalf("RemoveDatabase(1): %v", err)
	}
}

func TestMultiDBHealthCheckMergeAndDefault(t *testing.T) {
	global := newFakeHealthCheck(true)

	// db1 has a per-DB check; db2 has none (gets the global + nothing else).
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := &testDB{hook: &hookedDB{name: "db2"}}
	db2.cfg = redis.MultiDBClientConfig{
		Options: &redis.Options{Addr: "127.0.0.1:2"},
		Weight:  1.0,
	}

	opts := baseOptions()
	opts.HealthChecks = []redis.MultiDBHealthCheck{global}
	opts.Clients = append(opts.Clients, db1.cfg, db2.cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()

	// Global check ran for both databases; per-DB check ran for db1.
	if global.calls.Load() < 2 {
		t.Errorf("global check calls = %d, want >= 2 (both dbs)", global.calls.Load())
	}
	if db1.check.calls.Load() == 0 {
		t.Error("per-DB check on db1 never ran (merge semantics broken)")
	}
}

func TestMultiDBBackgroundFailover(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.HealthCheckInterval = 30 * time.Millisecond
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}

	mdb := newTestMultiDB(t, opts, db1, db2)

	// No command traffic at all: the background loop alone must move the
	// active index when the active database's health checks start failing.
	db1.check.healthy.Store(false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mdb.ActiveIndex() == 1 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("background loop never failed over; active = %d", mdb.ActiveIndex())
}

func TestMultiDBAutoFallback(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.HealthCheckInterval = 20 * time.Millisecond
	opts.AutoFallbackInterval = 50 * time.Millisecond
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      50 * time.Millisecond, // fast recovery for the test
	}

	var activeChanges atomic.Int32
	opts.OnActiveDatabaseChanged = func(from, to int) { activeChanges.Add(1) }

	mdb := newTestMultiDB(t, opts, db1, db2)

	// Fail db1 → background failover to db2.
	db1.check.healthy.Store(false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mdb.ActiveIndex() != 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if mdb.ActiveIndex() != 1 {
		t.Fatal("setup: background failover to db2 never happened")
	}

	// Recover db1 (higher weight) → auto-fallback should switch back.
	db1.check.healthy.Store(true)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && mdb.ActiveIndex() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if mdb.ActiveIndex() != 0 {
		t.Fatalf("auto-fallback never returned to db1; active = %d", mdb.ActiveIndex())
	}
	if activeChanges.Load() < 2 {
		t.Errorf("OnActiveDatabaseChanged fired %d times, want >= 2", activeChanges.Load())
	}
}

func TestMultiDBInitSelectsProbeHealthyDatabase(t *testing.T) {
	// The highest-weight member fails its startup probe; its breaker is still
	// closed (one failure < threshold), so selection must key off the probe
	// result, not the circuit state.
	db1 := newTestDB("db1", "127.0.0.1:1", 3.0, false) // unhealthy, highest weight
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	mdb := newTestMultiDB(t, opts, db1, db2)

	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("initial active = %d, want 1 (the probe-healthy member)", got)
	}
}

func TestMultiDBCloseWithOpenPubSub(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1.0, true)

	opts := baseOptions()
	opts.Clients = append(opts.Clients, db1.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}

	sub := mdb.Subscribe(context.Background()) // registered, never closed by the caller
	_ = sub

	// Close must not deadlock on the pubsub registry (onClose re-enters it).
	done := make(chan error, 1)
	go func() { done <- mdb.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked with an open PubSub")
	}
}

func TestMultiDBClientSideErrorsDoNotFailOver(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)

	// A canceled caller context is a client-side error: no retry, no
	// failover, no breaker damage.
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if err := mdb.Set(cctx, "k", "v", 0).Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Set with canceled ctx: err = %v, want context.Canceled", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("client-side error caused failover; active = %d", got)
	}
	// The database is still healthy and serving.
	if err := mdb.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after canceled ctx: %v", err)
	}
}

func TestMultiDBLocalRedisErrorIsNeutral(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	// A locally synthesized Redis error (no round trip) must surface to the
	// caller without failover and without being recorded as a success.
	db1.hook.custom.Store(&customErr{err: redis.ErrCrossSlot})
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrCrossSlot) {
		t.Fatalf("Get: err = %v, want ErrCrossSlot", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("local error caused failover; active = %d", got)
	}
	if got := db1.hook.commands.Load(); got != 1 {
		t.Fatalf("local error was retried; attempts = %d, want 1", got)
	}
}

func TestMultiDBNoRetryCommandNotReplayed(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)

	cmd := redis.NewRawWriteToCmd(ctx, io.Discard, "get", "k")
	if err := mdb.Process(ctx, cmd); err == nil {
		t.Fatal("NoRetry command should surface the transport failure")
	}
	// One attempt only: replaying a command that streams into caller-owned
	// buffers could corrupt output.
	if got := db1.hook.commands.Load(); got != 1 {
		t.Fatalf("NoRetry command executed %d times, want 1", got)
	}
	if db2.hook.commands.Load() != 0 {
		t.Error("NoRetry command was replayed on another member")
	}
}

func TestMultiDBManualFailoverResetsBreaker(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour, // breaker stays open without a reset
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	// Open db2's breaker: force it active while failing, let a command fail.
	db2.hook.fail.Store(true)
	if err := mdb.ForceActiveIndex(ctx, 1); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err)
	}
	_ = mdb.Set(ctx, "k", "v", 0).Err() // opens db2's breaker, fails over back to db1

	// db2 recovers before the grace period; the manual probe passes and must
	// reset the still-open breaker, or the switch would immediately fail away.
	db2.hook.fail.Store(false)
	if err := mdb.SetActiveIndex(ctx, 1); err != nil {
		t.Fatalf("SetActiveIndex after recovery: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set on manually selected member: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("switch did not stick; active = %d", got)
	}
}

func TestMultiDBProcessAfterClose(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1.0, true)

	opts := baseOptions()
	opts.Clients = append(opts.Clients, db1.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mdb.Set(context.Background(), "k", "v", 0).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("Set after Close: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBFailoverSkipsStartupUnhealthyMember(t *testing.T) {
	// db2 fails its startup probe. When the active db1 dies, failover must
	// escalate rather than switch to the member already known to be down.
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, false)

	opts := baseOptions()
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	opts.CommandRetries = 1
	opts.MaxFailoverAttempts = 2
	opts.FailoverAttemptDelay = 5 * time.Millisecond
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)

	sawUnavailable := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := mdb.Set(ctx, "k", "v", 0).Err()
		if errors.Is(err, redis.ErrTemporarilyNotAvailable) || errors.Is(err, redis.ErrPermanentlyNotAvailable) {
			sawUnavailable = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawUnavailable {
		t.Error("failover selected the startup-unhealthy member instead of escalating")
	}
	if db2.hook.commands.Load() != 0 {
		t.Errorf("startup-unhealthy db2 received %d commands", db2.hook.commands.Load())
	}
}

func TestMultiDBForceActiveIndexThroughOpenBreaker(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	// Open db2's breaker organically.
	db2.hook.fail.Store(true)
	_ = mdb.ForceActiveIndex(ctx, 1)
	_ = mdb.Set(ctx, "k", "v", 0).Err() // fails on db2, opens breaker, fails over back

	// db2 recovers; the forced override must reset the open breaker so the
	// switch sticks instead of failing away on the next command.
	db2.hook.fail.Store(false)
	if err := mdb.ForceActiveIndex(ctx, 1); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err)
	}
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set on forced member: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("forced switch did not stick; active = %d", got)
	}
}

func TestMultiDBValidation(t *testing.T) {
	ctx := context.Background()

	if _, err := redis.NewMultiDBClient(ctx, nil); err == nil {
		t.Error("nil options should fail")
	}
	if _, err := redis.NewMultiDBClient(ctx, &redis.MultiDBOptions{}); err == nil {
		t.Error("zero databases should fail")
	}

	// Exactly one of Options/FailoverOptions/ClusterOptions per database.
	bad := &redis.MultiDBOptions{Clients: []redis.MultiDBClientConfig{{
		Options:        &redis.Options{Addr: "127.0.0.1:1"},
		ClusterOptions: &redis.ClusterOptions{Addrs: []string{"127.0.0.1:2"}},
	}}}
	if _, err := redis.NewMultiDBClient(ctx, bad); err == nil {
		t.Error("config with two client types should fail")
	}

	none := &redis.MultiDBOptions{Clients: []redis.MultiDBClientConfig{{Weight: 1}}}
	if _, err := redis.NewMultiDBClient(ctx, none); err == nil {
		t.Error("config with no client type should fail")
	}
}

func TestMultiDBConcurrentProcess(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	// Concurrent commands racing a failover must not panic or deadlock: the
	// active database is already failing when the load starts, so every
	// goroutine contends on the same failover transition.
	db1.hook.fail.Store(true)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = mdb.Get(ctx, "k").Err()
			}
		}()
	}
	wg.Wait()

	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index = %d after concurrent failover, want 1", got)
	}
}

// fakeDetector is a controllable failure detector: ShouldFailover reports the
// trip flag, Reset counts and clears it.
type fakeDetector struct {
	tripped atomic.Bool
	resets  atomic.Int64
}

func (d *fakeDetector) RecordSuccess()       {}
func (d *fakeDetector) RecordFailure(error)  {}
func (d *fakeDetector) ShouldFailover() bool { return d.tripped.Load() }
func (d *fakeDetector) Reset() {
	d.resets.Add(1)
	d.tripped.Store(false)
}

// ctxHealthCheck fails when the probe context is already done and reports
// healthy otherwise — like a real network check would.
type ctxHealthCheck struct{}

func (ctxHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (ctxHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func TestMultiDBOptionsNotMutatedByConstruction(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	_ = newTestMultiDB(t, opts, dbA)

	// The caller's options value must stay untouched: a second client built
	// from it would otherwise see the normalized 0 and "default" it to 2,
	// silently re-enabling retries — also for the first client, which still
	// reads the shared struct.
	if opts.CommandRetries != redis.CommandRetriesNone {
		t.Fatalf("NewMultiDBClient mutated the caller's options: CommandRetries = %d", opts.CommandRetries)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb2, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("second NewMultiDBClient: %v", err)
	}
	defer mdb2.Close()
	installHooks(t, mdb2, dbA)

	dbA.hook.fail.Store(true)
	before := dbA.hook.commands.Load()
	_ = mdb2.Get(context.Background(), "k").Err()
	if got := dbA.hook.commands.Load() - before; got != 1 {
		t.Errorf("client with CommandRetriesNone made %d attempts, want 1", got)
	}
}

func TestMultiDBAddDatabaseAfterClose(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dbB := newTestDB("b", "db-b:6379", 1, true)
	if _, err := mdb.AddDatabase(context.Background(), dbB.cfg); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("AddDatabase after Close: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBRecoveredActiveClearsTrippedDetector(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	mdb := newTestMultiDB(t, opts, dbA)

	// Single member, detector tripped, breaker closed (health checks fine):
	// the command path must stay on the recovered active and clear the
	// detector instead of escalating unavailability forever.
	det.tripped.Store(true)
	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("command on recovered single-member client: %v", err)
	}
	if det.resets.Load() == 0 {
		t.Error("expected the detector to be reset after staying on the recovered active")
	}
}

func TestMultiDBAddDatabaseCallbackSeesNewIndex(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)

	var mu sync.Mutex
	var indexes []int
	opts := baseOptions()
	opts.OnCircuitStateChanged = func(dbIndex int, from, to string) {
		mu.Lock()
		indexes = append(indexes, dbIndex)
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// Adding an unhealthy member opens its breaker during the initial probe;
	// the state-change callback must report the member's real index, not the
	// zero default.
	dbB := newTestDB("b", "db-b:6379", 1, false)
	idx, err := mdb.AddDatabase(context.Background(), dbB.cfg)
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if idx != 1 {
		t.Fatalf("AddDatabase index = %d, want 1", idx)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		got := append([]int(nil), indexes...)
		mu.Unlock()
		if len(got) > 0 {
			for _, i := range got {
				if i != 1 {
					t.Fatalf("circuit state callback reported index %d, want 1 (all: %v)", i, got)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no circuit state change callback for the added unhealthy member")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMultiDBCircuitCallbackMayCallControlAPIs(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)
	dbB := newTestDB("b", "db-b:6379", 1, true)

	var mdbRef atomic.Pointer[redis.MultiDBClient]
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	opts.OnCircuitStateChanged = func(dbIndex int, from, to string) {
		if mdb := mdbRef.Load(); mdb != nil {
			// Any control API that serializes on the failover lock.
			_ = mdb.RemoveDatabase(context.Background(), 99)
		}
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	mdbRef.Store(mdb)

	// Open A's breaker: one failing command fails over to B.
	dbA.hook.fail.Store(true)
	_ = mdb.Get(context.Background(), "k").Err()
	dbA.hook.fail.Store(false)

	// Manual switch back to A: the probe passes and the open breaker is
	// reset, firing the state callback. A callback that re-enters a control
	// API must not deadlock the switch.
	done := make(chan error, 1)
	go func() { done <- mdb.SetActiveIndex(context.Background(), 0) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetActiveIndex: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SetActiveIndex deadlocked with a re-entrant circuit state callback")
	}
}

func TestMultiDBSubscribeAfterCloseReturnsErrClosed(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// pool.ErrClosed is the only error the PubSub channel loop treats as
	// terminal; anything else makes a post-close subscription retry forever.
	sub := mdb.Subscribe(context.Background(), "ch")
	defer sub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := sub.Receive(ctx); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("Receive after Close: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBDetectorFailoverDoesNotLeakProbeSlot(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one bounded half-open probe slot
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// A: open its breaker and wait out the grace period, so the active is
	// half-open (recovering) with its single probe slot free.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)

	// A detector-tripped command fails over to B. It must not reserve A's
	// half-open probe slot on the way out: nothing would ever record or
	// release it, and repeated calls would exhaust MaxHalfOpenRequests and
	// block A's recovery probes.
	det.tripped.Store(true)
	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("detector failover leaked A's half-open probe slot")
	}
}

// armableCancelCheck reports healthy until armed; once armed it cancels the
// caller's context mid-probe and reports the cancellation — simulating a
// command deadline expiring while a pre-failover target probe runs.
type armableCancelCheck struct {
	armed  atomic.Bool
	cancel context.CancelFunc
}

func (c *armableCancelCheck) CheckHealth(ctx context.Context, _ *redis.Client) (bool, error) {
	if c.armed.Load() {
		c.cancel()
		return false, context.Canceled
	}
	return true, nil
}

func (c *armableCancelCheck) CheckClusterHealth(ctx context.Context, _ *redis.ClusterClient) (bool, error) {
	if c.armed.Load() {
		c.cancel()
		return false, context.Canceled
	}
	return true, nil
}

func TestMultiDBCanceledPreFailoverProbeNotChargedAsAttempt(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	check := &armableCancelCheck{cancel: cancel}

	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := &testDB{
		hook: &hookedDB{name: "b"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "127.0.0.1:2"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{check},
		},
	}
	opts := baseOptions()
	opts.ProbeTargetBeforeFailover = true
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// A's failure opens its breaker; the retry's failover gate probes
	// candidate B and the caller's context dies mid-probe. The command must
	// surface context.Canceled — not consume a failover attempt and report
	// an availability verdict that was never established.
	check.armed.Store(true)
	dbA.hook.fail.Store(true)
	if err := mdb.Get(parent, "k").Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get = %v, want context.Canceled", err)
	}
}

func TestMultiDBSuccessResetsFailoverEscalation(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CommandRetries = 1
	opts.MaxFailoverAttempts = 2
	opts.FailoverAttemptDelay = time.Millisecond
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA)
	ctx := context.Background()

	// Outage one: open A's breaker and consume one failover attempt.
	dbA.hook.fail.Store(true)
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("first outage: err = %v, want ErrTemporarilyNotAvailable", err)
	}

	// Recovery: the grace period elapses and a successful command closes
	// the breaker again.
	dbA.hook.fail.Store(false)
	time.Sleep(50 * time.Millisecond)
	if err := mdb.Get(ctx, "k").Err(); err != nil {
		t.Fatalf("recovery command: %v", err)
	}

	// A fresh outage well past FailoverAttemptDelay must start escalating
	// from a clean slate — the successful traffic in between broke the
	// "consecutive failed attempts" chain.
	time.Sleep(5 * time.Millisecond)
	dbA.hook.fail.Store(true)
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("second outage: err = %v, want ErrTemporarilyNotAvailable (stale escalation state)", err)
	}
}

func TestMultiDBSubscribeOnClusterOnlyClientTerminates(t *testing.T) {
	opts := baseOptions()
	opts.Clients = []redis.MultiDBClientConfig{{
		ClusterOptions: &redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}},
		Weight:         1,
		HealthChecks:   []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()

	// No standalone member exists, so the subscription can never be served:
	// the dial must fail with the terminal ErrClosed (the only error the
	// PubSub channel loop treats as final) instead of retrying forever.
	sub := mdb.Subscribe(context.Background(), "ch")
	defer sub.Close()
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if _, err := sub.Receive(rctx); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("Receive on cluster-only client: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBFallbackResetsDetector(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.HealthCheckInterval = 50 * time.Millisecond
	opts.AutoFallbackInterval = 120 * time.Millisecond
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      100 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	ctx := context.Background()

	// Fail A over to B.
	dbA.hook.fail.Store(true)
	_ = mdb.Get(ctx, "k").Err()
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active = %d, want 1 after failover", got)
	}

	// A recovers; the background checks close its breaker and auto-fallback
	// switches back. A detector window still tripped from the outage must
	// be cleared by the fallback — otherwise the very next command fails
	// straight back over to the lower-weight member.
	dbA.hook.fail.Store(false)
	det.tripped.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for mdb.ActiveIndex() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("auto-fallback to member 0 never happened")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := mdb.Get(ctx, "k").Err(); err != nil {
		t.Fatalf("Get after fallback: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Errorf("tripped detector failed away from the just-recovered primary: active = %d", got)
	}
}

func TestMultiDBNoCallbacksForRemovedMember(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)

	var mu sync.Mutex
	var events []int
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	opts.OnCircuitStateChanged = func(dbIndex int, from, to string) {
		mu.Lock()
		events = append(events, dbIndex)
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// Reproduce the background-loop race deterministically: snapshot B like
	// runHealthChecksOnce does, remove it, then run the stale probe. The
	// removed member's breaker transition must not fire a user callback —
	// its recorded index is stale after the slice shift.
	dbB.check.healthy.Store(false)
	mdb.TestProbeRacingRemoval(1)

	time.Sleep(300 * time.Millisecond) // callbacks are delivered asynchronously
	mu.Lock()
	got := append([]int(nil), events...)
	mu.Unlock()
	if len(got) != 0 {
		t.Errorf("stale probe of a removed member fired callbacks: indexes %v", got)
	}
}

func TestMultiDBFailoverNotBlockedByPubSubReconnect(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	// B's dialer blocks: a PubSub reconnect onto B must not run on the
	// command path that triggered the failover.
	dbB.cfg.Options.DialerRetries = 1
	dbB.cfg.Options.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, io.EOF
	}
	opts := baseOptions()
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	sub := mdb.Subscribe(context.Background(), "ch")
	defer sub.Close()

	dbA.hook.fail.Store(true)
	start := time.Now()
	_ = mdb.Get(context.Background(), "k").Err()
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Errorf("command blocked %v on PubSub reconnect work during failover", elapsed)
	}
}

func TestMultiDBRejectsUnsupportedSentinelRouting(t *testing.T) {
	mk := func(mutate func(*redis.FailoverOptions)) error {
		fo := &redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{"127.0.0.1:26379"},
		}
		mutate(fo)
		opts := baseOptions()
		opts.Clients = []redis.MultiDBClientConfig{{
			FailoverOptions: fo,
			Weight:          1,
			HealthChecks:    []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mdb, err := redis.NewMultiDBClient(ctx, opts)
		if err == nil {
			_ = mdb.Close()
		}
		return err
	}

	// NewFailoverClient panics for these options; a bad member config must
	// surface as a constructor error, never a crash.
	if err := mk(func(fo *redis.FailoverOptions) { fo.RouteByLatency = true }); err == nil {
		t.Error("RouteByLatency member accepted, want a validation error")
	}
	if err := mk(func(fo *redis.FailoverOptions) { fo.RouteRandomly = true }); err == nil {
		t.Error("RouteRandomly member accepted, want a validation error")
	}
}

func TestMultiDBCanceledStartupProbeDoesNotOpenBreaker(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbA := newTestDB("a", "127.0.0.1:1", 3, true)
	dbB := newTestDB("b", "127.0.0.1:2", 2, true)
	cancelCheck := &armableCancelCheck{cancel: cancel}
	cancelCheck.armed.Store(true)
	dbC := &testDB{
		hook: &hookedDB{name: "c"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "127.0.0.1:3"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{cancelCheck},
		},
	}
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	opts.Clients = append(opts.Clients, dbA.cfg, dbB.cfg, dbC.cfg)

	// C's startup probe dies with the caller's context after quorum (A, B)
	// is already met: a canceled construction must not return a live client
	// (a probe reporting healthy after the deadline is no basis to start
	// background goroutines the caller already gave up on).
	mdb, err := redis.NewMultiDBClient(parent, opts)
	if err == nil {
		_ = mdb.Close()
		t.Fatal("NewMultiDBClient succeeded although its context was canceled mid-probe")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewMultiDBClient = %v, want context.Canceled", err)
	}
}

func TestMultiDBRejectsHImportCommands(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	ctx := context.Background()

	// HIMPORT fieldset registrations live in each member client's own
	// registry: prepared on the active member, they are silently absent on
	// the member a failover switches to. Until registrations fan out across
	// members, the command family must be rejected loudly, not half-work.
	if err := mdb.HImportPrepare(ctx, "fs", "f1").Err(); err == nil {
		t.Error("HImportPrepare must be rejected on MultiDBClient")
	}
	if err := mdb.HImportSet(ctx, "k", "fs", "v1").Err(); err == nil {
		t.Error("HImportSet must be rejected on MultiDBClient")
	}
	if err := mdb.HImportDiscard(ctx, "fs").Err(); err == nil {
		t.Error("HImportDiscard must be rejected on MultiDBClient")
	}
	if err := mdb.HImportDiscardAll(ctx).Err(); err == nil {
		t.Error("HImportDiscardAll must be rejected on MultiDBClient")
	}
}

func TestMultiDBClusterCommandsDelegateToActiveClusterClient(t *testing.T) {
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	opts.Clients = []redis.MultiDBClientConfig{{
		ClusterOptions: &redis.ClusterOptions{
			Addrs:       []string{"127.0.0.1:1"},
			DialTimeout: 100 * time.Millisecond,
			MaxRetries:  -1,
		},
		Weight:       1,
		HealthChecks: []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()

	// DBSize/ScriptLoad/... have cluster-specific fan-out overrides. Routed
	// through the generic single-command path they would run on a single
	// arbitrary shard — observable here because the generic path records the
	// (expected, serverless) transport failure on the MultiDB breaker, while
	// the delegated fan-out talks to the member client directly.
	_ = mdb.DBSize(context.Background()).Err()
	_ = mdb.ScriptLoad(context.Background(), "return 1").Err()
	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("cluster fan-out commands were routed through the generic command path (breaker recorded their failures)")
	}
}

func TestMultiDBHealthCheckSuccessKeepsCommandSlots(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 3, // MaxHalfOpenRequests defaults to this: 3 slots
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// Half-open member with every command probe slot in use.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if !mdb.TestBreakerReserveHalfOpen(0) {
			t.Fatalf("setup: could not reserve half-open slot %d", i)
		}
	}
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Fatal("setup: expected all half-open slots to be taken")
	}

	// A successful background health check never reserved a slot, so it
	// must not free one either — that would let a fourth command through to
	// the recovering member.
	mdb.TestRunHealthChecksOnce()
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a background health-check success released a command probe's slot")
	}
}

func TestMultiDBPanickyCircuitCallbackDoesNotStallQueue(t *testing.T) {
	var delivered atomic.Int64
	var panicked atomic.Bool
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	opts.OnCircuitStateChanged = func(dbIndex int, from, to string) {
		if panicked.CompareAndSwap(false, true) {
			panic("panicky circuit callback")
		}
		delivered.Add(1)
	}
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, opts, dbA)

	// Callbacks run on a library-owned goroutine: the first delivery panics,
	// which must neither crash the process nor wedge the queue — the next
	// transition's callback must still be delivered.
	mdb.TestBreakerRecordFailure(0) // closed -> open: first callback panics
	if err := mdb.ForceActiveIndex(context.Background(), 0); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err) // resets the breaker: open -> closed
	}

	deadline := time.Now().Add(2 * time.Second)
	for delivered.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("callback after the panicking one was never delivered: queue stalled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// cancelingHealthyCheck, once armed, cancels the caller's context mid-probe
// but still reports healthy — a probe whose PING succeeded right as the
// caller's deadline expired.
type cancelingHealthyCheck struct {
	armed  atomic.Bool
	cancel context.CancelFunc
}

func (c *cancelingHealthyCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	if c.armed.Load() {
		c.cancel()
	}
	return true, nil
}

func (c *cancelingHealthyCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	if c.armed.Load() {
		c.cancel()
	}
	return true, nil
}

func TestMultiDBCanceledHealthyProbeDoesNotSwitch(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	check := &cancelingHealthyCheck{cancel: cancel}

	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := &testDB{
		hook: &hookedDB{name: "b"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "127.0.0.1:2"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{check},
		},
	}
	opts := baseOptions()
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	check.armed.Store(true)

	// The probe reports healthy, but the caller's context died while it ran:
	// a canceled control operation must not mutate the active state.
	if err := mdb.SetActiveIndex(parent, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetActiveIndex = %v, want context.Canceled", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Errorf("canceled SetActiveIndex switched the active database to %d", got)
	}
}

func TestMultiDBCanceledHealthyPreFailoverProbeDoesNotSwitch(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	check := &cancelingHealthyCheck{cancel: cancel}

	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := &testDB{
		hook: &hookedDB{name: "b"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "127.0.0.1:2"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{check},
		},
	}
	opts := baseOptions()
	opts.ProbeTargetBeforeFailover = true
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	check.armed.Store(true)

	// A's failure opens its breaker; the failover gate probes candidate B,
	// the probe passes — but the caller's context died while it ran. The
	// canceled attempt must surface context.Canceled WITHOUT switching the
	// active database.
	dbA.hook.fail.Store(true)
	if err := mdb.Get(parent, "k").Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get = %v, want context.Canceled", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Errorf("canceled attempt switched the active database to %d", got)
	}
}

func TestMultiDBPoolTimeoutIsNeutral(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// Local pool saturation never reached the database: it must surface to
	// the caller but record nothing on the breaker — capacity pressure on a
	// busy client must not open the circuit of a healthy member.
	dbA.hook.custom.Store(&customErr{err: redis.ErrPoolTimeout})
	if err := mdb.Get(context.Background(), "k").Err(); !errors.Is(err, redis.ErrPoolTimeout) {
		t.Fatalf("Get = %v, want ErrPoolTimeout", err)
	}
	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a pool timeout opened the member's circuit breaker")
	}
}

func TestMultiDBAllHalfOpenFullReturnsUnavailable(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one probe slot per member
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// Every member half-open with its probe budget exhausted: the gate
	// rejects each candidate, and failover keeps finding "selectable"
	// half-open members. This must surface ErrTemporarilyNotAvailable, not
	// ping-pong the active index in a busy loop until the context dies.
	mdb.TestBreakerRecordFailure(0)
	mdb.TestBreakerRecordFailure(1)
	time.Sleep(50 * time.Millisecond)
	if !mdb.TestBreakerReserveHalfOpen(0) || !mdb.TestBreakerReserveHalfOpen(1) {
		t.Fatal("setup: expected to reserve both members' probe slots")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := mdb.Get(ctx, "k").Err()
	if !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("Get = %v after %v, want ErrTemporarilyNotAvailable", err, time.Since(start))
	}
}

func TestMultiDBStaleBreakerRecordAfterRemovalFiresNoCallback(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)

	var mu sync.Mutex
	var events []int
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	opts.OnCircuitStateChanged = func(dbIndex int, from, to string) {
		mu.Lock()
		events = append(events, dbIndex)
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// A breaker outcome that lands AFTER the member was removed (the probe
	// passed its removed-check just before the removal) must not surface a
	// user callback: the recorded index is stale after the slice shift.
	mdb.TestStaleRecordAfterRemoval(1)

	time.Sleep(300 * time.Millisecond) // callbacks are delivered asynchronously
	mu.Lock()
	got := append([]int(nil), events...)
	mu.Unlock()
	if len(got) != 0 {
		t.Errorf("stale breaker record on a removed member fired callbacks: indexes %v", got)
	}
}

// armableBlockingCheck reports healthy immediately until armed; once armed it
// blocks on its gate, letting a test hold the failover lock (via a probing
// control operation) at will.
type armableBlockingCheck struct {
	armed atomic.Bool
	gate  chan struct{}
}

func (c *armableBlockingCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	if c.armed.Load() {
		<-c.gate
	}
	return true, nil
}

func (c *armableBlockingCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	if c.armed.Load() {
		<-c.gate
	}
	return true, nil
}

func TestMultiDBMembershipOpsCanceledWhileWaitingForLock(t *testing.T) {
	check := &armableBlockingCheck{gate: make(chan struct{})}
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := &testDB{
		hook: &hookedDB{name: "b"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "127.0.0.1:2"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{check},
		},
	}
	opts := baseOptions()
	opts.HealthCheckTimeout = time.Hour // the blocking probe holds failoverMu until released
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	check.armed.Store(true)

	// Hold failoverMu: SetActiveIndex probes B, and B's blocking check stalls
	// with the lock held.
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = mdb.SetActiveIndex(context.Background(), 1)
	}()
	time.Sleep(100 * time.Millisecond) // let the holder acquire the lock

	// Queue control operations whose context dies while they wait: neither
	// may mutate membership once it finally gets the lock.
	canceled, cancel := context.WithCancel(context.Background())
	addDone := make(chan error, 1)
	removeDone := make(chan error, 1)
	go func() {
		_, err := mdb.AddDatabase(canceled, redis.MultiDBClientConfig{
			Options:                &redis.Options{Addr: "127.0.0.1:3"},
			Weight:                 1,
			HealthChecks:           []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
			SkipInitialHealthCheck: true, // no probe: the post-probe ctx check never runs
		})
		addDone <- err
	}()
	go func() { removeDone <- mdb.RemoveDatabase(canceled, 1) }()
	time.Sleep(100 * time.Millisecond) // both queued behind the lock
	cancel()
	check.armed.Store(false)
	close(check.gate) // release the lock holder

	if err := <-addDone; !errors.Is(err, context.Canceled) {
		t.Errorf("AddDatabase after canceled lock wait: err = %v, want context.Canceled", err)
	}
	if err := <-removeDone; !errors.Is(err, context.Canceled) {
		t.Errorf("RemoveDatabase after canceled lock wait: err = %v, want context.Canceled", err)
	}
	<-holderDone

	// Membership unchanged: B still present, no third member.
	if err := mdb.SetWeight(1, 1.5); err != nil {
		t.Errorf("member 1 disappeared after a canceled RemoveDatabase: %v", err)
	}
	if err := mdb.SetWeight(2, 1.5); err == nil {
		t.Error("a member was added by a canceled AddDatabase")
	}
}

func TestMultiDBManualSelectionAfterClose(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Consistent with the command paths: after Close, control APIs report
	// ErrClosed rather than an out-of-range error from the drained slice.
	if err := mdb.SetActiveIndex(context.Background(), 0); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("SetActiveIndex after Close: err = %v, want ErrClosed", err)
	}
	if err := mdb.ForceActiveIndex(context.Background(), 0); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("ForceActiveIndex after Close: err = %v, want ErrClosed", err)
	}
	if err := mdb.RemoveDatabase(context.Background(), 0); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("RemoveDatabase after Close: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBConnectionResetRecordsOnBreaker(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// A connection reset mid-read (member crash) IS recorded: shouldRetry
	// matches every net.OpError via its Timeout() method, so the failure
	// classifies as an availability signal, opens the breaker (threshold 1)
	// and — with no other member — escalates to temporary unavailability.
	// This pins the behavior a review bot claimed was missing.
	reset := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	dbA.hook.custom.Store(&customErr{err: reset})
	if err := mdb.Get(context.Background(), "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("Get = %v, want ErrTemporarilyNotAvailable (reset recorded, breaker opened)", err)
	}
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a mid-read connection reset left the breaker closed")
	}
}

func TestMultiDBPanickyFailoverCallbacksDoNotCrash(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	opts := baseOptions()
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	opts.OnFailover = func(context.Context, int, int) { panic("panicky OnFailover") }
	opts.OnActiveDatabaseChanged = func(int, int) { panic("panicky OnActiveDatabaseChanged") }
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// The announce closure also runs on the library-owned background loop:
	// panicking user callbacks must be recovered, and the failover itself
	// must complete.
	dbA.hook.fail.Store(true)
	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("Get across failover: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

func TestMultiDBProcessRejectsRawHImportCmd(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)

	// A hand-built HIMPORT command through Process must be rejected like
	// the typed methods: routing it would register a fieldset on a single
	// member and break after failover.
	cmd := redis.NewHImportPrepareCmd(context.Background(), "fs", "f1")
	if err := mdb.Process(context.Background(), cmd); err == nil {
		t.Error("Process accepted a raw HIMPORT command")
	}
	if cmd.Err() == nil {
		t.Error("raw HIMPORT command left without an error")
	}
}

func TestMultiDBClusterOverridesAfterClose(t *testing.T) {
	opts := baseOptions()
	opts.Clients = []redis.MultiDBClientConfig{{
		ClusterOptions: &redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}},
		Weight:         1,
		HealthChecks:   []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The cluster fan-out overrides bypass core.process; they need their
	// own closed guard or they run against members being torn down.
	if err := mdb.DBSize(context.Background()).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("DBSize after Close: err = %v, want ErrClosed", err)
	}
	if err := mdb.ScriptLoad(context.Background(), "return 1").Err(); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("ScriptLoad after Close: err = %v, want ErrClosed", err)
	}
	if err := mdb.ScriptFlush(context.Background()).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("ScriptFlush after Close: err = %v, want ErrClosed", err)
	}
	if err := mdb.ScriptExists(context.Background(), "abc").Err(); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("ScriptExists after Close: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBSameIndexManualSelectionResetsEscalation(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CommandRetries = 1
	opts.MaxFailoverAttempts = 2
	opts.FailoverAttemptDelay = time.Millisecond
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA)
	ctx := context.Background()

	// Outage one: consume one failover attempt.
	dbA.hook.fail.Store(true)
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("first outage: err = %v, want ErrTemporarilyNotAvailable", err)
	}

	// Operator forces the already-active member (its breaker resets): an
	// explicit healthy selection ends the failed-failover chain even when
	// no index change happens.
	dbA.hook.fail.Store(false)
	if err := mdb.ForceActiveIndex(ctx, 0); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err)
	}

	// A fresh outage must escalate from a clean slate.
	time.Sleep(5 * time.Millisecond)
	dbA.hook.fail.Store(true)
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("second outage: err = %v, want ErrTemporarilyNotAvailable (stale escalation)", err)
	}
}

// panickyCheck panics on every probe — the worst-case custom health check.
type panickyCheck struct{}

func (panickyCheck) CheckHealth(context.Context, *redis.Client) (bool, error) {
	panic("panicky health check")
}

func (panickyCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	panic("panicky health check")
}

func TestMultiDBDefaultPolicySurvivesPanickyCheck(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := &testDB{
		hook: &hookedDB{name: "b"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "127.0.0.1:2"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{panickyCheck{}},
		},
	}
	opts := baseOptions()
	opts.InitialDBState = redis.InitialDBStateOneAvailable

	// A panicking custom check under the default policy must mark the member
	// unhealthy, not crash initialization or the background loop.
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("active = %d, want 0 (the member with the panicking check is unhealthy)", got)
	}
}

func TestMultiDBCanceledProbeDoesNotDamageBreaker(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)
	dbB := &testDB{
		hook: &hookedDB{name: "b"},
		cfg: redis.MultiDBClientConfig{
			Options:      &redis.Options{Addr: "db-b:6379"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{ctxHealthCheck{}},
		},
	}
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// Operator calls with an already-canceled context: the error must be the
	// context's own, and B's breaker must stay undamaged.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 3; i++ {
		if err := mdb.SetActiveIndex(canceled, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("SetActiveIndex with canceled ctx: err = %v, want context.Canceled", err)
		}
	}

	// B must still be selectable: failing A must move traffic onto B.
	dbA.hook.fail.Store(true)
	if err := mdb.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("command after failover: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Errorf("active = %d, want 1", got)
	}
}
