package redis_test

import (
	"context"
	"errors"
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

// hookedDB is a process hook that short-circuits every command (never dials),
// recording the commands it saw and failing while `fail` is set.
type hookedDB struct {
	name     string
	fail     atomic.Bool
	commands atomic.Int64
}

func (h *hookedDB) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *hookedDB) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		if h.fail.Load() {
			err := errors.New("hooked: " + h.name + " down")
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
