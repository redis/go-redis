package redis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
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

// TestMultiDBInitDeadlineReturnsInsufficientSentinel pins that when the
// constructor's deadline expires with the InitialDBState policy unsatisfied,
// the error is ErrInsufficientHealthyDatabases (as documented) and still wraps
// the deadline — regardless of which internal probe/retry step the deadline
// lands on. Previously the mid-pass and after-pass exits returned the bare
// context error, so which sentinel a caller saw was timing-dependent.
func TestMultiDBInitDeadlineReturnsInsufficientSentinel(t *testing.T) {
	unhealthy := newTestDB("a", "127.0.0.1:1", 1, false)
	opts := baseOptions()
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	opts.Clients = append(opts.Clients, unhealthy.cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := redis.NewMultiDBClient(ctx, opts)
	if !errors.Is(err, redis.ErrInsufficientHealthyDatabases) {
		t.Errorf("err = %v, want ErrInsufficientHealthyDatabases", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to also wrap context.DeadlineExceeded", err)
	}
}

func TestMultiDBActiveIsHighestWeight(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 0.5, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 2.0, true)
	db3 := newTestDB("db3", "127.0.0.1:3", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1, db2, db3)

	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active id = %d, want 1 (highest weight)", got)
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
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active id after failover = %d, want 1", got)
	}
	if db2.hook.commands.Load() == 0 {
		t.Error("db2 did not receive the retried command")
	}
	// OnFailover is delivered asynchronously on the announce queue; poll for it.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (failoverFrom.Load() != 0 || failoverTo.Load() != 1) {
		time.Sleep(5 * time.Millisecond)
	}
	if failoverFrom.Load() != 0 || failoverTo.Load() != 1 {
		t.Errorf("OnFailover(from=%d,to=%d), want (0,1)", failoverFrom.Load(), failoverTo.Load())
	}
}

// TestMultiDBSetActiveDatabaseUnhealthyTargetOpensBreaker covers fix C: a failed
// manual selection of a DIFFERENT member must open its breaker (like init and
// AddDatabase), so a known-down target is not left selectable for the next
// automatic failover. FailureThreshold=3, so the probe's single failure is not
// enough on its own.
func TestMultiDBSetActiveDatabaseUnhealthyTargetOpensBreaker(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true) // active
	dbB := newTestDB("b", "127.0.0.1:2", 1, true) // healthy at init, sick later
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour, // stay open once opened
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	ctx := context.Background()

	dbB.check.healthy.Store(false) // dbB now fails its probe

	if err := mdb.SetActiveDatabase(ctx, 1); !errors.Is(err, redis.ErrTargetUnhealthy) {
		t.Fatalf("SetActiveDatabase(1) = %v, want ErrTargetUnhealthy", err)
	}
	// The failed target's breaker must be open (not merely +1 failure).
	if mdb.TestBreakerReserveHalfOpen(1) {
		t.Error("dbB's breaker still admits after a failed SetActiveDatabase; want opened")
	}
	// The active is unchanged.
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("active = %d, want 0 (unchanged by the failed selection)", got)
	}
}

// TestMultiDBFallbackDeclinesLaggyTarget covers fix G: auto-fallback must not
// return to a higher-weight member that is currently failing its fail-back-only
// (lag) check, even though its breaker is closed — and the gate must NOT evict
// it (non-recording).
func TestMultiDBFallbackDeclinesLaggyTarget(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true) // lower weight
	dbB := newTestDB("b", "127.0.0.1:2", 2, true) // higher weight = initial active + fallback target
	fbB := newFailbackOnlyCheck(true)             // lag check, healthy at init
	dbB.cfg.HealthChecks = append(dbB.cfg.HealthChecks, fbB)
	mdb := newTestMultiDB(t, baseOptions(), dbA, dbB)
	ctx := context.Background()

	// Move the active to the lower-weight dbA so dbB is a fallback target.
	// ForceActiveDatabase is manual, so it does not arm dbB's fallback-suppression.
	if err := mdb.ForceActiveDatabase(ctx, 0); err != nil {
		t.Fatalf("ForceActiveDatabase(0): %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("active = %d, want 0 after force", got)
	}

	// dbB is now laggy (fail-back-only breach); its breaker is still closed.
	fbB.healthy.Store(false)
	mdb.TestTryFallback()

	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("fallback returned to the laggy higher-weight dbB (active=%d), want 0", got)
	}
	// The non-recording gate must not have evicted dbB.
	if !mdb.TestBreakerReserveHalfOpen(1) {
		t.Error("fallback lag gate opened dbB's breaker; a fail-back-only check must not evict")
	}
}

// TestMultiDBFallbackSkipsLaggyToNextCandidate: when the highest-weight
// fallback candidate fails its fail-back-only probe, fallback must drop it and
// try the next-highest healthy candidate instead of giving up (a laggy top
// member must not shadow a healthy lower-weight one forever).
func TestMultiDBFallbackSkipsLaggyToNextCandidate(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true) // lowest; forced active
	dbB := newTestDB("b", "127.0.0.1:2", 3, true) // highest; will be laggy
	dbC := newTestDB("c", "127.0.0.1:3", 2, true) // middle; healthy
	fbB := newFailbackOnlyCheck(true)
	fbC := newFailbackOnlyCheck(true)
	dbB.cfg.HealthChecks = append(dbB.cfg.HealthChecks, fbB)
	dbC.cfg.HealthChecks = append(dbC.cfg.HealthChecks, fbC)
	mdb := newTestMultiDB(t, baseOptions(), dbA, dbB, dbC)
	ctx := context.Background()

	// Init active = dbB (highest weight, id 1); force down to dbA.
	if err := mdb.ForceActiveDatabase(ctx, 0); err != nil {
		t.Fatalf("ForceActiveDatabase(0): %v", err)
	}

	fbB.healthy.Store(false) // top-weight candidate is laggy now
	mdb.TestTryFallback()

	if got := mdb.ActiveDatabaseID(); got != 2 {
		t.Errorf("fallback active = %d, want 2 (healthy middle dbC, skipping laggy dbB)", got)
	}
}

// TestMultiDBFailoverCallbackCarriesStableID: a failover callback identifies the
// target by a stable id, so a concurrent RemoveDatabase of another member does
// not make the delivered id name a different database.
func TestMultiDBFailoverCallbackCarriesStableID(t *testing.T) {
	db0 := newTestDB("db0", "127.0.0.1:1", 3.0, true) // id 0, initial active
	db1 := newTestDB("db1", "127.0.0.1:2", 1.0, true) // id 1, removed below target
	db2 := newTestDB("db2", "127.0.0.1:3", 2.0, true) // id 2, failover target

	opts := baseOptions()
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Hour,
	}
	var failoverTo atomic.Int32
	failoverTo.Store(-99)
	var gotCallback atomic.Bool
	opts.OnFailover = func(ctx context.Context, from, to int) {
		failoverTo.Store(int32(to))
		gotCallback.Store(true)
	}
	mdb := newTestMultiDB(t, opts, db0, db1, db2)
	ctx := context.Background()
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("initial active = %d, want 0", got)
	}
	db0.hook.fail.Store(true)
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after active failure: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !gotCallback.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if !gotCallback.Load() {
		t.Fatal("OnFailover not delivered")
	}
	to := int(failoverTo.Load())
	if err := mdb.RemoveDatabase(ctx, 1); err != nil { // remove a non-active member
		t.Fatalf("RemoveDatabase(1): %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != to {
		t.Errorf("OnFailover delivered to=%d but ActiveDatabaseID()=%d after removal; handle went stale", to, got)
	}
}

// TestMultiDBStableIDSurvivesRemoval: an id keeps naming the same database
// across a RemoveDatabase, a removed id stays invalid, and new members draw
// fresh ids without reusing freed ones.
func TestMultiDBStableIDSurvivesRemoval(t *testing.T) {
	db0 := newTestDB("db0", "127.0.0.1:1", 3.0, true) // id 0, active
	db1 := newTestDB("db1", "127.0.0.1:2", 1.0, true) // id 1
	db2 := newTestDB("db2", "127.0.0.1:3", 2.0, true) // id 2
	mdb := newTestMultiDB(t, baseOptions(), db0, db1, db2)
	ctx := context.Background()
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("initial active id = %d, want 0", got)
	}
	if err := mdb.RemoveDatabase(ctx, 1); err != nil {
		t.Fatalf("RemoveDatabase(1): %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("active id after removal = %d, want 0 (stable)", got)
	}
	if err := mdb.SetWeight(2, 5.0); err != nil {
		t.Errorf("SetWeight(2) after removal = %v, want nil (id 2 still valid)", err)
	}
	if err := mdb.RemoveDatabase(ctx, 1); !errors.Is(err, redis.ErrDatabaseNotFound) {
		t.Errorf("RemoveDatabase(1) again = %v, want ErrDatabaseNotFound", err)
	}
	if err := mdb.SetWeight(1, 1.0); !errors.Is(err, redis.ErrDatabaseNotFound) {
		t.Errorf("SetWeight(1) after removal = %v, want ErrDatabaseNotFound", err)
	}
	db3 := newTestDB("db3", "127.0.0.1:4", 1.0, true)
	newID, err := mdb.AddDatabase(ctx, db3.cfg)
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if newID != 3 {
		t.Errorf("new member id = %d, want 3 (monotonic, no reuse of freed id 1)", newID)
	}
}

type recordingStrategy struct {
	mu   sync.Mutex
	seen [][]int
}

func (s *recordingStrategy) Select(cands []redis.MultiDBDatabaseState) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]int, 0, len(cands))
	best, bestWeight := -1, 0.0
	for _, c := range cands {
		ids = append(ids, c.ID)
		if c.Allowed && (best == -1 || c.Weight > bestWeight) {
			best, bestWeight = c.ID, c.Weight
		}
	}
	s.seen = append(s.seen, ids)
	return best
}

func (s *recordingStrategy) lastSeen() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) == 0 {
		return nil
	}
	return s.seen[len(s.seen)-1]
}

// TestMultiDBStrategySeesStableID: the failover strategy is offered stable ids
// in MultiDBDatabaseState.ID, not renumbered positions.
func TestMultiDBStrategySeesStableID(t *testing.T) {
	db0 := newTestDB("db0", "127.0.0.1:1", 3.0, true) // id 0, active
	db1 := newTestDB("db1", "127.0.0.1:2", 1.0, true) // id 1, removed
	db2 := newTestDB("db2", "127.0.0.1:3", 2.0, true) // id 2, failover target
	strat := &recordingStrategy{}
	opts := baseOptions()
	opts.FailoverStrategy = strat
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db0, db1, db2)
	ctx := context.Background()
	if err := mdb.RemoveDatabase(ctx, 1); err != nil {
		t.Fatalf("RemoveDatabase(1): %v", err)
	}
	strat.mu.Lock()
	strat.seen = nil
	strat.mu.Unlock()
	db0.hook.fail.Store(true)
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after active failure: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 2 {
		t.Fatalf("active id after failover = %d, want 2 (db2)", got)
	}
	// After removing id 1 and excluding the active (id 0), the only candidate is
	// db2 — and it must be offered by its stable id 2, not a renumbered position.
	offered := strat.lastSeen()
	if len(offered) != 1 || offered[0] != 2 {
		t.Errorf("strategy offered candidate ids %v, want exactly [2] (db2 by stable id)", offered)
	}
}

// TestMultiDBOutOfRangeIDNotTruncated pins the int-width contract: an id outside
// the int32 range must resolve to no member, never alias one. With a map[int]
// store this is inherent; the test guards against a regression to narrow keys.
func TestMultiDBOutOfRangeIDNotTruncated(t *testing.T) {
	if ^uint(0)>>32 == 0 {
		t.Skip("int is 32-bit: high-bit truncation cannot occur")
	}
	db0 := newTestDB("db0", "127.0.0.1:1", 1, true)
	db1 := newTestDB("db1", "127.0.0.1:2", 2, true) // active
	mdb := newTestMultiDB(t, baseOptions(), db0, db1)
	ctx := context.Background()
	aliasID := int(int64(1) << 32) // int32(aliasID) == 0 would alias member 0
	if err := mdb.SetActiveDatabase(ctx, aliasID); !errors.Is(err, redis.ErrDatabaseNotFound) {
		t.Errorf("SetActiveDatabase(1<<32) = %v, want ErrDatabaseNotFound", err)
	}
	if err := mdb.RemoveDatabase(ctx, aliasID); !errors.Is(err, redis.ErrDatabaseNotFound) {
		t.Errorf("RemoveDatabase(1<<32) = %v, want ErrDatabaseNotFound", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Errorf("active id = %d, want 1 (unchanged by out-of-range ops)", got)
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

// After an operator reset (ForceActiveDatabase), a fresh outage must escalate
// from zero. If the reset clears failoverAttempts but not lastFailoverAttempt,
// the FailoverAttemptDelay gate treats the new chain's first failure as part of
// the old burst and never escalates — permanently stuck on temporary here,
// since the delay is an hour.
func TestMultiDBEscalationResetsTimestampOnOperatorSelection(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 1
	opts.MaxFailoverAttempts = 1          // first counted failed attempt -> permanent
	opts.FailoverAttemptDelay = time.Hour // wide window: a stale timestamp swallows the next chain
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	escalateToPermanent := func(phase string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if errors.Is(mdb.Set(ctx, "k", "v", 0).Err(), redis.ErrPermanentlyNotAvailable) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s: never reached ErrPermanentlyNotAvailable", phase)
	}

	// First outage on both members escalates to permanent and stamps
	// lastFailoverAttempt.
	db1.hook.fail.Store(true)
	db2.hook.fail.Store(true)
	escalateToPermanent("first outage")

	// Operator forces back to db1: resets the escalation chain.
	if err := mdb.ForceActiveDatabase(ctx, 0); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err)
	}

	// A distinct outage within FailoverAttemptDelay must escalate from zero
	// again. With the stale timestamp bug this stays temporary forever.
	escalateToPermanent("outage after operator reset")
}

func TestMultiDBManualFailover(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, false) // unhealthy target
	opts := baseOptions()
	// db2 is intentionally unhealthy; majority (2 of 2) would fail init.
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	// SetActiveDatabase probes the target and must refuse the unhealthy one.
	if err := mdb.SetActiveDatabase(ctx, 1); !errors.Is(err, redis.ErrTargetUnhealthy) {
		t.Fatalf("SetActiveDatabase to unhealthy target: err = %v, want ErrTargetUnhealthy", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("active changed to %d after refused switch", got)
	}

	// ForceActiveDatabase switches unconditionally.
	if err := mdb.ForceActiveDatabase(ctx, 1); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active id = %d after force, want 1", got)
	}

	// Once the target is healthy, SetActiveDatabase succeeds.
	db1.check.healthy.Store(true)
	if err := mdb.SetActiveDatabase(ctx, 0); err != nil {
		t.Fatalf("SetActiveDatabase to healthy target: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("active id = %d, want 0", got)
	}

	// An unknown id errors on both methods.
	if err := mdb.SetActiveDatabase(ctx, 9); err == nil {
		t.Error("SetActiveDatabase(9) should fail")
	}
	if err := mdb.ForceActiveDatabase(ctx, -1); err == nil {
		t.Error("ForceActiveDatabase(-1) should fail")
	}
}

func TestMultiDBRuntimeMembership(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1, db2)
	ctx := context.Background()

	db3 := newTestDB("db3", "127.0.0.1:3", 3.0, true)
	db3.cfg.SkipInitialHealthCheck = true
	id, err := mdb.AddDatabase(ctx, db3.cfg)
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if id != 2 {
		t.Fatalf("AddDatabase id = %d, want 2", id)
	}

	if err := mdb.SetWeight(id, 5.0); err != nil {
		t.Fatalf("SetWeight: %v", err)
	}
	if err := mdb.SetWeight(42, 1.0); err == nil {
		t.Error("SetWeight out of range should fail")
	}

	// Cannot remove the active database.
	if err := mdb.RemoveDatabase(ctx, mdb.ActiveDatabaseID()); err == nil {
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
	// active id when the active database's health checks start failing.
	db1.check.healthy.Store(false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mdb.ActiveDatabaseID() == 1 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("background loop never failed over; active = %d", mdb.ActiveDatabaseID())
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
	for time.Now().Before(deadline) && mdb.ActiveDatabaseID() != 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if mdb.ActiveDatabaseID() != 1 {
		t.Fatal("setup: background failover to db2 never happened")
	}

	// Recover db1 (higher weight) → auto-fallback should switch back.
	db1.check.healthy.Store(true)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && mdb.ActiveDatabaseID() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if mdb.ActiveDatabaseID() != 0 {
		t.Fatalf("auto-fallback never returned to db1; active = %d", mdb.ActiveDatabaseID())
	}
	// OnActiveDatabaseChanged is delivered asynchronously; give the queue a
	// beat to drain the second change before asserting the count.
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && activeChanges.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
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

	if got := mdb.ActiveDatabaseID(); got != 1 {
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

// ctxWaitHook blocks each command until the caller's context is done, then
// returns a fixed error — reproducing a dial that a short caller deadline cuts
// short (so ctx.Err() is set by the time process returns).
type ctxWaitHook struct{ err error }

func (ctxWaitHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h ctxWaitHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		<-ctx.Done()
		cmd.SetErr(h.err)
		return h.err
	}
}

func (ctxWaitHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A caller context that expires mid-command (typically cutting a dial short)
// is a client-side signal, not a database-health verdict: it must not record a
// breaker/detector failure or drive failover.
func TestMultiDBCallerCanceledDialStaysNeutral(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1, // one recorded failure would open the breaker
		SuccessThreshold: 1,
		GracePeriod:      time.Hour, // stay open once opened, so the probe is observable
	}
	opts.Clients = append(opts.Clients, db1.cfg, db2.cfg)

	ctxInit, cancelInit := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelInit()
	mdb, err := redis.NewMultiDBClient(ctxInit, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	// Install ONLY the ctx-wait hook on db1 (NOT the default short-circuit hook,
	// which answers before a later hook runs); db2 keeps its normal hook so a
	// failover, if it wrongly happened, would land somewhere observable. db1
	// returns a dial-phase timeout once the caller's context is already done —
	// exactly what net.Dialer yields for a short caller deadline.
	dialErr := &net.OpError{Op: "dial", Err: context.DeadlineExceeded}
	if err := mdb.AddDatabaseHook(0, ctxWaitHook{err: dialErr}); err != nil {
		t.Fatalf("AddDatabaseHook(0): %v", err)
	}
	if err := mdb.AddDatabaseHook(1, db2.hook); err != nil {
		t.Fatalf("AddDatabaseHook(1): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := mdb.Get(ctx, "k").Err(); err == nil {
		t.Fatal("expected the caller-canceled command to return an error")
	}

	// The dial failed only because the caller's context ended: db1's breaker
	// must still be closed (reservable), not opened by a phantom failure.
	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("caller-canceled dial opened db1 breaker; the outcome must be neutral")
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("active id = %d, want 0 (no failover on caller cancel)", got)
	}
}

// A cluster member that routes reads to replicas must be rejected: the cluster
// health checks probe masters only, so replica routing would let a member look
// healthy while the replicas serving traffic are down.
func TestMultiDBRejectsReplicaRoutedClusterMember(t *testing.T) {
	cases := map[string]*redis.ClusterOptions{
		"RouteByLatency": {Addrs: []string{"127.0.0.1:1"}, RouteByLatency: true},
		"RouteRandomly":  {Addrs: []string{"127.0.0.1:1"}, RouteRandomly: true},
		"ReadOnly":       {Addrs: []string{"127.0.0.1:1"}, ReadOnly: true},
	}
	for name, co := range cases {
		t.Run(name, func(t *testing.T) {
			opts := baseOptions()
			opts.Clients = []redis.MultiDBClientConfig{{ClusterOptions: co, Weight: 1}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			mdb, err := redis.NewMultiDBClient(ctx, opts)
			if mdb != nil {
				_ = mdb.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "not supported for cluster member") {
				t.Fatalf("want rejection error for %s, got %v", name, err)
			}
		})
	}
}

// gatedHealthCheck returns healthy immediately until armed; once armed it
// signals `started` on the first probe and blocks until `release` is closed —
// letting a test open a controlled window inside a probe.
type gatedHealthCheck struct {
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedHealthCheck() *gatedHealthCheck {
	return &gatedHealthCheck{started: make(chan struct{}), release: make(chan struct{})}
}

func (h *gatedHealthCheck) CheckHealth(ctx context.Context, _ *redis.Client) (bool, error) {
	if !h.armed.Load() {
		return true, nil // construction-time probes pass without blocking
	}
	h.once.Do(func() { close(h.started) })
	select {
	case <-h.release:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (h *gatedHealthCheck) CheckClusterHealth(context.Context, *redis.ClusterClient) (bool, error) {
	return true, nil
}

// setActiveDatabaseID must not mutate state or report success when the client is
// closed DURING its probe: close() takes no lock, so it can drain the
// membership while the probe (bounded by HealthCheckTimeout) is still running.
func TestMultiDBSetActiveDatabaseLosesToConcurrentClose(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true) // active (higher weight)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	gate := newGatedHealthCheck()
	db2.cfg.HealthChecks = []redis.MultiDBHealthCheck{gate}

	opts := baseOptions()
	opts.HealthCheckTimeout = time.Hour // block on the gate, not on a timeout
	opts.Clients = append(opts.Clients, db1.cfg, db2.cfg)

	ctxInit, cancelInit := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelInit()
	mdb, err := redis.NewMultiDBClient(ctxInit, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	gate.armed.Store(true) // construction done; the next probe blocks

	errCh := make(chan error, 1)
	go func() { errCh <- mdb.SetActiveDatabase(context.Background(), 1) }()

	<-gate.started // SetActiveDatabase is now inside db2's probe, holding failoverMu
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(gate.release) // let the probe finish; setActiveDatabaseID resumes post-probe

	if err := <-errCh; !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("SetActiveDatabase racing Close = %v, want ErrClosed", err)
	}
	// The ErrClosed return proves the switch was skipped; Close then drains the
	// membership, so the active id reports -1 rather than a stale value.
	if got := mdb.ActiveDatabaseID(); got != -1 {
		t.Errorf("active id = %d, want -1 (membership drained on a closed client)", got)
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
	if got := mdb.ActiveDatabaseID(); got != 0 {
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
	if got := mdb.ActiveDatabaseID(); got != 0 {
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
	if err := mdb.ForceActiveDatabase(ctx, 1); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err)
	}
	_ = mdb.Set(ctx, "k", "v", 0).Err() // opens db2's breaker, fails over back to db1

	// db2 recovers before the grace period; the manual probe passes and must
	// reset the still-open breaker, or the switch would immediately fail away.
	db2.hook.fail.Store(false)
	if err := mdb.SetActiveDatabase(ctx, 1); err != nil {
		t.Fatalf("SetActiveDatabase after recovery: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set on manually selected member: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
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

func TestMultiDBForceActiveDatabaseThroughOpenBreaker(t *testing.T) {
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
	_ = mdb.ForceActiveDatabase(ctx, 1)
	_ = mdb.Set(ctx, "k", "v", 0).Err() // fails on db2, opens breaker, fails over back

	// db2 recovers; the forced override must reset the open breaker so the
	// switch sticks instead of failing away on the next command.
	db2.hook.fail.Store(false)
	if err := mdb.ForceActiveDatabase(ctx, 1); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err)
	}
	if err := mdb.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set on forced member: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
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

	// Exactly one of Options/ClusterOptions per database.
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

	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active id = %d after concurrent failover, want 1", got)
	}
}

// fakeDetector is a controllable failure detector: ShouldFailover reports the
// trip flag, Reset counts and clears it.
type fakeDetector struct {
	tripped   atomic.Bool
	resets    atomic.Int64
	successes atomic.Int64
	failures  atomic.Int64
}

func (d *fakeDetector) RecordSuccess()       { d.successes.Add(1) }
func (d *fakeDetector) RecordFailure(error)  { d.failures.Add(1) }
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

func TestMultiDBAddDatabaseCallbackSeesNewID(t *testing.T) {
	dbA := newTestDB("a", "db-a:6379", 2, true)

	var mu sync.Mutex
	var ids []int
	opts := baseOptions()
	opts.OnCircuitStateChanged = func(dbID int, from, to string) {
		mu.Lock()
		ids = append(ids, dbID)
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// Adding an unhealthy member opens its breaker during the initial probe;
	// the state-change callback must report the member's real id, not the zero
	// default.
	dbB := newTestDB("b", "db-b:6379", 1, false)
	id, err := mdb.AddDatabase(context.Background(), dbB.cfg)
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if id != 1 {
		t.Fatalf("AddDatabase id = %d, want 1", id)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		got := append([]int(nil), ids...)
		mu.Unlock()
		if len(got) > 0 {
			for _, i := range got {
				if i != 1 {
					t.Fatalf("circuit state callback reported id %d, want 1 (all: %v)", i, got)
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
	opts.OnCircuitStateChanged = func(dbID int, from, to string) {
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
	go func() { done <- mdb.SetActiveDatabase(context.Background(), 0) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetActiveDatabase: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SetActiveDatabase deadlocked with a re-entrant circuit state callback")
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

// TestMultiDBLatchedDetectorDoesNotWedgeHalfOpenSingleMember pins that a single
// member whose breaker is half-open cannot be wedged unavailable by a latching
// custom detector. The detector stays tripped across the outage, so the gate
// can never admit the half-open probe; with no alternate candidate the failover
// path must clear the stale detector so the next command probes the recovering
// member instead of escalating forever.
func TestMultiDBLatchedDetectorDoesNotWedgeHalfOpenSingleMember(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA) // single member: no alternate candidate

	// Open A's breaker and let the grace period elapse so it is half-open.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)
	// Latch the custom detector: fakeDetector stays tripped until Reset.
	det.tripped.Store(true)

	ctx := context.Background()
	var lastErr error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lastErr = mdb.Get(ctx, "k").Err()
		if lastErr == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("half-open single member with a latched detector never recovered: err=%v", lastErr)
	}
	if det.resets.Load() == 0 {
		t.Error("detector was never reset — the half-open no-candidate recovery path was not taken")
	}
}

// Auto-fallback must not immediately undo a detector-driven failover: a flaky
// higher-weight primary whose failure rate trips the detector (without its
// breaker ever opening) must stay failed over, not ping-pong.
func TestMultiDBAutoFallbackDoesNotUndoDetectorFailover(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true) // higher weight, active
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}

	opts := baseOptions()
	opts.FailureDetector = det
	opts.CommandRetries = 1
	opts.AutoFallbackInterval = time.Hour // drive fallback explicitly via TestTryFallback
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 100, // high: the flaky-rate signal never opens A's breaker
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("setup: active=%d, want 0", got)
	}

	// Detector trips (flaky A): a command fails over A->B without opening A's
	// breaker (threshold 100).
	det.tripped.Store(true)
	_ = mdb.Set(context.Background(), "k", "v", 0).Err()
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("detector failover: active=%d, want 1", got)
	}
	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Fatal("A's breaker opened; the test needs a detector-only (breaker-closed) failover")
	}

	// A's breaker is still Closed and A has the higher weight, so the old
	// (breaker-only) gate would bounce back to A. The suppression window must
	// keep the client on B.
	det.tripped.Store(false)
	mdb.TestTryFallback()
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Errorf("auto-fallback bounced to A (active=%d); it must not undo the detector failover", got)
	}
}

// errHook returns a fixed error for every command (no dial).
type errHook struct{ err error }

func (errHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h errHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		cmd.SetErr(h.err)
		return h.err
	}
}

func (errHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { return nil }
}

// A MOVED/ASK that surfaces to the multidb layer (the cluster client exhausted
// its redirect budget) is an availability failure and must drive failover, not
// be treated as a healthy reply.
func TestMultiDBSurfacedRedirectTriggersFailover(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true) // active
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)

	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	opts.Clients = append(opts.Clients, dbA.cfg, dbB.cfg)
	ctxInit, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctxInit, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	// A surfaces a MOVED reply; B is healthy. (Install A's hook as the only one
	// so it isn't short-circuited by the default hook.)
	moved := proto.ParseErrorReply([]byte("-MOVED 3999 127.0.0.1:6381"))
	if err := mdb.AddDatabaseHook(0, errHook{err: moved}); err != nil {
		t.Fatalf("AddDatabaseHook(0): %v", err)
	}
	if err := mdb.AddDatabaseHook(1, dbB.hook); err != nil {
		t.Fatalf("AddDatabaseHook(1): %v", err)
	}

	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("Get should have failed over to B and succeeded: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Errorf("active id = %d, want 1 (surfaced MOVED must trigger failover)", got)
	}
}

// badStrategy always returns a fixed id, used to test that an unknown-id
// selection is rejected rather than stored as the active.
type badStrategy struct{ id int }

func (s badStrategy) Select([]redis.MultiDBDatabaseState) int { return s.id }

func TestMultiDBInvalidStrategyIDRejected(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1, true)
	opts := baseOptions()
	opts.FailoverStrategy = badStrategy{id: 99} // no such member
	opts.Clients = append(opts.Clients, db1.cfg, db2.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Both members are healthy, so the only reason selection can fail is the
	// strategy returning an id that names no candidate. It must be rejected
	// (not stored as the active or used to resolve a nil member) and surface as
	// a clean error rather than a panic.
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if mdb != nil {
		_ = mdb.Close()
	}
	if !errors.Is(err, redis.ErrInsufficientHealthyDatabases) {
		t.Fatalf("bad strategy id: err = %v, want ErrInsufficientHealthyDatabases", err)
	}
}

// panicPolicy panics in Execute/ExecuteCluster, to test that a custom policy
// panic on the probe path is recovered rather than crashing the process.
type panicPolicy struct{}

func (panicPolicy) Execute(context.Context, []redis.MultiDBHealthCheck, *redis.Client) bool {
	panic("policy boom")
}

func (panicPolicy) ExecuteCluster(context.Context, []redis.MultiDBHealthCheck, *redis.ClusterClient) bool {
	panic("policy boom")
}

func TestMultiDBPanickingHealthPolicyDoesNotCrash(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.HealthCheckPolicy = panicPolicy{}
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	opts.Clients = append(opts.Clients, db1.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// The panicking policy makes every probe unhealthy (recovered), so
	// construction fails the availability gate — but must NOT crash.
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if mdb != nil {
		_ = mdb.Close()
	}
	// The point: the policy panic was recovered, so construction returns an
	// error gracefully instead of crashing the process.
	if err == nil {
		t.Fatal("panicking policy: expected construction to fail gracefully, got nil")
	}
}

func TestMultiDBSetWeightAfterCloseReturnsErrClosed(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), db1)
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mdb.SetWeight(0, 5.0); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("SetWeight after Close = %v, want ErrClosed", err)
	}
}

// switchThenFailHook switches the active database on its first command and
// then fails that command, so its outcome is recorded against a member that is
// no longer the active.
type switchThenFailHook struct {
	mdb  *redis.MultiDBClient
	to   int
	once sync.Once
}

func (*switchThenFailHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *switchThenFailHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.once.Do(func() { _ = h.mdb.ForceActiveDatabase(context.Background(), h.to) })
		cmd.SetErr(io.EOF)
		return io.EOF
	}
}

func (*switchThenFailHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { return nil }
}

func TestMultiDBStaleOutcomeNotRecordedOnDetector(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2, true) // active
	db2 := newTestDB("db2", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 100, // never opens on this test's single failure
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	opts.Clients = append(opts.Clients, db1.cfg, db2.cfg)
	ctxInit, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctxInit, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	if err := mdb.AddDatabaseHook(0, &switchThenFailHook{mdb: mdb, to: 1}); err != nil {
		t.Fatalf("AddDatabaseHook(0): %v", err)
	}
	if err := mdb.AddDatabaseHook(1, db2.hook); err != nil {
		t.Fatalf("AddDatabaseHook(1): %v", err)
	}

	_ = mdb.Get(context.Background(), "k").Err()

	// db1's failure was recorded AFTER the hook switched the active to db2, so
	// the global detector must not have counted it against db2's window.
	if got := det.failures.Load(); got != 0 {
		t.Errorf("stale outcome from the old active polluted the detector: failures=%d, want 0", got)
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
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1 after failover", got)
	}

	// A recovers; the background checks close its breaker and auto-fallback
	// switches back. A detector window still tripped from the outage must
	// be cleared by the fallback — otherwise the very next command fails
	// straight back over to the lower-weight member.
	dbA.hook.fail.Store(false)
	det.tripped.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for mdb.ActiveDatabaseID() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("auto-fallback to member 0 never happened")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := mdb.Get(ctx, "k").Err(); err != nil {
		t.Fatalf("Get after fallback: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
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
	opts.OnCircuitStateChanged = func(dbID int, from, to string) {
		mu.Lock()
		events = append(events, dbID)
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// Reproduce the background-loop race deterministically: snapshot B like
	// runHealthChecksOnce does, remove it, then run the stale probe. The
	// removed member's breaker transition must not fire a user callback — its
	// `removed` flag, set on removal, suppresses delivery.
	dbB.check.healthy.Store(false)
	mdb.TestProbeRacingRemoval(1)

	time.Sleep(300 * time.Millisecond) // callbacks are delivered asynchronously
	mu.Lock()
	got := append([]int(nil), events...)
	mu.Unlock()
	if len(got) != 0 {
		t.Errorf("stale probe of a removed member fired callbacks: ids %v", got)
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

// close() must not block on an in-flight PubSub reconnect dial. The reconnect
// holds the subscription's lock across the dial; unless that dial is bound to
// client shutdown, Close hangs waiting for the same lock.
func TestMultiDBCloseUnblocksPubSubReconnectDial(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	// B's dial blocks until its context is canceled, with a long DialTimeout so
	// only a shutdown-bound reconnect context (not the dial timeout) unblocks
	// Close in time.
	dbB.cfg.Options.DialTimeout = 30 * time.Second
	dbB.cfg.Options.DialerRetries = 1
	dbB.cfg.Options.Dialer = func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
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

	// Fail A -> failover to B -> notifyPubSubs starts a reconnect that dials B
	// and blocks holding the subscription lock.
	dbA.hook.fail.Store(true)
	_ = mdb.Get(context.Background(), "k").Err()
	time.Sleep(50 * time.Millisecond) // let the reconnect enter the blocking dial

	done := make(chan error, 1)
	go func() { done <- mdb.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked on an in-flight PubSub reconnect dial")
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

// failbackOnlyCheck is a fakeHealthCheck that reports FailbackOnly, like the
// lag-aware REST check: its verdict may only gate routing traffic TO a
// member and must never evict the current active.
type failbackOnlyCheck struct{ fakeHealthCheck }

func newFailbackOnlyCheck(healthy bool) *failbackOnlyCheck {
	c := &failbackOnlyCheck{}
	c.healthy.Store(healthy)
	return c
}

func (c *failbackOnlyCheck) FailbackOnly() bool { return true }

func TestMultiDBFailbackOnlyCheckDoesNotEvictActive(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	// Both members carry a fail-back-only check (the lag-aware shape) next
	// to their healthy liveness check; the lag breach starts AFTER the
	// client is up, like a replication backlog building on a live pair.
	fbA := newFailbackOnlyCheck(true)
	fbB := newFailbackOnlyCheck(true)
	dbA.cfg.HealthChecks = append(dbA.cfg.HealthChecks, fbA)
	dbB.cfg.HealthChecks = append(dbB.cfg.HealthChecks, fbB)
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	fbA.healthy.Store(false)
	fbB.healthy.Store(false)
	mdb.TestRunHealthChecksOnce()

	// The ACTIVE member must not be evicted by the lag verdict: its breaker
	// stays closed and traffic keeps flowing.
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("active id = %d, want 0 (lag must not evict the active)", got)
	}
	if err := mdb.Ping(context.Background()).Err(); err != nil {
		t.Errorf("Ping after health pass: %v (active breaker must stay closed)", err)
	}

	// A NON-active member is a fail-back candidate: the same failing check
	// must open its breaker so fallback cannot route traffic back to a
	// member that has not caught up.
	if mdb.TestBreakerReserveHalfOpen(1) {
		t.Error("candidate member's breaker still admits — the fail-back-only check must gate re-entry")
	}
}

// Re-selecting the current active must not be gated by a fail-back-only check:
// SetActiveDatabase on the current active with a failing lag check must succeed
// and must not record a failure on the active's breaker.
func TestMultiDBSetActiveDatabaseReselectIgnoresFailbackOnly(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true) // active (higher weight)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	fbA := newFailbackOnlyCheck(true)
	dbA.cfg.HealthChecks = append(dbA.cfg.HealthChecks, fbA)

	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("setup: active=%d, want 0", got)
	}

	fbA.healthy.Store(false) // lag breach on the active
	if err := mdb.SetActiveDatabase(context.Background(), 0); err != nil {
		t.Fatalf("SetActiveDatabase(current active) with a failing fail-back-only check = %v, want nil", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("active id = %d, want 0", got)
	}
	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("re-selecting the active recorded a fail-back-only failure on its breaker")
	}
}

// Under concurrency, OnActiveDatabaseChanged deliveries must form a contiguous
// chain (each `to` == the next `from`). Switches are serialized by failoverMu
// and each does current->id, so the chain only holds if the callbacks are
// enqueued in switch order — the bug being that enqueuing in the announce
// closure (after failoverMu is released) lets two switches reorder.
func TestMultiDBActiveChangeCallbackOrderUnderConcurrency(t *testing.T) {
	db0 := newTestDB("d0", "127.0.0.1:10", 3, true)
	db1 := newTestDB("d1", "127.0.0.1:11", 2, true)
	db2 := newTestDB("d2", "127.0.0.1:12", 1, true)

	opts := baseOptions()
	var mu sync.Mutex
	var pairs [][2]int
	opts.OnActiveDatabaseChanged = func(from, to int) {
		mu.Lock()
		pairs = append(pairs, [2]int{from, to})
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, db0, db1, db2)

	ctx := context.Background()
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_ = mdb.ForceActiveDatabase(ctx, (g+i)%3)
			}
		}(g)
	}
	wg.Wait()

	// Wait for the async callback queue to drain (count stops growing).
	deadline := time.Now().Add(3 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(pairs)
		mu.Unlock()
		if n == last {
			break
		}
		last = n
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(pairs) == 0 {
		t.Fatal("no active-change callbacks recorded")
	}
	for i := 1; i < len(pairs); i++ {
		if pairs[i][0] != pairs[i-1][1] {
			t.Fatalf("out-of-order active-change delivery at %d: ...->%d then %d->%d (chain broken)",
				i, pairs[i-1][1], pairs[i][0], pairs[i][1])
		}
	}
}

// midflightHook runs an armable callback inside command execution and then
// serves the command locally (nil error) without dialing.
type midflightHook struct{ fn atomic.Pointer[func()] }

func (h *midflightHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *midflightHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if f := h.fn.Load(); f != nil {
			(*f)()
		}
		return nil
	}
}

func (h *midflightHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { return nil }
}

func TestMultiDBCommandClosedAdmissionDoesNotFreeProbeSlot(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 2, // one success must not close the circuit
		GracePeriod:      30 * time.Millisecond,
	}
	opts.Clients = append(opts.Clients, dbA.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	hook := &midflightHook{}
	if err := mdb.AddDatabaseHook(0, hook); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// The command is admitted while A's breaker is CLOSED — no probe slot is
	// reserved. While it executes, a failure opens the breaker, the grace
	// period elapses, and recovery probes reserve every half-open slot
	// (MaxHalfOpenRequests defaults to SuccessThreshold, so there are two).
	// The command's success must count toward closing WITHOUT freeing a
	// probe's slot.
	fn := func() {
		mdb.TestBreakerRecordFailure(0)
		time.Sleep(50 * time.Millisecond)
		if !mdb.TestBreakerReserveHalfOpen(0) || !mdb.TestBreakerReserveHalfOpen(0) {
			t.Error("setup: expected to reserve both half-open probe slots")
		}
	}
	hook.fn.Store(&fn)
	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("Get: %v", err)
	}
	hook.fn.Store(nil)

	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a second half-open reservation succeeded — the command released a slot it never reserved")
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
	opts.OnCircuitStateChanged = func(dbID int, from, to string) {
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
	if err := mdb.ForceActiveDatabase(context.Background(), 0); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err) // resets the breaker: open -> closed
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
	if err := mdb.SetActiveDatabase(parent, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetActiveDatabase = %v, want context.Canceled", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("canceled SetActiveDatabase switched the active database to %d", got)
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
	if got := mdb.ActiveDatabaseID(); got != 0 {
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
	// ping-pong the active id in a busy loop until the context dies.
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
	opts.OnCircuitStateChanged = func(dbID int, from, to string) {
		mu.Lock()
		events = append(events, dbID)
		mu.Unlock()
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// A breaker outcome that lands AFTER the member was removed (the probe
	// passed its removed-check just before the removal) must not surface a
	// user callback: the member is gone and its `removed` flag suppresses it.
	mdb.TestStaleRecordAfterRemoval(1)

	time.Sleep(300 * time.Millisecond) // callbacks are delivered asynchronously
	mu.Lock()
	got := append([]int(nil), events...)
	mu.Unlock()
	if len(got) != 0 {
		t.Errorf("stale breaker record on a removed member fired callbacks: ids %v", got)
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

	// Hold failoverMu: SetActiveDatabase probes B, and B's blocking check stalls
	// with the lock held.
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = mdb.SetActiveDatabase(context.Background(), 1)
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
	// ErrClosed rather than a not-found error from the drained membership.
	if err := mdb.SetActiveDatabase(context.Background(), 0); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("SetActiveDatabase after Close: err = %v, want ErrClosed", err)
	}
	if err := mdb.ForceActiveDatabase(context.Background(), 0); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("ForceActiveDatabase after Close: err = %v, want ErrClosed", err)
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

	// The user callbacks are delivered on the announce queue's own goroutine:
	// a panic there must be recovered (not crash the process), and the
	// failover itself must complete regardless.
	dbA.hook.fail.Store(true)
	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("Get across failover: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
	// Let the announce queue drain so the panicking callbacks actually run
	// (and get recovered) before the test returns.
	time.Sleep(100 * time.Millisecond)
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

	// Same for a generic Cmd spelled by name (Do-style): the typed marker
	// interface alone would miss it.
	raw := redis.NewCmd(context.Background(), "HIMPORT", "PREPARE", "fs", "f1")
	if err := mdb.Process(context.Background(), raw); err == nil {
		t.Error("Process accepted a name-spelled HIMPORT command")
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

func TestMultiDBSameActiveManualSelectionResetsEscalation(t *testing.T) {
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
	// the active id does not change.
	dbA.hook.fail.Store(false)
	if err := mdb.ForceActiveDatabase(ctx, 0); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err)
	}

	// A fresh outage must escalate from a clean slate.
	time.Sleep(5 * time.Millisecond)
	dbA.hook.fail.Store(true)
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("second outage: err = %v, want ErrTemporarilyNotAvailable (stale escalation)", err)
	}
}

func TestMultiDBControlOpsClosedWhileWaitingForLock(t *testing.T) {
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
	opts.HealthCheckTimeout = time.Hour
	mdb := newTestMultiDB(t, opts, dbA, dbB)
	check.armed.Store(true)

	// Hold failoverMu via a probing control operation.
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		_ = mdb.SetActiveDatabase(context.Background(), 1)
	}()
	time.Sleep(100 * time.Millisecond)

	// Queue control operations with LIVE contexts, then Close: once they
	// finally acquire the lock the client is closed — they must report
	// ErrClosed, not mutate the drained membership or report bogus ids.
	setDone := make(chan error, 1)
	removeDone := make(chan error, 1)
	go func() { setDone <- mdb.ForceActiveDatabase(context.Background(), 1) }()
	go func() { removeDone <- mdb.RemoveDatabase(context.Background(), 1) }()
	time.Sleep(100 * time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- mdb.Close() }()
	time.Sleep(100 * time.Millisecond)
	check.armed.Store(false)
	close(check.gate) // release the lock holder

	if err := <-setDone; !errors.Is(err, redis.ErrClosed) {
		t.Errorf("ForceActiveDatabase after Close during lock wait: err = %v, want ErrClosed", err)
	}
	if err := <-removeDone; !errors.Is(err, redis.ErrClosed) {
		t.Errorf("RemoveDatabase after Close during lock wait: err = %v, want ErrClosed", err)
	}
	<-holderDone
	if err := <-closeDone; err != nil {
		t.Errorf("Close: %v", err)
	}
}

// blockingFailHook blocks each command on a gate, then fails it with a
// transport error — long enough for a concurrent Close to land mid-loop.
type blockingFailHook struct{ gate chan struct{} }

func (h *blockingFailHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *blockingFailHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		<-h.gate
		err := fmt.Errorf("blocking hook: %w", io.EOF)
		cmd.SetErr(err)
		return err
	}
}

func (h *blockingFailHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestMultiDBProcessNoticesCloseMidRetry(t *testing.T) {
	hook := &blockingFailHook{gate: make(chan struct{})}
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = 3
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	opts.Clients = []redis.MultiDBClientConfig{{
		Options:      &redis.Options{Addr: "127.0.0.1:1"},
		Weight:       1,
		HealthChecks: []redis.MultiDBHealthCheck{check},
	}}
	initCtx, initCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer initCancel()
	mdb, err := redis.NewMultiDBClient(initCtx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	if err := mdb.AddDatabaseHook(0, hook); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// The command blocks in the hook; Close completes meanwhile; the
	// released attempt fails and the retry loop must notice the closed
	// client instead of escalating through an empty membership.
	getDone := make(chan error, 1)
	go func() { getDone <- mdb.Get(context.Background(), "k").Err() }()
	time.Sleep(100 * time.Millisecond)
	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(hook.gate)

	if err := <-getDone; !errors.Is(err, redis.ErrClosed) {
		t.Errorf("Get across Close = %v, want ErrClosed", err)
	}
}

func TestMultiDBCanceledHealthyProbeDoesNotTouchBreaker(t *testing.T) {
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
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // a single recorded success would close the circuit
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// B: half-open. A canceled operator probe that reports healthy must not
	// record that success — the caller discards the verdict, so the breaker
	// must not act on it either.
	mdb.TestBreakerRecordFailure(1)
	time.Sleep(50 * time.Millisecond)
	check.armed.Store(true)
	if err := mdb.SetActiveDatabase(parent, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetActiveDatabase = %v, want context.Canceled", err)
	}

	if !mdb.TestBreakerReserveHalfOpen(1) {
		t.Fatal("expected the breaker to still hand out its half-open slot")
	}
	if mdb.TestBreakerReserveHalfOpen(1) {
		t.Error("canceled healthy probe was recorded (circuit closed: unlimited admissions)")
	}
}

// panickyStrategy, once armed, panics on every selection — the worst-case
// custom strategy (unarmed it behaves like the default, so construction
// succeeds).
type panickyStrategy struct{ armed atomic.Bool }

func (s *panickyStrategy) Select(cands []redis.MultiDBDatabaseState) int {
	if s.armed.Load() {
		panic("panicky strategy")
	}
	return redis.WeightBasedFailoverStrategy{}.Select(cands)
}

func TestMultiDBPanickyStrategyDoesNotCrash(t *testing.T) {
	strategy := &panickyStrategy{}
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.FailoverStrategy = strategy
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, dbA)
	strategy.armed.Store(true)

	// The strategy also runs on the background loop: a panic must be
	// recovered and treated as "no candidate", surfacing unavailability
	// instead of crashing the process.
	dbA.hook.fail.Store(true)
	if err := mdb.Get(context.Background(), "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("Get = %v, want ErrTemporarilyNotAvailable", err)
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
	if got := mdb.ActiveDatabaseID(); got != 0 {
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
		if err := mdb.SetActiveDatabase(canceled, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("SetActiveDatabase with canceled ctx: err = %v, want context.Canceled", err)
		}
	}

	// B must still be selectable: failing A must move traffic onto B.
	dbA.hook.fail.Store(true)
	if err := mdb.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("command after failover: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Errorf("active = %d, want 1", got)
	}
}
