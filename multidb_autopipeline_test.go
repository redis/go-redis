package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
)

// MultiDBClient must be usable everywhere a UniversalClient is expected —
// that is also what the autopipeliner requires from its backend.
var _ redis.UniversalClient = (*redis.MultiDBClient)(nil)

// countingDetector counts detector records so tests can assert per-command
// outcome recording for batches.
type countingDetector struct {
	successes atomic.Int64
	failures  atomic.Int64
	trip      atomic.Bool
}

func (d *countingDetector) RecordSuccess()          { d.successes.Add(1) }
func (d *countingDetector) RecordFailure(err error) { d.failures.Add(1) }
func (d *countingDetector) ShouldFailover() bool    { return d.trip.Load() }
func (d *countingDetector) Reset()                  { d.trip.Store(false) }

func fastBreaker() *redis.MultiDBCircuitBreakerConfig {
	return &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
}

func TestMultiDBPipelineRoutesToActive(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1, db2)
	ctx := context.Background()

	pipe := mdb.Pipeline()
	pipe.Set(ctx, "a", "1", 0)
	pipe.Set(ctx, "b", "2", 0)
	pipe.Get(ctx, "a")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}
	if got := db1.hook.commands.Load(); got != 3 {
		t.Errorf("active db1 saw %d commands, want 3", got)
	}
	if db2.hook.commands.Load() != 0 {
		t.Error("passive db2 saw pipeline commands")
	}
	if db1.hook.batches.Load() != 1 {
		t.Errorf("db1 batches = %d, want 1", db1.hook.batches.Load())
	}
}

func TestMultiDBPipelineFailoverRetry(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = fastBreaker()
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)

	pipe := mdb.Pipeline()
	pipe.Set(ctx, "a", "1", 0)
	pipe.Get(ctx, "a")
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("pipeline exec after active failure: %v", err)
	}
	for _, cmd := range cmds {
		if cmd.Err() != nil {
			t.Errorf("command %v error after retry: %v", cmd.Name(), cmd.Err())
		}
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index = %d after pipeline failover, want 1", got)
	}
	if db2.hook.batches.Load() == 0 {
		t.Error("db2 never received the retried batch")
	}
}

func TestMultiDBPipelinePerCommandRecording(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	det := &countingDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 100, // stay closed; we only count
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)
	pipe := mdb.Pipeline()
	for i := 0; i < 5; i++ {
		pipe.Get(ctx, "k")
	}
	_, _ = pipe.Exec(ctx)

	if got := det.failures.Load(); got != 5 {
		t.Errorf("detector failures = %d, want 5 (one per command in the batch)", got)
	}

	db1.hook.fail.Store(false)
	pipe = mdb.Pipeline()
	for i := 0; i < 3; i++ {
		pipe.Get(ctx, "k")
	}
	_, _ = pipe.Exec(ctx)
	if got := det.successes.Load(); got != 3 {
		t.Errorf("detector successes = %d, want 3", got)
	}
}

func TestMultiDBPipelineNoRetryCommandDisablesRetry(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 3 // must be ignored when the batch has a NoRetry command
	opts.CircuitBreakerConfig = fastBreaker()
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)

	pipe := mdb.Pipeline()
	pipe.Get(ctx, "k")
	_ = pipe.Process(ctx, redis.NewRawWriteToCmd(ctx, io.Discard, "get", "k"))
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("batch with a NoRetry command should surface the failure, not retry")
	}
	if db2.hook.batches.Load() != 0 {
		t.Error("batch containing a NoRetry command was retried on db2")
	}
}

func TestMultiDBTxPipelineNotRetried(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 3 // must be ignored for tx pipelines
	opts.CircuitBreakerConfig = fastBreaker()
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)

	pipe := mdb.TxPipeline()
	pipe.Set(ctx, "a", "1", 0)
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("tx pipeline exec should surface the failure, not retry")
	}
	if db2.hook.batches.Load() != 0 {
		t.Error("tx pipeline was retried on db2 — MULTI/EXEC must be at-most-once")
	}
}

func TestMultiDBTxPipelineRecordsOnlyUserCommands(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	det := &countingDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 100,
		SuccessThreshold: 1,
		GracePeriod:      time.Hour,
	}
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)
	pipe := mdb.TxPipeline()
	pipe.Set(ctx, "a", "1", 0)
	_, _ = pipe.Exec(ctx)

	// One user command — one failure record, not three (MULTI/EXEC excluded).
	if got := det.failures.Load(); got != 1 {
		t.Errorf("detector failures = %d, want 1 (synthetic MULTI/EXEC must not count)", got)
	}
}

func TestMultiDBAutoPipelineRefusesClusterMembers(t *testing.T) {
	standalone := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	clusterCheck := newFakeHealthCheck(true)

	opts := baseOptions()
	opts.InitialDBState = redis.InitialDBStateOneAvailable
	opts.Clients = append(opts.Clients, standalone.cfg, redis.MultiDBClientConfig{
		ClusterOptions: &redis.ClusterOptions{Addrs: []string{"127.0.0.1:2"}},
		Weight:         1.0,
		HealthChecks:   []redis.MultiDBHealthCheck{clusterCheck},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()

	if _, err := mdb.AutoPipeline(); err == nil {
		t.Error("AutoPipeline with a cluster member should be refused")
	}
	if _, err := mdb.AsyncAutoPipeline(); err == nil {
		t.Error("AsyncAutoPipeline with a cluster member should be refused")
	}
}

func TestMultiDBAddClusterMemberVsAutoPipeliner(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1)
	ctx := context.Background()

	ap, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}

	clusterCfg := redis.MultiDBClientConfig{
		ClusterOptions:         &redis.ClusterOptions{Addrs: []string{"127.0.0.1:2"}},
		Weight:                 1.0,
		HealthChecks:           []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
		SkipInitialHealthCheck: true,
	}

	// Live autopipeliner blocks cluster additions.
	if _, err := mdb.AddDatabase(ctx, clusterCfg); err == nil {
		t.Fatal("AddDatabase(cluster) should be refused while an autopipeliner is live")
	}

	// A CLOSED autopipeliner must not block forever.
	if err := ap.Close(); err != nil {
		t.Fatalf("AutoPipeliner.Close: %v", err)
	}
	if _, err := mdb.AddDatabase(ctx, clusterCfg); err != nil {
		t.Fatalf("AddDatabase(cluster) after closing the autopipeliner: %v", err)
	}

	// And with a cluster member present, new autopipeliners are refused.
	if _, err := mdb.AutoPipeline(); err == nil {
		t.Error("AutoPipeline should be refused once a cluster member exists")
	}
}

func TestMultiDBAutoPipelineRoutesAndFailsOver(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = fastBreaker()
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	ap, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}

	// Happy path: commands execute on the active member.
	if err := ap.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("autopipelined Set: %v", err)
	}
	if db1.hook.commands.Load() == 0 {
		t.Error("active db1 saw no autopipelined commands")
	}

	// Kill the active member: autopipelined commands must fail over and
	// still succeed.
	db1.hook.fail.Store(true)
	if err := ap.Set(ctx, "k2", "v2", 0).Err(); err != nil {
		t.Fatalf("autopipelined Set after active failure: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index = %d after autopipeline failover, want 1", got)
	}
}

func TestMultiDBAutoPipelineEscalation(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 2.0, true)
	db2 := newTestDB("db2", "127.0.0.1:2", 1.0, true)

	opts := baseOptions()
	opts.CommandRetries = 1
	opts.MaxFailoverAttempts = 2
	opts.FailoverAttemptDelay = 5 * time.Millisecond
	opts.CircuitBreakerConfig = fastBreaker()
	mdb := newTestMultiDB(t, opts, db1, db2)
	ctx := context.Background()

	db1.hook.fail.Store(true)
	db2.hook.fail.Store(true)

	ap, err := mdb.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}

	sawUnavailable := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := ap.Get(ctx, "k").Err()
		if errors.Is(err, redis.ErrTemporarilyNotAvailable) || errors.Is(err, redis.ErrPermanentlyNotAvailable) {
			sawUnavailable = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawUnavailable {
		t.Error("autopipelined commands never surfaced the escalation error")
	}
}

func TestMultiDBAutoPipelineInstanceCachedAndClosed(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1)

	ap1, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	ap2, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline second call: %v", err)
	}
	if ap1 != ap2 {
		t.Error("AutoPipeline should cache and return the same instance")
	}

	if err := mdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := mdb.AutoPipeline(); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("AutoPipeline after Close: err = %v, want ErrClosed", err)
	}
}

func TestMultiDBDo(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1.0, true)
	mdb := newTestMultiDB(t, baseOptions(), db1)

	if err := mdb.Do(context.Background(), "set", "k", "v").Err(); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if db1.hook.commands.Load() == 0 {
		t.Error("Do did not reach the active database")
	}
}

func TestMultiDBTxPipelineNotRetriedByMemberClient(t *testing.T) {
	var dials atomic.Int64
	opts := baseOptions()
	opts.Clients = []redis.MultiDBClientConfig{{
		Weight: 1,
		Options: &redis.Options{
			Addr:          "127.0.0.1:1",
			MaxRetries:    2, // member-level retries that must NOT apply to transactions
			DialerRetries: 1, // exactly one dial per member pipeline attempt
			DialTimeout:   200 * time.Millisecond,
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				dials.Add(1)
				return nil, io.EOF
			},
		},
		HealthChecks: []redis.MultiDBHealthCheck{newFakeHealthCheck(true)},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()

	_, err = mdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	})
	if err == nil {
		t.Fatal("expected the transaction to fail (no server behind the dialer)")
	}
	// EXEC may have committed before a transport error surfaced: the member
	// client must not replay the transaction either — at-most-once holds for
	// the whole stack, not only the MultiDB retry layer.
	if got := dials.Load(); got != 1 {
		t.Errorf("transaction dialed %d times, want 1 (member retries must be suppressed)", got)
	}
}

func TestMultiDBBatchRegatesAfterFailover(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // MaxHalfOpenRequests defaults to this: one probe slot
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// B: open its breaker, wait out the grace period, then occupy the only
	// half-open probe slot — a recovering member whose probe budget a
	// concurrent request is already using.
	mdb.TestBreakerRecordFailure(1)
	time.Sleep(50 * time.Millisecond)
	if !mdb.TestBreakerReserveHalfOpen(1) {
		t.Fatal("setup: expected to reserve B's half-open probe slot")
	}

	// Trip the gate: the batch fails over from A, and the strategy picks
	// half-open B. With B's only probe slot taken, the batch must re-enter
	// the admission gate (landing back on closed A) rather than execute on
	// B without a reservation.
	det.tripped.Store(true)
	if _, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	}); err != nil {
		t.Fatalf("Pipelined: %v", err)
	}

	if got := dbB.hook.batches.Load(); got != 0 {
		t.Errorf("batch executed on B without a half-open slot (batches = %d)", got)
	}
	if got := dbA.hook.batches.Load(); got != 1 {
		t.Errorf("batches on A = %d, want 1", got)
	}
}

func TestMultiDBTxPipelineMarksAllCmdsNoRetry(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)

	cmds, err := mdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		p.Get(context.Background(), "k")
		return nil
	})
	if err != nil {
		t.Fatalf("TxPipelined: %v", err)
	}
	// The cluster tx path trims the MULTI/EXEC envelope before running its
	// retry check on the remaining user commands, so the at-most-once
	// marker must ride on every command — a marker only on the synthetic
	// MULTI lets a cluster member replay the transaction.
	for i, cmd := range cmds {
		if !cmd.NoRetry() {
			t.Errorf("cmd %d (%s) not marked NoRetry — a cluster member could replay the transaction", i, cmd.Name())
		}
	}
}

func TestMultiDBBatchDetectorFailoverDoesNotLeakProbeSlot(t *testing.T) {
	newClient := func(t *testing.T, det *fakeDetector) (*redis.MultiDBClient, *testDB, *testDB) {
		dbA := newTestDB("a", "127.0.0.1:1", 2, true)
		dbB := newTestDB("b", "127.0.0.1:2", 1, true)
		opts := baseOptions()
		opts.FailureDetector = det
		opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1, // one bounded half-open probe slot
			GracePeriod:      30 * time.Millisecond,
		}
		mdb := newTestMultiDB(t, opts, dbA, dbB)
		// A: half-open (recovering) with its single probe slot free.
		mdb.TestBreakerRecordFailure(0)
		time.Sleep(50 * time.Millisecond)
		det.tripped.Store(true)
		return mdb, dbA, dbB
	}

	// Detector-tripped batches must fail over without reserving the old
	// active's half-open probe slot — nothing would record or release it.
	t.Run("pipeline", func(t *testing.T) {
		det := &fakeDetector{}
		mdb, _, _ := newClient(t, det)
		if _, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
			p.Set(context.Background(), "k", "v", 0)
			return nil
		}); err != nil {
			t.Fatalf("Pipelined: %v", err)
		}
		if !mdb.TestBreakerReserveHalfOpen(0) {
			t.Error("pipeline detector failover leaked A's half-open probe slot")
		}
	})
	t.Run("tx", func(t *testing.T) {
		det := &fakeDetector{}
		mdb, _, _ := newClient(t, det)
		if _, err := mdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
			p.Set(context.Background(), "k", "v", 0)
			return nil
		}); err != nil {
			t.Fatalf("TxPipelined: %v", err)
		}
		if !mdb.TestBreakerReserveHalfOpen(0) {
			t.Error("tx detector failover leaked A's half-open probe slot")
		}
	})
}

// mixedResultHook simulates an executed batch on a recovering member: early
// commands succeeded, the last one hit a transport error (which is also the
// batch-level error).
type mixedResultHook struct{}

func (mixedResultHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (mixedResultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (mixedResultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) > 0 {
			cmds[len(cmds)-1].SetErr(io.EOF)
		}
		return io.EOF
	}
}

func TestMultiDBFailedBatchDoesNotCloseHalfOpenBreaker(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1, // one successful probe would close the circuit
		GracePeriod:      30 * time.Millisecond,
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
	defer mdb.Close()
	if err := mdb.AddDatabaseHook(0, mixedResultHook{}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// Half-open member admits one probe batch whose early command succeeds
	// but whose later command fails at transport level: the batch FAILED as
	// a recovery probe, so it must not close the circuit — failures must be
	// recorded before successes.
	mdb.TestBreakerRecordFailure(0)
	mdb.TestBreakerRecordFailure(0) // -> open (threshold 2)
	time.Sleep(50 * time.Millisecond)

	_, _ = mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k1", "v", 0)
		p.Set(context.Background(), "k2", "v", 0)
		return nil
	})

	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a failed recovery batch closed (or left admitting) the half-open breaker")
	}
}

// abortHook aborts every batch with an error WITHOUT stamping the commands
// and without calling next — the worst-case instrumentation hook.
type abortHook struct{}

func (abortHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (abortHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (abortHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return errors.New("aborted by hook")
	}
}

func TestMultiDBHookAbortedBatchNotRecordedAsSuccess(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one half-open probe slot; one success closes
		GracePeriod:      30 * time.Millisecond,
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
	defer mdb.Close()
	if err := mdb.AddDatabaseHook(0, abortHook{}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// Half-open member: a batch aborted by a local hook (error returned,
	// commands never stamped, Redis never contacted) must not be recorded
	// as per-command successes — that would close the breaker off a purely
	// client-side failure.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)

	cmds, _ := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	})

	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Fatal("setup: expected the breaker to still hand out its half-open slot")
	}
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("breaker closed (or slot accounting lost) after a hook-aborted batch: unlimited admissions")
	}
	// The abort must also be visible on the commands themselves: leaving
	// them nil makes per-command inspection (and cmdsFirstErr once retries
	// are exhausted) report success for a batch that never executed.
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			t.Errorf("cmd %d has a nil error after a hook-aborted batch", i)
		}
	}
}

func TestMultiDBBatchSuccessResetsFailoverEscalation(t *testing.T) {
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

	// Recovery through BATCH traffic only: a pipeline-only workload must
	// break the consecutive-failed-attempts chain exactly like the
	// single-command path does.
	dbA.hook.fail.Store(false)
	time.Sleep(50 * time.Millisecond)
	if _, err := mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k", "v", 0)
		return nil
	}); err != nil {
		t.Fatalf("recovery batch: %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	dbA.hook.fail.Store(true)
	if err := mdb.Get(ctx, "k").Err(); !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("second outage: err = %v, want ErrTemporarilyNotAvailable (stale escalation state)", err)
	}
}

// partialReplyHook simulates an executed batch with a successful prefix: the
// first command's nil error is a successfully-read reply, a later command got
// a retryable server reply that is also the batch-level error.
type partialReplyHook struct{}

func (partialReplyHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (partialReplyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (partialReplyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := proto.RedisError("LOADING Redis is loading the dataset in memory")
		if len(cmds) > 1 {
			cmds[len(cmds)-1].SetErr(err)
		}
		return err
	}
}

func TestMultiDBExecutedBatchKeepsSuccessfulPrefix(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
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
	defer mdb.Close()
	if err := mdb.AddDatabaseHook(0, partialReplyHook{}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// One executed batch: first command succeeded (nil), second got a
	// retryable LOADING reply which is also the batch error. Only ONE
	// failure may be recorded — substituting the batch error for the
	// successful prefix would open the breaker (threshold 2) off a single
	// partially-failed batch.
	_, _ = mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k1", "v", 0)
		p.Set(context.Background(), "k2", "v", 0)
		return nil
	})

	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("successful prefix was recorded as failures: breaker opened off one partial batch")
	}
}

// cancelingFailHook fails every batch AND cancels the given context, so the
// retry loop's next iteration sees a canceled context while the commands
// still carry the previous attempt's transport errors.
type cancelingFailHook struct{ cancel context.CancelFunc }

func (h *cancelingFailHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cancelingFailHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *cancelingFailHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.cancel()
		err := io.EOF
		for _, cmd := range cmds {
			cmd.SetErr(err)
		}
		return err
	}
}

func TestMultiDBPipelineContextErrorOverwritesStaleErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 10, // stay closed so the retry stays on this member
		SuccessThreshold: 1,
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
	defer mdb.Close()
	if err := mdb.AddDatabaseHook(0, &cancelingFailHook{cancel: cancel}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// Attempt one fails at transport level and cancels the caller's context;
	// the retry loop then returns the context error. The commands must carry
	// that context error too — not the stale EOF of the failed attempt.
	cmds, err := mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k", "v", 0)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pipelined err = %v, want context.Canceled", err)
	}
	for i, cmd := range cmds {
		if !errors.Is(cmd.Err(), context.Canceled) {
			t.Errorf("cmd %d error = %v, want context.Canceled (stale transport error kept)", i, cmd.Err())
		}
	}
}

func TestMultiDBTxAndWatchReturnContextErrorBeforeFailover(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// With the detector tripped, a done context must be reported as the
	// caller's own error BEFORE the failover gate runs: failing over (and
	// resetting the detector) for an operation that cannot execute would
	// mutate availability state for nothing.
	det.tripped.Store(true)
	if _, err := mdb.TxPipelined(canceled, func(p redis.Pipeliner) error {
		p.Set(canceled, "k", "v", 0)
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Errorf("TxPipelined with canceled ctx: err = %v, want context.Canceled", err)
	}
	if err := mdb.Watch(canceled, func(tx *redis.Tx) error { return nil }, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Watch with canceled ctx: err = %v, want context.Canceled", err)
	}
	if got := det.resets.Load(); got != 0 {
		t.Errorf("canceled operations advanced failover state: detector resets = %d", got)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Errorf("canceled operations switched the active database: active = %d", got)
	}
}
