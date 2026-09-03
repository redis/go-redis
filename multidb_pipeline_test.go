package redis_test

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
	if got := mdb.ActiveDatabaseID(); got != 1 {
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
	// The hook serves this batch locally (nil without calling next), so no
	// health successes may be recorded — nothing reached Redis. Executed
	// batches' per-command success recording is covered white-box in
	// multidb_pipeline_internal_test.go.
	if got := det.successes.Load(); got != 0 {
		t.Errorf("detector successes = %d for a hook-served batch, want 0", got)
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
	if got := mdb.ActiveDatabaseID(); got != 1 {
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

// noRetryPeekHook captures each command's NoRetry flag WHILE the member
// executes the batch (it serves the batch locally by returning nil without
// calling next), so a test can assert the at-most-once marker is set during
// execution rather than after it.
type noRetryPeekHook struct{ seen []bool }

func (*noRetryPeekHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*noRetryPeekHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *noRetryPeekHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.seen = append(h.seen, cmd.NoRetry())
		}
		return nil
	}
}

func TestMultiDBTxPipelineMarksCmdsNoRetryDuringExecOnly(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.Clients = []redis.MultiDBClientConfig{{
		Options:      &redis.Options{Addr: "127.0.0.1:1"},
		Weight:       1,
		HealthChecks: []redis.MultiDBHealthCheck{check},
	}}
	initCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(initCtx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()
	peek := &noRetryPeekHook{}
	if err := mdb.AddDatabaseHook(0, peek); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	cmds, err := mdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		p.Get(context.Background(), "k")
		return nil
	})
	if err != nil {
		t.Fatalf("TxPipelined: %v", err)
	}

	// During execution every command (including the synthetic MULTI/EXEC
	// envelope) must carry the at-most-once marker: the cluster tx path trims
	// the envelope before its retry check, so a marker only on the synthetic
	// MULTI would let a cluster member replay the transaction.
	if len(peek.seen) == 0 {
		t.Fatal("member hook never saw the tx batch")
	}
	for i, nr := range peek.seen {
		if !nr {
			t.Errorf("cmd %d not marked NoRetry during execution — a cluster member could replay", i)
		}
	}
	// After the call the caller's commands are restored to retryable, so a
	// reused *Cmd is not left permanently non-retryable.
	for i, cmd := range cmds {
		if cmd.NoRetry() {
			t.Errorf("cmd %d (%s) left NoRetry after TxPipeline — caller state must be restored", i, cmd.Name())
		}
	}
}

func TestMultiDBTxPipelinePanicRestoresNoRetry(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.Clients = []redis.MultiDBClientConfig{{
		Options:      &redis.Options{Addr: "127.0.0.1:1"},
		Weight:       1,
		HealthChecks: []redis.MultiDBHealthCheck{check},
	}}
	initCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(initCtx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()
	armed := &atomic.Bool{}
	armed.Store(true)
	if err := mdb.AddDatabaseHook(0, panicPipelineHook{armed: armed}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	pipe := mdb.TxPipeline()
	setCmd := pipe.Set(context.Background(), "k", "v", 0)
	getCmd := pipe.Get(context.Background(), "k")

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from member pipeline hook")
			}
		}()
		_, _ = pipe.Exec(context.Background())
	}()

	// The member path unwound through a panic. The caller still holds setCmd/
	// getCmd; the temporary at-most-once marker must have been cleared, or a
	// reused *Cmd would be left permanently non-retryable.
	for _, cmd := range []redis.Cmder{setCmd, getCmd} {
		if cmd.NoRetry() {
			t.Errorf("cmd %s left NoRetry after a panicking TxPipeline exec — restore must be panic-safe", cmd.Name())
		}
	}
}

// TestMultiDBBatchPanicReleasesHalfOpenProbeSlot pins the deferred release: a
// batch admitted on a half-open member reserves its single probe slot, and a
// panic in the member hook unwinds past recordBatchOutcomes and the cancel
// branch. The slot must still come back, or the breaker stays wedged at
// MaxHalfOpenRequests for good (Watch already defers the release).
func TestMultiDBBatchPanicReleasesHalfOpenProbeSlot(t *testing.T) {
	newClient := func(t *testing.T) *redis.MultiDBClient {
		// Built from raw options rather than newTestMultiDB: that helper installs
		// a per-member hookedDB which serves batches locally and would sit ahead
		// of the panicking hook in the chain, so the panic would never fire. A
		// single member: alone, A stays active and its half-open admission is
		// the one the panic has to give back.
		check := newFakeHealthCheck(true)
		opts := baseOptions()
		opts.Clients = []redis.MultiDBClientConfig{{
			Options:      &redis.Options{Addr: "127.0.0.1:1"},
			Weight:       1,
			HealthChecks: []redis.MultiDBHealthCheck{check},
		}}
		opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1, // one bounded half-open probe slot
			GracePeriod:      30 * time.Millisecond,
		}
		initCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mdb, err := redis.NewMultiDBClient(initCtx, opts)
		if err != nil {
			t.Fatalf("NewMultiDBClient: %v", err)
		}
		t.Cleanup(func() { _ = mdb.Close() })
		// A: half-open (recovering) with its single probe slot free.
		mdb.TestBreakerRecordFailure(0)
		time.Sleep(50 * time.Millisecond)
		armed := &atomic.Bool{}
		armed.Store(true)
		if err := mdb.AddDatabaseHook(0, panicPipelineHook{armed: armed}); err != nil {
			t.Fatalf("AddDatabaseHook: %v", err)
		}
		return mdb
	}
	mustPanic := func(t *testing.T, fn func() error) {
		t.Helper()
		var err error
		defer func() {
			if recover() == nil {
				t.Fatalf("expected the member hook panic to propagate; call returned err=%v", err)
			}
		}()
		err = fn()
	}

	t.Run("pipeline", func(t *testing.T) {
		mdb := newClient(t)
		mustPanic(t, func() error {
			_, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
				p.Set(context.Background(), "k", "v", 0)
				return nil
			})
			return err
		})
		if !mdb.TestBreakerReserveHalfOpen(0) {
			t.Error("pipeline panic leaked A's half-open probe slot")
		}
	})
	t.Run("tx", func(t *testing.T) {
		mdb := newClient(t)
		mustPanic(t, func() error {
			_, err := mdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
				p.Set(context.Background(), "k", "v", 0)
				return nil
			})
			return err
		})
		if !mdb.TestBreakerReserveHalfOpen(0) {
			t.Error("tx panic leaked A's half-open probe slot")
		}
	})
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

// (Batch successes resetting the failover escalation chain is covered
// white-box in multidb_pipeline_internal_test.go: an executed batch success
// sets successSinceFailover, a hook-served one does not.)

// (An executed batch keeping its successful prefix — one recorded failure,
// prefix left unstamped — is covered white-box in
// multidb_pipeline_internal_test.go, where the execution marker can be set
// directly.)

func TestMultiDBBatchRejectsHImport(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	ctx := context.Background()

	// The direct HImport* overrides reject the family, but pipeline and tx
	// builders dispatch through their own cmdable — the batch paths must
	// reject HIMPORT commands too, or registrations would land in a single
	// member's registry and break after failover. In plain pipelines only
	// the HIMPORT command itself fails (see the poisoning test below); a
	// transaction is atomic, so the whole tx is rejected.
	cmds, _ := mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.HImportPrepare(ctx, "fs", "f1")
		p.Set(ctx, "k", "v", 0)
		return nil
	})
	if len(cmds) != 2 {
		t.Fatalf("got %d cmds, want 2", len(cmds))
	}
	if cmds[0].Err() == nil {
		t.Error("pipelined HIMPORT command not rejected")
	}
	if err := cmds[1].Err(); err != nil {
		t.Errorf("innocent pipelined command rejected: %v", err)
	}

	if _, err := mdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HImportSet(ctx, "k", "fs", "v")
		return nil
	}); err == nil {
		t.Error("TxPipelined with HIMPORT succeeded, want rejection")
	}
}

// nilStampAbortHook aborts before next but stamps redis.Nil (a well-formed
// server reply sentinel) on the last command — e.g. a cache hook answering a
// read locally while failing the rest of the batch.
type nilStampAbortHook struct{}

func (nilStampAbortHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (nilStampAbortHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (nilStampAbortHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) > 0 {
			cmds[len(cmds)-1].SetErr(redis.Nil)
		}
		return errors.New("aborted by hook")
	}
}

func TestMultiDBUnexecutedBatchRecordsNoSuccesses(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one fabricated success would close the circuit
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
	if err := mdb.AddDatabaseHook(0, nilStampAbortHook{}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// Half-open member; the hook aborts before execution but stamped
	// redis.Nil (success-class) on one command. Nothing reached Redis, so
	// no database success may be recorded off hook-fabricated replies.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)

	_, _ = mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Get(context.Background(), "k1")
		p.Set(context.Background(), "k2", "v", 0)
		return nil
	})

	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Fatal("expected the breaker to still be half-open with its slot free")
	}
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("hook-fabricated reply was recorded as a database success (circuit closed or slot lost)")
	}
}

func TestMultiDBHookServedBatchIsNeutral(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one recorded success would close the circuit
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// Half-open member; the default test hook serves batches locally
	// (returns nil without calling next). The caller gets its results, but
	// nothing reached Redis — recording successes would close the breaker
	// off pure client-side activity.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)

	if _, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	}); err != nil {
		t.Fatalf("Pipelined: %v", err)
	}

	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Fatal("expected the breaker to still be half-open with its slot free")
	}
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("hook-served batch was recorded on the breaker (circuit closed or slot lost)")
	}
}

// partialNeutralAbortHook aborts before next with a client-side error,
// stamping only the LAST command — the worst-case partially-stamped abort.
type partialNeutralAbortHook struct{}

func (partialNeutralAbortHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (partialNeutralAbortHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (partialNeutralAbortHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := errors.New("aborted by hook")
		if len(cmds) > 0 {
			cmds[len(cmds)-1].SetErr(err)
		}
		return err
	}
}

func TestMultiDBPartiallyStampedAbortDoesNotFabricateSuccesses(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CommandRetries = redis.CommandRetriesNone
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one fabricated success would close the circuit
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
	if err := mdb.AddDatabaseHook(0, partialNeutralAbortHook{}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// Half-open member; the hook aborts before execution but stamped one of
	// the two commands. The other command's nil error is NOT a success —
	// nothing executed — so the breaker must stay half-open.
	mdb.TestBreakerRecordFailure(0)
	time.Sleep(50 * time.Millisecond)

	cmds, _ := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k1", "v", 0)
		p.Set(context.Background(), "k2", "v", 0)
		return nil
	})

	if !mdb.TestBreakerReserveHalfOpen(0) {
		t.Fatal("expected the breaker to still be half-open with its slot free")
	}
	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("partially-stamped abort fabricated a success (circuit closed or slot lost)")
	}
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			t.Errorf("cmd %d left without an error after an aborted batch", i)
		}
	}
}

func TestMultiDBBatchHImportDoesNotPoisonOtherCommands(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	ctx := context.Background()

	// An unsupported HIMPORT command in a batch (e.g. coalesced by the
	// autopipeliner with unrelated callers' commands) must fail ONLY itself:
	// poisoning the whole flush would fail innocent commands that merely
	// shared the batch.
	cmds, _ := mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k1", "v", 0)
		p.HImportPrepare(ctx, "fs", "f1")
		p.Set(ctx, "k2", "v", 0)
		return nil
	})
	if len(cmds) != 3 {
		t.Fatalf("got %d cmds, want 3", len(cmds))
	}
	if err := cmds[0].Err(); err != nil {
		t.Errorf("innocent cmd 0 poisoned by the HIMPORT rejection: %v", err)
	}
	if err := cmds[1].Err(); err == nil {
		t.Error("HIMPORT command not rejected")
	}
	if err := cmds[2].Err(); err != nil {
		t.Errorf("innocent cmd 2 poisoned by the HIMPORT rejection: %v", err)
	}
}

func TestMultiDBBatchEarlyExitKeepsPositionalFirstError(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	mdb := newTestMultiDB(t, baseOptions(), dbA)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// A rejected HIMPORT followed by a command whose gate exits early (the
	// context is already canceled, so the batch never executes): Exec must
	// still report the POSITIONALLY first error over the original slice —
	// the HIMPORT rejection — exactly like the executed path does.
	cmds, err := mdb.Pipelined(canceled, func(p redis.Pipeliner) error {
		p.HImportPrepare(canceled, "fs", "f1")
		p.Set(canceled, "k", "v", 0)
		return nil
	})
	if len(cmds) != 2 {
		t.Fatalf("got %d cmds, want 2", len(cmds))
	}
	if want := cmds[0].Err(); want == nil || !errors.Is(err, want) {
		t.Errorf("Exec = %v, want the positionally first error %v", err, want)
	}
	if !errors.Is(cmds[1].Err(), context.Canceled) {
		t.Errorf("cmd 1 err = %v, want context.Canceled", cmds[1].Err())
	}
}

func TestMultiDBTxRetriesAnotherMemberWhenHalfOpenFull(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one probe slot
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// B: half-open with its only probe slot taken; A: healthy and closed.
	// A detector-tripped transaction fails over to B, is denied a probe
	// slot — and must then fail over again (back to closed A) rather than
	// reject the transaction: no MULTI/EXEC was sent yet, so another
	// failover cannot violate at-most-once.
	mdb.TestBreakerRecordFailure(1)
	time.Sleep(50 * time.Millisecond)
	if !mdb.TestBreakerReserveHalfOpen(1) {
		t.Fatal("setup: expected to reserve B's probe slot")
	}

	det.tripped.Store(true)
	if _, err := mdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	}); err != nil {
		t.Fatalf("TxPipelined = %v, want success on the closed member", err)
	}
	if got := dbA.hook.batches.Load(); got != 1 {
		t.Errorf("batches on A = %d, want 1 (transaction should land back on the closed member)", got)
	}
	if got := dbB.hook.batches.Load(); got != 0 {
		t.Errorf("batches on B = %d, want 0 (no probe slot available)", got)
	}
}

func TestMultiDBWatchRegatesAfterFailover(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	det := &fakeDetector{}
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one probe slot
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// B: half-open with its only probe slot taken; A: healthy and closed.
	// A detector-tripped Watch fails over to B, is denied a probe slot — and
	// must then fail over again (back to closed A) like the batch gates do,
	// rather than reject the transaction while a healthy member is still
	// selectable: no WATCH was sent yet, so another failover is safe.
	mdb.TestBreakerRecordFailure(1)
	time.Sleep(50 * time.Millisecond)
	if !mdb.TestBreakerReserveHalfOpen(1) {
		t.Fatal("setup: expected to reserve B's probe slot")
	}

	det.tripped.Store(true)
	if err := mdb.Watch(context.Background(), func(tx *redis.Tx) error {
		return nil
	}, "k"); err != nil {
		t.Fatalf("Watch = %v, want success on the closed member", err)
	}
	if got := dbA.hook.commands.Load(); got == 0 {
		t.Error("no commands on A — Watch should land back on the closed member")
	}
	if got := dbB.hook.commands.Load(); got != 0 {
		t.Errorf("commands on B = %d, want 0 (no probe slot available)", got)
	}
}

func TestMultiDBWatchDoesNotReleaseUnreservedSlot(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1, // one bounded half-open probe slot
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA)

	// Watch is admitted while A's breaker is CLOSED — no probe slot is
	// reserved. While the transaction is open, other traffic opens the
	// breaker, the grace period moves it to half-open, and a recovery probe
	// takes the only slot. Watch's deferred release must not free that
	// probe's slot: it never reserved one.
	err := mdb.Watch(context.Background(), func(tx *redis.Tx) error {
		mdb.TestBreakerRecordFailure(0)
		time.Sleep(50 * time.Millisecond)
		if !mdb.TestBreakerReserveHalfOpen(0) {
			t.Fatal("setup: expected to reserve the half-open probe slot")
		}
		return nil
	}, "k")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a second half-open reservation succeeded — Watch released a slot it never reserved")
	}
}

// midflightBatchHook runs an armable callback inside batch execution and
// then serves the batch locally (nil error, no wire I/O).
type midflightBatchHook struct{ fn atomic.Pointer[func()] }

func (h *midflightBatchHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *midflightBatchHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { return nil }
}

func (h *midflightBatchHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if f := h.fn.Load(); f != nil {
			(*f)()
		}
		return nil
	}
}

func TestMultiDBBatchClosedAdmissionDoesNotFreeProbeSlot(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 2, // MaxHalfOpenRequests defaults to this: two probe slots
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
	hook := &midflightBatchHook{}
	if err := mdb.AddDatabaseHook(0, hook); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	// The batch is admitted while A's breaker is CLOSED — no probe slot is
	// reserved. While it executes, a failure opens the breaker, the grace
	// period elapses, and recovery probes reserve both half-open slots. The
	// hook serves the batch locally (nil, no wire I/O), which lands in the
	// unexecuted-clean settle path — it must not free a probe's slot.
	fn := func() {
		mdb.TestBreakerRecordFailure(0)
		time.Sleep(50 * time.Millisecond)
		if !mdb.TestBreakerReserveHalfOpen(0) || !mdb.TestBreakerReserveHalfOpen(0) {
			t.Error("setup: expected to reserve both half-open probe slots")
		}
	}
	hook.fn.Store(&fn)
	if _, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	}); err != nil {
		t.Fatalf("Pipelined: %v", err)
	}
	hook.fn.Store(nil)

	if mdb.TestBreakerReserveHalfOpen(0) {
		t.Error("a second half-open reservation succeeded — the batch released a slot it never reserved")
	}
}

func TestMultiDBBatchAllHalfOpenFullReturnsUnavailable(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 2, true)
	dbB := newTestDB("b", "127.0.0.1:2", 1, true)
	opts := baseOptions()
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		GracePeriod:      30 * time.Millisecond,
	}
	mdb := newTestMultiDB(t, opts, dbA, dbB)

	// Every member half-open with an exhausted probe budget: the batch gate
	// must surface ErrTemporarilyNotAvailable, not busy-loop failovers.
	mdb.TestBreakerRecordFailure(0)
	mdb.TestBreakerRecordFailure(1)
	time.Sleep(50 * time.Millisecond)
	if !mdb.TestBreakerReserveHalfOpen(0) || !mdb.TestBreakerReserveHalfOpen(1) {
		t.Fatal("setup: expected to reserve both members' probe slots")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k", "v", 0)
		return nil
	})
	if !errors.Is(err, redis.ErrTemporarilyNotAvailable) {
		t.Fatalf("Pipelined = %v, want ErrTemporarilyNotAvailable", err)
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

func TestMultiDBPipelineCallerCancelPreservesCommandResults(t *testing.T) {
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

	// The hook fails at transport level (io.EOF) and cancels the caller's
	// context. The batch surfaces the cancellation as its aggregate error, but
	// each command must keep the outcome it actually received — not be
	// rewritten to context.Canceled. Overwriting would hide which operations
	// completed (an applied INCR reported as canceled would prompt a replay),
	// so the pipeline preserves command state on caller cancellation, mirroring
	// the single-command and tx paths.
	cmds, err := mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k", "v", 0)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pipelined err = %v, want context.Canceled", err)
	}
	for i, cmd := range cmds {
		if !errors.Is(cmd.Err(), io.EOF) {
			t.Errorf("cmd %d error = %v, want io.EOF (command result preserved, not overwritten)", i, cmd.Err())
		}
	}
}

// TestMultiDBPipelineCallerCancelDoesNotPoisonBreaker is the regression test
// for the canceled-batch finding: when the caller's own context ends during
// batch execution, the failure is a client-side signal, not a database health
// verdict, so the pipeline must record nothing on the breaker or the detector
// (mirroring the single-command path's post-exec ctx.Err guard). FailureThreshold
// is 1, so any recorded failure would open the breaker.
func TestMultiDBPipelineCallerCancelDoesNotPoisonBreaker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	det := &fakeDetector{}
	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.FailureDetector = det
	opts.CommandRetries = 2
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1,
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

	_, err = mdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k", "v", 0)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pipelined err = %v, want context.Canceled", err)
	}
	if got := det.failures.Load(); got != 0 {
		t.Errorf("caller cancellation recorded %d detector failures, want 0 "+
			"(a client-side cancel must not poison the breaker/detector)", got)
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
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Errorf("canceled operations switched the active database: active = %d", got)
	}
}

// switchThenFailPipelineHook is the pipeline analogue of switchThenFailHook: on
// its first batch it switches the active database and then fails that batch, so
// the batch outcome is recorded against a member that is no longer the active.
type switchThenFailPipelineHook struct {
	mdb  *redis.MultiDBClient
	to   int
	once sync.Once
}

func (*switchThenFailPipelineHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*switchThenFailPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *switchThenFailPipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.once.Do(func() { _ = h.mdb.ForceActiveDatabase(context.Background(), h.to) })
		for _, cmd := range cmds {
			cmd.SetErr(io.EOF)
		}
		return io.EOF
	}
}

// TestMultiDBPipelineStaleOutcomeNotRecordedOnDetector is the batch-path
// counterpart of TestMultiDBStaleOutcomeNotRecordedOnDetector: a batch whose
// member is no longer the active by the time its outcome is recorded must not
// feed the global failover detector. Otherwise a large batch failing on the
// vacated member could immediately trip failover away from the healthy
// replacement. The per-member breaker is still updated — that is member-scoped.
func TestMultiDBPipelineStaleOutcomeNotRecordedOnDetector(t *testing.T) {
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

	if err := mdb.AddDatabaseHook(0, &switchThenFailPipelineHook{mdb: mdb, to: 1}); err != nil {
		t.Fatalf("AddDatabaseHook(0): %v", err)
	}
	if err := mdb.AddDatabaseHook(1, db2.hook); err != nil {
		t.Fatalf("AddDatabaseHook(1): %v", err)
	}

	_, _ = mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Get(context.Background(), "k")
		return nil
	})

	// db1's batch failure was recorded AFTER the hook switched the active to
	// db2, so the global detector must not have counted it against db2's window.
	if got := det.failures.Load(); got != 0 {
		t.Errorf("stale batch outcome from the old active polluted the detector: failures=%d, want 0", got)
	}
}

// TestMultiDBConcurrentCloseSerializes checks that concurrent Close calls are
// serialized through a single drain-then-teardown: a second caller must not
// race ahead to close the member clients while the first is still draining the
// autopipeliner. Run under -race, this guards the ordering (and the shared
// close result) the sync.Once in Close provides.
// TestMultiDBPipelineMaxIntRetriesDoesNotDropBatch pins the overflow guard:
// CommandRetries == math.MaxInt must not make attempts = MaxInt+1 wrap to a
// negative count, which would skip the loop and silently drop the batch
// (Pipelined returning a nil error with nothing executed).
func TestMultiDBPipelineMaxIntRetriesDoesNotDropBatch(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.CommandRetries = math.MaxInt
	mdb := newTestMultiDB(t, opts, dbA)

	if _, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k", "v", 0)
		return nil
	}); err != nil {
		t.Fatalf("Pipelined: %v", err)
	}
	if got := dbA.hook.batches.Load(); got == 0 {
		t.Fatal("batch was silently dropped — attempts overflowed to a negative count")
	}
}

func TestMultiDBConcurrentCloseSerializes(t *testing.T) {
	check := newFakeHealthCheck(true)
	opts := baseOptions()
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
	// Prime the cached autopipeliner so Close has a drain to run.
	if _, err := mdb.AutoPipeline(); err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = mdb.Close()
		}(i)
	}
	wg.Wait()

	// Every caller returns the one close's result; none observed a torn teardown.
	for i, e := range errs {
		if e != errs[0] {
			t.Errorf("Close #%d = %v, want same result as #0 (%v)", i, e, errs[0])
		}
	}
}

// TestMultiDBTxPipelineCallerCancelReturnsCancellation pins that a TxPipeline
// interrupted by the caller's own context surfaces the cancellation as its
// aggregate error (like the non-tx pipeline path), not the underlying transport
// error — so a caller does not mistake a deliberate cancel for a retryable
// disconnect.
func TestMultiDBTxPipelineCallerCancelReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	check := newFakeHealthCheck(true)
	opts := baseOptions()
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 10,
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

	// The hook fails at transport level (io.EOF) and cancels the caller's
	// context mid-transaction.
	_, err = mdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "k", "v", 0)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TxPipelined caller-cancel err = %v, want context.Canceled (aggregate, not the transport error)", err)
	}
}

// TestMultiDBPipelineRetryExitPreservesPriorAttemptResults pins that a terminal
// exit of the retry loop after a prior attempt has run does NOT reset+restamp
// the commands with the aggregate error: attempt 0 fails at transport level and
// opens the member's breaker, so attempt 1 is gate-rejected into an
// availability error, but the per-command results attempt 0 produced must
// survive (the same preserve-on-cancel reasoning applied to every retry-loop
// exit). A server-free fake cannot produce an executed success, so this asserts
// attempt 0's transport error survives instead of being overwritten.
func TestMultiDBPipelineRetryExitPreservesPriorAttemptResults(t *testing.T) {
	dbA := newTestDB("a", "127.0.0.1:1", 1, true)
	dbA.hook.fail.Store(true) // every batch fails at transport level (wrapped io.EOF)
	opts := baseOptions()
	opts.CommandRetries = 1
	opts.CircuitBreakerConfig = &redis.MultiDBCircuitBreakerConfig{
		FailureThreshold: 1, // attempt 0's failure opens the breaker
		SuccessThreshold: 1,
		GracePeriod:      time.Hour, // stays open, so attempt 1's gate is rejected
	}
	mdb := newTestMultiDB(t, opts, dbA)

	cmds, err := mdb.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		p.Set(context.Background(), "k0", "v", 0)
		p.Get(context.Background(), "k1")
		return nil
	})
	if err == nil {
		t.Fatal("expected an availability error after the member's breaker opened")
	}
	// attempt 1 exits via the gate (breaker open) with attempt > 0. The commands
	// must still carry attempt 0's transport error, not be reset and overwritten
	// with the aggregate availability error.
	for i, cmd := range cmds {
		if !errors.Is(cmd.Err(), io.EOF) {
			t.Errorf("cmds[%d] err = %v, want the preserved transport error (io.EOF)", i, cmd.Err())
		}
	}
}

// blockingPipelineHook holds a batch dispatch open: it closes entered once the
// hook is reached, then waits on release before returning. It never calls
// next, so it must be the only hook on the member (installed without
// installHooks, which would put a terminal hookedDB ahead of it in the FIFO
// chain and make it unreachable).
type blockingPipelineHook struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingPipelineHook() *blockingPipelineHook {
	return &blockingPipelineHook{entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *blockingPipelineHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *blockingPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *blockingPipelineHook) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.once.Do(func() { close(h.entered) })
		<-h.release
		return nil
	}
}

// TestMultiDBCloseWaitsForAutopipelinerDrain pins the AutoPipeliner.Close /
// WaitClosed contract at the MultiDBClient boundary. AutoPipeliner.Close
// returns immediately, without draining, when something else already claimed
// the shutdown (its own doc: a batch hook calling Close on its own executor
// goroutine). If MultiDBClient.Close only called Close and not WaitClosed, it
// would then tear down the member pool while that other drain was still
// writing to it. Here a direct external Close call stands in for that other
// claimant: it wins the CAS and blocks in the drain (a batch dispatch is held
// open), so MultiDBClient.Close's own Close call loses the CAS and returns
// nil immediately. MultiDBClient.Close must still block until the drain
// finishes.
func TestMultiDBCloseWaitsForAutopipelinerDrain(t *testing.T) {
	db1 := newTestDB("db1", "127.0.0.1:1", 1, true)
	opts := baseOptions()
	opts.Clients = append(opts.Clients, db1.cfg)
	mdb, err := redis.NewMultiDBClient(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()

	block := newBlockingPipelineHook()
	if err := mdb.AddDatabaseHook(0, block); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}

	ap, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}

	// Hold a batch dispatch open in the hook.
	setDone := make(chan struct{})
	go func() {
		_ = ap.Set(context.Background(), "k", "v", 0).Err()
		close(setDone)
	}()
	select {
	case <-block.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("batch dispatch never reached the blocking hook")
	}

	// Win the AutoPipeliner's shutdown CAS directly and block in its drain
	// (the dispatch above is still stuck in the hook).
	apCloseDone := make(chan error, 1)
	go func() { apCloseDone <- ap.Close() }()
	// Wait for that Close to actually claim the shutdown, rather than assuming
	// it gets scheduled inside a fixed window. IsClosed reports the flag the
	// shutdown CAS sets, so once it is true, MultiDBClient.Close's own Close
	// call is guaranteed to LOSE the CAS — which is the path under test. If
	// MultiDBClient.Close won it instead, its p.Close() would block on its own
	// drain and the test would pass even without the WaitClosed wiring.
	claimed := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if ap.IsClosed() {
			claimed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !claimed {
		t.Fatal("the direct AutoPipeliner.Close never claimed the shutdown")
	}

	// MultiDBClient.Close's own Close call on this autopipeliner now loses the
	// CAS and returns nil immediately; it must still wait for the winning
	// drain via WaitClosed before tearing down the member pool.
	mdbCloseDone := make(chan error, 1)
	go func() { mdbCloseDone <- mdb.Close() }()

	select {
	case err := <-mdbCloseDone:
		t.Fatalf("MultiDBClient.Close returned (err=%v) while the AutoPipeliner drain was still stuck — it did not wait for WaitClosed", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(block.release) // let the stuck dispatch, and both drains, finish

	select {
	case err := <-apCloseDone:
		if err != nil {
			t.Errorf("AutoPipeliner.Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AutoPipeliner.Close never returned after the block was released")
	}
	select {
	case err := <-mdbCloseDone:
		if err != nil {
			t.Errorf("MultiDBClient.Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MultiDBClient.Close did not unblock after the AutoPipeliner drain finished")
	}
	select {
	case <-setDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the blocked ap.Set call never returned")
	}
}
