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
