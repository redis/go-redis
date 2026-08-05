package redis_test

import (
	"context"
	"errors"
	"io"
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
