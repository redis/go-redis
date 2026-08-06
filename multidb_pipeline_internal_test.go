package redis

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/proto"
)

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
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, true); got != 0 {
		t.Errorf("transportFailures = %d for an executed all-success batch, want 0", got)
	}
	if err := cmds[0].Err(); err != nil {
		t.Errorf("executed batch had its successful command stamped with %v", err)
	}

	// Not executed (hook aborted before next): the batch error stands in
	// for the commands and is stamped so callers see it.
	resetCmds(cmds)
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, false); got != 1 {
		t.Errorf("transportFailures = %d for an unexecuted batch, want 1", got)
	}
	if err := cmds[0].Err(); err == nil {
		t.Error("unexecuted batch left the command unstamped")
	}
}

func TestMarkPipelineExecuted(t *testing.T) {
	flag := new(atomic.Bool)
	markPipelineExecuted(context.WithValue(context.Background(), pipelineExecutedKey{}, flag))
	if !flag.Load() {
		t.Error("marker did not flip the flag")
	}
	// Without a marker in the context it must be a no-op, not a panic.
	markPipelineExecuted(context.Background())
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

	if got := core.recordBatchOutcomes(db, cmds, loading, true); got != 1 {
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
	core.recordBatchOutcomes(db, cmds, io.EOF, true)

	if got := db.cb.State(); got != imultidb.CircuitOpen {
		t.Errorf("breaker state = %v after a failed recovery batch, want open", got)
	}
}

func TestRecordBatchOutcomesSuccessSinceFailover(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
	})}

	// An executed batch success is recovery traffic: it breaks the
	// consecutive-failed-failover escalation chain.
	cmds := []Cmder{NewStatusCmd(context.Background(), "set", "k", "v")}
	core.recordBatchOutcomes(db, cmds, nil, true)
	if !core.successSinceFailover.Load() {
		t.Error("executed batch success did not mark recovery traffic")
	}

	// A hook-served batch (nil without execution) is not.
	core.successSinceFailover.Store(false)
	resetCmds(cmds)
	core.recordBatchOutcomes(db, cmds, nil, false)
	if core.successSinceFailover.Load() {
		t.Error("hook-served batch counted as recovery traffic")
	}
}
