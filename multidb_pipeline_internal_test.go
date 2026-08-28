package redis

import (
	"context"
	"io"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/proto"
)

// execAll marks every command as executed — the per-command marker a fully
// executed batch produces (see executedCmds / markPipelineExecuted).
func execAll(cmds []Cmder) *executedCmds {
	ec := newExecutedCmds(len(cmds))
	ec.mark(cmds)
	return ec
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
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{}); got != 0 {
		t.Errorf("transportFailures = %d for an executed all-success batch, want 0", got)
	}
	if err := cmds[0].Err(); err != nil {
		t.Errorf("executed batch had its successful command stamped with %v", err)
	}

	// Not executed (hook aborted before next): the batch error stands in
	// for the commands and is stamped so callers see it.
	resetCmds(cmds)
	if got := core.recordBatchOutcomes(db, cmds, io.EOF, newExecutedCmds(0), imultidb.Reservation{}); got != 1 {
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

	if got := core.recordBatchOutcomes(db, cmds, loading, execAll(cmds), imultidb.Reservation{}); got != 1 {
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
	core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), res)

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
	core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{})

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
	core.recordBatchOutcomes(db, cmds, nil, execAll(cmds), imultidb.Reservation{})
	if !core.successSinceFailover.Load() {
		t.Error("executed batch success did not mark recovery traffic")
	}

	// A hook-served batch (nil without execution) is not.
	core.successSinceFailover.Store(false)
	resetCmds(cmds)
	core.recordBatchOutcomes(db, cmds, nil, newExecutedCmds(0), imultidb.Reservation{})
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

	core.recordBatchOutcomes(db, cmds, nil, ec, imultidb.Reservation{})

	if det.successes != 1 {
		t.Errorf("detector successes = %d, want 1 (an untouched command must not count as a success)", det.successes)
	}
	if det.failures != 0 {
		t.Errorf("detector failures = %d, want 0 (an untouched command must not count at all)", det.failures)
	}
}
