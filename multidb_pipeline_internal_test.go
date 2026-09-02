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

		got := core.recordBatchOutcomes(db, cmds, e, execAll(cmds), imultidb.Reservation{})
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

		if got := core.recordBatchOutcomes(db, cmds, e, execAll(cmds), imultidb.Reservation{}); got != 3 {
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

		if got := core.recordBatchOutcomes(db, cmds, origin, execAll(cmds), imultidb.Reservation{}); got != 2 {
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
		if got := core.recordBatchOutcomes(db, cmds, e, execAll(cmds), imultidb.Reservation{}); got != 2 {
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
		got := core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), stale)
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
		core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), cur)
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
		core.recordBatchOutcomes(db, cmds, io.EOF, execAll(cmds), imultidb.Reservation{})
		if st := db.cb.State(); st != imultidb.CircuitOpen {
			t.Errorf("breaker %v, want open", st)
		}
	})
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
