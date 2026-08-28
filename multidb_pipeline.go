package redis

import (
	"context"
	"errors"
	"math"
	"sync/atomic"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// This file makes MultiDBClient a full UniversalClient and wires pipelines
// and autopipelining through the MultiDB core, per the MultiDB × AutoPipeline
// design: batches resolve the active database at exec time, feed the circuit
// breaker and failure detector per command, and are retried against the newly
// selected database after a failover.

var _ UniversalClient = (*MultiDBClient)(nil)

// resetCmds clears per-command errors from a previous attempt so a retried
// batch does not report stale failures.
func resetCmds(cmds []Cmder) {
	for _, cmd := range cmds {
		cmd.SetErr(nil)
	}
}

// recordBatchOutcomes records one breaker/detector outcome per command,
// mirroring the single-command path: an error reply from the server proves
// the database is reachable and counts as a success; transport-level errors
// count as failures; client-side errors (context cancellation, deterministic
// local rejections per shouldRetry) record nothing and do not trigger
// failover. It returns the number of transport-level failures.
//
// Errors are read via rawErr, never Err: this runs on the pipeline execution
// path (including the autopipeliner's batch dispatcher, before the batch's
// done channel closes), where Err on an async command would await the very
// batch being completed and wedge the dispatcher.
func (c *multidbCore) recordBatchOutcomes(db *multidbDatabase, cmds []Cmder, batchErr error, executed bool, res imultidb.Reservation) int {
	// `executed` comes from the execution marker, not from inspecting
	// command state: an executed all-success batch whose error was injected
	// by a post-exec hook looks identical on the commands, and stamping it
	// would turn already-applied writes into phantom transport failures and
	// replay them. `res` is the gate admission's half-open reservation: the
	// whole batch is ONE admission, so RecordSuccessFor / ReleaseFor settle the
	// breaker's half-open slot exactly once however many commands succeed, and a
	// closed-state admission (res.held == false) holds no slot to settle.
	if !executed {
		if batchErr == nil {
			// A member hook served the batch locally (returned nil without
			// calling next): the results are valid for the caller, but
			// nothing reached Redis — record no health signal and give the
			// gate's probe slot back (ReleaseFor no-ops for a closed admission).
			db.cb.ReleaseFor(res)
			return 0
		}
		// Execution never started: the batch error stands in for every
		// command the aborting hook did not stamp itself (setCmdsErr fills
		// only empty slots), so a partially-stamped abort cannot fabricate
		// successes for the untouched commands.
		setCmdsErr(cmds, batchErr)
	}
	// Blocking commands carry their own read timeout; their local deadlines
	// are not retryable transport failures — the same per-command rule the
	// single-command path applies (cmd.readTimeout() == nil).
	classify := func(cmd Cmder) outcomeKind {
		return classifyOutcome(cmd.rawErr(), cmd.readTimeout() == nil)
	}
	// Feed the GLOBAL detector only while this batch's member is still the
	// active: if the active switched (or this member was removed) while the
	// batch was in flight, recording its outcomes would pollute the new
	// active's failover window and successSinceFailover. The per-member breaker
	// below is always updated — it is member-scoped. Mirrors the single-command
	// path (see process). One snapshot suffices: recordBatchOutcomes runs
	// synchronously at the end of a single attempt, the batch analogue of the
	// single-command post-exec check.
	cur, _ := c.activeSnapshot()
	stillActive := cur == db
	transportFailures := 0
	recorded := 0
	recordOne := func(cmd Cmder) {
		switch classify(cmd) {
		case outcomeSuccess:
			if !executed {
				// A nil (or reply-class) error fabricated by an aborting
				// hook is no proof the database answered: surface it to the
				// caller, record nothing.
				return
			}
			// Settle the batch's single reservation: RecordSuccessFor releases
			// (or, at SuccessThreshold, closes on) the half-open slot at most
			// once however many commands succeed, and records an external
			// success for a closed admission (res.held == false).
			db.cb.RecordSuccessFor(res)
			if stillActive {
				c.detector.RecordSuccess()
				// Recovery traffic breaks the consecutive-failed-failover chain
				// for batch-only workloads too (see recordFailedFailoverLocked).
				c.successSinceFailover.Store(true)
			}
			recorded++
		case outcomeFailure:
			// Failures record regardless of the execution marker — unlike
			// successes: a dial failure (the canonical down-member signal)
			// happens BEFORE the marker flips, and it is indistinguishable
			// from a pre-execution hook abort. Guarding failures on the
			// marker would leave breakers closed through real outages on
			// batch-only workloads. Successes are the asymmetric case: a
			// genuine reply cannot exist without execution.
			db.cb.RecordFailure()
			if stillActive {
				c.detector.RecordFailure(cmd.rawErr())
			}
			transportFailures++
			recorded++
		case outcomeNeutral:
			// Not a database-health signal (client-side error or a locally
			// synthesized Redis error such as ErrCrossSlot); surfaced to the
			// caller as-is.
		}
	}
	if db.cb.State() == imultidb.CircuitHalfOpen {
		// Recovery probe: record failures BEFORE successes, so a batch that
		// ultimately failed cannot close the circuit off its own successful
		// prefix. Everywhere else the arrival order stands — mirroring
		// sequential single commands, where a success resets the closed
		// state's failure count before a later failure increments it.
		for _, cmd := range cmds {
			if classify(cmd) == outcomeFailure {
				recordOne(cmd)
			}
		}
		for _, cmd := range cmds {
			if classify(cmd) != outcomeFailure {
				recordOne(cmd)
			}
		}
	} else {
		for _, cmd := range cmds {
			recordOne(cmd)
		}
	}
	if recorded == 0 {
		// The whole batch was neutral (e.g. a local ErrCrossSlot rejection):
		// nothing was recorded on the breaker, so give back the half-open
		// probe slot the admission reserved (ReleaseFor no-ops otherwise).
		db.cb.ReleaseFor(res)
	}
	return transportFailures
}

// errMultiDBHImportCmd matches the HIMPORT command family, which is rejected
// on MultiDBClient (see errMultiDBHImport): the direct methods are overridden,
// and the batch paths below must reject them too — pipeline builders dispatch
// through their own cmdable and would otherwise hand the command to a single
// member's registry.
func cmdsContainHImport(cmds []Cmder) bool {
	for _, cmd := range cmds {
		if isHImportCmd(cmd) {
			return true
		}
	}
	return false
}

// processPipeline is the batch analogue of process: an attempt loop bounded
// by CommandRetries that snapshots the active database, executes the whole
// batch through the member's hook-wrapped pipeline path, records per-command
// outcomes, and retries the batch against the newly selected database after
// a failover. Delivery is at-least-once when retries are enabled: a
// connection that broke mid-pipeline may have executed a prefix server-side.
func (c *multidbCore) processPipeline(ctx context.Context, cmds []Cmder) error {
	if len(cmds) == 0 {
		return nil
	}
	if c.closed.Load() {
		setCmdsErr(cmds, ErrClosed)
		return ErrClosed
	}
	// orig keeps the caller's slice for the final error: rejected HIMPORT
	// commands stay in it, so cmdsFirstErr reports the POSITIONALLY first
	// failure like Pipeline.Exec would.
	orig := cmds
	himportRejected := false
	if cmdsContainHImport(cmds) {
		// Reject only the HIMPORT commands themselves: the autopipeliner
		// coalesces unrelated callers into one flush, and poisoning the
		// whole batch would fail innocent commands that merely shared it.
		kept := make([]Cmder, 0, len(cmds))
		for _, cmd := range cmds {
			if isHImportCmd(cmd) {
				cmd.SetErr(errMultiDBHImport)
				continue
			}
			kept = append(kept, cmd)
		}
		if len(kept) == 0 {
			return errMultiDBHImport
		}
		cmds = kept
		himportRejected = true
	}
	attempts := c.opts.CommandRetries + 1
	if c.opts.CommandRetries == math.MaxInt {
		// Guard the +1 against wrapping to a negative attempt count — which
		// would skip the loop and silently drop the batch (returning nil).
		// Mirrors the single-command path in process().
		attempts = math.MaxInt
	}
	// A batch containing a non-retryable command (e.g. a streaming
	// RawWriteToCmd) must be executed at most once: retrying it after a
	// transport failure could duplicate execution or corrupt a partial write.
	if cmdsContainNoRetry(cmds) {
		attempts = 1
	}

	attempt := 0
	// Bound consecutive gate rejections, mirroring the single-command path:
	// with every selectable member half-open and probe budgets exhausted,
	// failover keeps handing back members the gate rejects — surface
	// unavailability instead of busy-looping the active index.
	gateRejections := 0
	maxGateRejections := c.memberCount() + 1

	// exitErr picks the batch error for every exit: with a rejected HIMPORT
	// in the batch the positionally first error over the ORIGINAL slice wins,
	// like Pipeline.Exec reports it — including early gate exits, whose
	// stamped error would otherwise displace a rejection that precedes the
	// stamped commands positionally.
	exitErr := func(err error) error {
		if himportRejected {
			if ferr := cmdsFirstErr(orig); ferr != nil {
				return ferr
			}
		}
		return err
	}

	for attempt < attempts {
		if c.closed.Load() {
			// Close landed mid-retry: report the terminal state instead of
			// escalating through the drained membership.
			resetCmds(cmds)
			setCmdsErr(cmds, ErrClosed)
			return exitErr(ErrClosed)
		}
		if err := ctx.Err(); err != nil {
			// Overwrite any prior attempt's transport errors: callers that
			// inspect per-command results must see the context error, not a
			// stale EOF from the failed attempt (setCmdsErr fills only
			// empty slots).
			resetCmds(cmds)
			setCmdsErr(cmds, err)
			return exitErr(err)
		}

		// Detector before the breaker admission: a half-open admission
		// reserves a bounded probe slot, and a tripped detector routes to
		// failover without executing the batch — the reservation would leak.
		db, idx := c.activeSnapshot()
		admitted := false
		var res imultidb.Reservation
		if db != nil && !c.detector.ShouldFailover() {
			admitted, res = db.cb.AllowReserve()
		}
		if !admitted {
			gateRejections++
			if gateRejections > maxGateRejections {
				err := ErrTemporarilyNotAvailable
				resetCmds(cmds)
				setCmdsErr(cmds, err)
				return exitErr(err)
			}
			if err := c.tryFailover(ctx, idx); err != nil {
				// Overwrite any prior attempt's transport errors so callers
				// see the availability error, not a stale EOF (setCmdsErr
				// only fills empty error slots).
				resetCmds(cmds)
				setCmdsErr(cmds, err)
				return exitErr(err)
			}
			// Re-enter the gate on the newly selected database — mirroring
			// the single-command path: its breaker may be half-open and the
			// AllowReserve call above is what reserves the bounded probe slot.
			// Re-gating does not consume a retry attempt.
			continue
		}
		gateRejections = 0

		if attempt > 0 {
			resetCmds(cmds)
		}
		attempt++
		// The marker tells recordBatchOutcomes whether execution started
		// (fresh per attempt): command state alone cannot distinguish a
		// pre-execution hook abort from a post-execution hook error.
		executed := new(atomic.Bool)
		err := db.processPipelineHook(context.WithValue(ctx, pipelineExecutedKey{}, executed), cmds)
		if err != nil && ctx.Err() != nil {
			// The caller's own context ended (deadline/cancel) while the batch
			// ran — typically a dial cut short, surfacing as a net timeout the
			// classifier would otherwise score as a member failure. That is a
			// client-side signal, not a health verdict: release any half-open
			// reservation and return WITHOUT recording.
			db.cb.ReleaseFor(res)
			// Do NOT reset or overwrite the commands. The deadline may have
			// interrupted reading a LATER reply while earlier commands already
			// received definitive results, and those are the caller's only
			// evidence of what applied — an already-executed INCR must not
			// resurface as canceled and prompt a replay. The member pipeline
			// stamped the interrupted/unread commands itself. Unlike the
			// pre-execution gate checks above (which reset because nothing ran,
			// so no result can exist), execution here may have produced
			// results; this mirrors the tx and single-command paths, which also
			// return without touching command state on caller cancellation.
			return exitErr(ctx.Err())
		}
		transportFailures := c.recordBatchOutcomes(db, cmds, err, executed.Load(), res)

		if transportFailures == 0 {
			// Only server replies (or clean success) — done, whatever the
			// user-level outcome is.
			return exitErr(err)
		}
	}
	return cmdsFirstErr(orig)
}

// processTxPipeline executes a MULTI/EXEC pipeline against the active
// database exactly once: EXEC may have committed before a connection broke,
// so a blind retry could double-apply the transaction. Failures are still
// recorded so the breaker and detector can move traffic for subsequent
// operations.
func (c *multidbCore) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	if len(cmds) == 0 {
		return nil
	}
	if c.closed.Load() {
		setCmdsErr(cmds, ErrClosed)
		return ErrClosed
	}
	if cmdsContainHImport(cmds) {
		setCmdsErr(cmds, errMultiDBHImport)
		return errMultiDBHImport
	}
	// A context that is already done must not reach the failover gate: with
	// ProbeTargetBeforeFailover the doomed probes would damage candidate
	// breakers and advance the escalation state, and without it the active
	// database could be switched for an operation that cannot run anyway.
	if err := ctx.Err(); err != nil {
		setCmdsErr(cmds, err)
		return err
	}
	// The gate loops like the other paths (detector before IsAllowed; a
	// denied member triggers another failover, bounded by the rejection
	// cap) — only the EXECUTION below stays single-shot, so at-most-once
	// holds: no MULTI/EXEC has been sent while the gate is still choosing.
	gateRejections := 0
	maxGateRejections := c.memberCount() + 1
	var db *multidbDatabase
	var res imultidb.Reservation
	for {
		if c.closed.Load() {
			resetCmds(cmds)
			setCmdsErr(cmds, ErrClosed)
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			resetCmds(cmds)
			setCmdsErr(cmds, err)
			return err
		}
		var idx int
		db, idx = c.activeSnapshot()
		admitted := false
		if db != nil && !c.detector.ShouldFailover() {
			admitted, res = db.cb.AllowReserve()
		}
		if !admitted {
			gateRejections++
			if gateRejections > maxGateRejections {
				err := ErrTemporarilyNotAvailable
				resetCmds(cmds)
				setCmdsErr(cmds, err)
				return err
			}
			if err := c.tryFailover(ctx, idx); err != nil {
				resetCmds(cmds)
				setCmdsErr(cmds, err)
				return err
			}
			continue
		}
		break
	}

	executed := new(atomic.Bool)
	err := db.processTxPipelineHook(context.WithValue(ctx, pipelineExecutedKey{}, executed), cmds)
	if err != nil && ctx.Err() != nil {
		// Caller's context ended mid-transaction (see processPipeline): a
		// client-side signal, not a health verdict. Release any half-open
		// reservation and return without recording. Do NOT reset the commands
		// here — EXEC may have committed, and their state is the caller's only
		// evidence of that.
		db.cb.ReleaseFor(res)
		return err
	}
	// Record outcomes only for the user's commands: cmds arrives wrapped by
	// wrapMultiExec (MULTI ... EXEC), and counting the synthetic envelope
	// would advance the breaker by three per single-command transaction.
	user := cmds
	if len(cmds) >= 3 {
		user = cmds[1 : len(cmds)-1]
	}
	c.recordBatchOutcomes(db, user, err, executed.Load(), res)
	return err
}

func (db *multidbDatabase) processPipelineHook(ctx context.Context, cmds []Cmder) error {
	if db.cc != nil {
		return db.cc.processPipelineHook(ctx, cmds)
	}
	return db.c.processPipelineHook(ctx, cmds)
}

func (db *multidbDatabase) processTxPipelineHook(ctx context.Context, cmds []Cmder) error {
	if db.cc != nil {
		return db.cc.processTxPipelineHook(ctx, cmds)
	}
	return db.c.processTxPipelineHook(ctx, cmds)
}

// process / processPipeline are the unhooked base entry points required by
// the autopipeliner's backend interface (cmdableClient); the hook-wrapped
// variants come from hooksMixin.
func (c *MultiDBClient) process(ctx context.Context, cmd Cmder) error {
	return c.core.process(ctx, cmd)
}

// hookCount reports one more hook than are installed on the MultiDBClient
// itself: member clients can carry their own hooks (AddDatabaseHook), which
// the autopipeliner cannot see, and its async dispatcher only arms the
// batch-executor self-deadlock guards when hookCount is non-zero. Always
// reporting at least one keeps those guards armed for member-level hooks
// that read command results.
func (c *MultiDBClient) hookCount() int {
	return c.hooksMixin.hookCount() + 1
}

func (c *MultiDBClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.core.processPipeline(ctx, cmds)
}

// Do creates a Cmd from the args and routes it through Process.
func (c *MultiDBClient) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Pipeline returns a pipeline whose Exec routes the whole batch to the active
// database, with failover and retry semantics (see the MultiDB design).
func (c *MultiDBClient) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: pipelineExecer(c.processPipelineHook),
	}
	pipe.init()
	return &pipe
}

// Pipelined executes fn inside a Pipeline and returns the queued commands.
func (c *MultiDBClient) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

// TxPipeline returns a MULTI/EXEC pipeline against the active database.
// Transactions are executed at most once: they are never automatically
// retried on another database after a failure.
func (c *MultiDBClient) TxPipeline() Pipeliner {
	pipe := Pipeline{
		exec: func(ctx context.Context, cmds []Cmder) error {
			cmds = wrapMultiExec(ctx, cmds)
			// Suppress the member client's own retry loop for the wrapped
			// batch (cmdsContainNoRetry): EXEC may have committed before a
			// transport error surfaced, and at-most-once must hold for the
			// whole stack, not only the MultiDB retry layer. Every command
			// is marked — cluster members trim the MULTI/EXEC envelope
			// before their retry check, so a marker only on the synthetic
			// MULTI would be lost there. The prior NoRetry is restored after
			// execution: cmds are caller-owned and may be reused, and a plain
			// client's TxPipeline does not leave them permanently non-retryable.
			type noRetrier interface {
				setNoRetry(bool)
				NoRetry() bool
			}
			prev := make([]bool, len(cmds))
			for i, cmd := range cmds {
				if bc, ok := cmd.(noRetrier); ok {
					prev[i] = bc.NoRetry()
					bc.setNoRetry(true)
				}
			}
			err := c.processTxPipelineHook(ctx, cmds)
			for i, cmd := range cmds {
				if bc, ok := cmd.(noRetrier); ok {
					bc.setNoRetry(prev[i])
				}
			}
			return err
		},
	}
	pipe.init()
	return &pipe
}

// TxPipelined executes fn inside a TxPipeline.
func (c *MultiDBClient) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// Watch runs a WATCH/MULTI/EXEC transaction on the database that is active
// when Watch is called; if the active database's circuit is open or the
// failure detector has tripped, a failover is attempted first so the
// transaction does not start on a known-unhealthy member. The transaction is
// then bound to that member for its whole lifetime: it does NOT follow a
// MultiDB failover, its outcome does not feed the breaker or detector, and it
// is never automatically retried on another database. If the bound member
// fails while the transaction is open, the transaction errors like it would
// on a plain client; MultiDB moves only subsequent operations.
//
// MultiDB-level hooks (AddHook on MultiDBClient) do not wrap the transaction:
// the Tx runs on the member client, so install member-level hooks via
// AddDatabaseHook when WATCH traffic must be instrumented.
func (c *MultiDBClient) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	if c.core.closed.Load() {
		return ErrClosed
	}
	// A done context must not reach the failover gate (see processTxPipeline).
	if err := ctx.Err(); err != nil {
		return err
	}
	// The gate loops like the batch paths (detector before the breaker
	// admission; a denied member triggers another failover, bounded by the
	// rejection cap), so a half-open candidate with an exhausted probe budget
	// does not fail the call while a healthy member is still selectable — no
	// WATCH has been sent while the gate is still choosing. A half-open
	// admission reserves a probe slot, so MaxHalfOpenRequests bounds
	// concurrent WATCH transactions too; the slot is released after the call
	// because the WATCH outcome deliberately never records on the breaker.
	gateRejections := 0
	maxGateRejections := c.core.memberCount() + 1
	var db *multidbDatabase
	var res imultidb.Reservation
	for {
		if c.core.closed.Load() {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var idx int
		db, idx = c.core.activeSnapshot()
		admitted := false
		if db != nil && !c.core.detector.ShouldFailover() {
			admitted, res = db.cb.AllowReserve()
		}
		if !admitted {
			gateRejections++
			if gateRejections > maxGateRejections {
				return ErrTemporarilyNotAvailable
			}
			if err := c.core.tryFailover(ctx, idx); err != nil {
				return err
			}
			continue
		}
		break
	}
	// Release the reservation after the call: the WATCH outcome deliberately
	// never records on the breaker. ReleaseFor settles once and only within the
	// reservation's own half-open episode — so a closed-state admission (no
	// slot) is a no-op, and a transaction that outlives an open -> half-open
	// cycle cannot free a slot a real recovery probe is holding in the new one.
	defer db.cb.ReleaseFor(res)
	if db.cc != nil {
		return db.cc.Watch(ctx, fn, keys...)
	}
	return db.c.Watch(ctx, fn, keys...)
}

// SSubscribe subscribes to shard channels on the database that is active at
// call time. Unlike Subscribe/PSubscribe, sharded subscriptions do not follow
// the active database across failovers yet.
func (c *MultiDBClient) SSubscribe(ctx context.Context, channels ...string) *PubSub {
	// The closed check keeps a Close race from delegating to a member that
	// is being torn down: the MultiDB PubSub path below fails dials with
	// the terminal ErrClosed instead.
	if !c.core.closed.Load() {
		db, _ := c.core.activeSnapshot()
		if db != nil && db.cc != nil {
			return db.cc.SSubscribe(ctx, channels...)
		}
		if db != nil {
			return db.c.SSubscribe(ctx, channels...)
		}
	}
	// No active database (or closed): return a PubSub that fails to
	// connect — with the requested channels registered, like Subscribe and
	// PSubscribe, so the failure surfaces on use instead of silently
	// subscribing to nothing.
	pubsub := c.core.newPubSub()
	if len(channels) > 0 {
		_ = pubsub.SSubscribe(ctx, channels...)
	}
	return pubsub
}

// PoolStats returns the connection pool statistics of the active database.
func (c *MultiDBClient) PoolStats() *PoolStats {
	db, _ := c.core.activeSnapshot()
	if db == nil {
		return &PoolStats{}
	}
	if db.cc != nil {
		return db.cc.PoolStats()
	}
	return db.c.PoolStats()
}

// errMultiDBAutoPipelineCluster is returned when an autopipeliner is
// requested while a cluster member database is configured: the MultiDB
// autopipeliner would bypass the cluster-specific autopipeline wiring
// (command preflight, diversion and slot sharding), so cluster members are
// not supported yet.
var errMultiDBAutoPipelineCluster = errors.New("redis: multidb: autopipeline does not support cluster member databases yet")

func (c *multidbCore) hasClusterMember() bool {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	for _, db := range c.dbs {
		if db.cc != nil {
			return true
		}
	}
	return false
}

// AutoPipeline returns the blocking autopipeliner for this MultiDB client:
// batches are flushed against whichever database is active at exec time, and
// a batch that fails at transport level is retried against the newly selected
// database (bounded by CommandRetries). The instance is cached and shared; the
// first call's config wins.
//
// Cluster member databases are not supported yet (checked at call time): the
// cluster-specific autopipeline safeguards would be bypassed.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *MultiDBClient) AutoPipeline() (*AutoPipeliner, error) {
	return c.AutoPipelineWithOptions(nil)
}

// AutoPipelineWithOptions is AutoPipeline with explicit options.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *MultiDBClient) AutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	// The cluster-member check runs inside the build function, under
	// autopipelinerMu: AddDatabase performs its cluster-vs-autopipeliner
	// check under the same mutex, so the two cannot interleave.
	return getOrCreateAutoPipeliner(c.autopipelinerMu, &c.autopipeliner, &c.autopipelinerClosed, nil, config,
		DefaultBlockingAutoPipelineOptions,
		func(cfg *AutoPipelineOptions) (*AutoPipeliner, error) {
			if c.core.hasClusterMember() {
				return nil, errMultiDBAutoPipelineCluster
			}
			return newAutoPipeliner(c, cfg, true)
		})
}

// AsyncAutoPipeline returns the deferred autopipeliner for this MultiDB
// client (see AutoPipeline for the failover semantics).
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *MultiDBClient) AsyncAutoPipeline() (*AutoPipeliner, error) {
	return c.AsyncAutoPipelineWithOptions(nil)
}

// AsyncAutoPipelineWithOptions is AsyncAutoPipeline with explicit options.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *MultiDBClient) AsyncAutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	return getOrCreateAutoPipeliner(c.autopipelinerMu, &c.asyncAutopipeliner, &c.autopipelinerClosed, nil, config,
		DefaultAutoPipelineOptions,
		func(cfg *AutoPipelineOptions) (*AutoPipeliner, error) {
			if c.core.hasClusterMember() {
				return nil, errMultiDBAutoPipelineCluster
			}
			return newAutoPipeliner(c, cfg, false)
		})
}
