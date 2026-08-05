package redis

import (
	"context"
	"errors"
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
func (c *multidbCore) recordBatchOutcomes(db *multidbDatabase, cmds []Cmder) int {
	transportFailures := 0
	for _, cmd := range cmds {
		err := cmd.rawErr()
		switch classifyOutcome(err) {
		case outcomeSuccess:
			db.cb.RecordSuccess()
			c.detector.RecordSuccess()
		case outcomeFailure:
			db.cb.RecordFailure()
			c.detector.RecordFailure(err)
			transportFailures++
		case outcomeNeutral:
			// Not a database-health signal (client-side error or a locally
			// synthesized Redis error such as ErrCrossSlot); surfaced to the
			// caller as-is.
		}
	}
	return transportFailures
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
	attempts := c.opts.CommandRetries + 1
	// A batch containing a non-retryable command (e.g. a streaming
	// RawWriteToCmd) must be executed at most once: retrying it after a
	// transport failure could duplicate execution or corrupt a partial write.
	if cmdsContainNoRetry(cmds) {
		attempts = 1
	}

	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			setCmdsErr(cmds, err)
			return err
		}

		db, idx := c.activeSnapshot()
		if db == nil || !db.cb.IsAllowed() || c.detector.ShouldFailover() {
			if err := c.tryFailover(ctx, idx); err != nil {
				// Overwrite any prior attempt's transport errors so callers
				// see the availability error, not a stale EOF (setCmdsErr
				// only fills empty error slots).
				resetCmds(cmds)
				setCmdsErr(cmds, err)
				return err
			}
			db, _ = c.activeSnapshot()
			if db == nil {
				continue
			}
		}

		if attempt > 0 {
			resetCmds(cmds)
		}
		err := db.processPipelineHook(ctx, cmds)
		transportFailures := c.recordBatchOutcomes(db, cmds)

		if transportFailures == 0 {
			// Only server replies (or clean success) — done, whatever the
			// user-level outcome is.
			return err
		}
	}
	return cmdsFirstErr(cmds)
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
	db, idx := c.activeSnapshot()
	if db == nil || !db.cb.IsAllowed() || c.detector.ShouldFailover() {
		if err := c.tryFailover(ctx, idx); err != nil {
			resetCmds(cmds)
			setCmdsErr(cmds, err)
			return err
		}
		db, _ = c.activeSnapshot()
		if db == nil {
			err := ErrTemporarilyNotAvailable
			resetCmds(cmds)
			setCmdsErr(cmds, err)
			return err
		}
	}

	err := db.processTxPipelineHook(ctx, cmds)
	// Record outcomes only for the user's commands: cmds arrives wrapped by
	// wrapMultiExec (MULTI ... EXEC), and counting the synthetic envelope
	// would advance the breaker by three per single-command transaction.
	user := cmds
	if len(cmds) >= 3 {
		user = cmds[1 : len(cmds)-1]
	}
	c.recordBatchOutcomes(db, user)
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
			return c.processTxPipelineHook(ctx, cmds)
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
	db, idx := c.core.activeSnapshot()
	if db == nil || !db.cb.IsAllowed() || c.core.detector.ShouldFailover() {
		if err := c.core.tryFailover(ctx, idx); err != nil {
			return err
		}
		db, _ = c.core.activeSnapshot()
		if db == nil {
			return ErrTemporarilyNotAvailable
		}
	}
	if db.cc != nil {
		return db.cc.Watch(ctx, fn, keys...)
	}
	return db.c.Watch(ctx, fn, keys...)
}

// SSubscribe subscribes to shard channels on the database that is active at
// call time. Unlike Subscribe/PSubscribe, sharded subscriptions do not follow
// the active database across failovers yet.
func (c *MultiDBClient) SSubscribe(ctx context.Context, channels ...string) *PubSub {
	db, _ := c.core.activeSnapshot()
	if db != nil && db.cc != nil {
		return db.cc.SSubscribe(ctx, channels...)
	}
	if db != nil {
		return db.c.SSubscribe(ctx, channels...)
	}
	// No active database: return a PubSub that fails to connect.
	return c.core.newPubSub()
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
