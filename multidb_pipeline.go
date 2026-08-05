package redis

import (
	"context"
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
// count as failures. It returns the number of transport-level failures.
func (c *multidbCore) recordBatchOutcomes(db *multidbDatabase, cmds []Cmder) int {
	transportFailures := 0
	for _, cmd := range cmds {
		err := cmd.Err()
		if err == nil || isRedisReplyError(err) {
			db.cb.RecordSuccess()
			c.detector.RecordSuccess()
			continue
		}
		db.cb.RecordFailure()
		c.detector.RecordFailure(err)
		transportFailures++
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
			setCmdsErr(cmds, err)
			return err
		}
		db, _ = c.activeSnapshot()
		if db == nil {
			err := ErrTemporarilyNotAvailable
			setCmdsErr(cmds, err)
			return err
		}
	}

	err := db.processTxPipelineHook(ctx, cmds)
	c.recordBatchOutcomes(db, cmds)
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
// when Watch is called. The transaction is bound to that member for its whole
// lifetime: it does NOT follow a MultiDB failover, and it is never
// automatically retried on another database. If the bound member fails while
// the transaction is open, the transaction errors like it would on a plain
// client; MultiDB moves only subsequent operations to the new active member.
func (c *MultiDBClient) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	db, _ := c.core.activeSnapshot()
	if db == nil {
		return ErrTemporarilyNotAvailable
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

// AutoPipeline returns the blocking autopipeliner for this MultiDB client:
// batches are flushed against whichever database is active at exec time, and
// a batch that fails at transport level is retried against the newly selected
// database (bounded by CommandRetries). The instance is cached and shared; the
// first call's config wins.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *MultiDBClient) AutoPipeline() (*AutoPipeliner, error) {
	return c.AutoPipelineWithOptions(nil)
}

// AutoPipelineWithOptions is AutoPipeline with explicit options.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
func (c *MultiDBClient) AutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	return getOrCreateAutoPipeliner(c.autopipelinerMu, &c.autopipeliner, &c.autopipelinerClosed, nil, config,
		DefaultBlockingAutoPipelineOptions,
		func(cfg *AutoPipelineOptions) (*AutoPipeliner, error) { return newAutoPipeliner(c, cfg, true) })
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
		func(cfg *AutoPipelineOptions) (*AutoPipeliner, error) { return newAutoPipeliner(c, cfg, false) })
}
