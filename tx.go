package redis

import (
	"context"

	"github.com/go-redis/redis/v7/internal/pool"
	"github.com/go-redis/redis/v7/internal/proto"
)

// TxFailedErr transaction redis failed.
const TxFailedErr = proto.RedisError("redis: transaction failed")

// Tx implements Redis transactions as described in
// http://redis.io/topics/transactions. It's NOT safe for concurrent use
// by multiple goroutines, because Exec resets list of watched keys.
// If you don't need WATCH it is better to use Pipeline.
type Tx struct {
	cmdable
	statefulCmdable
	baseClient

	ctx context.Context
}

func (c *Client) newTx(ctx context.Context) *Tx {
	tx := Tx{
		baseClient: baseClient{
			opt:      c.opt,
			connPool: pool.NewStickyConnPool(c.connPool.(*pool.ConnPool), true),
		},
		ctx: ctx,
	}
	tx.init()
	return &tx
}

func (c *Tx) init() {
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
}

func (c *Tx) Context() context.Context {
	return c.ctx
}

func (c *Tx) WithContext(ctx context.Context) *Tx {
	if ctx == nil {
		panic("nil context")
	}
	clone := *c
	clone.ctx = ctx
	return &clone
}

func (c *Tx) Process(cmd Cmder) error {
	return c.ProcessContext(c.ctx, cmd)
}

func (c *Tx) ProcessContext(ctx context.Context, cmd Cmder) error {
	return c.baseClient.process(ctx, cmd)
}

// Watch prepares a transaction and marks the keys to be watched
// for conditional execution if there are any keys.
//
// The transaction is automatically closed when fn exits.
func (c *Client) Watch(fn func(*Tx) error, keys ...string) error {
	return c.WatchContext(c.ctx, fn, keys...)
}

func (c *Client) WatchContext(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	tx := c.newTx(ctx)
	if len(keys) > 0 {
		if err := tx.Watch(keys...).Err(); err != nil {
			_ = tx.Close()
			return err
		}
	}

	err := fn(tx)
	_ = tx.Close()
	return err
}

// Close closes the transaction, releasing any open resources.
func (c *Tx) Close() error {
	_ = c.Unwatch().Err()
	return c.baseClient.Close()
}

// Watch marks the keys to be watched for conditional execution
// of a transaction.
func (c *Tx) Watch(keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "watch"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(args...)
	_ = c.Process(cmd)
	return cmd
}

// Unwatch flushes all the previously watched keys for a transaction.
func (c *Tx) Unwatch(keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "unwatch"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(args...)
	_ = c.Process(cmd)
	return cmd
}

// Pipeline creates a new pipeline. It is more convenient to use Pipelined.
func (c *Tx) Pipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processTxPipeline,
	}
	pipe.init()
	return &pipe
}

// Pipelined executes commands queued in the fn in a transaction.
//
// When using WATCH, EXEC will execute commands only if the watched keys
// were not modified, allowing for a check-and-set mechanism.
//
// Exec always returns list of commands. If transaction fails
// TxFailedErr is returned. Otherwise Exec returns an error of the first
// failed command or nil.
func (c *Tx) Pipelined(fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(fn)
}

// TxPipelined is an alias for Pipelined.
func (c *Tx) TxPipelined(fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipelined(fn)
}

// TxPipeline is an alias for Pipeline.
func (c *Tx) TxPipeline() Pipeliner {
	return c.Pipeline()
}
