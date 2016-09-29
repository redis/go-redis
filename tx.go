package redis

import (
	"errors"
	"fmt"

	"gopkg.in/redis.v4/internal"
	ierrors "gopkg.in/redis.v4/internal/errors"
	"gopkg.in/redis.v4/internal/pool"
	"gopkg.in/redis.v4/internal/proto"
)

// Redis transaction failed.
const TxFailedErr = ierrors.RedisError("redis: transaction failed")

var errDiscard = errors.New("redis: Discard can be used only inside Exec")

// Tx implements Redis transactions as described in
// http://redis.io/topics/transactions. It's NOT safe for concurrent use
// by multiple goroutines, because Exec resets list of watched keys.
// If you don't need WATCH it is better to use Pipeline.
type Tx struct {
	cmdable
	statefulCmdable
	baseClient

	cmds   []Cmder
	closed bool
}

func (c *Client) newTx() *Tx {
	tx := Tx{
		baseClient: baseClient{
			opt:      c.opt,
			connPool: pool.NewStickyConnPool(c.connPool.(*pool.ConnPool), true),
		},
	}
	tx.cmdable.process = tx.Process
	tx.statefulCmdable.process = tx.Process
	return &tx
}

func (c *Client) Watch(fn func(*Tx) error, keys ...string) error {
	tx := c.newTx()
	if len(keys) > 0 {
		if err := tx.Watch(keys...).Err(); err != nil {
			_ = tx.close()
			return err
		}
	}
	retErr := fn(tx)
	if err := tx.close(); err != nil && retErr == nil {
		retErr = err
	}
	return retErr
}

func (c *Tx) Process(cmd Cmder) error {
	if c.cmds == nil {
		return c.baseClient.Process(cmd)
	}
	c.cmds = append(c.cmds, cmd)
	return nil
}

// close closes the transaction, releasing any open resources.
func (c *Tx) close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if err := c.Unwatch().Err(); err != nil {
		internal.Logf("Unwatch failed: %s", err)
	}
	return c.baseClient.Close()
}

// Watch marks the keys to be watched for conditional execution
// of a transaction.
func (c *Tx) Watch(keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "WATCH"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

// Unwatch flushes all the previously watched keys for a transaction.
func (c *Tx) Unwatch(keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "UNWATCH"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

// Discard discards queued commands.
func (c *Tx) Discard() error {
	if c.cmds == nil {
		return errDiscard
	}
	c.cmds = c.cmds[:1]
	return nil
}

// MultiExec executes all previously queued commands in a transaction
// and restores the connection state to normal.
//
// When using WATCH, EXEC will execute commands only if the watched keys
// were not modified, allowing for a check-and-set mechanism.
//
// Exec always returns list of commands. If transaction fails
// TxFailedErr is returned. Otherwise Exec returns error of the first
// failed command or nil.
func (c *Tx) MultiExec(fn func() error) ([]Cmder, error) {
	if c.closed {
		return nil, pool.ErrClosed
	}

	c.cmds = []Cmder{NewStatusCmd("MULTI")}
	if err := fn(); err != nil {
		return nil, err
	}
	c.cmds = append(c.cmds, NewSliceCmd("EXEC"))

	cmds := c.cmds
	c.cmds = nil

	if len(cmds) == 2 {
		return []Cmder{}, nil
	}

	// Strip MULTI and EXEC commands.
	retCmds := cmds[1 : len(cmds)-1]

	cn, _, err := c.conn()
	if err != nil {
		setCmdsErr(retCmds, err)
		return retCmds, err
	}

	err = c.execCmds(cn, cmds)
	c.putConn(cn, err, false)
	return retCmds, err
}

func (c *Tx) execCmds(cn *pool.Conn, cmds []Cmder) error {
	err := writeCmd(cn, cmds...)
	if err != nil {
		setCmdsErr(cmds[1:len(cmds)-1], err)
		return err
	}

	statusCmd := NewStatusCmd()

	// Omit last command (EXEC).
	cmdsLen := len(cmds) - 1

	// Parse queued replies.
	for i := 0; i < cmdsLen; i++ {
		if err := statusCmd.readReply(cn); err != nil {
			setCmdsErr(cmds[1:len(cmds)-1], err)
			return err
		}
	}

	// Parse number of replies.
	line, err := cn.Rd.ReadLine()
	if err != nil {
		if err == Nil {
			err = TxFailedErr
		}
		setCmdsErr(cmds[1:len(cmds)-1], err)
		return err
	}
	if line[0] != proto.ArrayReply {
		err := fmt.Errorf("redis: expected '*', but got line %q", line)
		setCmdsErr(cmds[1:len(cmds)-1], err)
		return err
	}

	var firstCmdErr error

	// Parse replies.
	// Loop starts from 1 to omit MULTI cmd.
	for i := 1; i < cmdsLen; i++ {
		cmd := cmds[i]
		if err := cmd.readReply(cn); err != nil {
			if firstCmdErr == nil {
				firstCmdErr = err
			}
		}
	}

	return firstCmdErr
}
