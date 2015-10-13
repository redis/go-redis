package redis

import (
	"errors"
	"fmt"
	"log"
)

var errDiscard = errors.New("redis: Discard can be used only inside Exec")

// Multi implements Redis transactions as described in
// http://redis.io/topics/transactions. It's NOT safe for concurrent
// use by multiple goroutines.
type Multi struct {
	commandable

	base *baseClient
	cmds []Cmder
}

func (c *Client) Multi() *Multi {
	multi := &Multi{
		base: &baseClient{
			opt:      c.opt,
			connPool: newStickyConnPool(c.connPool, true),
		},
	}
	multi.commandable.process = multi.process
	return multi
}

func (c *Multi) process(cmd Cmder) {
	if c.cmds == nil {
		c.base.process(cmd)
	} else {
		c.cmds = append(c.cmds, cmd)
	}
}

func (c *Multi) Close() error {
	if err := c.Unwatch().Err(); err != nil {
		log.Printf("redis: Unwatch failed: %s", err)
	}
	return c.base.Close()
}

func (c *Multi) Watch(keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "WATCH"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *Multi) Unwatch(keys ...string) *StatusCmd {
	args := make([]interface{}, 1+len(keys))
	args[0] = "UNWATCH"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *Multi) Discard() error {
	if c.cmds == nil {
		return errDiscard
	}
	c.cmds = c.cmds[:1]
	return nil
}

// Exec always returns list of commands. If transaction fails
// TxFailedErr is returned. Otherwise Exec returns error of the first
// failed command or nil.
func (c *Multi) Exec(f func() error) ([]Cmder, error) {
	c.cmds = []Cmder{NewStatusCmd("MULTI")}
	if err := f(); err != nil {
		return nil, err
	}
	c.cmds = append(c.cmds, NewSliceCmd("EXEC"))

	cmds := c.cmds
	c.cmds = nil

	if len(cmds) == 2 {
		return []Cmder{}, nil
	}

	cn, err := c.base.conn()
	if err != nil {
		setCmdsErr(cmds[1:len(cmds)-1], err)
		return cmds[1 : len(cmds)-1], err
	}

	err = c.execCmds(cn, cmds)
	c.base.putConn(cn, err)
	return cmds[1 : len(cmds)-1], err
}

func (c *Multi) execCmds(cn *conn, cmds []Cmder) error {
	err := cn.writeCmds(cmds...)
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
	line, err := readLine(cn)
	if err != nil {
		setCmdsErr(cmds[1:len(cmds)-1], err)
		return err
	}
	if line[0] != '*' {
		err := fmt.Errorf("redis: expected '*', but got line %q", line)
		setCmdsErr(cmds[1:len(cmds)-1], err)
		return err
	}
	if len(line) == 3 && line[1] == '-' && line[2] == '1' {
		setCmdsErr(cmds[1:len(cmds)-1], TxFailedErr)
		return TxFailedErr
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
