package redis

import (
	"errors"
	"fmt"
)

var errDiscard = errors.New("redis: Discard can be used only inside Exec")

// Not thread-safe.
type Multi struct {
	*Client
}

func (c *Client) Multi() *Multi {
	return &Multi{
		Client: &Client{
			baseClient: &baseClient{
				opt:      c.opt,
				connPool: newSingleConnPool(c.connPool, nil, true),
			},
		},
	}
}

func (c *Multi) Close() error {
	c.Unwatch()
	return c.Client.Close()
}

func (c *Multi) Watch(keys ...string) *StatusCmd {
	args := append([]string{"WATCH"}, keys...)
	cmd := NewStatusCmd(args...)
	c.Process(cmd)
	return cmd
}

func (c *Multi) Unwatch(keys ...string) *StatusCmd {
	args := append([]string{"UNWATCH"}, keys...)
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

func (c *Multi) Exec(f func()) ([]Cmder, error) {
	c.cmds = []Cmder{NewStatusCmd("MULTI")}
	f()
	c.cmds = append(c.cmds, NewSliceCmd("EXEC"))

	cmds := c.cmds
	c.cmds = nil

	if len(cmds) == 2 {
		return []Cmder{}, nil
	}

	cn, err := c.conn()
	if err != nil {
		return nil, err
	}

	// Synchronize writes and reads to the connection using mutex.
	err = c.execCmds(cmds, cn)
	if err != nil {
		c.removeConn(cn)
		return nil, err
	}

	c.putConn(cn)
	return cmds[1 : len(cmds)-1], nil
}

func (c *Multi) execCmds(cmds []Cmder, cn *conn) error {
	err := c.writeCmd(cn, cmds...)
	if err != nil {
		return err
	}

	statusCmd := NewStatusCmd()

	// Omit last cmduest (EXEC).
	cmdsLen := len(cmds) - 1

	// Parse queued replies.
	for i := 0; i < cmdsLen; i++ {
		_, err = statusCmd.parseReply(cn.rd)
		if err != nil {
			return err
		}
	}

	// Parse number of replies.
	line, err := readLine(cn.rd)
	if err != nil {
		return err
	}
	if line[0] != '*' {
		return fmt.Errorf("redis: expected '*', but got line %q", line)
	}
	if len(line) == 3 && line[1] == '-' && line[2] == '1' {
		return Nil
	}

	// Parse replies.
	// Loop starts from 1 to omit first cmduest (MULTI).
	for i := 1; i < cmdsLen; i++ {
		cmd := cmds[i]
		val, err := cmd.parseReply(cn.rd)
		if err != nil {
			cmd.setErr(err)
		} else {
			cmd.setVal(val)
		}
	}

	return nil
}
