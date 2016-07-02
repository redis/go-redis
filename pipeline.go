package redis

import (
	"sync"
	"sync/atomic"

	"gopkg.in/redis.v4/internal/errors"
	"gopkg.in/redis.v4/internal/pool"
)

// Pipeline implements pipelining as described in
// http://redis.io/topics/pipelining. It's safe for concurrent use
// by multiple goroutines.
type Pipeline struct {
	cmdable
	statefulCmdable

	exec func([]Cmder) error

	mu   sync.Mutex // protects cmds
	cmds []Cmder

	closed int32
}

func (c *Pipeline) Process(cmd Cmder) error {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return nil
}

// Close closes the pipeline, releasing any open resources.
func (c *Pipeline) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	c.Discard()
	return nil
}

func (c *Pipeline) isClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// Discard resets the pipeline and discards queued commands.
func (c *Pipeline) Discard() error {
	defer c.mu.Unlock()
	c.mu.Lock()
	if c.isClosed() {
		return pool.ErrClosed
	}
	c.cmds = c.cmds[:0]
	return nil
}

// Exec executes all previously queued commands using one
// client-server roundtrip.
//
// Exec always returns list of commands and error of the first failed
// command if any.
func (c *Pipeline) Exec() ([]Cmder, error) {
	if c.isClosed() {
		return nil, pool.ErrClosed
	}

	defer c.mu.Unlock()
	c.mu.Lock()

	if len(c.cmds) == 0 {
		return c.cmds, nil
	}

	cmds := c.cmds
	c.cmds = nil

	return cmds, c.exec(cmds)
}

func (c *Pipeline) pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	if err := fn(c); err != nil {
		return nil, err
	}
	cmds, err := c.Exec()
	_ = c.Close()
	return cmds, err
}

func execCmds(cn *pool.Conn, cmds []Cmder) ([]Cmder, error) {
	if err := writeCmd(cn, cmds...); err != nil {
		setCmdsErr(cmds, err)
		return cmds, err
	}

	var firstCmdErr error
	var failedCmds []Cmder
	for _, cmd := range cmds {
		err := cmd.readReply(cn)
		if err == nil {
			continue
		}
		if firstCmdErr == nil {
			firstCmdErr = err
		}
		if errors.IsRetryable(err) {
			failedCmds = append(failedCmds, cmd)
		}
	}

	return failedCmds, firstCmdErr
}
