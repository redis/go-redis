package redis

import (
	"sync"

	"gopkg.in/redis.v5/internal"
	"gopkg.in/redis.v5/internal/pool"
)

// Pipeline implements pipelining as described in
// http://redis.io/topics/pipelining. It's safe for concurrent use
// by multiple goroutines.
type Pipeline struct {
	cmdable
	statefulCmdable

	exec func([]Cmder) error

	mu     sync.Mutex
	cmds   []Cmder
	closed bool
}

func (c *Pipeline) Process(cmd Cmder) error {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return nil
}

// Close closes the pipeline, releasing any open resources.
func (c *Pipeline) Close() error {
	c.mu.Lock()
	c.discard()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// Discard resets the pipeline and discards queued commands.
func (c *Pipeline) Discard() error {
	c.mu.Lock()
	err := c.discard()
	c.mu.Unlock()
	return err
}

func (c *Pipeline) discard() error {
	if c.closed {
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
	defer c.mu.Unlock()
	c.mu.Lock()

	if c.closed {
		return nil, pool.ErrClosed
	}

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

func execCmds(cn *pool.Conn, cmds []Cmder) (retry bool, firstErr error) {
	if err := writeCmd(cn, cmds...); err != nil {
		setCmdsErr(cmds, err)
		return true, err
	}

	for i, cmd := range cmds {
		err := cmd.readReply(cn)
		if err == nil {
			continue
		}
		if i == 0 && internal.IsNetworkError(err) {
			return true, err
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return false, firstErr
}
