package redis

import (
	"sync"
	"sync/atomic"

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

func (pipe *Pipeline) Process(cmd Cmder) {
	pipe.mu.Lock()
	pipe.cmds = append(pipe.cmds, cmd)
	pipe.mu.Unlock()
}

// Close closes the pipeline, releasing any open resources.
func (pipe *Pipeline) Close() error {
	atomic.StoreInt32(&pipe.closed, 1)
	pipe.Discard()
	return nil
}

func (pipe *Pipeline) isClosed() bool {
	return atomic.LoadInt32(&pipe.closed) == 1
}

// Discard resets the pipeline and discards queued commands.
func (pipe *Pipeline) Discard() error {
	defer pipe.mu.Unlock()
	pipe.mu.Lock()
	if pipe.isClosed() {
		return pool.ErrClosed
	}
	pipe.cmds = pipe.cmds[:0]
	return nil
}

// Exec executes all previously queued commands using one
// client-server roundtrip.
//
// Exec always returns list of commands and error of the first failed
// command if any.
func (pipe *Pipeline) Exec() ([]Cmder, error) {
	if pipe.isClosed() {
		return nil, pool.ErrClosed
	}

	defer pipe.mu.Unlock()
	pipe.mu.Lock()

	if len(pipe.cmds) == 0 {
		return pipe.cmds, nil
	}

	cmds := pipe.cmds
	pipe.cmds = nil

	return cmds, pipe.exec(cmds)
}

func (pipe *Pipeline) pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	if err := fn(pipe); err != nil {
		return nil, err
	}
	cmds, err := pipe.Exec()
	_ = pipe.Close()
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
		if shouldRetry(err) {
			failedCmds = append(failedCmds, cmd)
		}
	}

	return failedCmds, firstCmdErr
}
