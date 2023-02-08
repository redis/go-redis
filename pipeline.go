package redis

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type pipelineExecer func(context.Context, []Cmder) error

// Pipeliner is an mechanism to realise Redis Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Redis and read a replies in a
// singe step.
// See https://redis.io/topics/pipelining
//
// Pay attention, that Pipeline is not a transaction, so you can get unexpected
// results in case of big pipelines and small read/write timeouts.
// Redis client has retransmission logic in case of timeouts, pipeline
// can be retransmitted and commands can be executed more then once.
// To avoid this: it is good idea to use reasonable bigger read/write timeouts
// depends of your batch size and/or use TxPipeline.
type Pipeliner interface {
	StatefulCmdable
	Len() int
	Do(ctx context.Context, args ...interface{}) *Cmd
	Process(ctx context.Context, cmd Cmder) error
	Discard()
	Exec(ctx context.Context) ([]Cmder, error)
}

var _ Pipeliner = (*Pipeline)(nil)

// Pipeline implements pipelining as described in
// http://redis.io/topics/pipelining.
// Please note: it is not safe for concurrent use by multiple goroutines.
type Pipeline struct {
	cmdable
	statefulCmdable

	mu   sync.Mutex
	exec pipelineExecer
	cmds map[string]Cmder
}

func (c *Pipeline) init() {
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
	c.cmds = make(map[string]Cmder)
	c.mu = sync.Mutex{}
}

// Len returns the number of queued commands.
func (c *Pipeline) Len() int {
	return len(c.cmds)
}

// Do queues the custom command for later execution.
func (c *Pipeline) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Process queues the cmd for later execution.
func (c *Pipeline) Process(ctx context.Context, cmd Cmder) error {
	//c.cmds = append(c.cmds, cmd)
	uid := uuid.New().String()

	c.mu.Lock()
	defer func() { c.mu.Unlock() }()

	if c.cmds == nil {
		c.cmds = map[string]Cmder{}
	}

	if len(cmd.Args()) <= 4 {
		c.cmds[cmd.Args()[1].(string)+uid] = cmd
	} else {
		c.cmds[cmd.Args()[3].(string)+cmd.Args()[4].(string)] = cmd
	}

	return nil
}

// Discard resets the pipeline and discards queued commands.
func (c *Pipeline) Discard() {
	c.cmds = map[string]Cmder{}
}

// Exec executes all previously queued commands using one
// client-server roundtrip.
//
// Exec always returns list of commands and error of the first failed
// command if any.
func (c *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	if len(c.cmds) == 0 {
		return nil, nil
	}
	c.mu.Lock()
	defer func() { c.mu.Unlock() }()
	cmdSlice := make([]Cmder, 0)

	for _, c := range c.cmds {
		cmdSlice = append(cmdSlice, c)
	}

	return cmdSlice, c.exec(ctx, cmdSlice)
}

func (c *Pipeline) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	if err := fn(c); err != nil {
		return nil, err
	}
	return c.Exec(ctx)
}

func (c *Pipeline) Pipeline() Pipeliner {
	return c
}

func (c *Pipeline) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipelined(ctx, fn)
}

func (c *Pipeline) TxPipeline() Pipeliner {
	return c
}
