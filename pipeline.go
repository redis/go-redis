package redis

import (
	"context"
	"errors"
)

type pipelineExecer func(context.Context, []Cmder) error

// Pipeliner is a mechanism to realise Redis Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Redis and read a replies in a
// single step.
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

	// Len obtains the number of commands in the pipeline that have not yet been executed.
	Len() int

	// Do is an API for executing any command.
	// If a certain Redis command is not yet supported, you can use Do to execute it.
	Do(ctx context.Context, args ...interface{}) *Cmd

	// Process queues the cmd for later execution.
	Process(ctx context.Context, cmd Cmder) error

	// BatchProcess adds multiple commands to be executed into the pipeline buffer.
	BatchProcess(ctx context.Context, cmd ...Cmder) error

	// Discard discards all commands in the pipeline buffer that have not yet been executed.
	Discard()

	// Exec sends all the commands buffered in the pipeline to the redis server.
	Exec(ctx context.Context) ([]Cmder, error)

	// Cmds returns the list of queued commands.
	Cmds() []Cmder
}

var _ Pipeliner = (*Pipeline)(nil)

// Pipeline implements pipelining as described in
// https://redis.io/docs/latest/develop/using-commands/pipelining.
// Please note: it is not safe for concurrent use by multiple goroutines.
type Pipeline struct {
	cmdable
	statefulCmdable

	exec pipelineExecer
	cmds []Cmder
	// sticky is true only for a Pipeline created from a dedicated *Conn (a
	// persistent, caller-owned connection). Per-connection state commands
	// (CLIENT TRACKING / MAINT_NOTIFICATIONS) are then allowed. On a pooled
	// Pipeline (from a Client/Ring/ClusterClient, or a Tx) the connection is
	// borrowed for Exec and returned to the pool afterwards, so queuing those
	// commands would leave per-connection state on an arbitrary connection — the
	// overrides below reject them there (matching the pooled-client wrappers).
	sticky bool
}

func (c *Pipeline) init() {
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
}

// ClientTracking / ClientTrackingOn / ClientTrackingOff / ClientMaintNotifications
// are per-connection state. On a POOLED pipeline (from a Client/Ring/ClusterClient,
// or a Tx) the connection is borrowed for Exec and returned to the pool afterwards,
// so queuing these would leave state on an arbitrary connection — reject them with
// guidance, exactly like the pooled-client wrappers. On a pipeline from a dedicated
// *Conn (sticky) they are queued normally, since the state stays on that connection.
func (c *Pipeline) ClientTracking(ctx context.Context, on bool, opt *ClientTrackingOptions) *StatusCmd {
	if c.sticky {
		return c.statefulCmdable.ClientTracking(ctx, on, opt)
	}
	if !on {
		return c.ClientTrackingOff(ctx)
	}
	return c.ClientTrackingOn(ctx, opt)
}

func (c *Pipeline) ClientTrackingOn(ctx context.Context, opt *ClientTrackingOptions) *StatusCmd {
	if c.sticky {
		return c.statefulCmdable.ClientTrackingOn(ctx, opt)
	}
	args := []interface{}{"client", "tracking", "on"}
	if opt != nil {
		args = appendClientTrackingOptions(args, opt)
	}
	return pooledConnStateCmd(ctx, errClientTrackingOnPooledClient, args...)
}

func (c *Pipeline) ClientTrackingOff(ctx context.Context) *StatusCmd {
	if c.sticky {
		return c.statefulCmdable.ClientTrackingOff(ctx)
	}
	return pooledConnStateCmd(ctx, errClientTrackingOnPooledClient, "client", "tracking", "off")
}

func (c *Pipeline) ClientMaintNotifications(ctx context.Context, enabled bool, endpointType string) *StatusCmd {
	if c.sticky {
		return c.statefulCmdable.ClientMaintNotifications(ctx, enabled, endpointType)
	}
	args := []interface{}{"client", "maint_notifications"}
	if enabled {
		if endpointType == "" {
			endpointType = "none"
		}
		args = append(args, "on", "moving-endpoint-type", endpointType)
	} else {
		args = append(args, "off")
	}
	return pooledConnStateCmd(ctx, errClientMaintNotificationsOnPooledClient, args...)
}

// Len returns the number of queued commands.
func (c *Pipeline) Len() int {
	return len(c.cmds)
}

// Do queues the custom command for later execution.
func (c *Pipeline) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	if len(args) == 0 {
		cmd.SetErr(errors.New("redis: please enter the command to be executed"))
		return cmd
	}
	_ = c.Process(ctx, cmd)
	return cmd
}

// Process queues the cmd for later execution.
func (c *Pipeline) Process(ctx context.Context, cmd Cmder) error {
	return c.BatchProcess(ctx, cmd)
}

// BatchProcess queues multiple cmds for later execution.
func (c *Pipeline) BatchProcess(ctx context.Context, cmd ...Cmder) error {
	c.cmds = append(c.cmds, cmd...)
	return nil
}

// Discard resets the pipeline and discards queued commands.
func (c *Pipeline) Discard() {
	c.cmds = c.cmds[:0]
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

	cmds := c.cmds
	c.cmds = nil

	return cmds, c.exec(ctx, cmds)
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

func (c *Pipeline) Cmds() []Cmder {
	return c.cmds
}
