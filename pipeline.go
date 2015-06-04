package redis

// Pipeline implements pipelining as described in
// http://redis.io/topics/pipelining.
//
// Pipeline is not thread-safe.
type Pipeline struct {
	commandable

	client *baseClient

	cmds   []Cmder
	closed bool
}

func (c *Client) Pipeline() *Pipeline {
	pipe := &Pipeline{
		client: c.baseClient,
		cmds:   make([]Cmder, 0, 10),
	}
	pipe.commandable.process = pipe.process
	return pipe
}

func (c *Client) Pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	pipe := c.Pipeline()
	if err := fn(pipe); err != nil {
		return nil, err
	}
	cmds, err := pipe.Exec()
	pipe.Close()
	return cmds, err
}

func (pipe *Pipeline) process(cmd Cmder) {
	pipe.cmds = append(pipe.cmds, cmd)
}

func (pipe *Pipeline) Close() error {
	pipe.Discard()
	pipe.closed = true
	return nil
}

// Discard resets the pipeline and discards queued commands.
func (pipe *Pipeline) Discard() error {
	if pipe.closed {
		return errClosed
	}
	pipe.cmds = pipe.cmds[:0]
	return nil
}

// Exec always returns list of commands and error of the first failed
// command if any.
func (pipe *Pipeline) Exec() (cmds []Cmder, retErr error) {
	if pipe.closed {
		return nil, errClosed
	}
	if len(pipe.cmds) == 0 {
		return pipe.cmds, nil
	}

	cmds = pipe.cmds
	pipe.cmds = make([]Cmder, 0, 10)

	failedCmds := cmds
	for i := 0; i <= pipe.client.opt.MaxRetries; i++ {
		cn, err := pipe.client.conn()
		if err != nil {
			setCmdsErr(failedCmds, err)
			return cmds, err
		}

		if i > 0 {
			resetCmds(failedCmds)
		}
		failedCmds, err = execCmds(cn, failedCmds)
		pipe.client.putConn(cn, err)
		if err != nil && retErr == nil {
			retErr = err
		}
		if len(failedCmds) == 0 {
			break
		}
	}

	return cmds, retErr
}

func execCmds(cn *conn, cmds []Cmder) ([]Cmder, error) {
	if err := cn.writeCmds(cmds...); err != nil {
		setCmdsErr(cmds, err)
		return cmds, err
	}

	var firstCmdErr error
	var failedCmds []Cmder
	for _, cmd := range cmds {
		err := cmd.parseReply(cn.rd)
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
