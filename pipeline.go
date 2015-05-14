package redis

// Not thread-safe.
type Pipeline struct {
	commandable

	cmds   []Cmder
	client *baseClient
	closed bool
}

func (c *Client) Pipeline() *Pipeline {
	pipe := &Pipeline{
		client: &baseClient{
			opt:      c.opt,
			connPool: c.connPool,
		},
		cmds: make([]Cmder, 0, 10),
	}
	pipe.commandable.process = pipe.process
	return pipe
}

func (c *Client) Pipelined(f func(*Pipeline) error) ([]Cmder, error) {
	pc := c.Pipeline()
	if err := f(pc); err != nil {
		return nil, err
	}
	cmds, err := pc.Exec()
	pc.Close()
	return cmds, err
}

func (c *Pipeline) process(cmd Cmder) {
	c.cmds = append(c.cmds, cmd)
}

func (c *Pipeline) Close() error {
	c.closed = true
	return nil
}

func (c *Pipeline) Discard() error {
	if c.closed {
		return errClosed
	}
	c.cmds = c.cmds[:0]
	return nil
}

// Exec always returns list of commands and error of the first failed
// command if any.
func (c *Pipeline) Exec() (cmds []Cmder, retErr error) {
	if c.closed {
		return nil, errClosed
	}
	if len(c.cmds) == 0 {
		return c.cmds, nil
	}

	cmds = c.cmds
	c.cmds = make([]Cmder, 0, 0)

	for i := 0; i <= c.client.opt.MaxRetries; i++ {
		if i > 0 {
			resetCmds(cmds)
		}

		cn, err := c.client.conn()
		if err != nil {
			setCmdsErr(cmds, err)
			return cmds, err
		}

		retErr = c.execCmds(cn, cmds)
		c.client.putConn(cn, err)
		if shouldRetry(err) {
			continue
		}

		break
	}

	return cmds, retErr
}

func (c *Pipeline) execCmds(cn *conn, cmds []Cmder) error {
	if err := cn.writeCmds(cmds...); err != nil {
		setCmdsErr(cmds, err)
		return err
	}

	var firstCmdErr error
	for _, cmd := range cmds {
		err := cmd.parseReply(cn.rd)
		if err != nil && firstCmdErr == nil {
			firstCmdErr = err
		}
	}

	return firstCmdErr
}
