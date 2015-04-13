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
func (c *Pipeline) Exec() ([]Cmder, error) {
	if c.closed {
		return nil, errClosed
	}
	if len(c.cmds) == 0 {
		return []Cmder{}, nil
	}

	cmds := c.cmds
	c.cmds = make([]Cmder, 0, 0)

	cn, err := c.client.conn()
	if err != nil {
		setCmdsErr(cmds, err)
		return cmds, err
	}

	if err := c.execCmds(cn, cmds); err != nil {
		c.client.freeConn(cn, err)
		return cmds, err
	}

	c.client.putConn(cn)
	return cmds, nil
}

func (c *Pipeline) execCmds(cn *conn, cmds []Cmder) error {
	if err := cn.writeCmds(cmds...); err != nil {
		setCmdsErr(cmds, err)
		return err
	}

	var firstCmdErr error
	for i, cmd := range cmds {
		err := cmd.parseReply(cn.rd)
		if err == nil {
			continue
		}
		if firstCmdErr == nil {
			firstCmdErr = err
		}
		if isNetworkError(err) {
			setCmdsErr(cmds[i:], err)
		}
	}

	return firstCmdErr
}
