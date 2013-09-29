package redis

// Not thread-safe.
type Pipeline struct {
	*Client
}

func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{
		Client: &Client{
			baseClient: &baseClient{
				opt:      c.opt,
				connPool: c.connPool,

				reqs: make([]Req, 0),
			},
		},
	}
}

func (c *Client) Pipelined(f func(*Pipeline)) ([]Req, error) {
	pc := c.Pipeline()
	f(pc)
	reqs, err := pc.Exec()
	pc.Close()
	return reqs, err
}

func (c *Pipeline) Close() error {
	return nil
}

func (c *Pipeline) Discard() error {
	c.reqs = c.reqs[:0]
	return nil
}

// Always returns list of commands and error of the first failed
// command if any.
func (c *Pipeline) Exec() ([]Req, error) {
	reqs := c.reqs
	c.reqs = make([]Req, 0)

	if len(reqs) == 0 {
		return []Req{}, nil
	}

	cn, err := c.conn()
	if err != nil {
		return reqs, err
	}

	if err := c.execReqs(reqs, cn); err != nil {
		c.freeConn(cn, err)
		return reqs, err
	}

	c.putConn(cn)
	return reqs, nil
}

func (c *Pipeline) execReqs(reqs []Req, cn *conn) error {
	err := c.writeReq(cn, reqs...)
	if err != nil {
		for _, req := range reqs {
			req.SetErr(err)
		}
		return err
	}

	var firstReqErr error
	for _, req := range reqs {
		val, err := req.ParseReply(cn.Rd)
		if err != nil {
			req.SetErr(err)
			if err != nil {
				firstReqErr = err
			}
		} else {
			req.SetVal(val)
		}
	}

	return firstReqErr
}
