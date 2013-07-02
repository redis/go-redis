package redis

type PipelineClient struct {
	*Client
}

func (c *Client) PipelineClient() (*PipelineClient, error) {
	return &PipelineClient{
		Client: &Client{
			baseClient: &baseClient{
				connPool: c.connPool,

				password: c.password,
				db:       c.db,

				reqs: make([]Req, 0),
			},
		},
	}, nil
}

func (c *Client) Pipelined(do func(*PipelineClient)) ([]Req, error) {
	pc, err := c.PipelineClient()
	if err != nil {
		return nil, err
	}
	defer pc.Close()

	do(pc)

	return pc.RunQueued()
}

func (c *PipelineClient) Close() error {
	return nil
}

func (c *PipelineClient) DiscardQueued() {
	c.reqsMtx.Lock()
	c.reqs = c.reqs[:0]
	c.reqsMtx.Unlock()
}

func (c *PipelineClient) RunQueued() ([]Req, error) {
	c.reqsMtx.Lock()
	reqs := c.reqs
	c.reqs = make([]Req, 0)
	c.reqsMtx.Unlock()

	if len(reqs) == 0 {
		return []Req{}, nil
	}

	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	err = c.runReqs(reqs, conn)
	if err != nil {
		c.removeConn(conn)
		return nil, err
	}

	c.putConn(conn)
	return reqs, nil
}

func (c *PipelineClient) runReqs(reqs []Req, cn *conn) error {
	err := c.writeReq(cn, reqs...)
	if err != nil {
		return err
	}

	reqsLen := len(reqs)
	for i := 0; i < reqsLen; i++ {
		req := reqs[i]
		val, err := req.ParseReply(cn.Rd)
		if err != nil {
			req.SetErr(err)
		} else {
			req.SetVal(val)
		}
	}

	return nil
}
