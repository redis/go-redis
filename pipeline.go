package redis

type PipelineClient struct {
	*Client
}

func (c *Client) PipelineClient() (*PipelineClient, error) {
	return &PipelineClient{
		Client: &Client{
			BaseClient: &BaseClient{
				ConnPool: c.ConnPool,
				InitConn: c.InitConn,
				reqs:     make([]Req, 0),
			},
		},
	}, nil
}

func (c *PipelineClient) Close() error {
	return nil
}

func (c *PipelineClient) RunQueued() ([]Req, error) {
	c.mtx.Lock()
	if len(c.reqs) == 0 {
		c.mtx.Unlock()
		return c.reqs, nil
	}
	reqs := c.reqs
	c.reqs = make([]Req, 0)
	c.mtx.Unlock()

	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	err = c.RunReqs(reqs, conn)
	if err != nil {
		c.ConnPool.Remove(conn)
		return nil, err
	}

	c.ConnPool.Add(conn)
	return reqs, nil
}

func (c *PipelineClient) RunReqs(reqs []Req, conn *Conn) error {
	var multiReq []byte
	if len(reqs) == 1 {
		multiReq = reqs[0].Req()
	} else {
		multiReq = make([]byte, 0, 1024)
		for _, req := range reqs {
			multiReq = append(multiReq, req.Req()...)
		}
	}

	err := c.WriteReq(multiReq, conn)
	if err != nil {
		return err
	}

	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		val, err := req.ParseReply(conn.Rd)
		if err != nil {
			req.SetErr(err)
		} else {
			req.SetVal(val)
		}
	}

	return nil
}
