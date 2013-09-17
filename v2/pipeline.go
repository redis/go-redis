package redis

type PipelineClient struct {
	*Client
}

// TODO: rename to Pipeline
// TODO: return just *PipelineClient
func (c *Client) PipelineClient() (*PipelineClient, error) {
	return &PipelineClient{
		Client: &Client{
			baseClient: &baseClient{
				opt:      c.opt,
				connPool: c.connPool,

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

// TODO: rename to Run or ...
// TODO: should return error if one of the commands failed
func (c *PipelineClient) RunQueued() ([]Req, error) {
	c.reqsMtx.Lock()
	reqs := c.reqs
	c.reqs = make([]Req, 0)
	c.reqsMtx.Unlock()

	if len(reqs) == 0 {
		return []Req{}, nil
	}

	cn, err := c.conn()
	if err != nil {
		return nil, err
	}

	if err := c.runReqs(reqs, cn); err != nil {
		c.removeConn(cn)
		return nil, err
	}

	c.putConn(cn)
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
