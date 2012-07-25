package redis

type MultiClient struct {
	*Client
	reqs []Req
}

func NewMultiClient(connect connectFunc, disconnect disconnectFunc) *MultiClient {
	return &MultiClient{
		Client: NewClient(connect, disconnect),
		reqs:   make([]Req, 0),
	}

}

func (c *MultiClient) queueReq(req Req) {
	c.reqs = append(c.reqs, req)
}

func (c *MultiClient) run(req Req) {
	c.queueReq(req)
}

func (c *MultiClient) Exec() ([]Req, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	reqs := c.reqs
	c.reqs = make([]Req, 0)

	multiReq := make([]byte, 0, 8192)
	multiReq = append(multiReq, PackReq([]string{"MULTI"})...)
	for _, req := range reqs {
		multiReq = append(multiReq, req.Req()...)
	}
	multiReq = append(multiReq, PackReq([]string{"EXEC"})...)

	buf, err := c.WriteRead(multiReq)
	if err != nil {
		return nil, err
	}

	for _, req := range reqs {
		req.ParseReply(buf)
	}

	return reqs, nil
}
