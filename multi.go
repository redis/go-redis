package redis

import (
	"fmt"
)

type MultiClient struct {
	*Client
}

func (c *Client) MultiClient() (*MultiClient, error) {
	return &MultiClient{
		Client: &Client{
			BaseClient: &BaseClient{
				ConnPool: NewSingleConnPool(c.ConnPool, true),
				InitConn: c.InitConn,
			},
		},
	}, nil
}

func (c *MultiClient) Multi() {
	c.reqs = make([]Req, 0)
}

func (c *MultiClient) Watch(keys ...string) *StatusReq {
	args := append([]string{"WATCH"}, keys...)
	req := NewStatusReq(args...)
	c.Process(req)
	return req
}

func (c *MultiClient) Unwatch(keys ...string) *StatusReq {
	args := append([]string{"UNWATCH"}, keys...)
	req := NewStatusReq(args...)
	c.Process(req)
	return req
}

func (c *MultiClient) Discard() {
	c.mtx.Lock()
	c.reqs = c.reqs[:0]
	c.mtx.Unlock()
}

func (c *MultiClient) Exec() ([]Req, error) {
	c.mtx.Lock()
	if len(c.reqs) == 0 {
		c.mtx.Unlock()
		return c.reqs, nil
	}
	reqs := c.reqs
	c.reqs = nil
	c.mtx.Unlock()

	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	err = c.ExecReqs(reqs, conn)
	if err != nil {
		c.ConnPool.Remove(conn)
		return nil, err
	}

	c.ConnPool.Add(conn)
	return reqs, nil
}

func (c *MultiClient) ExecReqs(reqs []Req, conn *Conn) error {
	multiReq := make([]byte, 0, 1024)
	multiReq = append(multiReq, PackReq([]string{"MULTI"})...)
	for _, req := range reqs {
		multiReq = append(multiReq, req.Req()...)
	}
	multiReq = append(multiReq, PackReq([]string{"EXEC"})...)

	err := c.WriteReq(multiReq, conn)
	if err != nil {
		return err
	}

	statusReq := NewStatusReq()

	// Parse MULTI command reply.
	_, err = statusReq.ParseReply(conn.Rd)
	if err != nil {
		return err
	}

	// Parse queued replies.
	for _ = range reqs {
		_, err = statusReq.ParseReply(conn.Rd)
		if err != nil {
			return err
		}
	}

	// Parse number of replies.
	line, err := readLine(conn.Rd)
	if err != nil {
		return err
	}
	if line[0] != '*' {
		return fmt.Errorf("Expected '*', but got line %q", line)
	}
	if len(line) == 3 && line[1] == '-' && line[2] == '1' {
		return Nil
	}

	// Parse replies.
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
