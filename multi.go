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

func (c *MultiClient) Close() error {
	c.Unwatch()
	return c.Client.Close()
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
	c.reqs = []Req{NewStatusReq("MULTI")}
	c.mtx.Unlock()
}

func (c *MultiClient) Exec(do func()) ([]Req, error) {
	c.Discard()

	do()

	c.mtx.Lock()
	c.reqs = append(c.reqs, NewIfaceSliceReq("EXEC"))
	if len(c.reqs) == 2 {
		c.mtx.Unlock()
		return []Req{}, nil
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
	return reqs[1 : len(reqs)-1], nil
}

func (c *MultiClient) ExecReqs(reqs []Req, conn *Conn) error {
	err := c.WriteReq(conn, reqs...)
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
	for i := 1; i < len(reqs)-1; i++ {
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
	for i := 1; i < len(reqs)-1; i++ {
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
