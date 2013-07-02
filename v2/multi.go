package redis

import (
	"fmt"
	"sync"
)

type MultiClient struct {
	*Client
	execMtx sync.Mutex
}

func (c *Client) MultiClient() (*MultiClient, error) {
	return &MultiClient{
		Client: &Client{
			baseClient: &baseClient{
				connPool: newSingleConnPool(c.connPool, nil, true),

				password: c.password,
				db:       c.db,
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
	c.reqsMtx.Lock()
	if c.reqs == nil {
		panic("Discard can be used only inside Exec")
	}
	c.reqs = c.reqs[:1]
	c.reqsMtx.Unlock()
}

func (c *MultiClient) Exec(do func()) ([]Req, error) {
	c.reqsMtx.Lock()
	c.reqs = []Req{NewStatusReq("MULTI")}
	c.reqsMtx.Unlock()

	do()

	c.queue(NewIfaceSliceReq("EXEC"))

	c.reqsMtx.Lock()
	reqs := c.reqs
	c.reqs = nil
	c.reqsMtx.Unlock()

	if len(reqs) == 2 {
		return []Req{}, nil
	}

	cn, err := c.conn()
	if err != nil {
		return nil, err
	}

	// Synchronize writes and reads to the connection using mutex.
	c.execMtx.Lock()
	err = c.execReqs(reqs, cn)
	c.execMtx.Unlock()
	if err != nil {
		c.removeConn(cn)
		return nil, err
	}

	c.putConn(cn)
	return reqs[1 : len(reqs)-1], nil
}

func (c *MultiClient) execReqs(reqs []Req, cn *conn) error {
	err := c.writeReq(cn, reqs...)
	if err != nil {
		return err
	}

	statusReq := NewStatusReq()

	// Omit last request (EXEC).
	reqsLen := len(reqs) - 1

	// Parse queued replies.
	for i := 0; i < reqsLen; i++ {
		_, err = statusReq.ParseReply(cn.Rd)
		if err != nil {
			return err
		}
	}

	// Parse number of replies.
	line, err := readLine(cn.Rd)
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
	// Loop starts from 1 to omit first request (MULTI).
	for i := 1; i < reqsLen; i++ {
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
