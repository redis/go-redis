package redis

import (
	"errors"
	"fmt"
)

var errDiscard = errors.New("redis: Discard can be used only inside Exec")

// Not thread-safe.
type Multi struct {
	*Client
}

func (c *Client) Multi() *Multi {
	return &Multi{
		Client: &Client{
			baseClient: &baseClient{
				opt:      c.opt,
				connPool: newSingleConnPool(c.connPool, nil, true),
			},
		},
	}
}

func (c *Multi) Close() error {
	c.Unwatch()
	return c.Client.Close()
}

func (c *Multi) Watch(keys ...string) *StatusReq {
	args := append([]string{"WATCH"}, keys...)
	req := NewStatusReq(args...)
	c.Process(req)
	return req
}

func (c *Multi) Unwatch(keys ...string) *StatusReq {
	args := append([]string{"UNWATCH"}, keys...)
	req := NewStatusReq(args...)
	c.Process(req)
	return req
}

func (c *Multi) Discard() error {
	if c.reqs == nil {
		return errDiscard
	}
	c.reqs = c.reqs[:1]
	return nil
}

func (c *Multi) Exec(f func()) ([]Req, error) {
	c.reqs = []Req{NewStatusReq("MULTI")}
	f()
	c.reqs = append(c.reqs, NewIfaceSliceReq("EXEC"))

	reqs := c.reqs
	c.reqs = nil

	if len(reqs) == 2 {
		return []Req{}, nil
	}

	cn, err := c.conn()
	if err != nil {
		return nil, err
	}

	// Synchronize writes and reads to the connection using mutex.
	err = c.execReqs(reqs, cn)
	if err != nil {
		c.removeConn(cn)
		return nil, err
	}

	c.putConn(cn)
	return reqs[1 : len(reqs)-1], nil
}

func (c *Multi) execReqs(reqs []Req, cn *conn) error {
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
		return fmt.Errorf("redis: expected '*', but got line %q", line)
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
