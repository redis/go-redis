package redis

import (
	"fmt"
	"io"
	"sync"

	"github.com/togoio/redisgoproxy/bufreader"
)

type connectFunc func() (io.ReadWriter, error)
type disconnectFunc func(io.ReadWriter)

type Client struct {
	mtx        sync.Mutex
	connect    connectFunc
	disconnect disconnectFunc
	currConn   io.ReadWriter
	rd         *bufreader.Reader

	reqs []Req
}

func NewClient(connect connectFunc, disconnect disconnectFunc) *Client {
	return &Client{
		rd:         bufreader.NewSizedReader(8192),
		connect:    connect,
		disconnect: disconnect,
	}
}

func NewMultiClient(connect connectFunc, disconnect disconnectFunc) *Client {
	return &Client{
		rd:         bufreader.NewSizedReader(8192),
		connect:    connect,
		disconnect: disconnect,

		reqs: make([]Req, 0),
	}
}

func (c *Client) Close() error {
	if c.disconnect != nil {
		c.disconnect(c.currConn)
	}
	c.currConn = nil
	return nil
}

func (c *Client) conn() (io.ReadWriter, error) {
	if c.currConn == nil {
		currConn, err := c.connect()
		if err != nil {
			return nil, err
		}
		c.currConn = currConn
	}
	return c.currConn, nil
}

func (c *Client) WriteReq(buf []byte) error {
	conn, err := c.conn()
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	if err != nil {
		c.Close()
	}
	return err
}

func (c *Client) ReadReply() (*bufreader.Reader, error) {
	conn, err := c.conn()
	if err != nil {
		return nil, err
	}
	_, err = c.rd.ReadFrom(conn)
	if err != nil {
		c.Close()
		return nil, err
	}
	return c.rd, nil
}

func (c *Client) WriteRead(buf []byte) (*bufreader.Reader, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if err := c.WriteReq(buf); err != nil {
		return nil, err
	}
	return c.ReadReply()
}

func (c *Client) Run(req Req) {
	if c.reqs != nil {
		c.mtx.Lock()
		c.reqs = append(c.reqs, req)
		c.mtx.Unlock()
		return
	}

	rd, err := c.WriteRead(req.Req())
	if err != nil {
		req.SetErr(err)
		return
	}
	req.ParseReply(rd)
}

//------------------------------------------------------------------------------

func (c *Client) Discard() {
	if c.reqs == nil {
		panic("MultiClient required")
	}

	c.mtx.Lock()
	c.reqs = c.reqs[:0]
	c.mtx.Unlock()
}

func (c *Client) Exec() ([]Req, error) {
	if c.reqs == nil {
		panic("MultiClient required")
	}

	c.mtx.Lock()
	reqs := c.reqs
	c.reqs = make([]Req, 0)
	c.mtx.Unlock()

	multiReq := make([]byte, 0, 1024)
	multiReq = append(multiReq, PackReq([]string{"MULTI"})...)
	for _, req := range reqs {
		multiReq = append(multiReq, req.Req()...)
	}
	multiReq = append(multiReq, PackReq([]string{"EXEC"})...)

	rd, err := c.WriteRead(multiReq)
	if err != nil {
		return nil, err
	}

	statusReq := NewStatusReq()

	// multi
	statusReq.ParseReply(rd)
	_, err = statusReq.Reply()
	if err != nil {
		return nil, err
	}

	for _ = range reqs {
		// queue
		statusReq.ParseReply(rd)
		_, err = statusReq.Reply()
		if err != nil {
			return nil, err
		}
	}

	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		return nil, fmt.Errorf("Expected '*', but got line %q of %q.", line, rd.Bytes())
	}

	for _, req := range reqs {
		req.ParseReply(rd)
	}

	return reqs, nil
}
