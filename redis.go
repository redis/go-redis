package redis

import (
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
}

func NewClient(connect connectFunc, disconnect disconnectFunc) *Client {
	return &Client{
		rd:         bufreader.NewSizedReader(8192),
		connect:    connect,
		disconnect: disconnect,
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
	if err := c.WriteReq(buf); err != nil {
		return nil, err
	}
	return c.ReadReply()
}

func (c *Client) Run(req Req) {
	c.mtx.Lock()
	c.run(req)
	c.mtx.Unlock()
}

func (c *Client) run(req Req) {
	buf, err := c.WriteRead(req.Req())
	if err != nil {
		req.SetErr(err)
		return
	}
	req.ParseReply(buf)
}
