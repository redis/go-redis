package redis

import (
	"fmt"
	"io"
	"sync"

	"github.com/togoio/redisgoproxy/bufreader"
)

type connectFunc func() (io.ReadWriter, error)
type disconnectFunc func(io.ReadWriter)

func createReader() (*bufreader.Reader, error) {
	return bufreader.NewSizedReader(8192), nil
}

type Client struct {
	mtx        sync.Mutex
	connect    connectFunc
	disconnect disconnectFunc
	currConn   io.ReadWriter
	readerPool *bufreader.ReaderPool

	reqs []Req
}

func NewClient(connect connectFunc, disconnect disconnectFunc) *Client {
	return &Client{
		readerPool: bufreader.NewReaderPool(10, createReader),
		connect:    connect,
		disconnect: disconnect,
	}
}

func NewMultiClient(connect connectFunc, disconnect disconnectFunc) *Client {
	return &Client{
		readerPool: bufreader.NewReaderPool(10, createReader),
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

func (c *Client) ReadReply(rd *bufreader.Reader) error {
	conn, err := c.conn()
	if err != nil {
		return err
	}

	_, err = rd.ReadFrom(conn)
	if err != nil {
		c.Close()
		return err
	}

	return nil
}

func (c *Client) WriteRead(buf []byte, rd *bufreader.Reader) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if err := c.WriteReq(buf); err != nil {
		return err
	}
	return c.ReadReply(rd)
}

func (c *Client) Run(req Req) {
	if c.reqs != nil {
		c.mtx.Lock()
		c.reqs = append(c.reqs, req)
		c.mtx.Unlock()
		return
	}

	rd, err := c.readerPool.Get()
	if err != nil {
		req.SetErr(err)
		return
	}
	defer c.readerPool.Add(rd)

	err = c.WriteRead(req.Req(), rd)
	if err != nil {
		req.SetErr(err)
		return
	}

	val, err := req.ParseReply(rd)
	if err != nil {
		req.SetErr(err)
		return
	}
	req.SetVal(val)
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

	rd, err := c.readerPool.Get()
	if err != nil {
		return nil, err
	}
	defer c.readerPool.Add(rd)

	err = c.WriteRead(multiReq, rd)
	if err != nil {
		return nil, err
	}

	statusReq := NewStatusReq()

	// multi
	_, err = statusReq.ParseReply(rd)
	if err != nil {
		return nil, err
	}

	for _ = range reqs {
		// queue
		_, err = statusReq.ParseReply(rd)
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
		val, err := req.ParseReply(rd)
		if err != nil {
			req.SetErr(err)
		}
		req.SetVal(val)
	}

	return reqs, nil
}
