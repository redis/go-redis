package redis

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/vmihailenco/bufreader"
)

type OpenConnFunc func() (io.ReadWriter, error)
type CloseConnFunc func(io.ReadWriter)
type InitConnFunc func(*Client) error

func TCPConnector(addr string) OpenConnFunc {
	return func() (io.ReadWriter, error) {
		return net.Dial("tcp", addr)
	}
}

func TLSConnector(addr string, tlsConfig *tls.Config) OpenConnFunc {
	return func() (io.ReadWriter, error) {
		return tls.Dial("tcp", addr, tlsConfig)
	}
}

func AuthSelectFunc(password string, db int64) InitConnFunc {
	return func(client *Client) error {
		if password != "" {
			_, err := client.Auth(password).Reply()
			if err != nil {
				return err
			}
		}

		_, err := client.Select(db).Reply()
		if err != nil {
			return err
		}

		return nil
	}
}

func createReader() (*bufreader.Reader, error) {
	return bufreader.NewSizedReader(8192), nil
}

type Client struct {
	mtx      sync.Mutex
	ConnPool *ConnPool
	InitConn InitConnFunc

	reqs []Req
}

func NewClient(openConn OpenConnFunc, closeConn CloseConnFunc, initConn InitConnFunc) *Client {
	return &Client{
		ConnPool: NewConnPool(openConn, closeConn, 10),
		InitConn: initConn,
	}
}

func NewTCPClient(addr string, password string, db int64) *Client {
	return NewClient(TCPConnector(addr), nil, AuthSelectFunc(password, db))
}

func NewTLSClient(addr string, tlsConfig *tls.Config, password string, db int64) *Client {
	return NewClient(
		TLSConnector(addr, tlsConfig),
		nil,
		AuthSelectFunc(password, db),
	)
}

func (c *Client) WriteReq(buf []byte, conn *Conn) error {
	_, err := conn.RW.Write(buf)
	return err
}

func (c *Client) ReadReply(conn *Conn) error {
	_, err := conn.Rd.ReadFrom(conn.RW)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) WriteRead(buf []byte, conn *Conn) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if err := c.WriteReq(buf, conn); err != nil {
		return err
	}
	return c.ReadReply(conn)
}

func (c *Client) Process(req Req) {
	if c.reqs == nil {
		c.Run(req)
	} else {
		c.Queue(req)
	}
}

func (c *Client) Queue(req Req) {
	c.mtx.Lock()
	c.reqs = append(c.reqs, req)
	c.mtx.Unlock()
}

func (c *Client) Run(req Req) {
	conn, _, err := c.ConnPool.Get()
	if err != nil {
		c.ConnPool.Remove(conn)
		req.SetErr(err)
		return
	}

	err = c.WriteRead(req.Req(), conn)
	if err != nil {
		c.ConnPool.Remove(conn)
		req.SetErr(err)
		return
	}

	val, err := req.ParseReply(conn.Rd)
	if err != nil {
		c.ConnPool.Remove(conn)
		req.SetErr(err)
		return
	}

	c.ConnPool.Add(conn)
	req.SetVal(val)
}

func (c *Client) RunQueued() ([]Req, error) {
	c.mtx.Lock()
	if len(c.reqs) == 0 {
		c.mtx.Unlock()
		return c.reqs, nil
	}
	reqs := c.reqs
	c.reqs = make([]Req, 0)
	c.mtx.Unlock()

	return c.RunReqs(reqs)
}

func (c *Client) RunReqs(reqs []Req) ([]Req, error) {
	var multiReq []byte
	if len(reqs) == 1 {
		multiReq = reqs[0].Req()
	} else {
		multiReq = make([]byte, 0, 1024)
		for _, req := range reqs {
			multiReq = append(multiReq, req.Req()...)
		}
	}

	conn, _, err := c.ConnPool.Get()
	if err != nil {
		return nil, err
	}

	err = c.WriteRead(multiReq, conn)
	if err != nil {
		return nil, err
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

	return reqs, nil
}

//------------------------------------------------------------------------------

func (c *Client) Discard() {
	c.mtx.Lock()
	c.reqs = c.reqs[:0]
	c.mtx.Unlock()
}

func (c *Client) Exec() ([]Req, error) {
	c.mtx.Lock()
	if len(c.reqs) == 0 {
		c.mtx.Unlock()
		return c.reqs, nil
	}
	reqs := c.reqs
	c.reqs = make([]Req, 0)
	c.mtx.Unlock()

	multiReq := make([]byte, 0, 1024)
	multiReq = append(multiReq, PackReq([]string{"MULTI"})...)
	for _, req := range reqs {
		multiReq = append(multiReq, req.Req()...)
	}
	multiReq = append(multiReq, PackReq([]string{"EXEC"})...)

	conn, _, err := c.ConnPool.Get()
	if err != nil {
		return nil, err
	}

	err = c.WriteRead(multiReq, conn)
	if err != nil {
		return nil, err
	}

	statusReq := NewStatusReq()

	// Parse MULTI command reply.
	_, err = statusReq.ParseReply(conn.Rd)
	if err != nil {
		return nil, err
	}

	// Parse queued replies.
	for _ = range reqs {
		_, err = statusReq.ParseReply(conn.Rd)
		if err != nil {
			return nil, err
		}
	}

	// Parse number of replies.
	line, err := conn.Rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		return nil, fmt.Errorf("Expected '*', but got line %q of %q.", line, conn.Rd.Bytes())
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

	return reqs, nil
}
