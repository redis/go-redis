package redis

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
)

var (
	ErrReaderTooSmall = errors.New("redis: Reader is too small")
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
	if password == "" && db < 0 {
		return nil
	}

	return func(client *Client) error {
		if password != "" {
			auth := client.Auth(password)
			if auth.Err() != nil {
				return auth.Err()
			}
		}

		if db >= 0 {
			sel := client.Select(db)
			if sel.Err() != nil {
				return sel.Err()
			}
		}

		return nil
	}
}

//------------------------------------------------------------------------------

type Client struct {
	mtx      sync.Mutex
	ConnPool ConnPool
	InitConn InitConnFunc

	reqs []Req
}

func NewClient(openConn OpenConnFunc, closeConn CloseConnFunc, initConn InitConnFunc) *Client {
	return &Client{
		ConnPool: NewMultiConnPool(openConn, closeConn, 10),
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

func (c *Client) Close() {
	c.ConnPool.Close()
}

func (c *Client) conn() (*Conn, error) {
	conn, isNew, err := c.ConnPool.Get()
	if err != nil {
		return nil, err
	}
	if isNew && c.InitConn != nil {
		client := &Client{
			ConnPool: NewSingleConnPoolConn(c.ConnPool, conn),
		}
		err = c.InitConn(client)
		if err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (c *Client) WriteReq(buf []byte, conn *Conn) error {
	_, err := conn.RW.Write(buf)
	return err
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
	conn, err := c.conn()
	if err != nil {
		req.SetErr(err)
		return
	}

	err = c.WriteReq(req.Req(), conn)
	if err != nil {
		c.ConnPool.Remove(conn)
		req.SetErr(err)
		return
	}

	val, err := req.ParseReply(conn.Rd)
	if err != nil {
		c.ConnPool.Add(conn)
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

	conn, err := c.conn()
	if err != nil {
		return nil, err
	}

	err = c.RunReqs(reqs, conn)
	if err != nil {
		c.ConnPool.Remove(conn)
		return nil, err
	}

	c.ConnPool.Add(conn)
	return reqs, nil
}

func (c *Client) RunReqs(reqs []Req, conn *Conn) error {
	var multiReq []byte
	if len(reqs) == 1 {
		multiReq = reqs[0].Req()
	} else {
		multiReq = make([]byte, 0, 1024)
		for _, req := range reqs {
			multiReq = append(multiReq, req.Req()...)
		}
	}

	err := c.WriteReq(multiReq, conn)
	if err != nil {
		return err
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

	return nil
}
