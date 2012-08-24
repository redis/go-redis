package redis

import (
	"crypto/tls"
	"net"
	"sync"
)

type OpenConnFunc func() (net.Conn, error)
type CloseConnFunc func(net.Conn) error
type InitConnFunc func(*Client) error

func TCPConnector(addr string) OpenConnFunc {
	return func() (net.Conn, error) {
		return net.Dial("tcp", addr)
	}
}

func TLSConnector(addr string, tlsConfig *tls.Config) OpenConnFunc {
	return func() (net.Conn, error) {
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

type BaseClient struct {
	mtx      sync.Mutex
	ConnPool ConnPool
	InitConn InitConnFunc
	reqs     []Req
}

func (c *BaseClient) WriteReq(conn *Conn, reqs ...Req) error {
	buf := make([]byte, 0, 1000)
	for _, req := range reqs {
		buf = appendReq(buf, req.Args())
	}

	_, err := conn.RW.Write(buf)
	return err
}

func (c *BaseClient) conn() (*Conn, error) {
	conn, isNew, err := c.ConnPool.Get()
	if err != nil {
		return nil, err
	}

	if isNew && c.InitConn != nil {
		client := &Client{
			BaseClient: &BaseClient{
				ConnPool: NewSingleConnPoolConn(c.ConnPool, conn, true),
			},
		}
		err = c.InitConn(client)
		if err != nil {
			if err := c.ConnPool.Remove(conn); err != nil {
				panic(err)
			}
			return nil, err
		}
	}
	return conn, nil
}

func (c *BaseClient) Process(req Req) {
	if c.reqs == nil {
		c.Run(req)
	} else {
		c.Queue(req)
	}
}

func (c *BaseClient) Run(req Req) {
	conn, err := c.conn()
	if err != nil {
		req.SetErr(err)
		return
	}

	err = c.WriteReq(conn, req)
	if err != nil {
		if err := c.ConnPool.Remove(conn); err != nil {
			panic(err)
		}
		req.SetErr(err)
		return
	}

	val, err := req.ParseReply(conn.Rd)
	if err != nil {
		if err == Nil {
			if err := c.ConnPool.Add(conn); err != nil {
				panic(err)
			}
		} else {
			if err := c.ConnPool.Remove(conn); err != nil {
				panic(err)
			}
		}
		req.SetErr(err)
		return
	}

	if err := c.ConnPool.Add(conn); err != nil {
		panic(err)
	}
	req.SetVal(val)
}

func (c *BaseClient) Queue(req Req) {
	c.mtx.Lock()
	c.reqs = append(c.reqs, req)
	c.mtx.Unlock()
}

func (c *BaseClient) Close() error {
	return c.ConnPool.Close()
}

//------------------------------------------------------------------------------

type Client struct {
	*BaseClient
	TryEvalShaMinLen int
}

func NewClient(openConn OpenConnFunc, closeConn CloseConnFunc, initConn InitConnFunc) *Client {
	return &Client{
		BaseClient: &BaseClient{
			ConnPool: NewMultiConnPool(openConn, closeConn, 10),
			InitConn: initConn,
		},
		TryEvalShaMinLen: 80,
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
