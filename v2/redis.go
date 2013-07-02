package redis

import (
	"crypto/tls"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// Package logger.
var Logger = log.New(os.Stdout, "redis: ", log.Ldate|log.Ltime)

//------------------------------------------------------------------------------

type baseClient struct {
	connPool pool

	password string
	db       int64

	reqs    []Req
	reqsMtx sync.Mutex
}

func (c *baseClient) writeReq(cn *conn, reqs ...Req) error {
	buf := make([]byte, 0, 1000)
	for _, req := range reqs {
		buf = appendReq(buf, req.Args())
	}

	_, err := cn.Write(buf)
	return err
}

func (c *baseClient) conn() (*conn, error) {
	cn, isNew, err := c.connPool.Get()
	if err != nil {
		return nil, err
	}

	if isNew && (c.password != "" || c.db > 0) {
		if err = c.init(cn, c.password, c.db); err != nil {
			c.removeConn(cn)
			return nil, err
		}
	}

	return cn, nil
}

func (c *baseClient) init(cn *conn, password string, db int64) error {
	// Client is not closed on purpose.
	client := &Client{
		baseClient: &baseClient{
			connPool: newSingleConnPool(c.connPool, cn, false),
		},
	}

	if password != "" {
		auth := client.Auth(password)
		if auth.Err() != nil {
			return auth.Err()
		}
	}

	if db > 0 {
		sel := client.Select(db)
		if sel.Err() != nil {
			return sel.Err()
		}
	}

	return nil
}

func (c *baseClient) removeConn(cn *conn) {
	if err := c.connPool.Remove(cn); err != nil {
		Logger.Printf("connPool.Remove error: %v", err)
	}
}

func (c *baseClient) putConn(cn *conn) {
	if err := c.connPool.Put(cn); err != nil {
		Logger.Printf("connPool.Add error: %v", err)
	}
}

func (c *baseClient) Process(req Req) {
	if c.reqs == nil {
		c.run(req)
	} else {
		c.queue(req)
	}
}

func (c *baseClient) run(req Req) {
	cn, err := c.conn()
	if err != nil {
		req.SetErr(err)
		return
	}

	err = c.writeReq(cn, req)
	if err != nil {
		c.removeConn(cn)
		req.SetErr(err)
		return
	}

	val, err := req.ParseReply(cn.Rd)
	if err != nil {
		if _, ok := err.(*parserError); ok {
			c.removeConn(cn)
		} else {
			c.putConn(cn)
		}
		req.SetErr(err)
		return
	}

	c.putConn(cn)
	req.SetVal(val)
}

// Queues request to be executed later.
func (c *baseClient) queue(req Req) {
	c.reqsMtx.Lock()
	c.reqs = append(c.reqs, req)
	c.reqsMtx.Unlock()
}

func (c *baseClient) Close() error {
	return c.connPool.Close()
}

//------------------------------------------------------------------------------

type ClientFactory struct {
	Dial  func() (net.Conn, error)
	Close func(net.Conn) error

	Password string
	DB       int64

	PoolSize int

	ReadTimeout, WriteTimeout, IdleTimeout time.Duration
}

func (f *ClientFactory) New() *Client {
	return &Client{
		baseClient: &baseClient{
			password: f.Password,
			db:       f.DB,

			connPool: newConnPool(
				f.Dial, f.getClose(), f.getPoolSize(),
				f.ReadTimeout, f.WriteTimeout, f.IdleTimeout,
			),
		},
	}
}

func (f *ClientFactory) getClose() func(net.Conn) error {
	if f.Close == nil {
		return func(conn net.Conn) error {
			return conn.Close()
		}
	}
	return f.Close
}

func (f *ClientFactory) getPoolSize() int {
	if f.PoolSize == 0 {
		return 10
	}
	return f.PoolSize
}

//------------------------------------------------------------------------------

type Client struct {
	*baseClient
}

func NewTCPClient(addr string, password string, db int64) *Client {
	dial := func() (net.Conn, error) {
		return net.DialTimeout("tcp", addr, 3*time.Second)
	}
	return (&ClientFactory{
		Dial: dial,

		Password: password,
		DB:       db,
	}).New()
}

func NewTLSClient(addr string, tlsConfig *tls.Config, password string, db int64) *Client {
	dial := func() (net.Conn, error) {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			return nil, err
		}
		return tls.Client(conn, tlsConfig), nil
	}
	return (&ClientFactory{
		Dial: dial,

		Password: password,
		DB:       db,
	}).New()
}

func NewUnixClient(addr string, password string, db int64) *Client {
	dial := func() (net.Conn, error) {
		return net.DialTimeout("unix", addr, 3*time.Second)
	}
	return (&ClientFactory{
		Dial: dial,

		Password: password,
		DB:       db,
	}).New()
}
