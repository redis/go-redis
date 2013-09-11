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

	opt *Options

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

	if isNew && (c.opt.Password != "" || c.opt.DB > 0) {
		if err = c.init(cn, c.opt.Password, c.opt.DB); err != nil {
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

	cn.writeTimeout = c.opt.WriteTimeout
	if timeout := req.writeTimeout(); timeout != nil {
		cn.writeTimeout = *timeout
	}

	cn.readTimeout = c.opt.ReadTimeout
	if timeout := req.readTimeout(); timeout != nil {
		cn.readTimeout = *timeout
	}

	if err := c.writeReq(cn, req); err != nil {
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

type Options struct {
	Addr     string
	Password string
	DB       int64

	PoolSize int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

func (opt *Options) getPoolSize() int {
	if opt.PoolSize == 0 {
		return 10
	}
	return opt.PoolSize
}

func (opt *Options) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

//------------------------------------------------------------------------------

type Client struct {
	*baseClient
}

func newClient(opt *Options, dial func() (net.Conn, error)) *Client {
	return &Client{
		baseClient: &baseClient{
			opt: opt,

			connPool: newConnPool(
				dial, opt.getPoolSize(),
				opt.IdleTimeout,
			),
		},
	}
}

func DialTCP(opt *Options) *Client {
	dial := func() (net.Conn, error) {
		return net.DialTimeout("tcp", opt.Addr, opt.getDialTimeout())
	}
	return newClient(opt, dial)
}

func DialTLS(opt *Options, tlsConfig *tls.Config) *Client {
	dial := func() (net.Conn, error) {
		conn, err := net.DialTimeout("tcp", opt.Addr, opt.getDialTimeout())
		if err != nil {
			return nil, err
		}
		return tls.Client(conn, tlsConfig), nil
	}
	return newClient(opt, dial)
}

func DialUnix(opt *Options) *Client {
	dial := func() (net.Conn, error) {
		return net.DialTimeout("unix", opt.Addr, opt.getDialTimeout())
	}
	return newClient(opt, dial)
}
