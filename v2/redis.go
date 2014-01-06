package redis

import (
	"net"
	"time"

	"github.com/golang/glog"
)

type baseClient struct {
	connPool pool

	opt *Options

	cmds []Cmder
}

func (c *baseClient) writeCmd(cn *conn, cmds ...Cmder) error {
	buf := make([]byte, 0, 1000)
	for _, cmd := range cmds {
		buf = appendCmd(buf, cmd.args())
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
			opt:      c.opt,
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

func (c *baseClient) freeConn(cn *conn, err error) {
	if err == Nil || err == TxFailedErr {
		c.putConn(cn)
	} else {
		c.removeConn(cn)
	}
}

func (c *baseClient) removeConn(cn *conn) {
	if err := c.connPool.Remove(cn); err != nil {
		glog.Errorf("pool.Remove failed: %s", err)
	}
}

func (c *baseClient) putConn(cn *conn) {
	if err := c.connPool.Put(cn); err != nil {
		glog.Errorf("pool.Put failed: %s", err)
	}
}

func (c *baseClient) Process(cmd Cmder) {
	if c.cmds == nil {
		c.run(cmd)
	} else {
		c.cmds = append(c.cmds, cmd)
	}
}

func (c *baseClient) run(cmd Cmder) {
	cn, err := c.conn()
	if err != nil {
		cmd.setErr(err)
		return
	}

	cn.writeTimeout = c.opt.WriteTimeout
	if timeout := cmd.writeTimeout(); timeout != nil {
		cn.writeTimeout = *timeout
	}

	cn.readTimeout = c.opt.ReadTimeout
	if timeout := cmd.readTimeout(); timeout != nil {
		cn.readTimeout = *timeout
	}

	if err := c.writeCmd(cn, cmd); err != nil {
		c.freeConn(cn, err)
		cmd.setErr(err)
		return
	}

	if err := cmd.parseReply(cn.rd); err != nil {
		c.freeConn(cn, err)
		return
	}

	c.putConn(cn)
}

// Close closes the client, releasing any open resources.
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

			connPool: newConnPool(newConnFunc(dial), opt.getPoolSize(), opt.IdleTimeout),
		},
	}
}

func NewTCPClient(opt *Options) *Client {
	dial := func() (net.Conn, error) {
		return net.DialTimeout("tcp", opt.Addr, opt.getDialTimeout())
	}
	return newClient(opt, dial)
}

func NewUnixClient(opt *Options) *Client {
	dial := func() (net.Conn, error) {
		return net.DialTimeout("unix", opt.Addr, opt.getDialTimeout())
	}
	return newClient(opt, dial)
}
