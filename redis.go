package redis // import "gopkg.in/redis.v2"

import (
	"log"
	"net"
	"time"
)

type baseClient struct {
	connPool pool
	opt      *options
	cmds     []Cmder
}

func (c *baseClient) conn() (*conn, error) {
	cn, isNew, err := c.connPool.Get()
	if err != nil {
		return nil, err
	}

	if isNew {
		if err := c.initConn(cn); err != nil {
			c.removeConn(cn)
			return nil, err
		}
	}

	return cn, nil
}

func (c *baseClient) initConn(cn *conn) error {
	if c.opt.Password == "" && c.opt.DB == 0 {
		return nil
	}

	pool := newSingleConnPool(c.connPool, false)
	pool.SetConn(cn)

	// Client is not closed because we want to reuse underlying connection.
	client := &Client{
		baseClient: &baseClient{
			opt:      c.opt,
			connPool: pool,
		},
	}

	if c.opt.Password != "" {
		if err := client.Auth(c.opt.Password).Err(); err != nil {
			return err
		}
	}

	if c.opt.DB > 0 {
		if err := client.Select(c.opt.DB).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (c *baseClient) freeConn(cn *conn, ei error) error {
	if cn.rd.Buffered() > 0 {
		return c.connPool.Remove(cn)
	}
	if _, ok := ei.(redisError); ok {
		return c.connPool.Put(cn)
	}
	return c.connPool.Remove(cn)
}

func (c *baseClient) removeConn(cn *conn) {
	if err := c.connPool.Remove(cn); err != nil {
		log.Printf("pool.Remove failed: %s", err)
	}
}

func (c *baseClient) putConn(cn *conn) {
	if err := c.connPool.Put(cn); err != nil {
		log.Printf("pool.Put failed: %s", err)
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

	if timeout := cmd.writeTimeout(); timeout != nil {
		cn.writeTimeout = *timeout
	} else {
		cn.writeTimeout = c.opt.WriteTimeout
	}

	if timeout := cmd.readTimeout(); timeout != nil {
		cn.readTimeout = *timeout
	} else {
		cn.readTimeout = c.opt.ReadTimeout
	}

	if err := cn.writeCmds(cmd); err != nil {
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

type options struct {
	Password string
	DB       int64

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize    int
	PoolTimeout time.Duration
	IdleTimeout time.Duration
}

type Options struct {
	// The network type, either "tcp" or "unix".
	// Default: "tcp"
	Network string
	// The network address.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func() (net.Conn, error)

	// An optional password. Must match the password specified in the
	// `requirepass` server configuration option.
	Password string
	// Select a database.
	// Default: 0
	DB int64

	// Sets the deadline for establishing new connections. If reached,
	// deal attepts will fail with a timeout.
	DialTimeout time.Duration
	// Sets the deadline for socket reads. If reached, commands will
	// fail with a timeout instead of blocking.
	ReadTimeout time.Duration
	// Sets the deadline for socket writes. If reached, commands will
	// fail with a timeout instead of blocking.
	WriteTimeout time.Duration

	// The maximum number of socket connections.
	// Default: 10
	PoolSize int
	// If all socket connections is the pool are busy, the pool will wait
	// this amount of time for a conection to become available, before
	// returning an error.
	// Default: 5s
	PoolTimeout time.Duration
	// Evict connections from the pool after they have been idle for longer
	// than specified in this option.
	// Default: 0 = no eviction
	IdleTimeout time.Duration
}

func (opt *Options) getNetwork() string {
	if opt.Network == "" {
		return "tcp"
	}
	return opt.Network
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

func (opt *Options) getPoolTimeout() time.Duration {
	if opt.PoolTimeout == 0 {
		return 5 * time.Second
	}
	return opt.PoolTimeout
}

func (opt *Options) options() *options {
	return &options{
		DB:       opt.DB,
		Password: opt.Password,

		DialTimeout:  opt.getDialTimeout(),
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:    opt.getPoolSize(),
		PoolTimeout: opt.getPoolTimeout(),
		IdleTimeout: opt.IdleTimeout,
	}
}

type Client struct {
	*baseClient
}

func NewClient(clOpt *Options) *Client {
	opt := clOpt.options()
	dialer := clOpt.Dialer
	if dialer == nil {
		dialer = func() (net.Conn, error) {
			return net.DialTimeout(clOpt.getNetwork(), clOpt.Addr, opt.DialTimeout)
		}
	}
	return &Client{
		baseClient: &baseClient{
			opt:      opt,
			connPool: newConnPool(newConnFunc(dialer), opt),
		},
	}
}

// Deprecated. Use NewClient instead.
func NewTCPClient(opt *Options) *Client {
	opt.Network = "tcp"
	return NewClient(opt)
}

// Deprecated. Use NewClient instead.
func NewUnixClient(opt *Options) *Client {
	opt.Network = "unix"
	return NewClient(opt)
}
