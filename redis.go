package redis // import "gopkg.in/redis.v2"

import (
	"log"
	"net"
	"time"
)

type baseClient struct {
	connPool pool
	opt      *options
}

func (c *baseClient) conn() (*conn, error) {
	return c.connPool.Get()
}

func (c *baseClient) putConn(cn *conn, ei error) {
	var err error
	if cn.rd.Buffered() > 0 {
		err = c.connPool.Remove(cn)
	} else if ei == nil {
		err = c.connPool.Put(cn)
	} else if _, ok := ei.(redisError); ok {
		err = c.connPool.Put(cn)
	} else {
		err = c.connPool.Remove(cn)
	}
	if err != nil {
		log.Printf("redis: putConn failed: %s", err)
	}
}

func (c *baseClient) process(cmd Cmder) {
	for i := 0; i <= c.opt.MaxRetries; i++ {
		if i > 0 {
			cmd.reset()
		}

		cn, err := c.conn()
		if err != nil {
			cmd.setErr(err)
			return
		}

		if timeout := cmd.writeTimeout(); timeout != nil {
			cn.WriteTimeout = *timeout
		} else {
			cn.WriteTimeout = c.opt.WriteTimeout
		}

		if timeout := cmd.readTimeout(); timeout != nil {
			cn.ReadTimeout = *timeout
		} else {
			cn.ReadTimeout = c.opt.ReadTimeout
		}

		if err := cn.writeCmds(cmd); err != nil {
			c.putConn(cn, err)
			cmd.setErr(err)
			if shouldRetry(err) {
				continue
			}
			return
		}

		err = cmd.parseReply(cn.rd)
		c.putConn(cn, err)
		if shouldRetry(err) {
			continue
		}

		return
	}
}

// Close closes the client, releasing any open resources.
func (c *baseClient) Close() error {
	return c.connPool.Close()
}

//------------------------------------------------------------------------------

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
	// PoolTimeout specifies amount of time client waits for a free
	// connection in the pool. Default timeout is 1s.
	PoolTimeout time.Duration
	// Evict connections from the pool after they have been idle for longer
	// than specified in this option.
	// Default: 0 = no eviction
	IdleTimeout time.Duration

	// MaxRetries specifies maximum number of times client will retry
	// failed command. Default is to not retry failed command.
	MaxRetries int
}

func (opt *Options) getDialer() func() (net.Conn, error) {
	if opt.Dialer == nil {
		return func() (net.Conn, error) {
			return net.DialTimeout(opt.getNetwork(), opt.Addr, opt.getDialTimeout())
		}
	}
	return opt.Dialer
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
		return 1 * time.Second
	}
	return opt.PoolTimeout
}

func (opt *Options) options() *options {
	return &options{
		Dialer:      opt.getDialer(),
		PoolSize:    opt.getPoolSize(),
		PoolTimeout: opt.getPoolTimeout(),
		IdleTimeout: opt.IdleTimeout,

		DB:       opt.DB,
		Password: opt.Password,

		DialTimeout:  opt.getDialTimeout(),
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		MaxRetries: opt.MaxRetries,
	}
}

type options struct {
	Dialer      func() (net.Conn, error)
	PoolSize    int
	PoolTimeout time.Duration
	IdleTimeout time.Duration

	Password string
	DB       int64

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	MaxRetries int
}

func (opt *options) connPoolOptions() *connPoolOptions {
	return &connPoolOptions{
		Dialer:      newConnDialer(opt),
		PoolSize:    opt.PoolSize,
		PoolTimeout: opt.PoolTimeout,
		IdleTimeout: opt.IdleTimeout,
	}
}

//------------------------------------------------------------------------------

type Client struct {
	*baseClient
	commandable
}

func newClient(opt *options, pool pool) *Client {
	base := &baseClient{opt: opt, connPool: pool}
	return &Client{
		baseClient:  base,
		commandable: commandable{process: base.process},
	}
}

func NewClient(clOpt *Options) *Client {
	opt := clOpt.options()
	pool := newConnPool(opt.connPoolOptions())
	return newClient(opt, pool)
}
