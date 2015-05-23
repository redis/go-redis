package redis // import "gopkg.in/redis.v3"

import (
	"fmt"
	"log"
	"net"
	"time"
)

type baseClient struct {
	connPool pool
	opt      *Options
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Redis<%s db:%d>", c.opt.Addr, c.opt.DB)
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
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string
	// host:port address.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func() (net.Conn, error)

	// An optional password. Must match the password specified in the
	// requirepass server configuration option.
	Password string
	// A database to be selected after connecting to server.
	DB int64

	// The maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int

	// Sets the deadline for establishing new connections. If reached,
	// dial will fail with a timeout.
	DialTimeout time.Duration
	// Sets the deadline for socket reads. If reached, commands will
	// fail with a timeout instead of blocking.
	ReadTimeout time.Duration
	// Sets the deadline for socket writes. If reached, commands will
	// fail with a timeout instead of blocking.
	WriteTimeout time.Duration

	// The maximum number of socket connections.
	// Default is 10 connections.
	PoolSize int
	// Specifies amount of time client waits for connection if all
	// connections are busy before returning an error.
	// Default is 5 seconds.
	PoolTimeout time.Duration
	// Specifies amount of time after which client closes idle
	// connections. Should be less than server's timeout.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
}

func (opt *Options) getNetwork() string {
	if opt.Network == "" {
		return "tcp"
	}
	return opt.Network
}

func (opt *Options) getDialer() func() (net.Conn, error) {
	if opt.Dialer == nil {
		opt.Dialer = func() (net.Conn, error) {
			return net.DialTimeout(opt.getNetwork(), opt.Addr, opt.getDialTimeout())
		}
	}
	return opt.Dialer
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

func (opt *Options) getIdleTimeout() time.Duration {
	return opt.IdleTimeout
}

//------------------------------------------------------------------------------

type Client struct {
	*baseClient
	commandable
}

func newClient(opt *Options, pool pool) *Client {
	base := &baseClient{opt: opt, connPool: pool}
	return &Client{
		baseClient:  base,
		commandable: commandable{process: base.process},
	}
}

func NewClient(opt *Options) *Client {
	pool := newConnPool(opt)
	return newClient(opt, pool)
}
