package redis // import "gopkg.in/redis.v3"

import (
	"fmt"
	"log"

	"gopkg.in/redis.v3/internal"
	"gopkg.in/redis.v3/internal/pool"
)

var Logger *log.Logger

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

type baseClient struct {
	connPool pool.Pooler
	opt      *Options

	onClose func() error // hook called when client is closed
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Redis<%s db:%d>", c.opt.Addr, c.opt.DB)
}

func (c *baseClient) conn() (*pool.Conn, error) {
	cn, err := c.connPool.Get()
	if err != nil {
		return nil, err
	}
	if !cn.Inited {
		if err := c.initConn(cn); err != nil {
			_ = c.connPool.Remove(cn, err)
			return nil, err
		}
	}
	return cn, err
}

func (c *baseClient) putConn(cn *pool.Conn, err error, allowTimeout bool) bool {
	if isBadConn(err, allowTimeout) {
		_ = c.connPool.Remove(cn, err)
		return false
	}

	_ = c.connPool.Put(cn)
	return true
}

func (c *baseClient) initConn(cn *pool.Conn) error {
	cn.Inited = true

	if c.opt.Password == "" && c.opt.DB == 0 {
		return nil
	}

	// Temp client for Auth and Select.
	client := newClient(c.opt, pool.NewSingleConnPool(cn))

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

		readTimeout := cmd.readTimeout()
		if readTimeout != nil {
			cn.ReadTimeout = *readTimeout
		} else {
			cn.ReadTimeout = c.opt.ReadTimeout
		}
		cn.WriteTimeout = c.opt.WriteTimeout

		if err := writeCmd(cn, cmd); err != nil {
			c.putConn(cn, err, false)
			cmd.setErr(err)
			if err != nil && shouldRetry(err) {
				continue
			}
			return
		}

		err = cmd.readReply(cn)
		c.putConn(cn, err, readTimeout != nil)
		if err != nil && shouldRetry(err) {
			continue
		}

		return
	}
}

func (c *baseClient) closed() bool {
	return c.connPool.Closed()
}

// Close closes the client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *baseClient) Close() error {
	var retErr error
	if c.onClose != nil {
		if err := c.onClose(); err != nil && retErr == nil {
			retErr = err
		}
	}
	if err := c.connPool.Close(); err != nil && retErr == nil {
		retErr = err
	}
	return retErr
}

//------------------------------------------------------------------------------

// Client is a Redis client representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type Client struct {
	baseClient
	commandable
}

func newClient(opt *Options, pool pool.Pooler) *Client {
	base := baseClient{opt: opt, connPool: pool}
	return &Client{
		baseClient: base,
		commandable: commandable{
			process: base.process,
		},
	}
}

// NewClient returns a client to the Redis Server specified by Options.
func NewClient(opt *Options) *Client {
	return newClient(opt, newConnPool(opt))
}

// PoolStats returns connection pool stats.
func (c *Client) PoolStats() *PoolStats {
	s := c.connPool.Stats()
	return &PoolStats{
		Requests: s.Requests,
		Hits:     s.Hits,
		Waits:    s.Waits,
		Timeouts: s.Timeouts,

		TotalConns: s.TotalConns,
		FreeConns:  s.FreeConns,
	}
}
