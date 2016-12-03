package redis // import "gopkg.in/redis.v5"

import (
	"fmt"
	"log"
	"time"

	"gopkg.in/redis.v5/internal"
	"gopkg.in/redis.v5/internal/pool"
)

// Redis nil reply, .e.g. when key does not exist.
const Nil = internal.Nil

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

type baseClient struct {
	connPool pool.Pooler
	opt      *Options

	process func(Cmder) error
	onClose func() error // hook called when client is closed
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Redis<%s db:%d>", c.getAddr(), c.opt.DB)
}

func (c *baseClient) conn() (*pool.Conn, bool, error) {
	cn, isNew, err := c.connPool.Get()
	if err != nil {
		return nil, false, err
	}
	if !cn.Inited {
		if err := c.initConn(cn); err != nil {
			_ = c.connPool.Remove(cn, err)
			return nil, false, err
		}
	}
	return cn, isNew, nil
}

func (c *baseClient) putConn(cn *pool.Conn, err error, allowTimeout bool) bool {
	if internal.IsBadConn(err, allowTimeout) {
		_ = c.connPool.Remove(cn, err)
		return false
	}

	_ = c.connPool.Put(cn)
	return true
}

func (c *baseClient) initConn(cn *pool.Conn) error {
	cn.Inited = true

	if c.opt.Password == "" && c.opt.DB == 0 && !c.opt.ReadOnly {
		return nil
	}

	// Temp client for Auth and Select.
	client := newClient(c.opt, pool.NewSingleConnPool(cn))
	_, err := client.Pipelined(func(pipe *Pipeline) error {
		if c.opt.Password != "" {
			pipe.Auth(c.opt.Password)
		}

		if c.opt.DB > 0 {
			pipe.Select(c.opt.DB)
		}

		if c.opt.ReadOnly {
			pipe.ReadOnly()
		}

		return nil
	})
	return err
}

func (c *baseClient) Process(cmd Cmder) error {
	if c.process != nil {
		return c.process(cmd)
	}
	return c.defaultProcess(cmd)
}

// WrapProcess replaces the process func. It takes a function createWrapper
// which is supplied by the user. createWrapper takes the old process func as
// an input and returns the new wrapper process func. createWrapper should
// use call the old process func within the new process func.
func (c *baseClient) WrapProcess(fn func(oldProcess func(cmd Cmder) error) func(cmd Cmder) error) {
	c.process = fn(c.defaultProcess)
}

func (c *baseClient) defaultProcess(cmd Cmder) error {
	for i := 0; i <= c.opt.MaxRetries; i++ {
		if i > 0 {
			cmd.reset()
		}

		cn, _, err := c.conn()
		if err != nil {
			cmd.setErr(err)
			return err
		}

		cn.SetWriteTimeout(c.opt.WriteTimeout)
		if err := writeCmd(cn, cmd); err != nil {
			c.putConn(cn, err, false)
			cmd.setErr(err)
			if err != nil && internal.IsRetryableError(err) {
				continue
			}
			return err
		}

		cn.SetReadTimeout(c.cmdTimeout(cmd))
		err = cmd.readReply(cn)
		c.putConn(cn, err, false)
		if err != nil && internal.IsRetryableError(err) {
			continue
		}

		return err
	}

	return cmd.Err()
}

func (c *baseClient) cmdTimeout(cmd Cmder) time.Duration {
	if timeout := cmd.readTimeout(); timeout != nil {
		return *timeout
	} else {
		return c.opt.ReadTimeout
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
	var firstErr error
	if c.onClose != nil {
		if err := c.onClose(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := c.connPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (c *baseClient) getAddr() string {
	return c.opt.Addr
}

//------------------------------------------------------------------------------

// Client is a Redis client representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type Client struct {
	baseClient
	cmdable
}

var _ Cmdable = (*Client)(nil)

func newClient(opt *Options, pool pool.Pooler) *Client {
	base := baseClient{opt: opt, connPool: pool}
	client := &Client{
		baseClient: base,
		cmdable:    cmdable{base.Process},
	}
	return client
}

// NewClient returns a client to the Redis Server specified by Options.
func NewClient(opt *Options) *Client {
	opt.init()
	return newClient(opt, newConnPool(opt))
}

// PoolStats returns connection pool stats.
func (c *Client) PoolStats() *PoolStats {
	s := c.connPool.Stats()
	return &PoolStats{
		Requests: s.Requests,
		Hits:     s.Hits,
		Timeouts: s.Timeouts,

		TotalConns: s.TotalConns,
		FreeConns:  s.FreeConns,
	}
}

func (c *Client) Pipeline() *Pipeline {
	pipe := Pipeline{
		exec: c.pipelineExec,
	}
	pipe.cmdable.process = pipe.Process
	pipe.statefulCmdable.process = pipe.Process
	return &pipe
}

func (c *Client) Pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	return c.Pipeline().pipelined(fn)
}

func (c *Client) pipelineExec(cmds []Cmder) error {
	var firstErr error
	for i := 0; i <= c.opt.MaxRetries; i++ {
		if i > 0 {
			resetCmds(cmds)
		}

		cn, _, err := c.conn()
		if err != nil {
			setCmdsErr(cmds, err)
			return err
		}

		retry, err := c.execCmds(cn, cmds)
		c.putConn(cn, err, false)
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if !retry {
			break
		}
	}
	return firstErr
}

func (c *Client) execCmds(cn *pool.Conn, cmds []Cmder) (retry bool, firstErr error) {
	cn.SetWriteTimeout(c.opt.WriteTimeout)
	if err := writeCmd(cn, cmds...); err != nil {
		setCmdsErr(cmds, err)
		return true, err
	}

	// Set read timeout for all commands.
	cn.SetReadTimeout(c.opt.ReadTimeout)

	for i, cmd := range cmds {
		err := cmd.readReply(cn)
		if err == nil {
			continue
		}
		if i == 0 && internal.IsNetworkError(err) {
			return true, err
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return false, firstErr
}

func (c *Client) pubSub() *PubSub {
	return &PubSub{
		base: baseClient{
			opt:      c.opt,
			connPool: pool.NewStickyConnPool(c.connPool.(*pool.ConnPool), false),
		},
	}
}

// Subscribe subscribes the client to the specified channels.
func (c *Client) Subscribe(channels ...string) (*PubSub, error) {
	pubsub := c.pubSub()
	return pubsub, pubsub.Subscribe(channels...)
}

// PSubscribe subscribes the client to the given patterns.
func (c *Client) PSubscribe(channels ...string) (*PubSub, error) {
	pubsub := c.pubSub()
	return pubsub, pubsub.PSubscribe(channels...)
}
