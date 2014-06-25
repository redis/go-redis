package redis

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
)

type baseClient struct {
	opt  *options
	cmds []Cmder

	connPool     pool
	connPoolsMtx sync.Mutex
	connPools    map[string]pool
}

func (c *baseClient) writeCmd(cn *conn, cmds ...Cmder) error {
	buf := make([]byte, 0, 64)
	for _, cmd := range cmds {
		buf = appendArgs(buf, cmd.Args())
	}

	_, err := cn.Write(buf)
	return err
}

func (c *baseClient) connFromPool(p pool, opt *options) (*conn, error) {
	cn, isNew, err := p.Get()
	if err != nil {
		return nil, err
	}

	if isNew {
		if err = c.initConn(p, cn, opt); err != nil {
			c.removeConn(cn)
			return nil, err
		}
	}

	return cn, nil
}

func (c *baseClient) initConn(p pool, cn *conn, opt *options) error {
	if opt.Password == "" || opt.DB == 0 {
		return nil
	}

	pp := newSingleConnPool(p, false)
	pp.SetConn(cn)

	// Client is not closed because we want to reuse underlying connection.
	client := &Client{
		baseClient: &baseClient{
			opt:      opt,
			connPool: pp,
		},
	}

	if opt.Password != "" {
		if err := client.Auth(opt.Password).Err(); err != nil {
			return err
		}
	}

	if opt.DB > 0 {
		if err := client.Select(opt.DB).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (c *baseClient) conn(cmds ...Cmder) (*conn, error) {
	if c.connPools != nil {
		return c.shardConn(cmds...)
	}
	return c.connFromPool(c.connPool, c.opt)
}

func (c *baseClient) shardConn(cmds ...Cmder) (*conn, error) {
	shard, err := c.opt.Shard(cmds)
	if err != nil {
		return nil, err
	}
	switch opt := shard.(type) {
	case *Options:
		return c.connFromPool(c.getConnPool(opt), opt.options())
	default:
		return nil, fmt.Errorf("redis: unsupported shard type: %v", shard)
	}
}

func (c *baseClient) getConnPool(shardOpt *Options) pool {
	shardId := shardOpt.shardId()
	opt := shardOpt.options()

	c.connPoolsMtx.Lock()
	pool, ok := c.connPools[shardId]
	if !ok {
		dialer := func() (net.Conn, error) {
			return net.DialTimeout("tcp", shardOpt.Addr, opt.DialTimeout)
		}
		pool = newConnPool(newConnFunc(dialer), opt)
		c.connPools[shardId] = pool
	}
	c.connPoolsMtx.Unlock()

	return pool
}

func (c *baseClient) freeConn(cn *conn, ei error) error {
	if cn.rd.Buffered() > 0 {
		return cn.pool.Remove(cn)
	}
	if _, ok := ei.(redisError); ok {
		return cn.pool.Put(cn)
	}
	return cn.pool.Remove(cn)
}

func (c *baseClient) removeConn(cn *conn) {
	if err := cn.pool.Remove(cn); err != nil {
		glog.Errorf("pool.Remove failed: %s", err)
	}
}

func (c *baseClient) putConn(cn *conn) {
	if err := cn.pool.Put(cn); err != nil {
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
	cn, err := c.conn(cmd)
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

type options struct {
	Password string
	DB       int64

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize    int
	IdleTimeout time.Duration

	Shard func(cmds []Cmder) (interface{}, error)
}

type Options struct {
	Addr     string
	Password string
	DB       int64

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize    int
	IdleTimeout time.Duration

	Shard func(cmds []Cmder) (interface{}, error)
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

func (opt *Options) options() *options {
	return &options{
		DB:       opt.DB,
		Password: opt.Password,

		DialTimeout:  opt.getDialTimeout(),
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:    opt.getPoolSize(),
		IdleTimeout: opt.IdleTimeout,

		Shard: opt.Shard,
	}
}

func (opt *Options) shardId() string {
	return opt.Addr + opt.Password + strconv.FormatInt(opt.DB, 10)
}

type Client struct {
	*baseClient
}

func newClient(clOpt *Options, network string) *Client {
	opt := clOpt.options()
	dialer := func() (net.Conn, error) {
		return net.DialTimeout(network, clOpt.Addr, opt.DialTimeout)
	}
	return &Client{
		baseClient: &baseClient{
			opt:      opt,
			connPool: newConnPool(newConnFunc(dialer), opt),
		},
	}
}

func NewTCPClient(opt *Options) *Client {
	return newClient(opt, "tcp")
}

func NewUnixClient(opt *Options) *Client {
	return newClient(opt, "unix")
}

//------------------------------------------------------------------------------

type ShardingOptions struct {
	Shard func(cmds []Cmder) (interface{}, error)
}

func (opt *ShardingOptions) options() *options {
	return &options{
		Shard: opt.Shard,
	}
}

func NewShardingClient(shardingOpt *ShardingOptions) *Client {
	return &Client{
		baseClient: &baseClient{
			opt:       shardingOpt.options(),
			connPools: make(map[string]pool),
		},
	}
}
