package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v9/internal"
	"github.com/go-redis/redis/v9/internal/pool"
	"github.com/go-redis/redis/v9/internal/proto"
)

// Nil reply returned by Redis when key does not exist.
const Nil = proto.Nil

// SetLogger set custom log
func SetLogger(logger internal.Logging) {
	internal.Logger = logger
}

//------------------------------------------------------------------------------

type Hook interface {
	DialHook(hook DialHook) DialHook
	ProcessHook(hook ProcessHook) ProcessHook
	ProcessPipelineHook(hook ProcessPipelineHook) ProcessPipelineHook
}

type (
	DialHook            func(ctx context.Context, network, addr string) (net.Conn, error)
	ProcessHook         func(ctx context.Context, cmd Cmder) error
	ProcessPipelineHook func(ctx context.Context, cmds []Cmder) error
)

type hooks struct {
	slice                 []Hook
	dialHook              DialHook
	processHook           ProcessHook
	processPipelineHook   ProcessPipelineHook
	processTxPipelineHook ProcessPipelineHook
}

func (hs *hooks) AddHook(hook Hook) {
	hs.slice = append(hs.slice, hook)
	hs.dialHook = hook.DialHook(hs.dialHook)
	hs.processHook = hook.ProcessHook(hs.processHook)
	hs.processPipelineHook = hook.ProcessPipelineHook(hs.processPipelineHook)
	hs.processTxPipelineHook = hook.ProcessPipelineHook(hs.processTxPipelineHook)
}

func (hs *hooks) clone() hooks {
	clone := *hs
	l := len(clone.slice)
	clone.slice = clone.slice[:l:l]
	return clone
}

func (hs *hooks) setDial(dial DialHook) {
	hs.dialHook = dial
	for _, h := range hs.slice {
		if wrapped := h.DialHook(hs.dialHook); wrapped != nil {
			hs.dialHook = wrapped
		}
	}
}

func (hs *hooks) setProcess(process ProcessHook) {
	hs.processHook = process
	for _, h := range hs.slice {
		if wrapped := h.ProcessHook(hs.processHook); wrapped != nil {
			hs.processHook = wrapped
		}
	}
}

func (hs *hooks) setProcessPipeline(processPipeline ProcessPipelineHook) {
	hs.processPipelineHook = processPipeline
	for _, h := range hs.slice {
		if wrapped := h.ProcessPipelineHook(hs.processPipelineHook); wrapped != nil {
			hs.processPipelineHook = wrapped
		}
	}
}

func (hs *hooks) setProcessTxPipeline(processTxPipeline ProcessPipelineHook) {
	hs.processTxPipelineHook = processTxPipeline
	for _, h := range hs.slice {
		if wrapped := h.ProcessPipelineHook(hs.processTxPipelineHook); wrapped != nil {
			hs.processTxPipelineHook = wrapped
		}
	}
}

func (hs *hooks) withProcessHook(ctx context.Context, cmd Cmder, hook ProcessHook) error {
	for _, h := range hs.slice {
		if wrapped := h.ProcessHook(hook); wrapped != nil {
			hook = wrapped
		}
	}
	return hook(ctx, cmd)
}

func (hs *hooks) withProcessPipelineHook(
	ctx context.Context, cmds []Cmder, hook ProcessPipelineHook,
) error {
	for _, h := range hs.slice {
		if wrapped := h.ProcessPipelineHook(hook); wrapped != nil {
			hook = wrapped
		}
	}
	return hook(ctx, cmds)
}

func (hs *hooks) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	return hs.dialHook(ctx, network, addr)
}

func (hs *hooks) process(ctx context.Context, cmd Cmder) error {
	return hs.processHook(ctx, cmd)
}

func (hs *hooks) processPipeline(ctx context.Context, cmds []Cmder) error {
	return hs.processPipelineHook(ctx, cmds)
}

func (hs *hooks) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	return hs.processTxPipelineHook(ctx, cmds)
}

//------------------------------------------------------------------------------

type baseClient struct {
	opt      *Options
	connPool pool.Pooler

	onClose func() error // hook called when client is closed
}

func (c *baseClient) clone() *baseClient {
	clone := *c
	return &clone
}

func (c *baseClient) withTimeout(timeout time.Duration) *baseClient {
	opt := c.opt.clone()
	opt.ReadTimeout = timeout
	opt.WriteTimeout = timeout

	clone := c.clone()
	clone.opt = opt

	return clone
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Redis<%s db:%d>", c.getAddr(), c.opt.DB)
}

func (c *baseClient) newConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := c.connPool.NewConn(ctx)
	if err != nil {
		return nil, err
	}

	err = c.initConn(ctx, cn)
	if err != nil {
		_ = c.connPool.CloseConn(cn)
		return nil, err
	}

	return cn, nil
}

func (c *baseClient) getConn(ctx context.Context) (*pool.Conn, error) {
	if c.opt.Limiter != nil {
		err := c.opt.Limiter.Allow()
		if err != nil {
			return nil, err
		}
	}

	cn, err := c._getConn(ctx)
	if err != nil {
		if c.opt.Limiter != nil {
			c.opt.Limiter.ReportResult(err)
		}
		return nil, err
	}

	return cn, nil
}

func (c *baseClient) _getConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := c.connPool.Get(ctx)
	if err != nil {
		return nil, err
	}

	if cn.Inited {
		return cn, nil
	}

	if err := c.initConn(ctx, cn); err != nil {
		c.connPool.Remove(ctx, cn, err)
		if err := errors.Unwrap(err); err != nil {
			return nil, err
		}
		return nil, err
	}

	return cn, nil
}

func (c *baseClient) initConn(ctx context.Context, cn *pool.Conn) error {
	if cn.Inited {
		return nil
	}
	cn.Inited = true

	username, password := c.opt.Username, c.opt.Password
	if c.opt.CredentialsProvider != nil {
		username, password = c.opt.CredentialsProvider()
	}

	connPool := pool.NewSingleConnPool(c.connPool, cn)
	conn := newConn(c.opt, connPool)

	var auth bool

	// For redis-server < 6.0 that does not support the Hello command,
	// we continue to provide services with RESP2.
	if err := conn.Hello(ctx, 3, username, password, "").Err(); err == nil {
		auth = true
	} else if !strings.HasPrefix(err.Error(), "ERR unknown command") {
		return err
	}

	_, err := conn.Pipelined(ctx, func(pipe Pipeliner) error {
		if !auth && password != "" {
			if username != "" {
				pipe.AuthACL(ctx, username, password)
			} else {
				pipe.Auth(ctx, password)
			}
		}

		if c.opt.DB > 0 {
			pipe.Select(ctx, c.opt.DB)
		}

		if c.opt.readOnly {
			pipe.ReadOnly(ctx)
		}

		return nil
	})
	if err != nil {
		return err
	}

	if c.opt.OnConnect != nil {
		return c.opt.OnConnect(ctx, conn)
	}
	return nil
}

func (c *baseClient) releaseConn(ctx context.Context, cn *pool.Conn, err error) {
	if c.opt.Limiter != nil {
		c.opt.Limiter.ReportResult(err)
	}

	if isBadConn(err, false, c.opt.Addr) {
		c.connPool.Remove(ctx, cn, err)
	} else {
		c.connPool.Put(ctx, cn)
	}
}

func (c *baseClient) withConn(
	ctx context.Context, fn func(context.Context, *pool.Conn) error,
) error {
	cn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	err = fn(ctx, cn)
	c.releaseConn(ctx, cn, err)
	return err
}

func (c *baseClient) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	return c.opt.Dialer(ctx, network, addr)
}

func (c *baseClient) process(ctx context.Context, cmd Cmder) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRetries; attempt++ {
		attempt := attempt

		retry, err := c._process(ctx, cmd, attempt)
		if err == nil || !retry {
			return err
		}

		lastErr = err
	}
	return lastErr
}

func (c *baseClient) _process(ctx context.Context, cmd Cmder, attempt int) (bool, error) {
	if attempt > 0 {
		if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
			return false, err
		}
	}

	retryTimeout := uint32(0)
	if err := c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
			return writeCmd(wr, cmd)
		}); err != nil {
			atomic.StoreUint32(&retryTimeout, 1)
			return err
		}

		if err := cn.WithReader(c.context(ctx), c.cmdTimeout(cmd), cmd.readReply); err != nil {
			if cmd.readTimeout() == nil {
				atomic.StoreUint32(&retryTimeout, 1)
			} else {
				atomic.StoreUint32(&retryTimeout, 0)
			}
			return err
		}

		return nil
	}); err != nil {
		retry := shouldRetry(err, atomic.LoadUint32(&retryTimeout) == 1)
		return retry, err
	}

	return false, nil
}

func (c *baseClient) retryBackoff(attempt int) time.Duration {
	return internal.RetryBackoff(attempt, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
}

func (c *baseClient) cmdTimeout(cmd Cmder) time.Duration {
	if timeout := cmd.readTimeout(); timeout != nil {
		t := *timeout
		if t == 0 {
			return 0
		}
		return t + 10*time.Second
	}
	return c.opt.ReadTimeout
}

// Close closes the client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *baseClient) Close() error {
	var firstErr error
	if c.onClose != nil {
		if err := c.onClose(); err != nil {
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

func (c *baseClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	if err := c.generalProcessPipeline(ctx, cmds, c.pipelineProcessCmds); err != nil {
		return err
	}
	return cmdsFirstErr(cmds)
}

func (c *baseClient) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	if err := c.generalProcessPipeline(ctx, cmds, c.txPipelineProcessCmds); err != nil {
		return err
	}
	return cmdsFirstErr(cmds)
}

type pipelineProcessor func(context.Context, *pool.Conn, []Cmder) (bool, error)

func (c *baseClient) generalProcessPipeline(
	ctx context.Context, cmds []Cmder, p pipelineProcessor,
) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				setCmdsErr(cmds, err)
				return err
			}
		}

		// Enable retries by default to retry dial errors returned by withConn.
		canRetry := true
		lastErr = c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			var err error
			canRetry, err = p(ctx, cn, cmds)
			return err
		})
		if lastErr == nil || !canRetry || !shouldRetry(lastErr, true) {
			return lastErr
		}
	}
	return lastErr
}

func (c *baseClient) pipelineProcessCmds(
	ctx context.Context, cn *pool.Conn, cmds []Cmder,
) (bool, error) {
	if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmds(wr, cmds)
	}); err != nil {
		setCmdsErr(cmds, err)
		return true, err
	}

	if err := cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
		return pipelineReadCmds(rd, cmds)
	}); err != nil {
		return true, err
	}

	return false, nil
}

func pipelineReadCmds(rd *proto.Reader, cmds []Cmder) error {
	for i, cmd := range cmds {
		err := cmd.readReply(rd)
		cmd.SetErr(err)
		if err != nil && !isRedisError(err) {
			setCmdsErr(cmds[i+1:], err)
			return err
		}
	}
	// Retry errors like "LOADING redis is loading the dataset in memory".
	return cmds[0].Err()
}

func (c *baseClient) txPipelineProcessCmds(
	ctx context.Context, cn *pool.Conn, cmds []Cmder,
) (bool, error) {
	if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmds(wr, cmds)
	}); err != nil {
		setCmdsErr(cmds, err)
		return true, err
	}

	if err := cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
		statusCmd := cmds[0].(*StatusCmd)
		// Trim multi and exec.
		trimmedCmds := cmds[1 : len(cmds)-1]

		if err := txPipelineReadQueued(rd, statusCmd, trimmedCmds); err != nil {
			setCmdsErr(cmds, err)
			return err
		}

		return pipelineReadCmds(rd, trimmedCmds)
	}); err != nil {
		return false, err
	}

	return false, nil
}

func txPipelineReadQueued(rd *proto.Reader, statusCmd *StatusCmd, cmds []Cmder) error {
	// Parse +OK.
	if err := statusCmd.readReply(rd); err != nil {
		return err
	}

	// Parse +QUEUED.
	for range cmds {
		if err := statusCmd.readReply(rd); err != nil && !isRedisError(err) {
			return err
		}
	}

	// Parse number of replies.
	line, err := rd.ReadLine()
	if err != nil {
		if err == Nil {
			err = TxFailedErr
		}
		return err
	}

	if line[0] != proto.RespArray {
		return fmt.Errorf("redis: expected '*', but got line %q", line)
	}

	return nil
}

func (c *baseClient) context(ctx context.Context) context.Context {
	if c.opt.ContextTimeoutEnabled {
		return ctx
	}
	return context.Background()
}

//------------------------------------------------------------------------------

// Client is a Redis client representing a pool of zero or more underlying connections.
// It's safe for concurrent use by multiple goroutines.
//
// Client creates and frees connections automatically; it also maintains a free pool
// of idle connections. You can control the pool size with Config.PoolSize option.
type Client struct {
	*baseClient
	cmdable
	hooks
}

// NewClient returns a client to the Redis Server specified by Options.
func NewClient(opt *Options) *Client {
	opt.init()

	c := Client{
		baseClient: &baseClient{
			opt: opt,
		},
	}
	c.init()
	c.connPool = newConnPool(opt, c.hooks.dial)

	return &c
}

func (c *Client) init() {
	c.cmdable = c.Process
	c.hooks.setDial(c.baseClient.dial)
	c.hooks.setProcess(c.baseClient.process)
	c.hooks.setProcessPipeline(c.baseClient.processPipeline)
	c.hooks.setProcessTxPipeline(c.baseClient.processTxPipeline)
}

func (c *Client) WithTimeout(timeout time.Duration) *Client {
	clone := *c
	clone.baseClient = c.baseClient.withTimeout(timeout)
	clone.init()
	return &clone
}

func (c *Client) Conn() *Conn {
	return newConn(c.opt, pool.NewStickyConnPool(c.connPool))
}

// Do creates a Cmd from the args and processes the cmd.
func (c *Client) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *Client) Process(ctx context.Context, cmd Cmder) error {
	err := c.hooks.process(ctx, cmd)
	cmd.SetErr(err)
	return err
}

// Options returns read-only Options that were used to create the client.
func (c *Client) Options() *Options {
	return c.opt
}

type PoolStats pool.Stats

// PoolStats returns connection pool stats.
func (c *Client) PoolStats() *PoolStats {
	stats := c.connPool.Stats()
	return (*PoolStats)(stats)
}

func (c *Client) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Client) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: pipelineExecer(c.hooks.processPipeline),
	}
	pipe.init()
	return &pipe
}

func (c *Client) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *Client) TxPipeline() Pipeliner {
	pipe := Pipeline{
		exec: func(ctx context.Context, cmds []Cmder) error {
			cmds = wrapMultiExec(ctx, cmds)
			return c.hooks.processTxPipeline(ctx, cmds)
		},
	}
	pipe.init()
	return &pipe
}

func (c *Client) pubSub() *PubSub {
	pubsub := &PubSub{
		opt: c.opt,

		newConn: func(ctx context.Context, channels []string) (*pool.Conn, error) {
			return c.newConn(ctx)
		},
		closeConn: c.connPool.CloseConn,
	}
	pubsub.init()
	return pubsub
}

// Subscribe subscribes the client to the specified channels.
// Channels can be omitted to create empty subscription.
// Note that this method does not wait on a response from Redis, so the
// subscription may not be active immediately. To force the connection to wait,
// you may call the Receive() method on the returned *PubSub like so:
//
//    sub := client.Subscribe(queryResp)
//    iface, err := sub.Receive()
//    if err != nil {
//        // handle error
//    }
//
//    // Should be *Subscription, but others are possible if other actions have been
//    // taken on sub since it was created.
//    switch iface.(type) {
//    case *Subscription:
//        // subscribe succeeded
//    case *Message:
//        // received first message
//    case *Pong:
//        // pong received
//    default:
//        // handle error
//    }
//
//    ch := sub.Channel()
func (c *Client) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (c *Client) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.PSubscribe(ctx, channels...)
	}
	return pubsub
}

// SSubscribe Subscribes the client to the specified shard channels.
// Channels can be omitted to create empty subscription.
func (c *Client) SSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.SSubscribe(ctx, channels...)
	}
	return pubsub
}

//------------------------------------------------------------------------------

// Conn represents a single Redis connection rather than a pool of connections.
// Prefer running commands from Client unless there is a specific need
// for a continuous single Redis connection.
type Conn struct {
	baseClient
	cmdable
	statefulCmdable
	hooks
}

func newConn(opt *Options, connPool pool.Pooler) *Conn {
	c := Conn{
		baseClient: baseClient{
			opt:      opt,
			connPool: connPool,
		},
	}

	c.cmdable = c.Process
	c.statefulCmdable = c.Process

	c.hooks.setDial(c.baseClient.dial)
	c.hooks.setProcess(c.baseClient.process)
	c.hooks.setProcessPipeline(c.baseClient.processPipeline)
	c.hooks.setProcessTxPipeline(c.baseClient.processTxPipeline)

	return &c
}

func (c *Conn) Process(ctx context.Context, cmd Cmder) error {
	err := c.hooks.process(ctx, cmd)
	cmd.SetErr(err)
	return err
}

func (c *Conn) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Conn) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: c.hooks.processPipeline,
	}
	pipe.init()
	return &pipe
}

func (c *Conn) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *Conn) TxPipeline() Pipeliner {
	pipe := Pipeline{
		exec: func(ctx context.Context, cmds []Cmder) error {
			cmds = wrapMultiExec(ctx, cmds)
			return c.hooks.processTxPipeline(ctx, cmds)
		},
	}
	pipe.init()
	return &pipe
}
