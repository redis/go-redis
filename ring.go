package redis

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"gopkg.in/redis.v4/internal"
	"gopkg.in/redis.v4/internal/consistenthash"
	"gopkg.in/redis.v4/internal/hashtag"
	"gopkg.in/redis.v4/internal/pool"
)

var (
	errRingShardsDown = errors.New("redis: all ring shards are down")
)

// RingOptions are used to configure a ring client and should be
// passed to NewRing.
type RingOptions struct {
	// A map of name => host:port addresses of ring shards.
	Addrs map[string]string

	// Following options are copied from Options struct.

	DB       int
	Password string

	MaxRetries int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

func (opt *RingOptions) init() {}

func (opt *RingOptions) clientOptions() *Options {
	return &Options{
		DB:       opt.DB,
		Password: opt.Password,

		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:           opt.PoolSize,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: opt.IdleCheckFrequency,
	}
}

type ringShard struct {
	Client *Client
	down   int
}

func (shard *ringShard) String() string {
	var state string
	if shard.IsUp() {
		state = "up"
	} else {
		state = "down"
	}
	return fmt.Sprintf("%s is %s", shard.Client, state)
}

func (shard *ringShard) IsDown() bool {
	const threshold = 5
	return shard.down >= threshold
}

func (shard *ringShard) IsUp() bool {
	return !shard.IsDown()
}

// Vote votes to set shard state and returns true if state was changed.
func (shard *ringShard) Vote(up bool) bool {
	if up {
		changed := shard.IsDown()
		shard.down = 0
		return changed
	}

	if shard.IsDown() {
		return false
	}

	shard.down++
	return shard.IsDown()
}

// Ring is a Redis client that uses constistent hashing to distribute
// keys across multiple Redis servers (shards). It's safe for
// concurrent use by multiple goroutines.
//
// Ring monitors the state of each shard and removes dead shards from
// the ring. When shard comes online it is added back to the ring. This
// gives you maximum availability and partition tolerance, but no
// consistency between different shards or even clients. Each client
// uses shards that are available to the client and does not do any
// coordination when shard state is changed.
//
// Ring should be used when you use multiple Redis servers for caching
// and can tolerate losing data when one of the servers dies.
// Otherwise you should use Redis Cluster.
type Ring struct {
	cmdable

	opt       *RingOptions
	nreplicas int

	mu     sync.RWMutex
	hash   *consistenthash.Map
	shards map[string]*ringShard

	cmdsInfo     map[string]*CommandInfo
	cmdsInfoOnce *sync.Once

	closed bool
}

func NewRing(opt *RingOptions) *Ring {
	const nreplicas = 100
	opt.init()
	ring := &Ring{
		opt:       opt,
		nreplicas: nreplicas,

		hash:   consistenthash.New(nreplicas, nil),
		shards: make(map[string]*ringShard),

		cmdsInfoOnce: new(sync.Once),
	}
	ring.cmdable.process = ring.Process
	for name, addr := range opt.Addrs {
		clopt := opt.clientOptions()
		clopt.Addr = addr
		ring.addClient(name, NewClient(clopt))
	}
	go ring.heartbeat()
	return ring
}

func (c *Ring) cmdInfo(name string) *CommandInfo {
	c.cmdsInfoOnce.Do(func() {
		for _, shard := range c.shards {
			cmdsInfo, err := shard.Client.Command().Result()
			if err == nil {
				c.cmdsInfo = cmdsInfo
				return
			}
		}
		c.cmdsInfoOnce = &sync.Once{}
	})
	if c.cmdsInfo == nil {
		return nil
	}
	return c.cmdsInfo[name]
}

func (c *Ring) cmdFirstKey(cmd Cmder) string {
	cmdInfo := c.cmdInfo(cmd.arg(0))
	if cmdInfo == nil {
		internal.Logf("info for cmd=%s not found", cmd.arg(0))
		return ""
	}
	return cmd.arg(int(cmdInfo.FirstKeyPos))
}

func (c *Ring) addClient(name string, cl *Client) {
	c.mu.Lock()
	c.hash.Add(name)
	c.shards[name] = &ringShard{Client: cl}
	c.mu.Unlock()
}

func (c *Ring) getClient(key string) (*Client, error) {
	c.mu.RLock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	name := c.hash.Get(hashtag.Key(key))
	if name == "" {
		c.mu.RUnlock()
		return nil, errRingShardsDown
	}

	cl := c.shards[name].Client
	c.mu.RUnlock()
	return cl, nil
}

func (c *Ring) Process(cmd Cmder) error {
	cl, err := c.getClient(c.cmdFirstKey(cmd))
	if err != nil {
		cmd.setErr(err)
		return err
	}
	return cl.baseClient.Process(cmd)
}

// rebalance removes dead shards from the c.
func (c *Ring) rebalance() {
	defer c.mu.Unlock()
	c.mu.Lock()

	c.hash = consistenthash.New(c.nreplicas, nil)
	for name, shard := range c.shards {
		if shard.IsUp() {
			c.hash.Add(name)
		}
	}
}

// heartbeat monitors state of each shard in the ring.
func (c *Ring) heartbeat() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for _ = range ticker.C {
		var rebalance bool

		c.mu.RLock()

		if c.closed {
			c.mu.RUnlock()
			break
		}

		for _, shard := range c.shards {
			err := shard.Client.Ping().Err()
			if shard.Vote(err == nil || err == pool.ErrPoolTimeout) {
				internal.Logf("ring shard state changed: %s", shard)
				rebalance = true
			}
		}

		c.mu.RUnlock()

		if rebalance {
			c.rebalance()
		}
	}
}

// Close closes the ring client, releasing any open resources.
//
// It is rare to Close a Ring, as the Ring is meant to be long-lived
// and shared between many goroutines.
func (c *Ring) Close() (retErr error) {
	defer c.mu.Unlock()
	c.mu.Lock()

	if c.closed {
		return nil
	}
	c.closed = true

	for _, shard := range c.shards {
		if err := shard.Client.Close(); err != nil {
			retErr = err
		}
	}
	c.hash = nil
	c.shards = nil

	return retErr
}

func (c *Ring) Pipeline() *Pipeline {
	pipe := Pipeline{
		exec: c.pipelineExec,
	}
	pipe.cmdable.process = pipe.Process
	pipe.statefulCmdable.process = pipe.Process
	return &pipe
}

func (c *Ring) Pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	return c.Pipeline().pipelined(fn)
}

func (c *Ring) pipelineExec(cmds []Cmder) error {
	var retErr error

	cmdsMap := make(map[string][]Cmder)
	for _, cmd := range cmds {
		name := c.hash.Get(hashtag.Key(c.cmdFirstKey(cmd)))
		if name == "" {
			cmd.setErr(errRingShardsDown)
			if retErr == nil {
				retErr = errRingShardsDown
			}
			continue
		}
		cmdsMap[name] = append(cmdsMap[name], cmd)
	}

	for i := 0; i <= c.opt.MaxRetries; i++ {
		failedCmdsMap := make(map[string][]Cmder)

		for name, cmds := range cmdsMap {
			client := c.shards[name].Client
			cn, err := client.conn()
			if err != nil {
				setCmdsErr(cmds, err)
				if retErr == nil {
					retErr = err
				}
				continue
			}

			if i > 0 {
				resetCmds(cmds)
			}
			failedCmds, err := execCmds(cn, cmds)
			client.putConn(cn, err, false)
			if err != nil && retErr == nil {
				retErr = err
			}
			if len(failedCmds) > 0 {
				failedCmdsMap[name] = failedCmds
			}
		}

		if len(failedCmdsMap) == 0 {
			break
		}
		cmdsMap = failedCmdsMap
	}

	return retErr
}
