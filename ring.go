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

func (ring *Ring) cmdInfo(name string) *CommandInfo {
	ring.cmdsInfoOnce.Do(func() {
		for _, shard := range ring.shards {
			cmdsInfo, err := shard.Client.Command().Result()
			if err == nil {
				ring.cmdsInfo = cmdsInfo
				return
			}
		}
		ring.cmdsInfoOnce = &sync.Once{}
	})
	if ring.cmdsInfo == nil {
		return nil
	}
	return ring.cmdsInfo[name]
}

func (ring *Ring) cmdFirstKey(cmd Cmder) string {
	cmdInfo := ring.cmdInfo(cmd.arg(0))
	if cmdInfo == nil {
		return ""
	}
	return cmd.arg(int(cmdInfo.FirstKeyPos))
}

func (ring *Ring) addClient(name string, cl *Client) {
	ring.mu.Lock()
	ring.hash.Add(name)
	ring.shards[name] = &ringShard{Client: cl}
	ring.mu.Unlock()
}

func (ring *Ring) getClient(key string) (*Client, error) {
	ring.mu.RLock()

	if ring.closed {
		return nil, pool.ErrClosed
	}

	name := ring.hash.Get(hashtag.Key(key))
	if name == "" {
		ring.mu.RUnlock()
		return nil, errRingShardsDown
	}

	cl := ring.shards[name].Client
	ring.mu.RUnlock()
	return cl, nil
}

func (ring *Ring) Process(cmd Cmder) {
	cl, err := ring.getClient(ring.cmdFirstKey(cmd))
	if err != nil {
		cmd.setErr(err)
		return
	}
	cl.baseClient.Process(cmd)
}

// rebalance removes dead shards from the ring.
func (ring *Ring) rebalance() {
	defer ring.mu.Unlock()
	ring.mu.Lock()

	ring.hash = consistenthash.New(ring.nreplicas, nil)
	for name, shard := range ring.shards {
		if shard.IsUp() {
			ring.hash.Add(name)
		}
	}
}

// heartbeat monitors state of each shard in the ring.
func (ring *Ring) heartbeat() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for _ = range ticker.C {
		var rebalance bool

		ring.mu.RLock()

		if ring.closed {
			ring.mu.RUnlock()
			break
		}

		for _, shard := range ring.shards {
			err := shard.Client.Ping().Err()
			if shard.Vote(err == nil || err == pool.ErrPoolTimeout) {
				internal.Logf("ring shard state changed: %s", shard)
				rebalance = true
			}
		}

		ring.mu.RUnlock()

		if rebalance {
			ring.rebalance()
		}
	}
}

// Close closes the ring client, releasing any open resources.
//
// It is rare to Close a Ring, as the Ring is meant to be long-lived
// and shared between many goroutines.
func (ring *Ring) Close() (retErr error) {
	defer ring.mu.Unlock()
	ring.mu.Lock()

	if ring.closed {
		return nil
	}
	ring.closed = true

	for _, shard := range ring.shards {
		if err := shard.Client.Close(); err != nil {
			retErr = err
		}
	}
	ring.hash = nil
	ring.shards = nil

	return retErr
}

func (ring *Ring) Pipeline() *Pipeline {
	pipe := Pipeline{
		exec: ring.pipelineExec,
	}
	pipe.cmdable.process = pipe.Process
	pipe.statefulCmdable.process = pipe.Process
	return &pipe
}

func (ring *Ring) Pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	return ring.Pipeline().pipelined(fn)
}

func (ring *Ring) pipelineExec(cmds []Cmder) error {
	var retErr error

	cmdsMap := make(map[string][]Cmder)
	for _, cmd := range cmds {
		name := ring.hash.Get(hashtag.Key(ring.cmdFirstKey(cmd)))
		if name == "" {
			cmd.setErr(errRingShardsDown)
			if retErr == nil {
				retErr = errRingShardsDown
			}
			continue
		}
		cmdsMap[name] = append(cmdsMap[name], cmd)
	}

	for i := 0; i <= ring.opt.MaxRetries; i++ {
		failedCmdsMap := make(map[string][]Cmder)

		for name, cmds := range cmdsMap {
			client := ring.shards[name].Client
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
