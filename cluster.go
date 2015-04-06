package redis

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterClient struct {
	commandable

	addrs   map[string]struct{}
	slots   [][]string
	slotsMx sync.RWMutex // protects slots & addrs cache

	conns   map[string]*Client
	connsMx sync.Mutex // protects conns

	opt *ClusterOptions

	_reload uint32
}

// NewClusterClient initializes a new cluster-aware client using given options.
// A list of seed addresses must be provided.
func NewClusterClient(opt *ClusterOptions) (*ClusterClient, error) {
	addrs, err := opt.getAddrSet()
	if err != nil {
		return nil, err
	}

	client := &ClusterClient{
		addrs:   addrs,
		conns:   make(map[string]*Client),
		opt:     opt,
		_reload: 1,
	}
	client.commandable.process = client.process
	client.reloadIfDue()
	go client.reaper(time.NewTicker(5 * time.Minute))
	return client, nil
}

// Close closes the cluster connection
func (c *ClusterClient) Close() error {
	c.slotsMx.Lock()
	defer c.slotsMx.Unlock()

	return c.reset()
}

// ------------------------------------------------------------------------

// Finds the current master address for a given hash slot
func (c *ClusterClient) getMasterAddrBySlot(hashSlot int) string {
	if addrs := c.slots[hashSlot]; len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

// Returns a node's client for a given address
func (c *ClusterClient) getNodeClientByAddr(addr string) *Client {
	c.connsMx.Lock()
	client, ok := c.conns[addr]
	if !ok {
		opt := c.opt.clientOptions()
		opt.Addr = addr
		client = NewTCPClient(opt)
		c.conns[addr] = client
	}
	c.connsMx.Unlock()
	return client
}

// Process a command
func (c *ClusterClient) process(cmd Cmder) {
	var ask bool

	c.reloadIfDue()

	hashSlot := hashSlot(cmd.clusterKey())

	c.slotsMx.RLock()
	defer c.slotsMx.RUnlock()

	tried := make(map[string]struct{}, len(c.addrs))
	addr := c.getMasterAddrBySlot(hashSlot)
	for attempt := 0; attempt <= c.opt.getMaxRedirects(); attempt++ {
		tried[addr] = struct{}{}

		// Pick the connection, process request
		conn := c.getNodeClientByAddr(addr)
		if ask {
			pipe := conn.Pipeline()
			pipe.Process(NewCmd("ASKING"))
			pipe.Process(cmd)
			_, _ = pipe.Exec()
			ask = false
		} else {
			conn.Process(cmd)
		}

		// If there is no (real) error, we are done!
		err := cmd.Err()
		if err == nil || err == Nil {
			return
		}

		// On connection errors, pick a random, previosuly untried connection
		// and request again.
		if _, ok := err.(*net.OpError); ok || err == io.EOF {
			if addr = c.findNextAddr(tried); addr == "" {
				return
			}
			cmd.reset()
			continue
		}

		// Check the error message, return if unexpected
		parts := strings.SplitN(err.Error(), " ", 3)
		if len(parts) != 3 {
			return
		}

		// Handle MOVE and ASK redirections, return on any other error
		switch parts[0] {
		case "MOVED":
			c.forceReload()
			addr = parts[2]
		case "ASK":
			ask = true
			addr = parts[2]
		default:
			return
		}
		cmd.reset()
	}
}

// Closes all connections and reloads slot cache, if due
func (c *ClusterClient) reloadIfDue() (err error) {
	if !atomic.CompareAndSwapUint32(&c._reload, 1, 0) {
		return
	}

	var infos []ClusterSlotInfo

	c.slotsMx.Lock()
	defer c.slotsMx.Unlock()

	// Try known addresses in random order (map interation order is random in Go)
	// http://redis.io/topics/cluster-spec#clients-first-connection-and-handling-of-redirections
	// https://github.com/antirez/redis-rb-cluster/blob/fd931ed/cluster.rb#L157
	for addr := range c.addrs {
		c.reset()

		infos, err = c.fetchClusterSlots(addr)
		if err == nil {
			c.update(infos)
			break
		}
	}
	return
}

// Closes all connections and flushes slots cache
func (c *ClusterClient) reset() (err error) {
	c.connsMx.Lock()
	for addr, client := range c.conns {
		if e := client.Close(); e != nil {
			err = e
		}
		delete(c.conns, addr)
	}
	c.connsMx.Unlock()
	c.slots = make([][]string, hashSlots)
	return
}

// Forces a cache reload on next request
func (c *ClusterClient) forceReload() {
	atomic.StoreUint32(&c._reload, 1)
}

// Find the next untried address
func (c *ClusterClient) findNextAddr(tried map[string]struct{}) string {
	for addr := range c.addrs {
		if _, ok := tried[addr]; !ok {
			return addr
		}
	}
	return ""
}

// Fetch slot information
func (c *ClusterClient) fetchClusterSlots(addr string) ([]ClusterSlotInfo, error) {
	opt := c.opt.clientOptions()
	opt.Addr = addr
	client := NewClient(opt)
	defer client.Close()

	return client.ClusterSlots().Result()
}

// Update slot information, populate slots
func (c *ClusterClient) update(infos []ClusterSlotInfo) {
	for _, info := range infos {
		for i := info.Start; i <= info.End; i++ {
			c.slots[i] = info.Addrs
		}

		for _, addr := range info.Addrs {
			c.addrs[addr] = struct{}{}
		}
	}
}

// reaper closes idle connections to the cluster.
func (c *ClusterClient) reaper(ticker *time.Ticker) {
	for _ = range ticker.C {
		for _, client := range c.conns {
			pool := client.connPool
			// pool.First removes idle connections from the pool for us. So
			// just put returned connection back.
			if cn := pool.First(); cn != nil {
				pool.Put(cn)
			}
		}
	}
}

//------------------------------------------------------------------------------

var errNoAddrs = errors.New("redis: no addresses")

type ClusterOptions struct {
	// A seed-list of host:port addresses of known cluster nodes
	Addrs []string

	// An optional password
	Password string

	// The maximum number of MOVED/ASK redirects to follow, before
	// giving up. Default: 16
	MaxRedirects int

	// The maximum number of TCP sockets per connection. Default: 5
	PoolSize int

	// Timeout settings
	DialTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
}

func (opt *ClusterOptions) getPoolSize() int {
	if opt.PoolSize < 1 {
		return 5
	}
	return opt.PoolSize
}

func (opt *ClusterOptions) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

func (opt *ClusterOptions) getMaxRedirects() int {
	if opt.MaxRedirects < 1 {
		return 16
	}
	return opt.MaxRedirects
}

func (opt *ClusterOptions) getAddrSet() (map[string]struct{}, error) {
	size := len(opt.Addrs)
	if size < 1 {
		return nil, errNoAddrs
	}

	addrs := make(map[string]struct{}, size)
	for _, addr := range opt.Addrs {
		addrs[addr] = struct{}{}
	}
	return addrs, nil
}

func (opt *ClusterOptions) clientOptions() *Options {
	return &Options{
		DB:       0,
		Password: opt.Password,

		DialTimeout:  opt.getDialTimeout(),
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:    opt.getPoolSize(),
		IdleTimeout: opt.IdleTimeout,
	}
}

//------------------------------------------------------------------------------

const hashSlots = 16384

// hashSlot returns a consistent slot number between 0 and 16383
// for any given string key.
func hashSlot(key string) int {
	if s := strings.IndexByte(key, '{'); s > -1 {
		if e := strings.IndexByte(key[s+1:], '}'); e > 0 {
			key = key[s+1 : s+e+1]
		}
	}
	if key == "" {
		return rand.Intn(hashSlots)
	}
	return int(crc16sum(key)) % hashSlots
}
