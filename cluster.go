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

	addrs []string
	slots [][]string
	conns *lruPool
	opt   *ClusterOptions

	cachemx sync.RWMutex
	_reload uint32
}

func NewClusterClient(opt *ClusterOptions) (*ClusterClient, error) {
	addrs, err := opt.getAddrs()
	if err != nil {
		return nil, err
	}

	client := &ClusterClient{
		addrs:   addrs,
		conns:   newLRUPool(opt),
		opt:     opt,
		_reload: 1,
	}
	client.commandable.process = client.process
	client.reloadIfDue()
	return client, nil
}

// Closes the pool
func (c *ClusterClient) Close() error {
	c.cachemx.Lock()
	defer c.cachemx.Unlock()

	return c.reset()
}

// GetMasterAddrBySlot finds the current master address for a given hash slot
func (c *ClusterClient) GetMasterAddrBySlot(hashSlot int) string {
	if addrs := c.slots[hashSlot]; len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

// GetSlaveAddrsBySlot finds the current slave addresses for a given hash slot
func (c *ClusterClient) GetSlaveAddrsBySlot(hashSlot int) []string {
	if addrs := c.slots[hashSlot]; len(addrs) > 0 {
		return addrs[1:]
	}
	return nil
}

// GetNodeClientByAddr returns a node's client for a given address
func (c *ClusterClient) GetNodeClientByAddr(addr string) *Client {
	return c.conns.Fetch(addr)
}

// ------------------------------------------------------------------------

func (c *ClusterClient) process(cmd Cmder) {
	var hashSlot int

	c.reloadIfDue()
	ask := false

	key := cmd.firstKey()
	if key != "" {
		hashSlot = HashSlot(key)
	} else {
		hashSlot = rand.Intn(hashSlots)
	}

	c.cachemx.RLock()
	defer c.cachemx.RUnlock()

	tried := make(map[string]struct{}, len(c.addrs))
	addr := c.GetMasterAddrBySlot(hashSlot)
	for attempt := 0; attempt < c.opt.getMaxRedirects(); attempt++ {
		tried[addr] = struct{}{}

		// Pick the connection, process request
		conn := c.GetNodeClientByAddr(addr)
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

		// On connection errors, pick the next (not previosuly) tried connection
		// and try again
		if _, ok := err.(*net.OpError); ok || err == io.EOF {
			if addr = c.next(tried); addr == "" {
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

	c.cachemx.Lock()
	defer c.cachemx.Unlock()

	for _, addr := range c.addrs {
		c.reset()

		infos, err = c.fetch(addr)
		if err == nil {
			c.update(infos)
			break
		}
	}
	return
}

// Closes all connections and flushes slots cache
func (c *ClusterClient) reset() error {
	err := c.conns.Clear()
	c.slots = make([][]string, hashSlots)
	return err
}

// Forces a cache reload on next request
func (c *ClusterClient) forceReload() {
	atomic.StoreUint32(&c._reload, 1)
}

// Find the next unseen address
func (c *ClusterClient) next(seen map[string]struct{}) string {
	for _, addr := range c.addrs {
		if _, ok := seen[addr]; !ok {
			return addr
		}
	}
	return ""
}

// Fetch slot information
func (c *ClusterClient) fetch(addr string) ([]ClusterSlotInfo, error) {
	client := NewClient(c.opt.ClientOptions(addr))
	defer client.Close()

	return client.ClusterSlots().Result()
}

// Update slot information
func (c *ClusterClient) update(infos []ClusterSlotInfo) {

	// Create a map of known nodes
	known := make(map[string]struct{}, len(c.addrs))
	for _, addr := range c.addrs {
		known[addr] = struct{}{}
	}

	// Populate slots, store unknown nodes
	for _, info := range infos {
		for i := info.Min; i <= info.Max; i++ {
			c.slots[i] = info.Addrs
		}

		for _, addr := range info.Addrs {
			if _, ok := known[addr]; !ok {
				c.addrs = append(c.addrs, addr)
				known[addr] = struct{}{}
			}
		}
	}

	// Shuffle addresses
	for i := range c.addrs {
		j := rand.Intn(i + 1)
		c.addrs[i], c.addrs[j] = c.addrs[j], c.addrs[i]
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

	// The maximum number of open connections to cluster nodes. Default: 10.
	// WARNING: Each connection maintains its own connection pool. The maximum
	// possible number of socket connections is therefore MaxConns x PoolSize
	MaxConns int

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

func (opt *ClusterOptions) getMaxConns() int {
	if opt.MaxConns < 1 {
		return 10
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

func (opt *ClusterOptions) getAddrs() ([]string, error) {
	if len(opt.Addrs) < 1 {
		return nil, errNoAddrs
	}
	return opt.Addrs, nil
}

func (opt *ClusterOptions) ClientOptions(addr string) *Options {
	return &Options{
		Addr:     addr,
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

// HashSlot returns a consistent slot number between 0 and 16383
// for any given string key
func HashSlot(key string) int {
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
