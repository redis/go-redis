package redis

import (
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterClient struct {
	commandable

	addrs   []string
	slots   [][]string
	slotsMx sync.RWMutex // protects slots & addrs cache

	clients   map[string]*Client
	clientsMx sync.RWMutex

	opt *ClusterOptions

	_reload uint32
}

// NewClusterClient initializes a new cluster-aware client using given options.
// A list of seed addresses must be provided.
func NewClusterClient(opt *ClusterOptions) *ClusterClient {
	client := &ClusterClient{
		addrs:   opt.Addrs,
		slots:   make([][]string, hashSlots),
		clients: make(map[string]*Client),
		opt:     opt,
		_reload: 1,
	}
	client.commandable.process = client.process
	client.reloadIfDue()

	go client.reaper(time.NewTicker(5 * time.Minute))
	return client
}

// Close closes the cluster client.
func (c *ClusterClient) Close() error {
	// TODO: close should make client unusable
	c.setSlots(nil)
	return nil
}

// ------------------------------------------------------------------------

// getClient returns a Client for a given address.
func (c *ClusterClient) getClient(addr string) (*Client, error) {
	if addr == "" {
		return c.randomClient()
	}

	c.clientsMx.RLock()
	client, ok := c.clients[addr]
	if ok {
		c.clientsMx.RUnlock()
		return client, nil
	}
	c.clientsMx.RUnlock()

	c.clientsMx.Lock()
	client, ok = c.clients[addr]
	if !ok {
		opt := c.opt.clientOptions()
		opt.Addr = addr
		client = NewTCPClient(opt)
		c.clients[addr] = client
	}
	c.clientsMx.Unlock()

	return client, nil
}

func (c *ClusterClient) slotAddrs(slot int) []string {
	c.slotsMx.RLock()
	addrs := c.slots[slot]
	c.slotsMx.RUnlock()
	return addrs
}

// randomClient returns a Client for the first live node.
func (c *ClusterClient) randomClient() (client *Client, err error) {
	for i := 0; i < 10; i++ {
		n := rand.Intn(len(c.addrs))
		client, err = c.getClient(c.addrs[n])
		if err != nil {
			continue
		}
		err = client.Ping().Err()
		if err == nil {
			return client, nil
		}
	}
	return nil, err
}

func (c *ClusterClient) process(cmd Cmder) {
	var ask bool

	c.reloadIfDue()

	slot := hashSlot(cmd.clusterKey())

	var addr string
	if addrs := c.slotAddrs(slot); len(addrs) > 0 {
		addr = addrs[0] // First address is master.
	}

	client, err := c.getClient(addr)
	if err != nil {
		cmd.setErr(err)
		return
	}

	for attempt := 0; attempt <= c.opt.getMaxRedirects(); attempt++ {
		if ask {
			pipe := client.Pipeline()
			pipe.Process(NewCmd("ASKING"))
			pipe.Process(cmd)
			_, _ = pipe.Exec()
			ask = false
		} else {
			client.Process(cmd)
		}

		// If there is no (real) error, we are done!
		err := cmd.Err()
		if err == nil || err == Nil || err == TxFailedErr {
			return
		}

		// On network errors try random node.
		if isNetworkError(err) {
			client, err = c.randomClient()
			if err != nil {
				return
			}
			cmd.reset()
			continue
		}

		var moved bool
		var addr string
		moved, ask, addr = isMovedError(err)
		if moved || ask {
			if moved {
				c.scheduleReload()
			}
			client, err = c.getClient(addr)
			if err != nil {
				return
			}
			cmd.reset()
			continue
		}

		break
	}
}

// Closes all clients and returns last error if there are any.
func (c *ClusterClient) resetClients() (err error) {
	c.clientsMx.Lock()
	for addr, client := range c.clients {
		if e := client.Close(); e != nil {
			err = e
		}
		delete(c.clients, addr)
	}
	c.clientsMx.Unlock()
	return err
}

func (c *ClusterClient) setSlots(slots []ClusterSlotInfo) {
	c.slotsMx.Lock()

	c.resetClients()
	seen := make(map[string]struct{})
	for _, addr := range c.addrs {
		seen[addr] = struct{}{}
	}

	for i := 0; i < hashSlots; i++ {
		c.slots[i] = c.slots[i][:0]
	}
	for _, info := range slots {
		for slot := info.Start; slot <= info.End; slot++ {
			c.slots[slot] = info.Addrs
		}

		for _, addr := range info.Addrs {
			if _, ok := seen[addr]; !ok {
				c.addrs = append(c.addrs, addr)
				seen[addr] = struct{}{}
			}
		}
	}

	c.slotsMx.Unlock()
}

// Closes all connections and reloads slot cache, if due.
func (c *ClusterClient) reloadIfDue() (err error) {
	if !atomic.CompareAndSwapUint32(&c._reload, 1, 0) {
		return
	}

	client, err := c.randomClient()
	if err != nil {
		return err
	}

	slots, err := client.ClusterSlots().Result()
	if err != nil {
		return err
	}
	c.setSlots(slots)

	return nil
}

// Schedules slots reload on next request.
func (c *ClusterClient) scheduleReload() {
	atomic.StoreUint32(&c._reload, 1)
}

// reaper closes idle connections to the cluster.
func (c *ClusterClient) reaper(ticker *time.Ticker) {
	for _ = range ticker.C {
		for _, client := range c.clients {
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
