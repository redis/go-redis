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
	slotsMx sync.RWMutex // Protects slots and addrs.

	clients   map[string]*Client
	closed    bool
	clientsMx sync.RWMutex // Protects clients and closed.

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
	go client.reaper()
	return client
}

// Close closes the cluster client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *ClusterClient) Close() error {
	defer c.clientsMx.Unlock()
	c.clientsMx.Lock()

	if c.closed {
		return nil
	}
	c.closed = true
	c.resetClients()
	c.setSlots(nil)
	return nil
}

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
	if c.closed {
		c.clientsMx.Unlock()
		return nil, errClosed
	}

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

// randomClient returns a Client for the first pingable node.
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
	for addr, client := range c.clients {
		if e := client.Close(); e != nil {
			err = e
		}
		delete(c.clients, addr)
	}
	return err
}

func (c *ClusterClient) setSlots(slots []ClusterSlotInfo) {
	c.slotsMx.Lock()

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
func (c *ClusterClient) reaper() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for _ = range ticker.C {
		c.clientsMx.RLock()

		if c.closed {
			c.clientsMx.RUnlock()
			break
		}

		for _, client := range c.clients {
			pool := client.connPool
			// pool.First removes idle connections from the pool and
			// returns first non-idle connection. So just put returned
			// connection back.
			if cn := pool.First(); cn != nil {
				pool.Put(cn)
			}
		}

		c.clientsMx.RUnlock()
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

	// Following options are copied from `redis.Options`.
	PoolSize                                                         int
	DialTimeout, ReadTimeout, WriteTimeout, PoolTimeout, IdleTimeout time.Duration
}

func (opt *ClusterOptions) getMaxRedirects() int {
	if opt.MaxRedirects == 0 {
		return 16
	}
	return opt.MaxRedirects
}

func (opt *ClusterOptions) clientOptions() *Options {
	return &Options{
		DB:       0,
		Password: opt.Password,

		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:    opt.PoolSize,
		PoolTimeout: opt.PoolTimeout,
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
