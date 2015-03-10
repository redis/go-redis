package redis

import (
	"container/list"
	"sync"
)

// Shamelessly copied and adapted from:
// https://github.com/camlistore/camlistore/blob/master/pkg/lru/cache.go
// Copyright 2011 Google Inc. - http://www.apache.org/licenses/LICENSE-2.0

// lruPool is a thread-safe connection LRU pool
type lruPool struct {
	opt   *ClusterOptions
	limit int
	order *list.List
	cache map[string]*list.Element

	sync.Mutex
}

type lruEntry struct {
	addr string
	conn *Client
}

func newLRUPool(opt *ClusterOptions) *lruPool {
	return &lruPool{
		opt:   opt,
		limit: opt.getMaxConns(),
		order: list.New(),
		cache: make(map[string]*list.Element, opt.getMaxConns()),
	}
}

// Fetch gets or creates a new connection
func (c *lruPool) Fetch(addr string) *Client {
	c.Lock()
	defer c.Unlock()

	conn, ok := c.get(addr)
	if !ok {
		conn = NewTCPClient(c.opt.ClientOptions(addr))
		c.add(addr, conn)
	}
	return conn
}

// Clear clears the cache
func (c *lruPool) Clear() (err error) {
	c.Lock()
	defer c.Unlock()

	for c.len() > 0 {
		if e := c.closeOldest(); e != nil {
			err = e
		}
	}
	return
}

func (c *lruPool) len() int { return c.order.Len() }

func (c *lruPool) get(addr string) (conn *Client, ok bool) {
	if ee, ok := c.cache[addr]; ok {
		c.order.MoveToFront(ee)
		return ee.Value.(*lruEntry).conn, true
	}
	return
}

func (c *lruPool) add(addr string, conn *Client) {
	if ee, ok := c.cache[addr]; ok {
		c.order.MoveToFront(ee)
		val := ee.Value.(*lruEntry)
		val.conn.Close() // Close existing
		val.conn = conn
		return
	}

	// Add to cache if not present
	ee := c.order.PushFront(&lruEntry{addr, conn})
	c.cache[addr] = ee

	if c.len() > c.limit {
		c.closeOldest()
	}
}

func (c *lruPool) closeOldest() error {
	ee := c.order.Back()
	if ee == nil {
		return nil
	}

	c.order.Remove(ee)
	val := ee.Value.(*lruEntry)
	delete(c.cache, val.addr)

	// Close connection
	return val.conn.Close()
}
