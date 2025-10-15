package auth

import (
	"sync"

	auth2 "github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

type CredentialsListeners struct {
	listeners map[*pool.Conn]auth2.CredentialsListener
	lock      sync.RWMutex
}

func NewCredentialsListeners() *CredentialsListeners {
	return &CredentialsListeners{
		listeners: make(map[*pool.Conn]auth2.CredentialsListener),
	}
}

func (c *CredentialsListeners) Add(poolCn *pool.Conn, listener auth2.CredentialsListener) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.listeners == nil {
		c.listeners = make(map[*pool.Conn]auth2.CredentialsListener)
	}
	c.listeners[poolCn] = listener
}

func (c *CredentialsListeners) Get(poolCn *pool.Conn) (auth2.CredentialsListener, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	listener, ok := c.listeners[poolCn]
	return listener, ok
}

func (c *CredentialsListeners) Remove(poolCn *pool.Conn) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.listeners, poolCn)
}
