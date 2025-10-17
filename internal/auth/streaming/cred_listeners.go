package streaming

import (
	"sync"

	"github.com/redis/go-redis/v9/auth"
)

type CredentialsListeners struct {
	// connid -> listener
	listeners map[uint64]auth.CredentialsListener
	lock      sync.RWMutex
}

func NewCredentialsListeners() *CredentialsListeners {
	return &CredentialsListeners{
		listeners: make(map[uint64]auth.CredentialsListener),
	}
}

func (c *CredentialsListeners) Add(connID uint64, listener auth.CredentialsListener) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.listeners == nil {
		c.listeners = make(map[uint64]auth.CredentialsListener)
	}
	c.listeners[connID] = listener
}

func (c *CredentialsListeners) Get(connID uint64) (auth.CredentialsListener, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(c.listeners) == 0 {
		return nil, false
	}
	listener, ok := c.listeners[connID]
	return listener, ok
}

func (c *CredentialsListeners) Remove(connID uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.listeners, connID)
}
