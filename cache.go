package redis

import (
	"github.com/dgraph-io/ristretto"
)

// Cache structure
type Cache struct {
	cache *ristretto.Cache
}

type CacheConfig struct {
	MaxSize int64 // maximum size of the cache in bytes
	MaxKeys int64 // maximum number of keys to store in the cache
	// other configuration options:
	// - ttl (time to live) for cache entries
	// - eviction policy
}

// NewCache creates a new Cache instance with the given configuration
func NewCache(numKeys int64, memSize int64) (*Cache, error) {
	// Create a new cache with the given configuration
	config := &ristretto.Config{
		NumCounters: numKeys * 10, // number of keys to track frequency of (10x number of items to cache)
		MaxCost:     memSize,      // maximum cost of cache (in bytes)
		BufferItems: 64,           // number of keys per Get buffer
	}

	cache, err := ristretto.NewCache(config)
	if err != nil {
		return nil, err
	}

	return &Cache{cache: cache}, nil
}

// Set adds a value to the cache
func (c *Cache) SetKey(key, value interface{}, cost int64) bool {
	return c.cache.Set(key, value, cost)
}

// Get retrieves a value from the cache
func (c *Cache) GetKey(key interface{}) (interface{}, bool) {
	return c.cache.Get(key)
}

// ClearKey clears a specific key from the cache
func (c *Cache) ClearKey(key interface{}) {
	c.cache.Del(key)
}

// Clear clears the entire cache
func (c *Cache) Clear() {
	c.cache.Clear()
}
