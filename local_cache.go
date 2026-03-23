package redis

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// CacheEntryState tracks the lifecycle of a local cache entry.
type CacheEntryState uint8

const (
	// CacheEntryInProgress marks a placeholder entry while a value is being fetched.
	CacheEntryInProgress CacheEntryState = iota
	// CacheEntryValid marks an entry that contains a value that can be returned.
	CacheEntryValid
)

// CacheEntry represents a cached command reply and its Redis-key associations.
type CacheEntry struct {
	CacheKey  string
	RedisKeys []string
	Value     []byte
	State     CacheEntryState

	token      uint64
	sizeBytes  int64
	reservedAt time.Time
	waitCh     chan struct{}
	waitClosed bool
	lruElem    *list.Element
}

// CacheSizer calculates estimated memory usage in bytes for a cache entry.
type CacheSizer func(cacheKey string, redisKeys []string, value []byte) int64

// CacheConfig configures a local cache instance.
type CacheConfig struct {
	// MaxEntries limits the number of entries. Zero or negative means unlimited.
	MaxEntries int
	// MaxMemoryBytes limits estimated memory usage in bytes. Zero or negative means unlimited.
	MaxMemoryBytes int64
	// Sizer estimates memory usage per entry. If nil, a built-in approximation is used.
	Sizer CacheSizer
	// StaleTimeout is the duration after which an IN_PROGRESS placeholder is
	// considered stale and eligible for takeover by a new Reserve call.
	// If zero, defaults to defaultStaleTimeout (5s).
	StaleTimeout time.Duration
}

// Cache is a thread-safe local cache used by client-side caching logic.
type Cache interface {
	Get(ctx context.Context, cacheKey string) ([]byte, bool)
	Set(cacheKey string, redisKeys []string, value []byte) bool
	Reserve(cacheKey string, redisKeys []string) (token uint64, shouldFetch bool)
	Fulfill(cacheKey string, token uint64, value []byte) bool
	Cancel(cacheKey string, token uint64) bool
	DeleteByRedisKey(redisKey string) int
	DeleteByCacheKey(cacheKey string) bool
	DeleteByCacheKeys(cacheKeys []string) int
	Flush() int
	Len() int
	MemoryUsage() int64
}

const defaultStaleTimeout = 5 * time.Second

// NewLocalCache creates a thread-safe local cache with strict LRU eviction.
func NewLocalCache(cfg CacheConfig) Cache {
	sizer := cfg.Sizer
	if sizer == nil {
		sizer = defaultCacheSizer
	}

	staleTimeout := cfg.StaleTimeout
	if staleTimeout <= 0 {
		staleTimeout = defaultStaleTimeout
	}

	return &localCache{
		entries:        make(map[string]*CacheEntry),
		byRedisKey:     make(map[string]map[string]struct{}),
		lru:            list.New(),
		maxEntries:     cfg.MaxEntries,
		maxMemoryBytes: cfg.MaxMemoryBytes,
		sizer:          sizer,
		staleTimeout:   staleTimeout,
	}
}

type localCache struct {
	// A single RWMutex protects both maps and the LRU list.
	// Reads are lock-free from each other, while writes keep map/list updates atomic.
	mu sync.RWMutex

	entries    map[string]*CacheEntry
	byRedisKey map[string]map[string]struct{}
	lru        *list.List

	usedBytes int64

	maxEntries     int
	maxMemoryBytes int64
	sizer          CacheSizer
	staleTimeout   time.Duration
	nextToken      uint64
}

const defaultCacheEntryOverhead int64 = 96

func defaultCacheSizer(cacheKey string, redisKeys []string, value []byte) int64 {
	size := defaultCacheEntryOverhead + int64(len(cacheKey)+len(value))
	for _, key := range redisKeys {
		size += int64(len(key)) + 16
	}
	if size < 0 {
		return 0
	}
	return size
}

func (c *localCache) Get(ctx context.Context, cacheKey string) ([]byte, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.RLock()
		entry, ok := c.entries[cacheKey]
		if !ok {
			c.mu.RUnlock()
			return nil, false
		}

		if entry.State == CacheEntryInProgress {
			waitCh := entry.waitCh
			c.mu.RUnlock()
			// Wait for the in-flight fetch to either publish (Fulfill) or abort (Cancel/Delete/Flush).
			if waitCh != nil {
				select {
				case <-waitCh:
				case <-ctx.Done():
					return nil, false
				}
			}
			continue
		}

		if entry.State != CacheEntryValid {
			c.mu.RUnlock()
			return nil, false
		}

		value := cloneBytes(entry.Value)
		c.mu.RUnlock()

		c.mu.Lock()
		// Touch under write lock keeps LRU metadata consistent with concurrent deletes/updates.
		if current, exists := c.entries[cacheKey]; exists && current == entry && current.State == CacheEntryValid {
			c.touchEntryLocked(current)
		}
		c.mu.Unlock()

		return value, true
	}
}

func (c *localCache) Set(cacheKey string, redisKeys []string, value []byte) bool {
	valueCopy := cloneBytes(value)
	keysCopy := cloneStrings(redisKeys)

	c.mu.Lock()
	defer c.mu.Unlock()

	entrySize := c.computeSizeLocked(cacheKey, keysCopy, valueCopy)
	if c.maxMemoryBytes > 0 && entrySize > c.maxMemoryBytes {
		c.removeEntryLocked(cacheKey)
		return false
	}

	entry := &CacheEntry{
		CacheKey:   cacheKey,
		RedisKeys:  keysCopy,
		Value:      valueCopy,
		State:      CacheEntryValid,
		sizeBytes:  entrySize,
		waitClosed: true,
	}

	c.setEntryLocked(entry)
	c.evictIfNeededLocked()
	return c.entries[cacheKey] == entry
}

func (c *localCache) Reserve(cacheKey string, redisKeys []string) (token uint64, shouldFetch bool) {
	keysCopy := cloneStrings(redisKeys)

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[cacheKey]; ok {
		switch entry.State {
		case CacheEntryValid:
			c.touchEntryLocked(entry)
			return 0, false
		case CacheEntryInProgress:
			if time.Since(entry.reservedAt) < c.staleTimeout {
				// Active fetch in progress — join the wait, don't take over.
				return 0, false
			}
			// Stale placeholder: the original fetcher likely died without
			// calling Fulfill/Cancel.  Remove the old entry (unblocking any
			// waiters) and fall through to create a fresh one.
			c.removeEntryLocked(cacheKey)
		default:
			return 0, false
		}
	}

	c.nextToken++
	token = c.nextToken

	entry := &CacheEntry{
		CacheKey:   cacheKey,
		RedisKeys:  keysCopy,
		State:      CacheEntryInProgress,
		token:      token,
		reservedAt: time.Now(),
		waitCh:     make(chan struct{}),
	}
	entry.sizeBytes = c.computeSizeLocked(cacheKey, keysCopy, nil)

	if c.maxMemoryBytes > 0 && entry.sizeBytes > c.maxMemoryBytes {
		return 0, true
	}

	c.setEntryLocked(entry)
	c.evictIfNeededLocked()
	if c.entries[cacheKey] != entry {
		return 0, true
	}
	return token, true
}

func (c *localCache) Fulfill(cacheKey string, token uint64, value []byte) bool {
	valueCopy := cloneBytes(value)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[cacheKey]
	if !ok || entry.State != CacheEntryInProgress || entry.token != token {
		return false
	}

	valueSize := c.computeSizeLocked(cacheKey, entry.RedisKeys, valueCopy)
	if c.maxMemoryBytes > 0 && valueSize > c.maxMemoryBytes {
		c.removeEntryLocked(cacheKey)
		return false
	}

	c.usedBytes += valueSize - entry.sizeBytes
	entry.Value = valueCopy
	entry.sizeBytes = valueSize
	entry.State = CacheEntryValid
	entry.token = 0
	c.closeWaitersLocked(entry)
	c.touchEntryLocked(entry)

	c.evictIfNeededLocked()
	current, stillExists := c.entries[cacheKey]
	return stillExists && current == entry && entry.State == CacheEntryValid
}

func (c *localCache) Cancel(cacheKey string, token uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[cacheKey]
	if !ok || entry.State != CacheEntryInProgress || entry.token != token {
		return false
	}

	c.removeEntryLocked(cacheKey)
	return true
}

func (c *localCache) DeleteByRedisKey(redisKey string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	cacheKeys, ok := c.byRedisKey[redisKey]
	if !ok {
		return 0
	}

	removed := 0
	for cacheKey := range cacheKeys {
		if c.removeEntryLocked(cacheKey) {
			removed++
		}
	}
	return removed
}

func (c *localCache) DeleteByCacheKey(cacheKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removeEntryLocked(cacheKey)
}

func (c *localCache) DeleteByCacheKeys(cacheKeys []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for _, cacheKey := range cacheKeys {
		if c.removeEntryLocked(cacheKey) {
			removed++
		}
	}
	return removed
}

func (c *localCache) Flush() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := len(c.entries)
	for _, entry := range c.entries {
		c.closeWaitersLocked(entry)
	}
	c.entries = make(map[string]*CacheEntry)
	c.byRedisKey = make(map[string]map[string]struct{})
	c.lru.Init()
	c.usedBytes = 0
	return removed
}

func (c *localCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *localCache) MemoryUsage() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

func (c *localCache) computeSizeLocked(cacheKey string, redisKeys []string, value []byte) int64 {
	size := c.sizer(cacheKey, redisKeys, value)
	if size < 0 {
		return 0
	}
	return size
}

func (c *localCache) setEntryLocked(entry *CacheEntry) {
	if old, exists := c.entries[entry.CacheKey]; exists {
		c.removeEntryLocked(old.CacheKey)
	}

	c.entries[entry.CacheKey] = entry
	c.usedBytes += entry.sizeBytes

	for _, redisKey := range entry.RedisKeys {
		cacheKeys := c.byRedisKey[redisKey]
		if cacheKeys == nil {
			cacheKeys = make(map[string]struct{})
			c.byRedisKey[redisKey] = cacheKeys
		}
		cacheKeys[entry.CacheKey] = struct{}{}
	}

	entry.lruElem = c.lru.PushFront(entry)
}

func (c *localCache) removeEntryLocked(cacheKey string) bool {
	entry, exists := c.entries[cacheKey]
	if !exists {
		return false
	}

	delete(c.entries, cacheKey)
	c.usedBytes -= entry.sizeBytes
	if c.usedBytes < 0 {
		c.usedBytes = 0
	}

	for _, redisKey := range entry.RedisKeys {
		cacheKeys := c.byRedisKey[redisKey]
		if cacheKeys == nil {
			continue
		}
		delete(cacheKeys, cacheKey)
		if len(cacheKeys) == 0 {
			delete(c.byRedisKey, redisKey)
		}
	}

	if entry.lruElem != nil {
		c.lru.Remove(entry.lruElem)
		entry.lruElem = nil
	}

	c.closeWaitersLocked(entry)
	return true
}

func (c *localCache) closeWaitersLocked(entry *CacheEntry) {
	if entry.waitCh != nil && !entry.waitClosed {
		close(entry.waitCh)
		entry.waitClosed = true
	}
}

func (c *localCache) touchEntryLocked(entry *CacheEntry) {
	// O(1) recency update using list element pointer stored on the entry.
	if entry.lruElem == nil {
		entry.lruElem = c.lru.PushFront(entry)
		return
	}
	c.lru.MoveToFront(entry.lruElem)
}

func (c *localCache) overCapacityLocked() bool {
	if c.maxEntries > 0 && len(c.entries) > c.maxEntries {
		return true
	}
	if c.maxMemoryBytes > 0 && c.usedBytes > c.maxMemoryBytes {
		return true
	}
	return false
}

func (c *localCache) evictIfNeededLocked() {
	// Strict LRU eviction: list back is always the least recently used entry.
	for c.overCapacityLocked() {
		victimElem := c.lru.Back()
		if victimElem == nil {
			return
		}

		victim, ok := victimElem.Value.(*CacheEntry)
		if !ok || victim == nil {
			c.lru.Remove(victimElem)
			continue
		}

		c.removeEntryLocked(victim.CacheKey)
	}
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func cloneStrings(src []string) []string {
	if len(src) == 0 {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}
