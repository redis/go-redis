// Package cache provides the local in-memory cache used by client-side caching.
// Implementation details are intentionally kept here so the public API in the
// redis package can expose only the Cache interface and configuration types.
package cache

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// entryState tracks the lifecycle of a local cache entry.
type entryState uint8

const (
	// inProgress marks a placeholder entry while a value is being fetched.
	inProgress entryState = iota
	// valid marks an entry that contains a value that can be returned.
	valid
)

// entry represents a cached command reply and its Redis-key associations.
type entry struct {
	cacheKey   string
	redisKeys  []string
	value      []byte
	state      entryState
	token      uint64
	sizeBytes  int64
	reservedAt time.Time
	waitCh     chan struct{}
	waitClosed bool
	lruElem    *list.Element
}

// Sizer calculates estimated memory usage in bytes for a cache entry.
type Sizer func(cacheKey string, redisKeys []string, value []byte) int64

// Config configures a local cache instance.
type Config struct {
	// MaxEntries limits the number of entries. Zero or negative means unlimited.
	MaxEntries int
	// MaxMemoryBytes limits estimated memory usage in bytes. Zero or negative means unlimited.
	MaxMemoryBytes int64
	// Sizer estimates memory usage per entry. If nil, a built-in approximation is used.
	Sizer Sizer
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

// New creates a thread-safe local cache with strict LRU eviction.
func New(cfg Config) Cache {
	sizer := cfg.Sizer
	if sizer == nil {
		sizer = defaultSizer
	}

	staleTimeout := cfg.StaleTimeout
	if staleTimeout <= 0 {
		staleTimeout = defaultStaleTimeout
	}

	return &localCache{
		entries:        make(map[string]*entry),
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

	entries    map[string]*entry
	byRedisKey map[string]map[string]struct{}
	lru        *list.List

	// usedBytes is updated under mu but exposed atomically so MemoryUsage
	// reads do not need to take the lock. The value is approximate when
	// observed concurrently with a writer.
	usedBytes atomic.Int64

	maxEntries     int
	maxMemoryBytes int64
	sizer          Sizer
	staleTimeout   time.Duration
	nextToken      uint64
}

func defaultSizer(cacheKey string, redisKeys []string, value []byte) int64 {
	size := int64(len(cacheKey) + len(value))
	for _, key := range redisKeys {
		size += int64(len(key))
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
		e, ok := c.entries[cacheKey]
		if !ok {
			c.mu.RUnlock()
			return nil, false
		}

		if e.state == inProgress {
			waitCh := e.waitCh
			c.mu.RUnlock()
			// Wait for the in-flight fetch to either publish (Fulfill) or abort (Cancel/Delete/Flush).
			if waitCh != nil {
				select {
				case <-waitCh:
				case <-ctx.Done():
					return nil, false
				}
			} else {
				// Defensive: Reserve always creates a waitCh for IN_PROGRESS entries.
				// If it is nil, treat as a cache miss to avoid busy-looping.
				return nil, false
			}
			continue
		}

		if e.state != valid {
			c.mu.RUnlock()
			return nil, false
		}

		value := cloneBytes(e.value)
		// Skip the write-lock upgrade when the entry is already the most
		// recently used: hot keys stay at the front of the LRU list, so
		// repeated reads of them never need to serialize on mu.Lock().
		alreadyFront := e.lruElem != nil && c.lru.Front() == e.lruElem
		c.mu.RUnlock()

		if !alreadyFront {
			c.mu.Lock()
			// Re-check under write lock: the entry may have been deleted or
			// replaced between the RUnlock and Lock above.
			if current, exists := c.entries[cacheKey]; exists && current == e && current.state == valid {
				c.touchEntryLocked(current)
			}
			c.mu.Unlock()
		}

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

	e := &entry{
		cacheKey:   cacheKey,
		redisKeys:  keysCopy,
		value:      valueCopy,
		state:      valid,
		sizeBytes:  entrySize,
		waitClosed: true,
	}

	c.setEntryLocked(e)
	c.evictIfNeededLocked()
	return c.entries[cacheKey] == e
}

func (c *localCache) Reserve(cacheKey string, redisKeys []string) (token uint64, shouldFetch bool) {
	keysCopy := cloneStrings(redisKeys)

	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[cacheKey]; ok {
		switch e.state {
		case valid:
			c.touchEntryLocked(e)
			return 0, false
		case inProgress:
			if time.Since(e.reservedAt) < c.staleTimeout {
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

	e := &entry{
		cacheKey:   cacheKey,
		redisKeys:  keysCopy,
		state:      inProgress,
		token:      token,
		reservedAt: time.Now(),
		waitCh:     make(chan struct{}),
	}
	e.sizeBytes = c.computeSizeLocked(cacheKey, keysCopy, nil)

	if c.maxMemoryBytes > 0 && e.sizeBytes > c.maxMemoryBytes {
		return 0, true
	}

	c.setEntryLocked(e)
	c.evictIfNeededLocked()
	if c.entries[cacheKey] != e {
		return 0, true
	}
	return token, true
}

func (c *localCache) Fulfill(cacheKey string, token uint64, value []byte) bool {
	valueCopy := cloneBytes(value)

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[cacheKey]
	if !ok || e.state != inProgress || e.token != token {
		return false
	}

	valueSize := c.computeSizeLocked(cacheKey, e.redisKeys, valueCopy)
	if c.maxMemoryBytes > 0 && valueSize > c.maxMemoryBytes {
		c.removeEntryLocked(cacheKey)
		return false
	}

	c.usedBytes.Add(valueSize - e.sizeBytes)
	e.value = valueCopy
	e.sizeBytes = valueSize
	e.state = valid
	e.token = 0
	c.closeWaitersLocked(e)
	c.touchEntryLocked(e)

	c.evictIfNeededLocked()
	current, stillExists := c.entries[cacheKey]
	return stillExists && current == e && e.state == valid
}

func (c *localCache) Cancel(cacheKey string, token uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[cacheKey]
	if !ok || e.state != inProgress || e.token != token {
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
	for _, e := range c.entries {
		c.closeWaitersLocked(e)
	}
	c.entries = make(map[string]*entry)
	c.byRedisKey = make(map[string]map[string]struct{})
	c.lru.Init()
	c.usedBytes.Store(0)
	return removed
}

func (c *localCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *localCache) MemoryUsage() int64 {
	return c.usedBytes.Load()
}

func (c *localCache) computeSizeLocked(cacheKey string, redisKeys []string, value []byte) int64 {
	size := c.sizer(cacheKey, redisKeys, value)
	if size < 0 {
		return 0
	}
	return size
}

func (c *localCache) setEntryLocked(e *entry) {
	if old, exists := c.entries[e.cacheKey]; exists {
		c.removeEntryLocked(old.cacheKey)
	}

	c.entries[e.cacheKey] = e
	c.usedBytes.Add(e.sizeBytes)

	for _, redisKey := range e.redisKeys {
		cacheKeys := c.byRedisKey[redisKey]
		if cacheKeys == nil {
			cacheKeys = make(map[string]struct{})
			c.byRedisKey[redisKey] = cacheKeys
		}
		cacheKeys[e.cacheKey] = struct{}{}
	}

	e.lruElem = c.lru.PushFront(e)
}

func (c *localCache) removeEntryLocked(cacheKey string) bool {
	e, exists := c.entries[cacheKey]
	if !exists {
		return false
	}

	delete(c.entries, cacheKey)
	if c.usedBytes.Add(-e.sizeBytes) < 0 {
		c.usedBytes.Store(0)
	}

	for _, redisKey := range e.redisKeys {
		cacheKeys := c.byRedisKey[redisKey]
		if cacheKeys == nil {
			continue
		}
		delete(cacheKeys, cacheKey)
		if len(cacheKeys) == 0 {
			delete(c.byRedisKey, redisKey)
		}
	}

	if e.lruElem != nil {
		c.lru.Remove(e.lruElem)
		e.lruElem = nil
	}

	c.closeWaitersLocked(e)
	return true
}

func (c *localCache) closeWaitersLocked(e *entry) {
	if e.waitCh != nil && !e.waitClosed {
		close(e.waitCh)
		e.waitClosed = true
	}
}

func (c *localCache) touchEntryLocked(e *entry) {
	// O(1) recency update using list element pointer stored on the entry.
	if e.lruElem == nil {
		e.lruElem = c.lru.PushFront(e)
		return
	}
	c.lru.MoveToFront(e.lruElem)
}

func (c *localCache) overCapacityLocked() bool {
	if c.maxEntries > 0 && len(c.entries) > c.maxEntries {
		return true
	}
	if c.maxMemoryBytes > 0 && c.usedBytes.Load() > c.maxMemoryBytes {
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

		victim, ok := victimElem.Value.(*entry)
		if !ok || victim == nil {
			c.lru.Remove(victimElem)
			continue
		}

		c.removeEntryLocked(victim.cacheKey)
	}
}

func cloneBytes(src []byte) []byte {
	if src == nil {
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
