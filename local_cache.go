package redis

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
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

	// lastAccessNs is a recency token for LRU eviction: a global atomic counter
	// bumped on every access, stored atomically so the read path can mark a
	// touch under the shard's RLock without upgrading to a write lock.
	lastAccessNs atomic.Int64
}

// lruSequence is the global monotonic counter feeding lastAccessNs. It totally
// orders recency across all entries in all shards for approximate-LRU eviction.
var lruSequence atomic.Int64

// nextLRUToken returns the next strictly-greater LRU token.
func nextLRUToken() int64 {
	return lruSequence.Add(1)
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

const (
	defaultStaleTimeout    = 5 * time.Second
	defaultCacheShardCount = 16

	// shardingThresholdEntries / shardingThresholdBytes: caches with capacity
	// below these thresholds fall back to a single shard so global LRU /
	// memory-cap semantics behave exactly as a non-sharded cache would.
	shardingThresholdEntries = 64
	shardingThresholdBytes   = 64 * 1024
)

// NewLocalCache creates a thread-safe local cache with approximate-LRU
// eviction. The cache is internally sharded by cache-key hash to reduce
// mutex contention under high concurrent access.
func NewLocalCache(cfg CacheConfig) Cache {
	sizer := cfg.Sizer
	if sizer == nil {
		sizer = defaultCacheSizer
	}

	staleTimeout := cfg.StaleTimeout
	if staleTimeout <= 0 {
		staleTimeout = defaultStaleTimeout
	}

	shardCount := defaultCacheShardCount
	if cfg.MaxEntries > 0 && cfg.MaxEntries < shardingThresholdEntries {
		shardCount = 1
	}
	if cfg.MaxMemoryBytes > 0 && cfg.MaxMemoryBytes < int64(shardingThresholdBytes) {
		shardCount = 1
	}

	perShardEntries := 0
	if cfg.MaxEntries > 0 {
		perShardEntries = (cfg.MaxEntries + shardCount - 1) / shardCount
	}
	var perShardBytes int64
	if cfg.MaxMemoryBytes > 0 {
		perShardBytes = (cfg.MaxMemoryBytes + int64(shardCount) - 1) / int64(shardCount)
	}

	c := &localCache{
		shards:     make([]cacheShard, shardCount),
		shardCount: uint32(shardCount),
		shardMask:  uint32(shardCount - 1),
		sizer:      sizer,
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.entries = make(map[string]*CacheEntry)
		s.byRedisKey = make(map[string]map[string]struct{})
		s.maxEntries = perShardEntries
		s.maxMemoryBytes = perShardBytes
		s.sizer = sizer
		s.staleTimeout = staleTimeout
	}
	return c
}

// localCache is a sharded approximate-LRU cache.
type localCache struct {
	shards     []cacheShard
	shardCount uint32
	shardMask  uint32
	sizer      CacheSizer

	nextToken atomic.Uint64
	hits      atomic.Uint64
	misses    atomic.Uint64
}

// cacheShard holds the state for one shard of localCache. The mutex
// protects entries, byRedisKey, and usedBytes.
type cacheShard struct {
	mu         sync.RWMutex
	entries    map[string]*CacheEntry
	byRedisKey map[string]map[string]struct{}
	usedBytes  int64

	maxEntries     int
	maxMemoryBytes int64
	sizer          CacheSizer
	staleTimeout   time.Duration
}

// shardFor returns the shard responsible for cacheKey.
func (c *localCache) shardFor(cacheKey string) *cacheShard {
	if c.shardCount == 1 {
		return &c.shards[0]
	}
	return &c.shards[fnv1a32(cacheKey)&c.shardMask]
}

// fnv1a32 returns the FNV-1a 32-bit hash of s. Allocation-free.
func fnv1a32(s string) uint32 {
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
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
	value, ok := c.shardFor(cacheKey).get(ctx, cacheKey)
	if ok {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return value, ok
}

// get is the read-side hot path. Holds only the shard's read lock; updates
// the LRU recency timestamp via atomic store on the entry — no write-lock
// upgrade is needed.
func (s *cacheShard) get(ctx context.Context, cacheKey string) ([]byte, bool) {
	for {
		s.mu.RLock()
		entry, ok := s.entries[cacheKey]
		if !ok {
			s.mu.RUnlock()
			return nil, false
		}

		if entry.State == CacheEntryInProgress {
			waitCh := entry.waitCh
			s.mu.RUnlock()
			if waitCh != nil {
				select {
				case <-waitCh:
				case <-ctx.Done():
					return nil, false
				}
			} else {
				return nil, false
			}
			continue
		}

		if entry.State != CacheEntryValid {
			s.mu.RUnlock()
			return nil, false
		}

		value := cloneBytes(entry.Value)
		// Record access timestamp without upgrading the lock. Last writer
		// wins; cross-goroutine ordering of timestamps is fine for
		// approximate-LRU semantics.
		entry.lastAccessNs.Store(nextLRUToken())
		s.mu.RUnlock()
		return value, true
	}
}

// Stats returns cumulative (hits, misses). Exposed via the optional
// cacheStatsReporter interface (see csc_stats.go).
func (c *localCache) Stats() (hits, misses uint64) {
	return c.hits.Load(), c.misses.Load()
}

func (c *localCache) Set(cacheKey string, redisKeys []string, value []byte) bool {
	valueCopy := cloneBytes(value)
	keysCopy := cloneStrings(redisKeys)
	entrySize := c.sizer(cacheKey, keysCopy, valueCopy)
	if entrySize < 0 {
		entrySize = 0
	}
	entry := &CacheEntry{
		CacheKey:   cacheKey,
		RedisKeys:  keysCopy,
		Value:      valueCopy,
		State:      CacheEntryValid,
		sizeBytes:  entrySize,
		waitClosed: true,
	}
	entry.lastAccessNs.Store(nextLRUToken())

	s := c.shardFor(cacheKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.maxMemoryBytes > 0 && entrySize > s.maxMemoryBytes {
		s.removeEntryLocked(cacheKey)
		return false
	}

	s.setEntryLocked(entry)
	s.evictIfNeededLocked()
	return s.entries[cacheKey] == entry
}

func (c *localCache) Reserve(cacheKey string, redisKeys []string) (token uint64, shouldFetch bool) {
	keysCopy := cloneStrings(redisKeys)
	waitCh := make(chan struct{})
	reservedAt := time.Now()
	sizeBytes := c.sizer(cacheKey, keysCopy, nil)
	if sizeBytes < 0 {
		sizeBytes = 0
	}
	newToken := c.nextToken.Add(1)

	s := c.shardFor(cacheKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.entries[cacheKey]; ok {
		switch entry.State {
		case CacheEntryValid:
			// Existing-VALID hit: record access; caller will re-Get to
			// retrieve.
			entry.lastAccessNs.Store(nextLRUToken())
			return 0, false
		case CacheEntryInProgress:
			if time.Since(entry.reservedAt) < s.staleTimeout {
				return 0, false
			}
			s.removeEntryLocked(cacheKey)
		default:
			return 0, false
		}
	}

	if s.maxMemoryBytes > 0 && sizeBytes > s.maxMemoryBytes {
		return 0, true
	}

	entry := &CacheEntry{
		CacheKey:   cacheKey,
		RedisKeys:  keysCopy,
		State:      CacheEntryInProgress,
		token:      newToken,
		reservedAt: reservedAt,
		waitCh:     waitCh,
		sizeBytes:  sizeBytes,
	}
	entry.lastAccessNs.Store(nextLRUToken())
	_ = reservedAt

	s.setEntryLocked(entry)
	s.evictIfNeededLocked()
	if s.entries[cacheKey] != entry {
		return 0, true
	}
	return newToken, true
}

func (c *localCache) Fulfill(cacheKey string, token uint64, value []byte) bool {
	valueCopy := cloneBytes(value)

	s := c.shardFor(cacheKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[cacheKey]
	if !ok || entry.State != CacheEntryInProgress || entry.token != token {
		return false
	}

	valueSize := s.sizer(cacheKey, entry.RedisKeys, valueCopy)
	if valueSize < 0 {
		valueSize = 0
	}
	if s.maxMemoryBytes > 0 && valueSize > s.maxMemoryBytes {
		s.removeEntryLocked(cacheKey)
		return false
	}

	s.usedBytes += valueSize - entry.sizeBytes
	entry.Value = valueCopy
	entry.sizeBytes = valueSize
	entry.State = CacheEntryValid
	entry.token = 0
	entry.lastAccessNs.Store(nextLRUToken())
	s.closeWaitersLocked(entry)

	s.evictIfNeededLocked()
	current, stillExists := s.entries[cacheKey]
	return stillExists && current == entry && entry.State == CacheEntryValid
}

func (c *localCache) Cancel(cacheKey string, token uint64) bool {
	s := c.shardFor(cacheKey)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[cacheKey]
	if !ok || entry.State != CacheEntryInProgress || entry.token != token {
		return false
	}

	s.removeEntryLocked(cacheKey)
	return true
}

func (c *localCache) DeleteByRedisKey(redisKey string) int {
	removed := 0
	for i := range c.shards {
		removed += c.shards[i].deleteByRedisKey(redisKey)
	}
	return removed
}

func (s *cacheShard) deleteByRedisKey(redisKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cacheKeys, ok := s.byRedisKey[redisKey]
	if !ok {
		return 0
	}

	toRemove := make([]string, 0, len(cacheKeys))
	for cacheKey := range cacheKeys {
		if entry, exists := s.entries[cacheKey]; exists && entry.State == CacheEntryInProgress {
			continue
		}
		toRemove = append(toRemove, cacheKey)
	}

	removed := 0
	for _, cacheKey := range toRemove {
		if s.removeEntryLocked(cacheKey) {
			removed++
		}
	}
	return removed
}

func (c *localCache) DeleteByCacheKey(cacheKey string) bool {
	s := c.shardFor(cacheKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removeEntryLocked(cacheKey)
}

func (c *localCache) DeleteByCacheKeys(cacheKeys []string) int {
	removed := 0
	if c.shardCount == 1 {
		s := &c.shards[0]
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, k := range cacheKeys {
			if s.removeEntryLocked(k) {
				removed++
			}
		}
		return removed
	}
	groups := make(map[uint32][]string)
	for _, k := range cacheKeys {
		idx := fnv1a32(k) & c.shardMask
		groups[idx] = append(groups[idx], k)
	}
	for idx, keys := range groups {
		s := &c.shards[idx]
		s.mu.Lock()
		for _, k := range keys {
			if s.removeEntryLocked(k) {
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

func (c *localCache) Flush() int {
	removed := 0
	for i := range c.shards {
		removed += c.shards[i].flush()
	}
	return removed
}

func (s *cacheShard) flush() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for cacheKey, entry := range s.entries {
		if entry.State == CacheEntryInProgress {
			continue
		}
		if s.removeEntryLocked(cacheKey) {
			removed++
		}
	}
	return removed
}

func (c *localCache) Len() int {
	n := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		n += len(s.entries)
		s.mu.RUnlock()
	}
	return n
}

func (c *localCache) MemoryUsage() int64 {
	var total int64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		total += s.usedBytes
		s.mu.RUnlock()
	}
	return total
}

func (s *cacheShard) setEntryLocked(entry *CacheEntry) {
	if old, exists := s.entries[entry.CacheKey]; exists {
		s.removeEntryLocked(old.CacheKey)
	}

	s.entries[entry.CacheKey] = entry
	s.usedBytes += entry.sizeBytes

	for _, redisKey := range entry.RedisKeys {
		cacheKeys := s.byRedisKey[redisKey]
		if cacheKeys == nil {
			cacheKeys = make(map[string]struct{})
			s.byRedisKey[redisKey] = cacheKeys
		}
		cacheKeys[entry.CacheKey] = struct{}{}
	}
}

func (s *cacheShard) removeEntryLocked(cacheKey string) bool {
	entry, exists := s.entries[cacheKey]
	if !exists {
		return false
	}

	delete(s.entries, cacheKey)
	s.usedBytes -= entry.sizeBytes
	if s.usedBytes < 0 {
		s.usedBytes = 0
	}

	for _, redisKey := range entry.RedisKeys {
		cacheKeys := s.byRedisKey[redisKey]
		if cacheKeys == nil {
			continue
		}
		delete(cacheKeys, cacheKey)
		if len(cacheKeys) == 0 {
			delete(s.byRedisKey, redisKey)
		}
	}

	s.closeWaitersLocked(entry)
	return true
}

func (s *cacheShard) closeWaitersLocked(entry *CacheEntry) {
	if entry.waitCh != nil && !entry.waitClosed {
		close(entry.waitCh)
		entry.waitClosed = true
	}
}

func (s *cacheShard) overCapacityLocked() bool {
	if s.maxEntries > 0 && len(s.entries) > s.maxEntries {
		return true
	}
	if s.maxMemoryBytes > 0 && s.usedBytes > s.maxMemoryBytes {
		return true
	}
	return false
}

// evictIfNeededLocked is an approximate-LRU eviction: it scans the shard's
// entries for the one with the oldest lastAccessNs and removes it. This is
// O(N) per eviction, but evictions are rare in production caches that
// allocate enough capacity.
// IN_PROGRESS entries are not evictable — the in-flight reply may complete
// and rely on the slot.
func (s *cacheShard) evictIfNeededLocked() {
	for s.overCapacityLocked() {
		var victim *CacheEntry
		var oldestNs int64 = math.MaxInt64
		for _, e := range s.entries {
			if e.State != CacheEntryValid {
				continue
			}
			ns := e.lastAccessNs.Load()
			if ns < oldestNs {
				oldestNs = ns
				victim = e
			}
		}
		if victim == nil {
			// Only IN_PROGRESS entries left; can't evict any.
			return
		}
		s.removeEntryLocked(victim.CacheKey)
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
