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

	// validAtNs is the wall-clock UnixNano when the entry became Valid, used by the
	// MaxStaleness backstop. Written under Lock (Set/Fulfill), read under RLock (get).
	validAtNs int64

	// ownerConnID is the conn that fetched this entry (set by FulfillOwned; 0 =
	// none). Default CLIENT TRACKING sends a key's invalidation only to that
	// conn, so the entry must be evicted when it goes away (see EvictByConn).
	ownerConnID uint64
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
	//
	// If both MaxEntries and MaxMemoryBytes are unlimited, MaxEntries defaults to
	// defaultCacheMaxEntries so the cache cannot grow without bound. The cache is
	// sharded 16 ways (above small thresholds) and each shard enforces its 1/16
	// share, so an entry larger than MaxMemoryBytes/16 is never admitted (counted
	// by CacheAdmissionRejects) — size it to at least 16× your largest reply.
	MaxMemoryBytes int64
	// Sizer estimates memory usage per entry. If nil, a built-in approximation is used.
	//
	// Sizer is invoked while the cache's internal lock is held: it must return
	// quickly and must not call back into the cache (Get, Set, Delete*, Flush,
	// etc.), or it will deadlock.
	Sizer CacheSizer
	// StaleTimeout is the duration after which an IN_PROGRESS placeholder is
	// considered stale and eligible for takeover by a new Reserve call.
	// If zero, defaults to defaultStaleTimeout (5s).
	StaleTimeout time.Duration

	// DrainInterval is the SharedTracking background-drainer period (default 5ms;
	// zero uses the default): how often idle pool conns are swept for buffered
	// "invalidate" frames, roughly bounding cache-hit staleness. Values below 1ms
	// are clamped to 1ms. Ignored by Broadcast and PerConnection.
	DrainInterval time.Duration

	// MaxStaleness caps how long a cached entry is served after it became valid,
	// regardless of invalidation. Zero (default) disables it. It is a correctness
	// BACKSTOP for lost invalidations or connection-lifecycle gaps ("Window 2"), not
	// the primary freshness mechanism. Keep it well above the invalidation round-trip
	// (e.g. seconds); per-entry refetch overhead scales ~1/MaxStaleness.
	MaxStaleness time.Duration
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

// connCacheOwner is an optional Cache capability (like cacheStatsReporter):
// attribute entries to the conn that fetched them and evict a conn's entries in
// one shot. localCache implements it; SharedTracking uses it to close Window 2.
// Caches without it fall back to plain Fulfill.
type connCacheOwner interface {
	FulfillOwned(cacheKey string, token, ownerConnID uint64, value []byte) bool
	EvictByConn(connID uint64) int
}

const (
	defaultStaleTimeout    = 5 * time.Second
	defaultCacheShardCount = 16

	// defaultCacheMaxEntries bounds the cache when the config leaves both
	// MaxEntries and MaxMemoryBytes unlimited (matches the 10k-entry default
	// other Redis clients use, e.g. redis-py).
	defaultCacheMaxEntries = 10000

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

	maxEntries := cfg.MaxEntries
	maxMemoryBytes := cfg.MaxMemoryBytes
	// An unbounded cache can grow until the process OOMs; require at least
	// one limit.
	if maxEntries <= 0 && maxMemoryBytes <= 0 {
		maxEntries = defaultCacheMaxEntries
	}

	shardCount := defaultCacheShardCount
	if maxEntries > 0 && maxEntries < shardingThresholdEntries {
		shardCount = 1
	}
	if maxMemoryBytes > 0 && maxMemoryBytes < int64(shardingThresholdBytes) {
		shardCount = 1
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
		s.byConnID = make(map[uint64]map[string]struct{})
		// Distribute capacity so the per-shard caps sum to exactly the
		// configured limits; a ceil-per-shard split would let total residency
		// exceed MaxEntries/MaxMemoryBytes.
		if maxEntries > 0 {
			s.maxEntries = maxEntries / shardCount
			if i < maxEntries%shardCount {
				s.maxEntries++
			}
		}
		if maxMemoryBytes > 0 {
			s.maxMemoryBytes = maxMemoryBytes / int64(shardCount)
			if int64(i) < maxMemoryBytes%int64(shardCount) {
				s.maxMemoryBytes++
			}
		}
		s.maxStalenessNs = int64(cfg.MaxStaleness)
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
// protects entries, byRedisKey, byConnID, and usedBytes.
type cacheShard struct {
	mu         sync.RWMutex
	entries    map[string]*CacheEntry
	byRedisKey map[string]map[string]struct{}
	// byConnID is the owning-conn reverse index (twin of byRedisKey): conn id ->
	// its cache keys. Populated by FulfillOwned, cleaned in removeEntryLocked,
	// consumed by EvictByConn.
	byConnID  map[uint64]map[string]struct{}
	usedBytes int64

	maxEntries     int
	maxMemoryBytes int64
	maxStalenessNs int64
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
			// Bound the wait by the placeholder's remaining stale window so an
			// abandoned reservation cannot block waiters indefinitely.
			remaining := s.staleTimeout - time.Since(entry.reservedAt)
			s.mu.RUnlock()
			if waitCh == nil {
				// Defensive: treat a missing waitCh as a miss to avoid busy-looping.
				return nil, false
			}
			if remaining <= 0 {
				// Placeholder already stale; miss so the caller refetches.
				return nil, false
			}
			// Wait for the in-flight fetch to either publish (Fulfill) or abort (Cancel/Delete/Flush).
			timer := time.NewTimer(remaining)
			select {
			case <-waitCh:
				timer.Stop()
			case <-ctx.Done():
				timer.Stop()
				return nil, false
			case <-timer.C:
				return nil, false
			}
			continue
		}

		if entry.State != CacheEntryValid {
			s.mu.RUnlock()
			return nil, false
		}

		// Max-staleness backstop: a Valid entry older than maxStalenessNs is treated
		// as a miss and evicted, so a lost invalidation or connection-lifecycle
		// staleness (Window 2) cannot keep a stale value resident past MaxStaleness.
		// Evict under the write lock so the next access re-fetches — a stale-but-present
		// entry would otherwise suppress the re-fetch via Reserve.
		if s.maxStalenessNs > 0 && time.Now().UnixNano()-entry.validAtNs > s.maxStalenessNs {
			s.mu.RUnlock()
			s.mu.Lock()
			if cur, ok := s.entries[cacheKey]; ok && cur == entry {
				s.removeEntryLocked(cacheKey)
			}
			s.mu.Unlock()
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
		validAtNs:  time.Now().UnixNano(),
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

	s.setEntryLocked(entry)
	// Evict only Valid victims. If still over capacity the shard holds only
	// in-flight placeholders: rather than abort a peer's fetch, drop this
	// reservation (the caller fetches uncached). The hard cap holds either way.
	s.evictValidLocked()
	if s.overCapacityLocked() {
		s.removeEntryLocked(cacheKey)
		return 0, true
	}
	if s.entries[cacheKey] != entry {
		return 0, true
	}
	return newToken, true
}

func (c *localCache) Fulfill(cacheKey string, token uint64, value []byte) bool {
	return c.fulfill(cacheKey, token, 0, value)
}

// FulfillOwned is Fulfill that also records ownerConnID so EvictByConn can drop
// the entry when that conn is removed. ownerConnID == 0 behaves like Fulfill.
func (c *localCache) FulfillOwned(cacheKey string, token, ownerConnID uint64, value []byte) bool {
	return c.fulfill(cacheKey, token, ownerConnID, value)
}

func (c *localCache) fulfill(cacheKey string, token, ownerConnID uint64, value []byte) bool {
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
	entry.validAtNs = time.Now().UnixNano()
	entry.token = 0
	entry.lastAccessNs.Store(nextLRUToken())
	if ownerConnID != 0 {
		entry.ownerConnID = ownerConnID
		s.indexConnLocked(ownerConnID, cacheKey)
	}
	s.closeWaitersLocked(entry)

	s.evictIfNeededLocked()
	current, stillExists := s.entries[cacheKey]
	return stillExists && current == entry && entry.State == CacheEntryValid
}

// EvictByConn removes every entry fetched by connID and returns the count.
// Called when a conn is removed/swapped: the server stops delivering those
// keys' invalidations, so keeping them risks stale serves. Errs toward a miss.
func (c *localCache) EvictByConn(connID uint64) int {
	if connID == 0 {
		return 0
	}
	removed := 0
	for i := range c.shards {
		removed += c.shards[i].evictByConn(connID)
	}
	return removed
}

func (s *cacheShard) evictByConn(connID uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cacheKeys, ok := s.byConnID[connID]
	if !ok {
		return 0
	}
	toRemove := make([]string, 0, len(cacheKeys))
	for cacheKey := range cacheKeys {
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

// indexConnLocked records cacheKey under connID in the owning-connection index.
func (s *cacheShard) indexConnLocked(connID uint64, cacheKey string) {
	cacheKeys := s.byConnID[connID]
	if cacheKeys == nil {
		cacheKeys = make(map[string]struct{})
		s.byConnID[connID] = cacheKeys
	}
	cacheKeys[cacheKey] = struct{}{}
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

	// Remove IN_PROGRESS placeholders too: an invalidation can arrive on a
	// different stream than the in-flight reply (BCAST sidecar, drainer), so the
	// fetch may predate the write. Removing makes the racing Fulfill fail and
	// waiters refetch, so a raced-invalidation value is never published.
	toRemove := make([]string, 0, len(cacheKeys))
	for cacheKey := range cacheKeys {
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

	// Flush placeholders too (see deleteByRedisKey): a flush (FLUSHDB, sidecar
	// teardown) means everything, including in-flight fetches, may be stale.
	removed := 0
	for cacheKey := range s.entries {
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

	if entry.ownerConnID != 0 {
		if cacheKeys := s.byConnID[entry.ownerConnID]; cacheKeys != nil {
			delete(cacheKeys, cacheKey)
			if len(cacheKeys) == 0 {
				delete(s.byConnID, entry.ownerConnID)
			}
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

// evictIfNeededLocked evicts by approximate LRU (O(N) scan; rare in
// well-sized caches) until under capacity. Used by Set/Fulfill: it prefers a
// Valid victim but falls back to the oldest IN_PROGRESS placeholder to keep the
// hard cap (that placeholder's Fulfill then fails and its waiters refetch).
func (s *cacheShard) evictIfNeededLocked() {
	for s.overCapacityLocked() {
		victim := s.oldestLocked(CacheEntryValid)
		if victim == nil {
			victim = s.oldestLocked(CacheEntryInProgress)
		}
		if victim == nil {
			return
		}
		s.removeEntryLocked(victim.CacheKey)
	}
}

// evictValidLocked evicts only Valid entries until under capacity. Unlike
// evictIfNeededLocked it never evicts a placeholder, so Reserve can't abort a
// peer's in-flight fetch.
func (s *cacheShard) evictValidLocked() {
	for s.overCapacityLocked() {
		victim := s.oldestLocked(CacheEntryValid)
		if victim == nil {
			return
		}
		s.removeEntryLocked(victim.CacheKey)
	}
}

// oldestLocked returns the entry in the given state with the smallest
// lastAccessNs (the least-recently-used), or nil when none exists.
func (s *cacheShard) oldestLocked(state CacheEntryState) *CacheEntry {
	var victim *CacheEntry
	var oldestNs int64 = math.MaxInt64
	for _, e := range s.entries {
		if e.State != state {
			continue
		}
		if ns := e.lastAccessNs.Load(); ns < oldestNs {
			oldestNs = ns
			victim = e
		}
	}
	return victim
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
