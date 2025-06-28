package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ClientSideCache represents a client-side cache with Redis invalidation support.
// It provides automatic cache invalidation through Redis CLIENT TRACKING and push notifications.
type ClientSideCache struct {
	// Local cache storage
	cache map[string]*cacheEntry
	mu    sync.RWMutex

	// Cache configuration
	maxSize    int
	defaultTTL time.Duration

	// Redis client for operations and tracking
	client *Client

	// Invalidation processing
	invalidations chan []string
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// Cache statistics
	hits   int64
	misses int64
	evictions int64
}

// cacheEntry represents a cached value with metadata
type cacheEntry struct {
	Value     interface{}
	ExpiresAt time.Time
	Key       string
	CreatedAt time.Time
}

// ClientSideCacheOptions configures the client-side cache
type ClientSideCacheOptions struct {
	// Cache size and TTL settings
	MaxSize    int           // Maximum number of entries (default: 10000)
	DefaultTTL time.Duration // Default TTL for cached entries (default: 5 minutes)

	// Redis client tracking options
	EnableTracking bool     // Enable Redis CLIENT TRACKING (default: true)
	TrackingPrefix []string // Only track keys with these prefixes (optional)
	NoLoop         bool     // Don't track keys modified by this client (default: true)

	// Cache behavior
	InvalidationBufferSize int // Buffer size for invalidation channel (default: 1000)
}

// NewClientSideCache creates a new client-side cache with Redis invalidation support.
// It automatically enables Redis CLIENT TRACKING and registers an invalidation handler.
func NewClientSideCache(client *Client, opts *ClientSideCacheOptions) (*ClientSideCache, error) {
	if opts == nil {
		opts = &ClientSideCacheOptions{
			MaxSize:                10000,
			DefaultTTL:             5 * time.Minute,
			EnableTracking:         true,
			NoLoop:                 true,
			InvalidationBufferSize: 1000,
		}
	}

	// Set defaults for zero values
	if opts.MaxSize <= 0 {
		opts.MaxSize = 10000
	}
	if opts.DefaultTTL <= 0 {
		opts.DefaultTTL = 5 * time.Minute
	}
	if opts.InvalidationBufferSize <= 0 {
		opts.InvalidationBufferSize = 1000
	}

	csc := &ClientSideCache{
		cache:         make(map[string]*cacheEntry),
		maxSize:       opts.MaxSize,
		defaultTTL:    opts.DefaultTTL,
		client:        client,
		invalidations: make(chan []string, opts.InvalidationBufferSize),
		stopCh:        make(chan struct{}),
	}

	// Enable Redis client tracking
	if opts.EnableTracking {
		if err := csc.enableClientTracking(opts); err != nil {
			return nil, err
		}
	}

	// Register invalidation handler
	handler := &clientSideCacheInvalidationHandler{cache: csc}
	if err := client.RegisterPushNotificationHandler("invalidate", handler, true); err != nil {
		return nil, err
	}

	// Start invalidation processor
	csc.wg.Add(1)
	go csc.processInvalidations()

	return csc, nil
}

// enableClientTracking enables Redis CLIENT TRACKING for cache invalidation
func (csc *ClientSideCache) enableClientTracking(opts *ClientSideCacheOptions) error {
	ctx := context.Background()

	// Build CLIENT TRACKING command
	args := []interface{}{"CLIENT", "TRACKING", "ON"}

	if opts.NoLoop {
		args = append(args, "NOLOOP")
	}

	// If prefixes are specified, we need to use BCAST mode
	if len(opts.TrackingPrefix) > 0 {
		args = append(args, "BCAST")
		for _, prefix := range opts.TrackingPrefix {
			args = append(args, "PREFIX", prefix)
		}
	}

	// Enable tracking
	cmd := csc.client.Do(ctx, args...)
	return cmd.Err()
}

// Get retrieves a value from the cache, falling back to Redis if not found.
// If the key is found in the local cache and not expired, it returns immediately.
// Otherwise, it fetches from Redis and stores the result in the local cache.
func (csc *ClientSideCache) Get(ctx context.Context, key string) *StringCmd {
	// Try local cache first
	if value, found := csc.getFromCache(key); found {
		// Create a successful StringCmd with the cached value
		cmd := NewStringCmd(ctx, "get", key)
		cmd.SetVal(value.(string))
		return cmd
	}

	// Cache miss - get from Redis
	cmd := csc.client.Get(ctx, key)
	if cmd.Err() == nil {
		// Store successful result in local cache
		csc.setInCache(key, cmd.Val(), csc.defaultTTL)
	}

	return cmd
}

// Set stores a value in Redis and updates the local cache.
// The value is first stored in Redis, and if successful, also cached locally.
func (csc *ClientSideCache) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *StatusCmd {
	// Set in Redis first
	cmd := csc.client.Set(ctx, key, value, expiration)
	if cmd.Err() == nil {
		// Update local cache on success
		ttl := expiration
		if ttl <= 0 {
			ttl = csc.defaultTTL
		}
		csc.setInCache(key, value, ttl)
	}

	return cmd
}

// Del deletes keys from Redis and removes them from the local cache.
func (csc *ClientSideCache) Del(ctx context.Context, keys ...string) *IntCmd {
	// Delete from Redis first
	cmd := csc.client.Del(ctx, keys...)
	if cmd.Err() == nil {
		// Remove from local cache on success
		csc.invalidateKeys(keys)
	}

	return cmd
}

// getFromCache retrieves a value from the local cache
func (csc *ClientSideCache) getFromCache(key string) (interface{}, bool) {
	csc.mu.RLock()
	defer csc.mu.RUnlock()

	entry, exists := csc.cache[key]
	if !exists {
		atomic.AddInt64(&csc.misses, 1)
		return nil, false
	}

	// Check expiration
	if time.Now().After(entry.ExpiresAt) {
		// Entry expired - remove it
		delete(csc.cache, key)
		atomic.AddInt64(&csc.misses, 1)
		return nil, false
	}

	atomic.AddInt64(&csc.hits, 1)
	return entry.Value, true
}

// setInCache stores a value in the local cache
func (csc *ClientSideCache) setInCache(key string, value interface{}, ttl time.Duration) {
	csc.mu.Lock()
	defer csc.mu.Unlock()

	// Check cache size limit
	if len(csc.cache) >= csc.maxSize {
		// Simple LRU eviction - remove oldest entry
		csc.evictOldest()
	}

	// Store entry
	now := time.Now()
	csc.cache[key] = &cacheEntry{
		Value:     value,
		ExpiresAt: now.Add(ttl),
		Key:       key,
		CreatedAt: now,
	}
}

// evictOldest removes the oldest cache entry (simple LRU based on creation time)
func (csc *ClientSideCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range csc.cache {
		if oldestKey == "" || entry.CreatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.CreatedAt
		}
	}

	if oldestKey != "" {
		delete(csc.cache, oldestKey)
		atomic.AddInt64(&csc.evictions, 1)
	}
}

// processInvalidations processes cache invalidation notifications from Redis
func (csc *ClientSideCache) processInvalidations() {
	defer csc.wg.Done()

	for {
		select {
		case keys := <-csc.invalidations:
			csc.invalidateKeys(keys)
		case <-csc.stopCh:
			return
		}
	}
}

// invalidateKeys removes specified keys from the local cache
func (csc *ClientSideCache) invalidateKeys(keys []string) {
	if len(keys) == 0 {
		return
	}

	csc.mu.Lock()
	defer csc.mu.Unlock()

	for _, key := range keys {
		delete(csc.cache, key)
	}
}

// GetStats returns cache statistics
func (csc *ClientSideCache) GetStats() (hits, misses, evictions int64, hitRatio float64, size int) {
	csc.mu.RLock()
	size = len(csc.cache)
	csc.mu.RUnlock()

	hits = atomic.LoadInt64(&csc.hits)
	misses = atomic.LoadInt64(&csc.misses)
	evictions = atomic.LoadInt64(&csc.evictions)

	total := hits + misses
	if total > 0 {
		hitRatio = float64(hits) / float64(total)
	}

	return hits, misses, evictions, hitRatio, size
}

// Clear removes all entries from the local cache
func (csc *ClientSideCache) Clear() {
	csc.mu.Lock()
	defer csc.mu.Unlock()

	csc.cache = make(map[string]*cacheEntry)
}

// Close shuts down the client-side cache and disables Redis client tracking
func (csc *ClientSideCache) Close() error {
	// Stop invalidation processor
	close(csc.stopCh)
	csc.wg.Wait()

	// Close invalidation channel
	close(csc.invalidations)

	// Unregister invalidation handler
	csc.client.UnregisterPushNotificationHandler("invalidate")

	// Disable Redis client tracking
	ctx := context.Background()
	return csc.client.Do(ctx, "CLIENT", "TRACKING", "OFF").Err()
}

// clientSideCacheInvalidationHandler handles Redis invalidate push notifications
type clientSideCacheInvalidationHandler struct {
	cache *ClientSideCache
}

// HandlePushNotification processes invalidate notifications from Redis
func (h *clientSideCacheInvalidationHandler) HandlePushNotification(ctx context.Context, notification []interface{}) bool {
	if len(notification) < 2 {
		return false
	}

	// Extract invalidated keys from the notification
	// Format: ["invalidate", [key1, key2, ...]]
	var keys []string
	if keyList, ok := notification[1].([]interface{}); ok {
		for _, k := range keyList {
			if key, ok := k.(string); ok {
				keys = append(keys, key)
			}
		}
	}

	if len(keys) == 0 {
		return false
	}

	// Send to invalidation processor (non-blocking)
	select {
	case h.cache.invalidations <- keys:
		return true
	default:
		// Channel full - invalidations will be dropped, but cache entries will eventually expire
		// This is acceptable for performance reasons
		return false
	}
}
