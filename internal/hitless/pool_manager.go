package hitless

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
)

// PoolEndpointManager manages endpoint transitions for connection pools during hitless upgrades.
// It provides functionality to redirect new connections to new endpoints while maintaining
// existing connections until they can be gracefully transitioned.
type PoolEndpointManager struct {
	mu sync.RWMutex

	// Map of pools to their endpoint redirections
	redirections map[interface{}]*EndpointRedirection

	// Original dialers for pools (to restore after transition)
	originalDialers map[interface{}]func(context.Context) (net.Conn, error)
}

// EndpointRedirection represents an active endpoint redirection
type EndpointRedirection struct {
	OriginalEndpoint string
	NewEndpoint      string
	StartTime        time.Time
	Timeout          time.Duration

	// Statistics
	NewConnections    int64
	FailedConnections int64
}

// NewPoolEndpointManager creates a new pool endpoint manager
func NewPoolEndpointManager() *PoolEndpointManager {
	return &PoolEndpointManager{
		redirections:    make(map[interface{}]*EndpointRedirection),
		originalDialers: make(map[interface{}]func(context.Context) (net.Conn, error)),
	}
}

// RedirectPool redirects new connections from a pool to a new endpoint
func (m *PoolEndpointManager) RedirectPool(ctx context.Context, pooler pool.Pooler, newEndpoint string, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if pool is already being redirected
	if _, exists := m.redirections[pooler]; exists {
		return fmt.Errorf("pool is already being redirected")
	}

	// Get the current dialer from the pool
	connPool, ok := pooler.(*pool.ConnPool)
	if !ok {
		return fmt.Errorf("unsupported pool type: %T", pooler)
	}

	// Store original dialer
	originalDialer := m.getPoolDialer(connPool)
	if originalDialer == nil {
		return fmt.Errorf("could not get original dialer from pool")
	}

	m.originalDialers[pooler] = originalDialer

	// Create new dialer that connects to the new endpoint
	newDialer := m.createRedirectDialer(ctx, newEndpoint, originalDialer)

	// Replace the pool's dialer
	if err := m.setPoolDialer(connPool, newDialer); err != nil {
		delete(m.originalDialers, pooler)
		return fmt.Errorf("failed to set new dialer: %w", err)
	}

	// Record the redirection
	m.redirections[pooler] = &EndpointRedirection{
		OriginalEndpoint: m.extractEndpointFromDialer(originalDialer),
		NewEndpoint:      newEndpoint,
		StartTime:        time.Now(),
		Timeout:          timeout,
	}

	internal.Logger.Printf(ctx, "hitless: redirected pool to new endpoint %s", newEndpoint)

	return nil
}

// IsPoolRedirected checks if a pool is currently being redirected
func (m *PoolEndpointManager) IsPoolRedirected(pooler pool.Pooler) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.redirections[pooler]
	return exists
}

// GetRedirection returns redirection information for a pool
func (m *PoolEndpointManager) GetRedirection(pooler pool.Pooler) (*EndpointRedirection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	redirection, exists := m.redirections[pooler]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	redirectionCopy := *redirection
	return &redirectionCopy, true
}

// CleanupExpiredRedirections removes expired redirections
func (m *PoolEndpointManager) CleanupExpiredRedirections(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for pooler, redirection := range m.redirections {
		if now.Sub(redirection.StartTime) > redirection.Timeout {
			// TODO: Here we should decide if we need to failback to the original dialer,
			// i.e. if the new endpoint did not produce any active connections.
			delete(m.redirections, pooler)
			delete(m.originalDialers, pooler)
			internal.Logger.Printf(ctx, "hitless: cleaned up expired redirection for pool")
		}
	}
}

// createRedirectDialer creates a dialer that connects to the new endpoint
func (m *PoolEndpointManager) createRedirectDialer(ctx context.Context, newEndpoint string, originalDialer func(context.Context) (net.Conn, error)) func(context.Context) (net.Conn, error) {
	return func(dialCtx context.Context) (net.Conn, error) {
		// Try to connect to the new endpoint
		conn, err := net.DialTimeout("tcp", newEndpoint, 10*time.Second)
		if err != nil {
			internal.Logger.Printf(ctx, "hitless: failed to connect to new endpoint %s: %v", newEndpoint, err)

			// Fallback to original dialer
			return originalDialer(dialCtx)
		}

		internal.Logger.Printf(ctx, "hitless: successfully connected to new endpoint %s", newEndpoint)
		return conn, nil
	}
}

// getPoolDialer extracts the dialer from a connection pool
func (m *PoolEndpointManager) getPoolDialer(connPool *pool.ConnPool) func(context.Context) (net.Conn, error) {
	return connPool.GetDialer()
}

// setPoolDialer sets a new dialer for a connection pool
func (m *PoolEndpointManager) setPoolDialer(connPool *pool.ConnPool, dialer func(context.Context) (net.Conn, error)) error {
	return connPool.SetDialer(dialer)
}

// extractEndpointFromDialer extracts the endpoint address from a dialer
func (m *PoolEndpointManager) extractEndpointFromDialer(dialer func(context.Context) (net.Conn, error)) string {
	// Try to extract endpoint by making a test connection
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	conn, err := dialer(ctx)
	if err != nil {
		return "unknown"
	}
	defer conn.Close()

	if conn.RemoteAddr() != nil {
		return conn.RemoteAddr().String()
	}

	return "unknown"
}

// GetActiveRedirections returns all active redirections
func (m *PoolEndpointManager) GetActiveRedirections() map[interface{}]*EndpointRedirection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create copies to avoid race conditions
	redirections := make(map[interface{}]*EndpointRedirection)
	for pooler, redirection := range m.redirections {
		redirectionCopy := *redirection
		redirections[pooler] = &redirectionCopy
	}

	return redirections
}

// UpdateRedirectionStats updates statistics for a redirection
func (m *PoolEndpointManager) UpdateRedirectionStats(pooler pool.Pooler, newConnections, failedConnections int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if redirection, exists := m.redirections[pooler]; exists {
		redirection.NewConnections += newConnections
		redirection.FailedConnections += failedConnections
	}
}
