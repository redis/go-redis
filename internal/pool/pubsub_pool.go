package pool

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config contains configuration for PubSub connections.
type Config struct {
	PoolFIFO        bool
	PoolSize        int
	PoolTimeout     time.Duration
	DialTimeout     time.Duration
	MinIdleConns    int
	MaxIdleConns    int
	MaxActiveConns  int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration

	// PoolHooks provides a flexible hook system for connection processing
	PoolHooks *PoolHookManager
}

// PubSubPool manages connections specifically for PubSub operations.
// Unlike regular connections, PubSub connections are not pooled and are
// immediately closed when no longer needed.
type PubSubPool struct {
	cfg    *Config
	dialer func(context.Context) (net.Conn, error)

	mu          sync.RWMutex
	connections map[*Conn]struct{}
	closed      bool

	stats struct {
		created uint32
		active  uint32
		closed  uint32
	}
}

func NewPubSubPool(cfg *Config, dialer func(context.Context) (net.Conn, error)) *PubSubPool {
	return &PubSubPool{
		cfg:         cfg,
		dialer:      dialer,
		connections: make(map[*Conn]struct{}),
	}
}

// Get creates a new PubSub connection. PubSub connections are never pooled.
func (p *PubSubPool) Get(ctx context.Context) (*Conn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClosed
	}
	p.mu.RUnlock()

	// Create new connection
	netConn, err := p.dialer(ctx)
	if err != nil {
		return nil, err
	}

	cn := NewConn(netConn)
	cn.pooled = false

	// Process connection if hooks are available
	if p.cfg.PoolHooks != nil {
		if err := p.cfg.PoolHooks.ProcessOnGet(ctx, cn, true); err != nil {
			_ = cn.Close()
			return nil, err
		}
	}

	// Track the connection
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = cn.Close()
		return nil, ErrClosed
	}
	p.connections[cn] = struct{}{}
	p.mu.Unlock()

	atomic.AddUint32(&p.stats.created, 1)
	atomic.AddUint32(&p.stats.active, 1)

	return cn, nil
}

// Put closes the PubSub connection immediately. PubSub connections are never reused.
func (p *PubSubPool) Put(ctx context.Context, cn *Conn) {
	p.Remove(ctx, cn, nil)
}

// Remove closes and removes the PubSub connection.
func (p *PubSubPool) Remove(ctx context.Context, cn *Conn, err error) {
	p.mu.Lock()
	if _, exists := p.connections[cn]; exists {
		delete(p.connections, cn)
		atomic.AddUint32(&p.stats.active, ^uint32(0)) // decrement
		atomic.AddUint32(&p.stats.closed, 1)
	}
	p.mu.Unlock()

	// Process connection before closing if hooks are available
	if p.cfg.PoolHooks != nil {
		_, _, _ = p.cfg.PoolHooks.ProcessOnPut(ctx, cn)
	}

	_ = cn.Close()
}

// Stats returns PubSub connection statistics.
func (p *PubSubPool) Stats() PubSubStats {
	return PubSubStats{
		Created: atomic.LoadUint32(&p.stats.created),
		Active:  atomic.LoadUint32(&p.stats.active),
		Closed:  atomic.LoadUint32(&p.stats.closed),
	}
}

// Close closes all active PubSub connections.
func (p *PubSubPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}
	p.closed = true

	// Close all active connections
	for cn := range p.connections {
		_ = cn.Close()
		atomic.AddUint32(&p.stats.active, ^uint32(0)) // decrement
		atomic.AddUint32(&p.stats.closed, 1)
	}
	p.connections = nil

	return nil
}

// PubSubStats contains statistics for PubSub connections.
type PubSubStats struct {
	Created uint32 // Total PubSub connections created
	Active  uint32 // Currently active PubSub connections
	Closed  uint32 // Total PubSub connections closed
}
