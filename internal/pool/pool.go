package pool

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/interfaces"
)

var (
	// ErrClosed performs any operation on the closed client will return this error.
	ErrClosed = errors.New("redis: client is closed")

	// ErrPoolExhausted is returned from a pool connection method
	// when the maximum number of database connections in the pool has been reached.
	ErrPoolExhausted = errors.New("redis: connection pool exhausted")

	// ErrPoolTimeout timed out waiting to get a connection from the connection pool.
	ErrPoolTimeout = errors.New("redis: connection pool timeout")

	minTime      = time.Unix(-2208988800, 0) // Jan 1, 1900
	maxTime      = minTime.Add(1<<63 - 1)
	noExpiration = maxTime
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits           uint32 // number of times free connection was found in the pool
	Misses         uint32 // number of times free connection was NOT found in the pool
	Timeouts       uint32 // number of times a wait timeout occurred
	WaitCount      uint32 // number of times a connection was waited
	Unusable       uint32 // number of times a connection was found to be unusable
	WaitDurationNs int64  // total time spent for waiting a connection in nanoseconds

	TotalConns  uint32 // number of total connections in the pool
	IdleConns   uint32 // number of idle connections in the pool
	StaleConns  uint32 // number of stale connections removed from the pool
	PubSubStats PubSubStats
}

type Pooler interface {
	NewConn(context.Context) (*Conn, error)
	CloseConn(*Conn) error

	Get(context.Context) (*Conn, error)
	Put(context.Context, *Conn)
	Remove(context.Context, *Conn, error)

	Len() int
	IdleLen() int
	Stats() *Stats

	Close() error
}

type Options struct {
	Dialer          func(context.Context) (net.Conn, error)
	ReadBufferSize  int
	WriteBufferSize int

	PoolFIFO        bool
	PoolSize        int
	DialTimeout     time.Duration
	PoolTimeout     time.Duration
	MinIdleConns    int
	MaxIdleConns    int
	MaxActiveConns  int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration

	// ConnectionProcessor handles protocol-specific connection processing
	// If nil, connections are processed with default behavior
	ConnectionProcessor interfaces.ConnectionProcessor
}

type lastDialErrorWrap struct {
	err error
}

type ConnPool struct {
	cfg *Options

	dialErrorsNum uint32 // atomic
	lastDialError atomic.Value

	queue chan struct{}

	connsMu   sync.Mutex
	conns     []*Conn
	idleConns []*Conn

	poolSize     atomic.Int32
	idleConnsLen int

	stats          Stats
	waitDurationNs atomic.Int64

	_closed uint32 // atomic
}

var _ Pooler = (*ConnPool)(nil)

func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		cfg: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
	}

	// Only create MinIdleConns if explicitly requested (> 0)
	// This avoids creating connections during pool initialization for tests
	if opt.MinIdleConns > 0 {
		p.connsMu.Lock()
		p.checkMinIdleConns()
		p.connsMu.Unlock()
	}

	return p
}

func (p *ConnPool) checkMinIdleConns() {
	if p.cfg.MinIdleConns == 0 {
		return
	}

	// Only create idle connections if we haven't reached the total pool size limit
	// MinIdleConns should be a subset of PoolSize, not additional connections
	for p.poolSize.Load() < int32(p.cfg.PoolSize) && p.idleConnsLen < p.cfg.MinIdleConns {
		select {
		case p.queue <- struct{}{}:
			p.poolSize.Add(1)
			p.idleConnsLen++
			go func() {
				err := p.addIdleConn()
				if err != nil && err != ErrClosed {
					p.connsMu.Lock()
					p.poolSize.Add(-1)
					p.idleConnsLen--
					p.connsMu.Unlock()
				}
				p.freeTurn()
			}()
		default:
			return
		}
	}
}

func (p *ConnPool) addIdleConn() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.DialTimeout)
	defer cancel()

	cn, err := p.dialConn(ctx, true)
	if err != nil {
		return err
	}
	// Mark connection as usable after successful creation
	// This is essential for normal pool operations
	cn.SetUsable(true)

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		_ = cn.Close()
		return ErrClosed
	}

	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	return nil
}

func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.newConn(ctx, false)
}

func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	p.connsMu.Lock()
	if p.cfg.MaxActiveConns > 0 && p.poolSize.Load() >= int32(p.cfg.MaxActiveConns) {
		p.connsMu.Unlock()
		return nil, ErrPoolExhausted
	}
	p.connsMu.Unlock()

	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}
	// Mark connection as usable after successful creation
	// This is essential for normal pool operations
	cn.SetUsable(true)

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	if p.cfg.MaxActiveConns > 0 && p.poolSize.Load() > int32(p.cfg.MaxActiveConns) {
		_ = cn.Close()
		return nil, ErrPoolExhausted
	}

	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		currentPoolSize := p.poolSize.Load()
		if currentPoolSize >= int32(p.cfg.PoolSize) {
			cn.pooled = false
		} else {
			p.poolSize.Add(1)
		}
	}

	return cn, nil
}

func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.cfg.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.cfg.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.cfg.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConnWithBufferSize(netConn, p.cfg.ReadBufferSize, p.cfg.WriteBufferSize)
	cn.pooled = pooled
	if p.cfg.ConnMaxLifetime > 0 {
		cn.expiresAt = time.Now().Add(p.cfg.ConnMaxLifetime)
	} else {
		cn.expiresAt = noExpiration
	}

	return cn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.DialTimeout)

		conn, err := p.cfg.Dialer(ctx)
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			cancel()
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		cancel()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	return p.getConn(ctx)
}

// getConn returns a connection from the pool.
func (p *ConnPool) getConn(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}

	tries := 0
	now := time.Now()
	for {
		if tries > 10 {
			log.Printf("redis: connection pool: failed to get a connection after %d tries", tries)
			break
		}
		tries++
		p.connsMu.Lock()
		cn, err := p.popIdle()
		p.connsMu.Unlock()

		if err != nil {
			p.freeTurn()
			return nil, err
		}

		if cn == nil {
			break
		}

		if !p.isHealthyConn(cn, now) {
			_ = p.CloseConn(cn)
			continue
		}

		// Process connection using the connection processor if available
		// Fast path: check processor existence once and cache the result
		if processor := p.cfg.ConnectionProcessor; processor != nil {
			if err := processor.ProcessConnectionOnGet(ctx, cn); err != nil {
				// Failed to process connection, discard it
				_ = p.CloseConn(cn)
				continue
			}
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	newcn, err := p.newConn(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

func (p *ConnPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	start := time.Now()
	timer := timers.Get().(*time.Timer)
	defer timers.Put(timer)
	timer.Reset(p.cfg.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	case p.queue <- struct{}{}:
		p.waitDurationNs.Add(time.Now().UnixNano() - start.UnixNano())
		atomic.AddUint32(&p.stats.WaitCount, 1)
		if !timer.Stop() {
			<-timer.C
		}
		return nil
	case <-timer.C:
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

func (p *ConnPool) freeTurn() {
	<-p.queue
}

func (p *ConnPool) popIdle() (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	n := len(p.idleConns)
	if n == 0 {
		return nil, nil
	}

	var cn *Conn
	attempts := 0
	maxAttempts := len(p.idleConns) + 1 // Prevent infinite loop

	for attempts < maxAttempts {
		if len(p.idleConns) == 0 {
			return nil, nil
		}

		if p.cfg.PoolFIFO {
			cn = p.idleConns[0]
			copy(p.idleConns, p.idleConns[1:])
			p.idleConns = p.idleConns[:len(p.idleConns)-1]
		} else {
			idx := len(p.idleConns) - 1
			cn = p.idleConns[idx]
			p.idleConns = p.idleConns[:idx]
		}
		attempts++

		if cn.IsUsable() {
			p.idleConnsLen--
			break
		}

		// Connection is not usable, put it back in the pool
		if p.cfg.PoolFIFO {
			// FIFO: put at end (will be picked up last since we pop from front)
			p.idleConns = append(p.idleConns, cn)
		} else {
			// LIFO: put at beginning (will be picked up last since we pop from end)
			// currently isUsable is only set for hitless upgrades, so this is a no-op
			// but we may need it in the future, so leaving it here for now.
			p.idleConns = append([]*Conn{cn}, p.idleConns...)
		}
	}

	// If we exhausted all attempts without finding a usable connection, return nil
	if attempts >= maxAttempts {
		return nil, nil
	}

	p.checkMinIdleConns()
	return cn, nil
}

func (p *ConnPool) Put(ctx context.Context, cn *Conn) {
	// Process connection using the connection processor if available
	shouldPool := true
	shouldRemove := false
	var err error

	// Fast path: cache processor reference to avoid repeated field access
	if processor := p.cfg.ConnectionProcessor; processor != nil {
		shouldPool, shouldRemove, err = processor.ProcessConnectionOnPut(ctx, cn)
		if err != nil {
			internal.Logger.Printf(ctx, "Connection processor error: %v", err)
			p.Remove(ctx, cn, err)
			return
		}
	}

	// If processor says to remove the connection, do so
	if shouldRemove {
		p.Remove(ctx, cn, nil)
		return
	}

	// If processor says not to pool the connection, remove it
	if !shouldPool {
		p.Remove(ctx, cn, nil)
		return
	}

	if !cn.pooled {
		p.Remove(ctx, cn, nil)
		return
	}

	var shouldCloseConn bool

	p.connsMu.Lock()

	if p.cfg.MaxIdleConns == 0 || p.idleConnsLen < p.cfg.MaxIdleConns {
		p.idleConns = append(p.idleConns, cn)
		p.idleConnsLen++
	} else {
		p.removeConn(cn)
		shouldCloseConn = true
	}

	p.connsMu.Unlock()

	p.freeTurn()

	if shouldCloseConn {
		_ = p.closeConn(cn)
	}
}

func (p *ConnPool) Remove(_ context.Context, cn *Conn, reason error) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.removeConn(cn)
}

func (p *ConnPool) removeConn(cn *Conn) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				p.poolSize.Add(-1)
				// Immediately check for minimum idle connections when a pooled connection is removed
				p.checkMinIdleConns()
			}
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, 1)
}

func (p *ConnPool) closeConn(cn *Conn) error {
	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

func (p *ConnPool) Stats() *Stats {
	return &Stats{
		Hits:           atomic.LoadUint32(&p.stats.Hits),
		Misses:         atomic.LoadUint32(&p.stats.Misses),
		Timeouts:       atomic.LoadUint32(&p.stats.Timeouts),
		WaitCount:      atomic.LoadUint32(&p.stats.WaitCount),
		Unusable:       atomic.LoadUint32(&p.stats.Unusable),
		WaitDurationNs: p.waitDurationNs.Load(),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(p.IdleLen()),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	var firstErr error
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize.Store(0)
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

func (p *ConnPool) isHealthyConn(cn *Conn, now time.Time) bool {
	// slight optimization, check expiresAt first.
	if cn.expiresAt.Before(now) {
		return false
	}

	if p.cfg.ConnMaxIdleTime > 0 && now.Sub(cn.UsedAt()) >= p.cfg.ConnMaxIdleTime {
		return false
	}

	// Check basic connection health
	// Use GetNetConn() to safely access netConn and avoid data races
	if err := connCheck(cn.getNetConn()); err != nil {
		return false
	}

	cn.SetUsedAt(now)
	return true
}
