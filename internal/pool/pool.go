package pool

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
)

var Logger *log.Logger

var (
	errClosed      = errors.New("redis: client is closed")
	ErrPoolTimeout = errors.New("redis: connection pool timeout")
)

// PoolStats contains pool state information and accumulated stats.
type PoolStats struct {
	Requests uint32 // number of times a connection was requested by the pool
	Hits     uint32 // number of times free connection was found in the pool
	Waits    uint32 // number of times the pool had to wait for a connection
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // the number of total connections in the pool
	FreeConns  uint32 // the number of free connections in the pool
}

type Pooler interface {
	First() *Conn
	Get() (*Conn, bool, error)
	Put(*Conn) error
	Replace(*Conn, error) error
	Len() int
	FreeLen() int
	Close() error
	Stats() *PoolStats
}

type dialer func() (net.Conn, error)

type ConnPool struct {
	_dial       dialer
	DialLimiter *ratelimit.RateLimiter

	poolTimeout time.Duration
	idleTimeout time.Duration

	conns     *connList
	freeConns *connStack
	stats     PoolStats

	_closed int32

	lastErr atomic.Value
}

func NewConnPool(dial dialer, poolSize int, poolTimeout, idleTimeout time.Duration) *ConnPool {
	p := &ConnPool{
		_dial:       dial,
		DialLimiter: ratelimit.New(3*poolSize, time.Second),

		poolTimeout: poolTimeout,
		idleTimeout: idleTimeout,

		conns:     newConnList(poolSize),
		freeConns: newConnStack(poolSize),
	}
	if idleTimeout > 0 {
		go p.reaper()
	}
	return p
}

func (p *ConnPool) closed() bool {
	return atomic.LoadInt32(&p._closed) == 1
}

func (p *ConnPool) isIdle(cn *Conn) bool {
	return p.idleTimeout > 0 && time.Since(cn.UsedAt) > p.idleTimeout
}

// First returns first non-idle connection from the pool or nil if
// there are no connections.
func (p *ConnPool) First() *Conn {
	for {
		cn := p.freeConns.Pop()
		if cn != nil && cn.IsStale(p.idleTimeout) {
			var err error
			cn, err = p.replace(cn)
			if err != nil {
				Logger.Printf("pool.replace failed: %s", err)
				continue
			}
		}
		return cn
	}
}

// wait waits for free non-idle connection. It returns nil on timeout.
func (p *ConnPool) wait() *Conn {
	for {
		cn := p.freeConns.PopWithTimeout(p.poolTimeout)
		if cn != nil && cn.IsStale(p.idleTimeout) {
			var err error
			cn, err = p.replace(cn)
			if err != nil {
				Logger.Printf("pool.replace failed: %s", err)
				continue
			}
		}
		return cn
	}
}

func (p *ConnPool) dial() (net.Conn, error) {
	if p.DialLimiter != nil && p.DialLimiter.Limit() {
		err := fmt.Errorf(
			"redis: you open connections too fast (last_error=%q)",
			p.loadLastErr(),
		)
		return nil, err
	}

	cn, err := p._dial()
	if err != nil {
		p.storeLastErr(err.Error())
		return nil, err
	}
	return cn, nil
}

func (p *ConnPool) newConn() (*Conn, error) {
	netConn, err := p.dial()
	if err != nil {
		return nil, err
	}
	return NewConn(netConn), nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get() (cn *Conn, isNew bool, err error) {
	if p.closed() {
		err = errClosed
		return
	}

	atomic.AddUint32(&p.stats.Requests, 1)

	// Fetch first non-idle connection, if available.
	if cn = p.First(); cn != nil {
		atomic.AddUint32(&p.stats.Hits, 1)
		return
	}

	// Try to create a new one.
	if p.conns.Reserve() {
		isNew = true

		cn, err = p.newConn()
		if err != nil {
			p.conns.Remove(nil)
			return
		}
		p.conns.Add(cn)
		return
	}

	// Otherwise, wait for the available connection.
	atomic.AddUint32(&p.stats.Waits, 1)
	if cn = p.wait(); cn != nil {
		return
	}

	atomic.AddUint32(&p.stats.Timeouts, 1)
	err = ErrPoolTimeout
	return
}

func (p *ConnPool) Put(cn *Conn) error {
	if cn.Rd.Buffered() != 0 {
		b, _ := cn.Rd.Peek(cn.Rd.Buffered())
		err := fmt.Errorf("connection has unread data: %q", b)
		Logger.Print(err)
		return p.Replace(cn, err)
	}
	p.freeConns.Push(cn)
	return nil
}

func (p *ConnPool) replace(cn *Conn) (*Conn, error) {
	_ = cn.Close()

	netConn, err := p.dial()
	if err != nil {
		_ = p.conns.Remove(cn)
		return nil, err
	}
	cn.SetNetConn(netConn)

	return cn, nil
}

func (p *ConnPool) Replace(cn *Conn, reason error) error {
	p.storeLastErr(reason.Error())

	// Replace existing connection with new one and unblock waiter.
	newcn, err := p.replace(cn)
	if err != nil {
		return err
	}
	p.freeConns.Push(newcn)
	return nil
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	return p.conns.Len()
}

// FreeLen returns number of free connections.
func (p *ConnPool) FreeLen() int {
	return p.freeConns.Len()
}

func (p *ConnPool) Stats() *PoolStats {
	stats := p.stats
	stats.Requests = atomic.LoadUint32(&p.stats.Requests)
	stats.Waits = atomic.LoadUint32(&p.stats.Waits)
	stats.Timeouts = atomic.LoadUint32(&p.stats.Timeouts)
	stats.TotalConns = uint32(p.Len())
	stats.FreeConns = uint32(p.FreeLen())
	return &stats
}

func (p *ConnPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p._closed, 0, 1) {
		return errClosed
	}
	// Wait for app to free connections, but don't close them immediately.
	for i := 0; i < p.Len(); i++ {
		if cn := p.wait(); cn == nil {
			break
		}
	}
	// Close all connections.
	if err := p.conns.Close(); err != nil {
		retErr = err
	}
	return retErr
}

func (p *ConnPool) reaper() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for _ = range ticker.C {
		if p.closed() {
			break
		}

		for {
			cn := p.freeConns.ShiftStale(p.idleTimeout)
			if cn == nil {
				break
			}
			_ = p.conns.Remove(cn)
		}
	}
}

func (p *ConnPool) storeLastErr(err string) {
	p.lastErr.Store(err)
}

func (p *ConnPool) loadLastErr() string {
	if v := p.lastErr.Load(); v != nil {
		return v.(string)
	}
	return ""
}
