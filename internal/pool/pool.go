package pool

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
)

var Logger = log.New(os.Stderr, "pg: ", log.LstdFlags)

var (
	ErrClosed      = errors.New("redis: client is closed")
	errConnClosed  = errors.New("redis: connection is closed")
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
	Get() (*Conn, error)
	Put(*Conn) error
	Replace(*Conn, error) error
	Len() int
	FreeLen() int
	Stats() *PoolStats
	Close() error
	Closed() bool
}

type dialer func() (net.Conn, error)

type ConnPool struct {
	_dial       dialer
	DialLimiter *ratelimit.RateLimiter
	OnClose     func(*Conn) error

	poolTimeout time.Duration
	idleTimeout time.Duration

	conns     *connList
	freeConns *connStack
	stats     PoolStats

	_closed int32

	lastErr atomic.Value
}

var _ Pooler = (*ConnPool)(nil)

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
		go p.reaper(getIdleCheckFrequency())
	}
	return p
}

func (p *ConnPool) Add(cn *Conn) bool {
	if !p.conns.Reserve() {
		return false
	}
	p.conns.Add(cn)
	p.Put(cn)
	return true
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

func (p *ConnPool) NewConn() (*Conn, error) {
	netConn, err := p.dial()
	if err != nil {
		return nil, err
	}
	return NewConn(netConn), nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get() (*Conn, error) {
	if p.Closed() {
		return nil, ErrClosed
	}

	atomic.AddUint32(&p.stats.Requests, 1)

	// Fetch first non-idle connection, if available.
	if cn := p.First(); cn != nil {
		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	// Try to create a new one.
	if p.conns.Reserve() {
		cn, err := p.NewConn()
		if err != nil {
			p.conns.CancelReservation()
			return nil, err
		}
		p.conns.Add(cn)
		return cn, nil
	}

	// Otherwise, wait for the available connection.
	atomic.AddUint32(&p.stats.Waits, 1)
	if cn := p.wait(); cn != nil {
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Timeouts, 1)
	return nil, ErrPoolTimeout
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

	idx := cn.SetIndex(-1)
	if idx == -1 {
		return nil, errConnClosed
	}

	netConn, err := p.dial()
	if err != nil {
		p.conns.Remove(idx)
		return nil, err
	}

	cn = NewConn(netConn)
	cn.SetIndex(idx)
	p.conns.Replace(cn)

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

func (p *ConnPool) Remove(cn *Conn, reason error) error {
	_ = cn.Close()

	idx := cn.SetIndex(-1)
	if idx == -1 {
		return errConnClosed
	}

	p.storeLastErr(reason.Error())
	p.conns.Remove(idx)
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

func (p *ConnPool) Closed() bool {
	return atomic.LoadInt32(&p._closed) == 1
}

func (p *ConnPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p._closed, 0, 1) {
		return ErrClosed
	}

	// Wait for app to free connections, but don't close them immediately.
	for i := 0; i < p.Len(); i++ {
		if cn := p.wait(); cn == nil {
			break
		}
	}

	// Close all connections.
	cns := p.conns.Reset()
	for _, cn := range cns {
		if cn == nil {
			continue
		}
		if err := p.closeConn(cn); err != nil && retErr == nil {
			retErr = err
		}
	}

	return retErr
}

func (p *ConnPool) closeConn(cn *Conn) error {
	if p.OnClose != nil {
		_ = p.OnClose(cn)
	}
	return cn.Close()
}

func (p *ConnPool) ReapStaleConns() (n int, err error) {
	for {
		cn := p.freeConns.ShiftStale(p.idleTimeout)
		if cn == nil {
			break
		}
		if err = p.Remove(cn, errors.New("connection is stale")); err != nil {
			return
		}
		n++
	}
	return
}

func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for _ = range ticker.C {
		if p.Closed() {
			break
		}
		n, err := p.ReapStaleConns()
		if err != nil {
			Logger.Printf("ReapStaleConns failed: %s", err)
		} else if n > 0 {
			Logger.Printf("removed %d stale connections", n)
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

//------------------------------------------------------------------------------

var idleCheckFrequency atomic.Value

func SetIdleCheckFrequency(d time.Duration) {
	idleCheckFrequency.Store(d)
}

func getIdleCheckFrequency() time.Duration {
	v := idleCheckFrequency.Load()
	if v == nil {
		return time.Minute
	}
	return v.(time.Duration)
}
