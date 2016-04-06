package pool

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
)

var Logger = log.New(ioutil.Discard, "redis: ", log.LstdFlags)

var (
	ErrClosed      = errors.New("redis: client is closed")
	ErrPoolTimeout = errors.New("redis: connection pool timeout")
	errConnStale   = errors.New("connection is stale")
)

var timers = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(0)
	},
}

// PoolStats contains pool state information and accumulated stats.
// TODO: remove Waits
type PoolStats struct {
	Requests uint32 // number of times a connection was requested by the pool
	Hits     uint32 // number of times free connection was found in the pool
	Waits    uint32 // number of times the pool had to wait for a connection
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // the number of total connections in the pool
	FreeConns  uint32 // the number of free connections in the pool
}

type Pooler interface {
	Get() (*Conn, error)
	Put(*Conn) error
	Remove(*Conn, error) error
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

	queue chan struct{}

	connsMu sync.Mutex
	conns   []*Conn

	freeConnsMu sync.Mutex
	freeConns   []*Conn

	stats PoolStats

	_closed int32 // atomic
	lastErr atomic.Value
}

var _ Pooler = (*ConnPool)(nil)

func NewConnPool(dial dialer, poolSize int, poolTimeout, idleTimeout, idleCheckFrequency time.Duration) *ConnPool {
	p := &ConnPool{
		_dial:       dial,
		DialLimiter: ratelimit.New(3*poolSize, time.Second),

		poolTimeout: poolTimeout,
		idleTimeout: idleTimeout,

		queue:     make(chan struct{}, poolSize),
		conns:     make([]*Conn, 0, poolSize),
		freeConns: make([]*Conn, 0, poolSize),
	}
	for i := 0; i < poolSize; i++ {
		p.queue <- struct{}{}
	}
	if idleTimeout > 0 && idleCheckFrequency > 0 {
		go p.reaper(idleCheckFrequency)
	}
	return p
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

func (p *ConnPool) PopFree() *Conn {
	timer := timers.Get().(*time.Timer)
	if !timer.Reset(p.poolTimeout) {
		<-timer.C
	}

	select {
	case <-p.queue:
		timers.Put(timer)
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return nil
	}

	p.freeConnsMu.Lock()
	cn := p.popFree()
	p.freeConnsMu.Unlock()

	if cn == nil {
		p.queue <- struct{}{}
	}
	return cn
}

func (p *ConnPool) popFree() *Conn {
	if len(p.freeConns) == 0 {
		return nil
	}

	idx := len(p.freeConns) - 1
	cn := p.freeConns[idx]
	p.freeConns = p.freeConns[:idx]
	return cn
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get() (*Conn, error) {
	if p.Closed() {
		return nil, ErrClosed
	}

	atomic.AddUint32(&p.stats.Requests, 1)

	timer := timers.Get().(*time.Timer)
	if !timer.Reset(p.poolTimeout) {
		<-timer.C
	}

	select {
	case <-p.queue:
		timers.Put(timer)
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return nil, ErrPoolTimeout
	}

	p.freeConnsMu.Lock()
	cn := p.popFree()
	p.freeConnsMu.Unlock()

	if cn != nil {
		atomic.AddUint32(&p.stats.Hits, 1)
		if !cn.IsStale(p.idleTimeout) {
			return cn, nil
		}
		_ = cn.Close()
	}

	newcn, err := p.NewConn()
	if err != nil {
		p.queue <- struct{}{}
		return nil, err
	}

	p.connsMu.Lock()
	if cn != nil {
		p.remove(cn, errConnStale)
	}
	p.conns = append(p.conns, newcn)
	p.connsMu.Unlock()

	return newcn, nil
}

func (p *ConnPool) Put(cn *Conn) error {
	if cn.Rd.Buffered() != 0 {
		b, _ := cn.Rd.Peek(cn.Rd.Buffered())
		err := fmt.Errorf("connection has unread data: %q", b)
		Logger.Print(err)
		return p.Remove(cn, err)
	}
	p.freeConnsMu.Lock()
	p.freeConns = append(p.freeConns, cn)
	p.freeConnsMu.Unlock()
	p.queue <- struct{}{}
	return nil
}

func (p *ConnPool) Remove(cn *Conn, reason error) error {
	_ = cn.Close()
	p.connsMu.Lock()
	p.remove(cn, reason)
	p.connsMu.Unlock()
	p.queue <- struct{}{}
	return nil
}

func (p *ConnPool) remove(cn *Conn, reason error) {
	p.storeLastErr(reason.Error())
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	l := len(p.conns)
	p.connsMu.Unlock()
	return l
}

// FreeLen returns number of free connections.
func (p *ConnPool) FreeLen() int {
	p.freeConnsMu.Lock()
	l := len(p.freeConns)
	p.freeConnsMu.Unlock()
	return l
}

func (p *ConnPool) Stats() *PoolStats {
	stats := PoolStats{}
	stats.Requests = atomic.LoadUint32(&p.stats.Requests)
	stats.Hits = atomic.LoadUint32(&p.stats.Hits)
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

	p.connsMu.Lock()

	// Close all connections.
	for _, cn := range p.conns {
		if cn == nil {
			continue
		}
		if err := p.closeConn(cn); err != nil && retErr == nil {
			retErr = err
		}
	}
	p.conns = nil
	p.connsMu.Unlock()

	p.freeConnsMu.Lock()
	p.freeConns = nil
	p.freeConnsMu.Unlock()

	return retErr
}

func (p *ConnPool) closeConn(cn *Conn) error {
	if p.OnClose != nil {
		_ = p.OnClose(cn)
	}
	return cn.Close()
}

func (p *ConnPool) ReapStaleConns() (n int, err error) {
	<-p.queue
	p.freeConnsMu.Lock()

	if len(p.freeConns) == 0 {
		p.freeConnsMu.Unlock()
		p.queue <- struct{}{}
		return
	}

	var idx int
	var cn *Conn
	for idx, cn = range p.freeConns {
		if !cn.IsStale(p.idleTimeout) {
			break
		}
		p.connsMu.Lock()
		p.remove(cn, errConnStale)
		p.connsMu.Unlock()
		n++
	}
	if idx > 0 {
		p.freeConns = append(p.freeConns[:0], p.freeConns[idx:]...)
	}

	p.freeConnsMu.Unlock()
	p.queue <- struct{}{}
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
			continue
		}
		s := p.Stats()
		Logger.Printf(
			"reaper: removed %d stale conns (TotalConns=%d FreeConns=%d Requests=%d Hits=%d Timeouts=%d)",
			n, s.TotalConns, s.FreeConns, s.Requests, s.Hits, s.Timeouts,
		)
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
