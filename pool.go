package redis

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
)

var (
	errClosed      = errors.New("redis: client is closed")
	errPoolTimeout = errors.New("redis: connection pool timeout")
)

type pool interface {
	First() *conn
	Get() (*conn, error)
	Put(*conn) error
	Remove(*conn) error
	Len() int
	FreeLen() int
	Close() error
}

type connList struct {
	cns  []*conn
	mx   sync.Mutex
	len  int32 // atomic
	size int32
}

func newConnList(size int) *connList {
	return &connList{
		cns:  make([]*conn, 0, size),
		size: int32(size),
	}
}

func (l *connList) Len() int {
	return int(atomic.LoadInt32(&l.len))
}

// Reserve reserves place in the list and returns true on success. The
// caller must add or remove connection if place was reserved.
func (l *connList) Reserve() bool {
	len := atomic.AddInt32(&l.len, 1)
	reserved := len <= l.size
	if !reserved {
		atomic.AddInt32(&l.len, -1)
	}
	return reserved
}

// Add adds connection to the list. The caller must reserve place first.
func (l *connList) Add(cn *conn) {
	l.mx.Lock()
	l.cns = append(l.cns, cn)
	l.mx.Unlock()
}

// Remove closes connection and removes it from the list.
func (l *connList) Remove(cn *conn) error {
	defer l.mx.Unlock()
	l.mx.Lock()

	if cn == nil {
		atomic.AddInt32(&l.len, -1)
		return nil
	}

	for i, c := range l.cns {
		if c == cn {
			l.cns = append(l.cns[:i], l.cns[i+1:]...)
			atomic.AddInt32(&l.len, -1)
			return cn.Close()
		}
	}

	panic("conn not found in the list")
}

func (l *connList) Replace(cn, newcn *conn) error {
	defer l.mx.Unlock()
	l.mx.Lock()

	for i, c := range l.cns {
		if c == cn {
			l.cns[i] = newcn
			return cn.Close()
		}
	}

	panic("conn not found in the list")
}

func (l *connList) Close() (retErr error) {
	l.mx.Lock()
	for _, c := range l.cns {
		if err := c.Close(); err != nil {
			retErr = err
		}
	}
	l.cns = nil
	atomic.StoreInt32(&l.len, 0)
	l.mx.Unlock()
	return retErr
}

type connPoolOptions struct {
	Dialer             func() (*conn, error)
	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type connPool struct {
	rl        *ratelimit.RateLimiter
	opt       *connPoolOptions
	conns     *connList
	freeConns chan *conn

	_closed int32

	lastDialErr error
}

func newConnPool(opt *connPoolOptions) *connPool {
	p := &connPool{
		rl:        ratelimit.New(2*opt.PoolSize, time.Second),
		opt:       opt,
		conns:     newConnList(opt.PoolSize),
		freeConns: make(chan *conn, opt.PoolSize),
	}
	if p.opt.IdleTimeout > 0 && p.opt.IdleCheckFrequency > 0 {
		go p.reaper()
	}
	return p
}

func (p *connPool) closed() bool {
	return atomic.LoadInt32(&p._closed) == 1
}

func (p *connPool) isIdle(cn *conn) bool {
	return p.opt.IdleTimeout > 0 && time.Since(cn.usedAt) > p.opt.IdleTimeout
}

// First returns first non-idle connection from the pool or nil if
// there are no connections.
func (p *connPool) First() *conn {
	for {
		select {
		case cn := <-p.freeConns:
			if p.isIdle(cn) {
				p.conns.Remove(cn)
				continue
			}
			return cn
		default:
			return nil
		}
	}
	panic("not reached")
}

// wait waits for free non-idle connection. It returns nil on timeout.
func (p *connPool) wait() *conn {
	deadline := time.After(p.opt.PoolTimeout)
	for {
		select {
		case cn := <-p.freeConns:
			if p.isIdle(cn) {
				p.Remove(cn)
				continue
			}
			return cn
		case <-deadline:
			return nil
		}
	}
	panic("not reached")
}

// Establish a new connection
func (p *connPool) new() (*conn, error) {
	if p.rl.Limit() {
		err := fmt.Errorf(
			"redis: you open connections too fast (last error: %v)",
			p.lastDialErr,
		)
		return nil, err
	}

	cn, err := p.opt.Dialer()
	if err != nil {
		p.lastDialErr = err
		return nil, err
	}

	return cn, nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *connPool) Get() (*conn, error) {
	if p.closed() {
		return nil, errClosed
	}

	// Fetch first non-idle connection, if available.
	if cn := p.First(); cn != nil {
		return cn, nil
	}

	// Try to create a new one.
	if p.conns.Reserve() {
		cn, err := p.new()
		if err != nil {
			p.conns.Remove(nil)
			return nil, err
		}
		p.conns.Add(cn)
		return cn, nil
	}

	// Otherwise, wait for the available connection.
	if cn := p.wait(); cn != nil {
		return cn, nil
	}

	return nil, errPoolTimeout
}

func (p *connPool) Put(cn *conn) error {
	if cn.rd.Buffered() != 0 {
		b, _ := cn.rd.ReadN(cn.rd.Buffered())
		log.Printf("redis: connection has unread data: %q", b)
		return p.Remove(cn)
	}
	if p.opt.IdleTimeout > 0 {
		cn.usedAt = time.Now()
	}
	p.freeConns <- cn
	return nil
}

func (p *connPool) Remove(cn *conn) error {
	if p.closed() {
		// Close already closed all connections.
		return nil
	}

	// Replace existing connection with new one and unblock waiter.
	newcn, err := p.new()
	if err != nil {
		return p.conns.Remove(cn)
	}
	p.freeConns <- newcn
	return p.conns.Replace(cn, newcn)
}

// Len returns total number of connections.
func (p *connPool) Len() int {
	return p.conns.Len()
}

// FreeLen returns number of free connections.
func (p *connPool) FreeLen() int {
	return len(p.freeConns)
}

func (p *connPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p._closed, 0, 1) {
		return errClosed
	}
	// First close free connections.
	for p.Len() > 0 {
		cn := p.wait()
		if cn == nil {
			break
		}
		if err := p.conns.Remove(cn); err != nil {
			retErr = err
		}
	}
	// Then close the rest.
	if err := p.conns.Close(); err != nil {
		retErr = err
	}
	return retErr
}

func (p *connPool) reaper() {
	ticker := time.NewTicker(p.opt.IdleCheckFrequency)
	defer ticker.Stop()

	for _ = range ticker.C {
		if p.closed() {
			break
		}

		// pool.First removes idle connections from the pool and
		// returns first non-idle connection. So just put returned
		// connection back.
		if cn := p.First(); cn != nil {
			p.Put(cn)
		}
	}
}

//------------------------------------------------------------------------------

type singleConnPool struct {
	pool pool

	cnMtx sync.Mutex
	cn    *conn

	reusable bool

	closed bool
}

func newSingleConnPool(pool pool, reusable bool) *singleConnPool {
	return &singleConnPool{
		pool:     pool,
		reusable: reusable,
	}
}

func (p *singleConnPool) SetConn(cn *conn) {
	p.cnMtx.Lock()
	p.cn = cn
	p.cnMtx.Unlock()
}

func (p *singleConnPool) First() *conn {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	return p.cn
}

func (p *singleConnPool) Get() (*conn, error) {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()

	if p.closed {
		return nil, errClosed
	}
	if p.cn != nil {
		return p.cn, nil
	}

	cn, err := p.pool.Get()
	if err != nil {
		return nil, err
	}
	p.cn = cn

	return p.cn, nil
}

func (p *singleConnPool) Put(cn *conn) error {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	if p.cn != cn {
		panic("p.cn != cn")
	}
	if p.closed {
		return errClosed
	}
	return nil
}

func (p *singleConnPool) put() error {
	err := p.pool.Put(p.cn)
	p.cn = nil
	return err
}

func (p *singleConnPool) Remove(cn *conn) error {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	if p.cn == nil {
		panic("p.cn == nil")
	}
	if p.cn != cn {
		panic("p.cn != cn")
	}
	if p.closed {
		return errClosed
	}
	return p.remove()
}

func (p *singleConnPool) remove() error {
	err := p.pool.Remove(p.cn)
	p.cn = nil
	return err
}

func (p *singleConnPool) Len() int {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) FreeLen() int {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) Close() error {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	if p.closed {
		return nil
	}
	p.closed = true
	var err error
	if p.cn != nil {
		if p.reusable {
			err = p.put()
		} else {
			err = p.remove()
		}
	}
	return err
}
