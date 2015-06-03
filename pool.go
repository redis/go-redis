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

	if l.closed() {
		return nil
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

	if l.closed() {
		return newcn.Close()
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

func (l *connList) closed() bool {
	return l.cns == nil
}

type connPool struct {
	dialer func() (*conn, error)

	rl        *ratelimit.RateLimiter
	opt       *Options
	conns     *connList
	freeConns chan *conn

	_closed int32

	lastDialErr error
}

func newConnPool(opt *Options) *connPool {
	p := &connPool{
		dialer: newConnDialer(opt),

		rl:        ratelimit.New(2*opt.getPoolSize(), time.Second),
		opt:       opt,
		conns:     newConnList(opt.getPoolSize()),
		freeConns: make(chan *conn, opt.getPoolSize()),
	}
	if p.opt.getIdleTimeout() > 0 {
		go p.reaper()
	}
	return p
}

func (p *connPool) closed() bool {
	return atomic.LoadInt32(&p._closed) == 1
}

func (p *connPool) isIdle(cn *conn) bool {
	return p.opt.getIdleTimeout() > 0 && time.Since(cn.usedAt) > p.opt.getIdleTimeout()
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
	deadline := time.After(p.opt.getPoolTimeout())
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

	cn, err := p.dialer()
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
	if p.opt.getIdleTimeout() > 0 {
		cn.usedAt = time.Now()
	}
	p.freeConns <- cn
	return nil
}

func (p *connPool) Remove(cn *conn) error {
	// Replace existing connection with new one and unblock waiter.
	newcn, err := p.new()
	if err != nil {
		log.Printf("redis: new failed: %s", err)
		return p.conns.Remove(cn)
	}
	err = p.conns.Replace(cn, newcn)
	p.freeConns <- newcn
	return err
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

func (p *connPool) reaper() {
	ticker := time.NewTicker(time.Minute)
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
	pool     pool
	reusable bool

	cn     *conn
	closed bool
	mx     sync.Mutex
}

func newSingleConnPool(pool pool, reusable bool) *singleConnPool {
	return &singleConnPool{
		pool:     pool,
		reusable: reusable,
	}
}

func (p *singleConnPool) SetConn(cn *conn) {
	p.mx.Lock()
	if p.cn != nil {
		panic("p.cn != nil")
	}
	p.cn = cn
	p.mx.Unlock()
}

func (p *singleConnPool) First() *conn {
	p.mx.Lock()
	cn := p.cn
	p.mx.Unlock()
	return cn
}

func (p *singleConnPool) Get() (*conn, error) {
	defer p.mx.Unlock()
	p.mx.Lock()

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
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.cn != cn {
		panic("p.cn != cn")
	}
	if p.closed {
		return errClosed
	}
	return nil
}

func (p *singleConnPool) Remove(cn *conn) error {
	defer p.mx.Unlock()
	p.mx.Lock()
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
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) FreeLen() int {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.cn == nil {
		return 1
	}
	return 0
}

func (p *singleConnPool) Close() error {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.closed {
		return errClosed
	}
	p.closed = true
	var err error
	if p.cn != nil {
		if p.reusable {
			err = p.pool.Put(p.cn)
			p.cn = nil
		} else {
			err = p.remove()
		}
	}
	return err
}
