package redis

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/bufio.v1"
)

var (
	errClosed      = errors.New("redis: client is closed")
	errPoolTimeout = errors.New("redis: connection pool timeout")
)

var (
	zeroTime = time.Time{}
)

type pool interface {
	Get() (*conn, bool, error)
	Put(*conn) error
	Remove(*conn) error
	Len() int
	Size() int
	Close() error
	Filter(func(*conn) bool)
}

//------------------------------------------------------------------------------

type conn struct {
	netcn net.Conn
	rd    *bufio.Reader
	buf   []byte

	usedAt       time.Time
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func newConnFunc(dial func() (net.Conn, error)) func() (*conn, error) {
	return func() (*conn, error) {
		netcn, err := dial()
		if err != nil {
			return nil, err
		}
		cn := &conn{
			netcn: netcn,
			buf:   make([]byte, 0, 64),
		}
		cn.rd = bufio.NewReader(cn)
		return cn, nil
	}
}

func (cn *conn) Read(b []byte) (int, error) {
	if cn.readTimeout != 0 {
		cn.netcn.SetReadDeadline(time.Now().Add(cn.readTimeout))
	} else {
		cn.netcn.SetReadDeadline(zeroTime)
	}
	return cn.netcn.Read(b)
}

func (cn *conn) Write(b []byte) (int, error) {
	if cn.writeTimeout != 0 {
		cn.netcn.SetWriteDeadline(time.Now().Add(cn.writeTimeout))
	} else {
		cn.netcn.SetWriteDeadline(zeroTime)
	}
	return cn.netcn.Write(b)
}

func (cn *conn) RemoteAddr() net.Addr {
	return cn.netcn.RemoteAddr()
}

func (cn *conn) Close() error {
	return cn.netcn.Close()
}

func (cn *conn) isIdle(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.usedAt) > timeout
}

//------------------------------------------------------------------------------

type connPool struct {
	dial func() (*conn, error)
	rl   *rateLimiter

	opt   *options
	conns chan *conn

	size   int32
	closed int32

	lastDialErr error
}

func newConnPool(dial func() (*conn, error), opt *options) *connPool {
	return &connPool{
		dial: dial,
		rl:   newRateLimiter(time.Second, 2*opt.PoolSize),

		opt:   opt,
		conns: make(chan *conn, opt.PoolSize),
	}
}

func (p *connPool) isClosed() bool { return atomic.LoadInt32(&p.closed) > 0 }

// First available connection, non-blocking
func (p *connPool) first() *conn {
	for {
		select {
		case cn := <-p.conns:
			if !cn.isIdle(p.opt.IdleTimeout) {
				return cn
			}
			p.remove(cn)
		default:
			return nil
		}
	}
	panic("not reached")
}

// Wait for available connection, blocking
func (p *connPool) wait() (*conn, error) {
	deadline := time.After(p.opt.PoolTimeout)
	for {
		select {
		case cn := <-p.conns:
			if !cn.isIdle(p.opt.IdleTimeout) {
				return cn, nil
			}
			p.remove(cn)
		case <-deadline:
			return nil, errPoolTimeout
		}
	}
	panic("not reached")
}

// Establish a new connection
func (p *connPool) new() (*conn, error) {
	if !p.rl.Check() {
		err := fmt.Errorf(
			"redis: you open connections too fast (last error: %s)",
			p.lastDialErr,
		)
		return nil, err
	}
	cn, err := p.dial()
	if err != nil {
		p.lastDialErr = err
	}
	return cn, err
}

func (p *connPool) Get() (*conn, bool, error) {
	if p.isClosed() {
		return nil, false, errClosed
	}

	// Fetch first non-idle connection, if available
	if cn := p.first(); cn != nil {
		return cn, false, nil
	}

	// Try to create a new one
	if ref := atomic.AddInt32(&p.size, 1); int(ref) <= p.opt.PoolSize {
		cn, err := p.new()
		if err != nil {
			atomic.AddInt32(&p.size, -1) // Undo ref increment
			return nil, false, err
		}
		return cn, true, nil
	}
	atomic.AddInt32(&p.size, -1)

	// Otherwise, wait for the available connection
	cn, err := p.wait()
	return cn, false, err
}

func (p *connPool) Put(cn *conn) error {
	if cn.rd.Buffered() != 0 {
		b, _ := cn.rd.ReadN(cn.rd.Buffered())
		log.Printf("redis: connection has unread data: %q", b)
		return p.Remove(cn)
	}

	if p.isClosed() {
		return errClosed
	}
	if p.opt.IdleTimeout > 0 {
		cn.usedAt = time.Now()
	}
	p.conns <- cn
	return nil
}

func (p *connPool) Remove(cn *conn) error {
	if p.isClosed() {
		return nil
	}
	return p.remove(cn)
}

func (p *connPool) remove(cn *conn) error {
	atomic.AddInt32(&p.size, -1)
	return cn.Close()
}

// Len returns number of idle connections.
func (p *connPool) Len() int {
	return len(p.conns)
}

// Size returns number of connections in the pool.
func (p *connPool) Size() int {
	return int(atomic.LoadInt32(&p.size))
}

func (p *connPool) Filter(f func(*conn) bool) {
	for {
		select {
		case cn := <-p.conns:
			if !f(cn) {
				p.remove(cn)
			}
		default:
			return
		}
	}
	panic("not reached")
}

func (p *connPool) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}
	p.rl.Close()

	for {
		if p.Size() < 1 {
			break
		}
		if e := p.remove(<-p.conns); e != nil {
			err = e
		}
	}
	return
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

func (p *singleConnPool) Get() (*conn, bool, error) {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()

	if p.closed {
		return nil, false, errClosed
	}
	if p.cn != nil {
		return p.cn, false, nil
	}

	cn, isNew, err := p.pool.Get()
	if err != nil {
		return nil, false, err
	}
	p.cn = cn

	return p.cn, isNew, nil
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

func (p *singleConnPool) Size() int {
	defer p.cnMtx.Unlock()
	p.cnMtx.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) Filter(f func(*conn) bool) {
	p.cnMtx.Lock()
	if p.cn != nil {
		if !f(p.cn) {
			p.remove()
		}
	}
	p.cnMtx.Unlock()
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
