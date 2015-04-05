package redis

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
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
	First() *conn
	Get() (*conn, bool, error)
	Put(*conn) error
	Remove(*conn) error
	Len() int
	Size() int
	Close() error
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

func (cn *conn) writeCmds(cmds ...Cmder) error {
	buf := cn.buf[:0]
	for _, cmd := range cmds {
		buf = appendArgs(buf, cmd.args())
	}

	_, err := cn.Write(buf)
	return err
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
	rl   *ratelimit.RateLimiter

	opt       *options
	freeConns chan *conn

	size   int32
	closed int32

	lastDialErr error
}

func newConnPool(dial func() (*conn, error), opt *options) *connPool {
	return &connPool{
		dial: dial,
		rl:   ratelimit.New(2*opt.PoolSize, time.Second),

		opt:       opt,
		freeConns: make(chan *conn, opt.PoolSize),
	}
}

func (p *connPool) isClosed() bool { return atomic.LoadInt32(&p.closed) > 0 }

// First returns first non-idle connection from the pool or nil if
// there are no connections.
func (p *connPool) First() *conn {
	for {
		select {
		case cn := <-p.freeConns:
			if cn.isIdle(p.opt.IdleTimeout) {
				p.remove(cn)
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
func (p *connPool) wait(timeout time.Duration) *conn {
	deadline := time.After(timeout)
	for {
		select {
		case cn := <-p.freeConns:
			if cn.isIdle(p.opt.IdleTimeout) {
				p.remove(cn)
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
	cn, err := p.dial()
	if err != nil {
		p.lastDialErr = err
	}
	return cn, err
}

// Get returns existed connection from the pool or creates a new one
// if needed.
func (p *connPool) Get() (*conn, bool, error) {
	if p.isClosed() {
		return nil, false, errClosed
	}

	// Fetch first non-idle connection, if available
	if cn := p.First(); cn != nil {
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
	if cn := p.wait(p.opt.PoolTimeout); cn != nil {
		return cn, false, nil
	}

	return nil, false, errPoolTimeout
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
	p.freeConns <- cn
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
	return len(p.freeConns)
}

// Size returns number of connections in the pool.
func (p *connPool) Size() int {
	return int(atomic.LoadInt32(&p.size))
}

func (p *connPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	// Wait until pool has no connections
	for p.Size() > 0 {
		cn := p.wait(p.opt.PoolTimeout)
		if cn == nil {
			break
		}
		if err := p.remove(cn); err != nil {
			retErr = err
		}
	}

	return retErr
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
