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
	errClosed = errors.New("redis: client is closed")
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
				p.Remove(cn)
				continue
			}
			return cn
		default:
			return nil
		}
	}
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

	// Create a new one
	cn, err := p.new()
	return cn, true, err
}

func (p *connPool) Put(cn *conn) error {
	if cn.rd.Buffered() != 0 {
		b, _ := cn.rd.ReadN(cn.rd.Buffered())
		log.Printf("redis: connection has unread data: %q", b)
		return p.Remove(cn)
	}

	if p.isClosed() {
		cn.Close()
		return errClosed
	}
	if p.opt.IdleTimeout > 0 {
		cn.usedAt = time.Now()
	}

	// Try to return a connection for re-use,
	// or close and discard it if the pool is full
	select {
	case p.freeConns <- cn:
		return nil
	default:
		return p.Remove(cn)
	}
}

// Remove removes a connection
func (p *connPool) Remove(cn *conn) error { return cn.Close() }

// Len returns number of idle connections.
func (p *connPool) Len() int { return len(p.freeConns) }

// Close closes the pool
func (p *connPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	for {
		cn := p.First()
		if cn == nil {
			return
		} else if err := p.Remove(cn); err != nil {
			retErr = err
		}
	}
}

//------------------------------------------------------------------------------

type singleConnPool struct {
	parent pool

	cnMtx sync.Mutex
	cn    *conn

	reusable bool

	closed int32
}

func newSingleConnPool(parent pool, reusable bool) *singleConnPool {
	return &singleConnPool{
		parent:   parent,
		reusable: reusable,
	}
}

func (p *singleConnPool) isClosed() bool { return atomic.LoadInt32(&p.closed) > 0 }

func (p *singleConnPool) SetConn(cn *conn) {
	p.cnMtx.Lock()
	p.cn = cn
	p.cnMtx.Unlock()
}

func (p *singleConnPool) First() *conn {
	p.cnMtx.Lock()
	cn := p.cn
	p.cnMtx.Unlock()
	return cn
}

func (p *singleConnPool) Get() (*conn, bool, error) {
	if p.isClosed() {
		return nil, false, errClosed
	}

	p.cnMtx.Lock()
	defer p.cnMtx.Unlock()

	if p.cn != nil {
		return p.cn, false, nil
	}

	cn, isNew, err := p.parent.Get()
	if err != nil {
		return nil, false, err
	}
	p.cn = cn

	return p.cn, isNew, nil
}

func (p *singleConnPool) Put(cn *conn) error {
	if p.isClosed() {
		return errClosed
	}

	p.cnMtx.Lock()
	ok := p.cn == cn
	p.cnMtx.Unlock()
	if !ok {
		panic("p.cn != cn")
	}
	return nil
}

func (p *singleConnPool) Remove(cn *conn) error {
	return p.parent.Remove(cn)
}

func (p *singleConnPool) Len() int {
	p.cnMtx.Lock()
	cn := p.cn
	p.cnMtx.Unlock()

	if cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	p.cnMtx.Lock()
	if p.cn != nil {
		if p.reusable {
			err = p.parent.Put(p.cn)
		} else {
			err = p.cn.Close()
		}
		p.cn = nil
	}
	p.cnMtx.Unlock()
	return
}
