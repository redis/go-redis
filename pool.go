package redis

import (
	"container/list"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/vmihailenco/bufio"
)

var (
	errClosed = errors.New("redis: client is closed")
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
}

//------------------------------------------------------------------------------

type conn struct {
	cn    net.Conn
	rd    reader
	inUse bool

	usedAt time.Time

	readTimeout  time.Duration
	writeTimeout time.Duration

	elem *list.Element
}

func newConnFunc(dial func() (net.Conn, error)) func() (*conn, error) {
	return func() (*conn, error) {
		netcn, err := dial()
		if err != nil {
			return nil, err
		}

		cn := &conn{
			cn: netcn,
		}
		cn.rd = bufio.NewReader(cn)
		return cn, nil
	}
}

func (cn *conn) Read(b []byte) (int, error) {
	if cn.readTimeout != 0 {
		cn.cn.SetReadDeadline(time.Now().Add(cn.readTimeout))
	} else {
		cn.cn.SetReadDeadline(zeroTime)
	}
	return cn.cn.Read(b)
}

func (cn *conn) Write(b []byte) (int, error) {
	if cn.writeTimeout != 0 {
		cn.cn.SetWriteDeadline(time.Now().Add(cn.writeTimeout))
	} else {
		cn.cn.SetWriteDeadline(zeroTime)
	}
	return cn.cn.Write(b)
}

func (cn *conn) Close() error {
	return cn.cn.Close()
}

//------------------------------------------------------------------------------

type connPool struct {
	New func() (*conn, error)

	cond  *sync.Cond
	conns *list.List

	idleNum     int
	maxSize     int
	idleTimeout time.Duration

	closed bool
}

func newConnPool(
	dial func() (*conn, error),
	maxSize int,
	idleTimeout time.Duration,
) *connPool {
	return &connPool{
		New: dial,

		cond:  sync.NewCond(&sync.Mutex{}),
		conns: list.New(),

		maxSize:     maxSize,
		idleTimeout: idleTimeout,
	}
}

func (p *connPool) Get() (*conn, bool, error) {
	p.cond.L.Lock()

	if p.closed {
		p.cond.L.Unlock()
		return nil, false, errClosed
	}

	if p.idleTimeout > 0 {
		for el := p.conns.Front(); el != nil; el = el.Next() {
			cn := el.Value.(*conn)
			if cn.inUse {
				break
			}
			if time.Since(cn.usedAt) > p.idleTimeout {
				if err := p.remove(cn); err != nil {
					glog.Errorf("remove failed: %s", err)
				}
			}
		}
	}

	for p.conns.Len() >= p.maxSize && p.idleNum == 0 {
		p.cond.Wait()
	}

	if p.idleNum > 0 {
		elem := p.conns.Front()
		cn := elem.Value.(*conn)
		if cn.inUse {
			panic("pool: precondition failed")
		}
		cn.inUse = true
		p.conns.MoveToBack(elem)
		p.idleNum--

		p.cond.L.Unlock()
		return cn, false, nil
	}

	if p.conns.Len() < p.maxSize {
		cn, err := p.New()
		if err != nil {
			p.cond.L.Unlock()
			return nil, false, err
		}

		cn.inUse = true
		cn.elem = p.conns.PushBack(cn)

		p.cond.L.Unlock()
		return cn, true, nil
	}

	panic("not reached")
}

func (p *connPool) Put(cn *conn) error {
	if cn.rd.Buffered() != 0 {
		b, _ := cn.rd.ReadN(cn.rd.Buffered())
		glog.Errorf("redis: connection has unread data: %q", b)
		return p.Remove(cn)
	}

	if p.idleTimeout > 0 {
		cn.usedAt = time.Now()
	}

	p.cond.L.Lock()
	if p.closed {
		p.cond.L.Unlock()
		return errClosed
	}
	cn.inUse = false
	p.conns.MoveToFront(cn.elem)
	p.idleNum++
	p.cond.Signal()
	p.cond.L.Unlock()

	return nil
}

func (p *connPool) Remove(cn *conn) error {
	p.cond.L.Lock()
	if p.closed {
		// Noop, connection is already closed.
		p.cond.L.Unlock()
		return nil
	}
	err := p.remove(cn)
	p.cond.Signal()
	p.cond.L.Unlock()
	return err
}

func (p *connPool) remove(cn *conn) error {
	p.conns.Remove(cn.elem)
	cn.elem = nil
	if !cn.inUse {
		p.idleNum--
	}
	return cn.Close()
}

// Len returns number of idle connections.
func (p *connPool) Len() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.idleNum
}

// Size returns number of connections in the pool.
func (p *connPool) Size() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.conns.Len()
}

func (p *connPool) Close() error {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	if p.closed {
		return nil
	}
	p.closed = true
	var retErr error
	for e := p.conns.Front(); e != nil; e = e.Next() {
		if err := p.remove(e.Value.(*conn)); err != nil {
			glog.Errorf("cn.Close failed: %s", err)
			retErr = err
		}
	}
	return retErr
}

//------------------------------------------------------------------------------

type singleConnPool struct {
	pool pool

	l        sync.RWMutex
	cn       *conn
	reusable bool

	closed bool
}

func newSingleConnPool(pool pool, cn *conn, reusable bool) *singleConnPool {
	return &singleConnPool{
		pool:     pool,
		cn:       cn,
		reusable: reusable,
	}
}

func (p *singleConnPool) Get() (*conn, bool, error) {
	p.l.RLock()
	if p.closed {
		p.l.RUnlock()
		return nil, false, errClosed
	}
	if p.cn != nil {
		p.l.RUnlock()
		return p.cn, false, nil
	}
	p.l.RUnlock()

	p.l.Lock()
	cn, isNew, err := p.pool.Get()
	if err != nil {
		p.l.Unlock()
		return nil, false, err
	}
	p.cn = cn
	p.l.Unlock()
	return cn, isNew, nil
}

func (p *singleConnPool) Put(cn *conn) error {
	p.l.Lock()
	if p.cn != cn {
		panic("p.cn != cn")
	}
	if p.closed {
		p.l.Unlock()
		return errClosed
	}
	p.l.Unlock()
	return nil
}

func (p *singleConnPool) Remove(cn *conn) error {
	p.l.Lock()
	if p.cn != cn {
		panic("p.cn != cn")
	}
	if p.closed {
		p.l.Unlock()
		return errClosed
	}
	p.cn = nil
	p.l.Unlock()
	return nil
}

func (p *singleConnPool) Len() int {
	defer p.l.Unlock()
	p.l.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) Size() int {
	defer p.l.Unlock()
	p.l.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *singleConnPool) Close() error {
	defer p.l.Unlock()
	p.l.Lock()

	if p.closed {
		return nil
	}
	p.closed = true

	var err error
	if p.cn != nil {
		if p.reusable {
			err = p.pool.Put(p.cn)
		} else {
			err = p.pool.Remove(p.cn)
		}
	}
	p.cn = nil

	return err
}
