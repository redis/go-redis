package redis

import (
	"container/list"
	"net"
	"sync"
	"time"

	"github.com/vmihailenco/bufio"
)

type pool interface {
	Get() (*conn, bool, error)
	Put(*conn) error
	Remove(*conn) error
	Len() int
	Close() error
}

//------------------------------------------------------------------------------

type conn struct {
	cn     net.Conn
	rd     reader
	usedAt time.Time

	readTimeout, writeTimeout time.Duration
}

func newConn(netcn net.Conn) *conn {
	cn := &conn{
		cn: netcn,
	}
	cn.rd = bufio.NewReaderSize(cn, 1024)
	return cn
}

func (cn *conn) Read(b []byte) (int, error) {
	if cn.readTimeout != 0 {
		cn.cn.SetReadDeadline(time.Now().Add(cn.readTimeout))
	} else {
		cn.cn.SetReadDeadline(time.Time{})
	}
	return cn.cn.Read(b)
}

func (cn *conn) Write(b []byte) (int, error) {
	if cn.writeTimeout != 0 {
		cn.cn.SetWriteDeadline(time.Now().Add(cn.writeTimeout))
	} else {
		cn.cn.SetReadDeadline(time.Time{})
	}
	return cn.cn.Write(b)
}

//------------------------------------------------------------------------------

type connPool struct {
	dial func() (net.Conn, error)

	cond  *sync.Cond
	conns *list.List

	size, maxSize int
	idleTimeout   time.Duration
}

func newConnPool(
	dial func() (net.Conn, error),
	maxSize int,
	idleTimeout time.Duration,
) *connPool {
	return &connPool{
		dial: dial,

		cond:  sync.NewCond(&sync.Mutex{}),
		conns: list.New(),

		maxSize:     maxSize,
		idleTimeout: idleTimeout,
	}
}

func (p *connPool) Get() (*conn, bool, error) {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	for p.conns.Len() == 0 && p.size >= p.maxSize {
		p.cond.Wait()
	}

	if p.idleTimeout > 0 {
		for e := p.conns.Front(); e != nil; e = e.Next() {
			cn := e.Value.(*conn)
			if time.Since(cn.usedAt) > p.idleTimeout {
				p.conns.Remove(e)
			}
		}
	}

	if p.conns.Len() == 0 {
		rw, err := p.dial()
		if err != nil {
			return nil, false, err
		}

		p.size++
		return newConn(rw), true, nil
	}

	elem := p.conns.Front()
	p.conns.Remove(elem)
	return elem.Value.(*conn), false, nil
}

func (p *connPool) Put(cn *conn) error {
	if cn.rd.Buffered() != 0 {
		panic("redis: attempt to put connection with buffered data")
	}

	p.cond.L.Lock()
	cn.usedAt = time.Now()
	p.conns.PushFront(cn)
	p.cond.Signal()
	p.cond.L.Unlock()
	return nil
}

func (p *connPool) Remove(cn *conn) error {
	var err error
	if cn != nil {
		err = p.closeConn(cn)
	}
	p.cond.L.Lock()
	p.size--
	p.cond.Signal()
	p.cond.L.Unlock()
	return err
}

func (p *connPool) Len() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.conns.Len()
}

func (p *connPool) Size() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.size
}

func (p *connPool) Close() error {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	for e := p.conns.Front(); e != nil; e = e.Next() {
		if err := p.closeConn(e.Value.(*conn)); err != nil {
			return err
		}
	}
	p.conns.Init()
	p.size = 0

	return nil
}

func (p *connPool) closeConn(cn *conn) error {
	return cn.cn.Close()
}

//------------------------------------------------------------------------------

type singleConnPool struct {
	pool pool

	l        sync.RWMutex
	cn       *conn
	reusable bool
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
	if p.cn != nil {
		p.l.RUnlock()
		return p.cn, false, nil
	}
	p.l.RUnlock()

	defer p.l.Unlock()
	p.l.Lock()

	cn, isNew, err := p.pool.Get()
	if err != nil {
		return nil, false, err
	}
	p.cn = cn

	return cn, isNew, nil
}

func (p *singleConnPool) Put(cn *conn) error {
	defer p.l.Unlock()
	p.l.Lock()
	if p.cn != cn {
		panic("p.cn != cn")
	}
	return nil
}

func (p *singleConnPool) Remove(cn *conn) error {
	defer p.l.Unlock()
	p.l.Lock()
	if p.cn != cn {
		panic("p.cn != cn")
	}
	p.cn = nil
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

func (p *singleConnPool) Close() error {
	defer p.l.Unlock()
	p.l.Lock()

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
