package pool

import (
	"errors"
	"sync"
)

type StickyConnPool struct {
	pool     *ConnPool
	reusable bool

	cn     *Conn
	closed bool
	mx     sync.Mutex
}

var _ Pooler = (*StickyConnPool)(nil)

func NewStickyConnPool(pool *ConnPool, reusable bool) *StickyConnPool {
	return &StickyConnPool{
		pool:     pool,
		reusable: reusable,
	}
}

func (p *StickyConnPool) First() *Conn {
	p.mx.Lock()
	cn := p.cn
	p.mx.Unlock()
	return cn
}

func (p *StickyConnPool) Get() (*Conn, bool, error) {
	defer p.mx.Unlock()
	p.mx.Lock()

	if p.closed {
		return nil, false, ErrClosed
	}
	if p.cn != nil {
		return p.cn, false, nil
	}

	cn, _, err := p.pool.Get()
	if err != nil {
		return nil, false, err
	}
	p.cn = cn
	return cn, true, nil
}

func (p *StickyConnPool) put() (err error) {
	err = p.pool.Put(p.cn)
	p.cn = nil
	return err
}

func (p *StickyConnPool) Put(cn *Conn) error {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.closed {
		return ErrClosed
	}
	if p.cn != cn {
		panic("p.cn != cn")
	}
	return nil
}

func (p *StickyConnPool) remove(reason error) error {
	err := p.pool.Remove(p.cn, reason)
	p.cn = nil
	return err
}

func (p *StickyConnPool) Remove(cn *Conn, reason error) error {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.closed {
		return nil
	}
	if p.cn == nil {
		panic("p.cn == nil")
	}
	if cn != nil && p.cn != cn {
		panic("p.cn != cn")
	}
	return p.remove(reason)
}

func (p *StickyConnPool) Len() int {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.cn == nil {
		return 0
	}
	return 1
}

func (p *StickyConnPool) FreeLen() int {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.cn == nil {
		return 1
	}
	return 0
}

func (p *StickyConnPool) Stats() *Stats {
	return nil
}

func (p *StickyConnPool) Close() error {
	defer p.mx.Unlock()
	p.mx.Lock()
	if p.closed {
		return ErrClosed
	}
	p.closed = true
	var err error
	if p.cn != nil {
		if p.reusable {
			err = p.put()
		} else {
			reason := errors.New("redis: sticky not reusable connection")
			err = p.remove(reason)
		}
	}
	return err
}

func (p *StickyConnPool) Closed() bool {
	p.mx.Lock()
	closed := p.closed
	p.mx.Unlock()
	return closed
}
