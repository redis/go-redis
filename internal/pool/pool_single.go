package pool

import (
	"context"
	"time"
)

type SingleConnPool struct {
	pool      Pooler
	cn        *Conn
	stickyErr error
}

var _ Pooler = (*SingleConnPool)(nil)

func NewSingleConnPool(pool Pooler, cn *Conn) *SingleConnPool {
	return &SingleConnPool{
		pool: pool,
		cn:   cn,
	}
}

func (p *SingleConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.pool.NewConn(ctx)
}

func (p *SingleConnPool) CloseConn(cn *Conn) error {
	return p.pool.CloseConn(cn)
}

func (p *SingleConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.stickyErr != nil {
		return nil, p.stickyErr
	}
	if p.cn == nil {
		return nil, ErrClosed
	}
	p.cn.Used.Store(true)
	p.cn.SetUsedAt(time.Now())
	return p.cn, nil
}

func (p *SingleConnPool) Put(ctx context.Context, cn *Conn) {
	if p.cn == nil {
		return
	}
	if p.cn != cn {
		return
	}
	p.cn.Used.Store(false)
}

func (p *SingleConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	cn.Used.Store(false)
	p.cn = nil
	p.stickyErr = reason
}

func (p *SingleConnPool) Close() error {
	p.cn = nil
	p.stickyErr = ErrClosed
	return nil
}

func (p *SingleConnPool) Len() int {
	return 0
}

func (p *SingleConnPool) IdleLen() int {
	return 0
}

func (p *SingleConnPool) Size() int { return 1 }

func (p *SingleConnPool) Stats() *Stats {
	return &Stats{}
}

func (p *SingleConnPool) AddPoolHook(hook PoolHook) {}

func (p *SingleConnPool) RemovePoolHook(hook PoolHook) {}
