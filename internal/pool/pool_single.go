package pool

import (
	"context"

	"github.com/go-redis/redis/internal"
)

type SingleConnPool struct {
	cn       *Conn
	cnClosed bool
}

var _ Pooler = (*SingleConnPool)(nil)

func NewSingleConnPool(cn *Conn) *SingleConnPool {
	return &SingleConnPool{
		cn: cn,
	}
}

func (p *SingleConnPool) NewConn(context.Context) (*Conn, error) {
	panic("not implemented")
}

func (p *SingleConnPool) CloseConn(*Conn) error {
	panic("not implemented")
}

func (p *SingleConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.cnClosed {
		return nil, internal.ErrSingleConnPoolClosed
	}
	return p.cn, nil
}

func (p *SingleConnPool) Put(cn *Conn) {
	if p.cn != cn {
		panic("p.cn != cn")
	}
}

func (p *SingleConnPool) Remove(cn *Conn) {
	if p.cn != cn {
		panic("p.cn != cn")
	}
	p.cnClosed = true
}

func (p *SingleConnPool) Len() int {
	if p.cnClosed {
		return 0
	}
	return 1
}

func (p *SingleConnPool) IdleLen() int {
	return 0
}

func (p *SingleConnPool) Stats() *Stats {
	return nil
}

func (p *SingleConnPool) Close() error {
	return nil
}
