package pool

import (
	"net"
	"time"
)

func (cn *Conn) SetCreatedAt(tm time.Time) {
	cn.createdAt = tm
}

func (cn *Conn) NetConn() net.Conn {
	return cn.netConn
}

func (p *ConnPool) CheckMinIdleConns() {
	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()
}

func (p *ConnPool) QueueLen() int {
	return len(p.queue)
}
