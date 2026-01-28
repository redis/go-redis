package pool

import (
	"net"
	"time"
)

func (cn *Conn) SetCreatedAt(tm time.Time) {
	cn.createdAt = tm
}

func (cn *Conn) NetConn() net.Conn {
	return cn.getNetConn()
}

func (p *ConnPool) CheckMinIdleConns() {
	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()
}

func (p *ConnPool) QueueLen() int {
	return int(p.semaphore.Len())
}

func (p *ConnPool) DialsQueueLen() int {
	return p.dialsQueue.len()
}

var NoExpiration = noExpiration

func (p *ConnPool) CalcConnExpiresAt() time.Time {
	return p.calcConnExpiresAt()
}
