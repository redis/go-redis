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

// WriterBufSize / ReaderBufSize expose the connection's buffer sizes for tests
// without reaching into the struct layout via unsafe.
func (cn *Conn) WriterBufSize() int {
	if cn.bw == nil {
		return -1
	}
	return cn.bw.Size()
}

func (cn *Conn) ReaderBufSize() int {
	if cn.rd == nil {
		return -1
	}
	return cn.rd.Size()
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
