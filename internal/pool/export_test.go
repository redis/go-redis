package pool

import (
	"sync/atomic"
	"time"
)

func (cn *Conn) SetCreatedAt(tm time.Time) {
	cn.createdAt = tm
}

func (p *ConnPool) RunGoroutineNumber() int {
	return int(atomic.LoadInt32(&p.runGoroutine))
}
