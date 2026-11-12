package pool

import (
	"context"
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

// PooledLen returns the number of pooled conns (PoolSize soft cap); excess
// non-pooled conns are not counted.
func (p *ConnPool) PooledLen() int {
	return int(p.poolSize.Load())
}

func (p *ConnPool) QueueLen() int {
	return int(p.semaphore.Len())
}

func (p *ConnPool) DialsQueueLen() int {
	return p.dialsQueue.len()
}

// DialWaitQueueLen returns the number of Get() callers currently parked waiting
// for an idle connection after being throttled by the dial rate limiter.
func (p *ConnPool) DialWaitQueueLen() int {
	if p.dialWaitQueue == nil {
		return 0
	}
	return p.dialWaitQueue.len()
}

// DialTokens returns the dial rate limiter's current token count (after
// refill), or 0 when rate limiting is disabled.
func (p *ConnPool) DialTokens() float64 {
	l := p.dialLimiter
	if l == nil {
		return 0
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(now)
	return l.tokens
}

// dialConn is dialConnTracked without dial tracking, for the dial retry tests.
func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	return p.dialConnTracked(ctx, pooled, nil)
}

var NoExpiration = noExpiration

func (p *ConnPool) CalcConnExpiresAt() time.Time {
	return p.calcConnExpiresAt()
}
