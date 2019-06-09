package pool

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/internal/proto"
)

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	rd *proto.Reader
	wr *proto.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time
	usedAt    int64 // atomic
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(netConn)
	cn.wr = proto.NewWriter(netConn)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.wr.Reset(netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	tm := cn.deadline(ctx, timeout)
	_ = cn.netConn.SetReadDeadline(tm)
	return fn(cn.rd)
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	tm := cn.deadline(ctx, timeout)
	_ = cn.netConn.SetWriteDeadline(tm)

	firstErr := fn(cn.wr)
	err := cn.wr.Flush()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	now := time.Now()
	cn.SetUsedAt(now)

	if ctx != nil {
		tm, ok := ctx.Deadline()
		if ok {
			return tm
		}
	}

	if timeout > 0 {
		return now.Add(timeout)
	}

	return noDeadline
}
