package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/proto"
	"go.opentelemetry.io/otel/trace"
)

var noDeadline = time.Time{}

type Conn struct {
	usedAt  int64 // atomic
	netConn *netConn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time
}

func NewConn(c net.Conn) *Conn {
	cn := &Conn{
		netConn:   newNetConn(c),
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(cn.netConn)
	cn.bw = bufio.NewWriter(cn.netConn)
	cn.wr = proto.NewWriter(cn.bw)
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

func (cn *Conn) SetNetConn(c net.Conn) {
	cn.netConn = newNetConn(c)
	cn.rd.Reset(cn.netConn)
	cn.bw.Reset(cn.netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	return internal.WithSpan(ctx, "redis.with_reader", func(ctx context.Context, span trace.Span) error {
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			_ = cancel
		}
		cn.netConn.SetReadContext(ctx)

		if err := fn(cn.rd); err != nil {
			return internal.RecordError(ctx, span, err)
		}
		return nil
	})
}

func (cn *Conn) WithWriter(ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error) error {
	return internal.WithSpan(ctx, "redis.with_writer", func(ctx context.Context, span trace.Span) error {
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			_ = cancel
		}
		cn.netConn.SetWriteContext(ctx)

		if cn.bw.Buffered() > 0 {
			cn.bw.Reset(cn.netConn)
		}

		if err := fn(cn.wr); err != nil {
			return internal.RecordError(ctx, span, err)
		}

		if err := cn.bw.Flush(); err != nil {
			return internal.RecordError(ctx, span, err)
		}

		internal.WritesCounter.Add(ctx, 1)

		return nil
	})
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}
