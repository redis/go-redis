package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/proto"
)

var noDeadline = time.Time{}

type tracedConn struct {
	net.Conn
	ctx context.Context
}

type Conn struct {
	usedAt  int64 // atomic
	netConn net.Conn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time
}

func newTracedConn(conn net.Conn) net.Conn {
	return &tracedConn{
		conn,
		context.Background(),
	}
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(newTracedConn(netConn))
	cn.bw = bufio.NewWriter(newTracedConn(netConn))
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

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	return internal.WithSpan(ctx, "with_reader", func(ctx context.Context) error {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			internal.CountReadError(ctx, err)
			return internal.RecordError(ctx, err)
		}
		if err := fn(cn.rd); err != nil {
			internal.CountReadError(ctx, err)
			return internal.RecordError(ctx, err)
		}

		internal.ReadsCounter.Add(ctx, 1)

		return nil
	})
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	return internal.WithSpan(ctx, "with_writer", func(ctx context.Context) error {
		if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
			internal.CountWriteError(ctx, err)
			return internal.RecordError(ctx, err)
		}

		if cn.bw.Buffered() > 0 {
			cn.bw.Reset(cn.netConn)
		}

		if err := fn(cn.wr); err != nil {
			internal.CountWriteError(ctx, err)
			return internal.RecordError(ctx, err)
		}

		if err := cn.bw.Flush(); err != nil {
			internal.CountWriteError(ctx, err)
			return internal.RecordError(ctx, err)
		}

		internal.WritesCounter.Add(ctx, 1)

		return nil
	})
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}

func (t *tracedConn) Write(b []byte) (int, error) {
	n, err := t.Conn.Write(b)
	internal.BytesWritten.Record(t.ctx, int64(n))
	return n, err
}

func (t *tracedConn) Read(b []byte) (int, error) {
	n, err := t.Conn.Read(b)
	internal.BytesRead.Record(t.ctx, int64(n))
	return n, err
}
