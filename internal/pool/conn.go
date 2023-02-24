package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

var (
	// aLongTimeAgo is a non-zero time, used to immediately unblock the network.
	aLongTimeAgo = time.Unix(1, 0)
	noDeadline   = time.Time{}
)

type Conn struct {
	usedAt  int64 // atomic
	netConn net.Conn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time

	_closed       int32 // atomic
	closeChan     chan struct{}
	watchChan     chan context.Context
	finishChan    chan struct{}
	interruptChan chan error
	watching      bool
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.wr = proto.NewWriter(cn.bw)
	cn.SetUsedAt(time.Now())

	cn.closeChan = make(chan struct{})
	cn.interruptChan = make(chan error)
	cn.finishChan = make(chan struct{})
	cn.watchChan = make(chan context.Context, 1)

	go cn.loopWatcher()

	return cn
}

func (cn *Conn) loopWatcher() {
	var ctx context.Context
	for {
		select {
		case ctx = <-cn.watchChan:
		case <-cn.closeChan:
			return
		}

		select {
		case <-ctx.Done():
			_ = cn.netConn.SetDeadline(aLongTimeAgo)
			cn.interruptChan <- ctx.Err()
		case <-cn.finishChan:
		case <-cn.closeChan:
			return
		}
	}
}

func (cn *Conn) WatchFinish() error {
	if !cn.watching {
		return nil
	}

	var err error
	select {
	case cn.finishChan <- struct{}{}:
		cn.watching = false
	case err = <-cn.interruptChan:
		cn.watching = false
	case <-cn.closeChan:
	}
	return err
}

func (cn *Conn) WatchCancel(ctx context.Context) error {
	if cn.watching {
		panic("repeat watchCancel")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Done() == nil {
		return nil
	}
	if cn.closed() {
		return nil
	}

	cn.watching = true
	cn.watchChan <- ctx
	return nil
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
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

func (cn *Conn) WithReader(
	ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error,
) error {
	if timeout >= 0 {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}
	return fn(cn.rd)
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	if timeout >= 0 {
		if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}

	if cn.bw.Buffered() > 0 {
		cn.bw.Reset(cn.netConn)
	}

	if err := fn(cn.wr); err != nil {
		return err
	}

	return cn.bw.Flush()
}

func (cn *Conn) closed() bool {
	return atomic.LoadInt32(&cn._closed) == 1
}

func (cn *Conn) Close() error {
	if atomic.CompareAndSwapInt32(&cn._closed, 0, 1) {
		close(cn.closeChan)
		return cn.netConn.Close()
	}
	return nil
}

func (cn *Conn) deadline(_ context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		return tm.Add(timeout)
	}

	return noDeadline
}
