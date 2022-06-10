package pool

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal/proto"
)

var noDeadline = time.Time{}

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

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
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
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	cleanupCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)

	errCh := make(chan error, 1)

	// start cancellation goroutine
	go func() {
		defer func() {
			wg.Done()
		}()

		select {
		// catch external context cancellation
		case <-ctx.Done():
			// cancel in-flight read of net.Conn
			errCh <- cn.netConn.SetReadDeadline(time.Unix(1, 0))
			return

		// clean read, cleanup
		case <-cleanupCtx.Done():
			return
		}
	}()

	if timeout != 0 {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			return fmt.Errorf("SetReadDeadline: %w", err)
		}
	}

	err := fn(cn.rd)

	cancel()
	wg.Wait()

	defer func() {
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			// We wrap ctx.Err() to check context.Canceled. SetReadDeadline doesn't
			// produce an error we can check, but it would be good to use something
			// like uber-go/multierr, so the errors can be combined if some of them
			// can be checked against the net, io public package errors
			return fmt.Errorf("SetReadDeadline (background): %s: %w", err.Error(), ctx.Err())
		}
		return ctx.Err()
	default:
		return err
	}
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	if timeout != 0 {
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
