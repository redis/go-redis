package pool

import (
	"context"
	"net"
	"sync"
	"time"
)

type netConn struct {
	c       net.Conn
	updated chan struct{}
	wg      sync.WaitGroup

	mu           sync.RWMutex
	readCtx      context.Context
	readCtxVer   int64
	lastReadErr  error
	writeCtx     context.Context
	writeCtxVer  int64
	lastWriteErr error
	closed       bool
}

func newNetConn(c net.Conn) *netConn {
	var nc netConn
	nc.c = c
	nc.updated = make(chan struct{}, 1)
	nc.wg.Add(1)
	go func() {
		nc.serveContexts()
		nc.wg.Done()
	}()
	return &nc
}

func (nc *netConn) serveContexts() {
	readCtx := context.Background()
	readCtxVer := int64(0)
	writeCtx := context.Background()
	writeCtxVer := int64(0)
	for {
		select {
		case <-nc.updated:
			nc.mu.Lock()
			if nc.closed {
				nc.mu.Unlock()
				return
			}
			newReadCtx := nc.readCtx
			newReadCtxVer := nc.readCtxVer
			newWriteCtx := nc.writeCtx
			newWriteCtxVer := nc.writeCtxVer
			nc.mu.Unlock()
			if newReadCtxVer != readCtxVer {
				readCtx = newReadCtx
				readCtxVer = newReadCtxVer
			}
			if newWriteCtxVer != writeCtxVer {
				writeCtx = newWriteCtx
				writeCtxVer = newWriteCtxVer
			}
		case <-readCtx.Done():
			nc.mu.Lock()
			if nc.closed {
				nc.mu.Unlock()
				return
			}
			if nc.readCtxVer == readCtxVer {
				nc.lastReadErr = readCtx.Err()
				// Make Read() return immediately.
				_ = nc.c.SetReadDeadline(time.Now())
			}
			nc.mu.Unlock()
			readCtx = context.Background()
			readCtxVer = 0
		case <-writeCtx.Done():
			nc.mu.Lock()
			if nc.closed {
				nc.mu.Unlock()
				return
			}
			if nc.writeCtxVer == writeCtxVer {
				nc.lastWriteErr = writeCtx.Err()
				// Make Write() return immediately.
				_ = nc.c.SetWriteDeadline(time.Now())
			}
			nc.mu.Unlock()
			writeCtx = context.Background()
			writeCtxVer = 0
		}
	}
}

func (nc *netConn) Read(b []byte) (int, error) {
	n, err := nc.c.Read(b)
	if err, ok := err.(net.Error); ok && err.Timeout() {
		nc.mu.Lock()
		lastReadErr := nc.lastReadErr
		nc.mu.Unlock()
		switch lastReadErr {
		case nil, context.DeadlineExceeded:
		default:
			return n, lastReadErr
		}
	}
	return n, err
}

func (nc *netConn) Write(b []byte) (int, error) {
	n, err := nc.c.Write(b)
	if err, ok := err.(net.Error); ok && err.Timeout() {
		nc.mu.Lock()
		lastWriteErr := nc.lastWriteErr
		nc.mu.Unlock()
		switch lastWriteErr {
		case nil, context.DeadlineExceeded:
		default:
			return n, lastWriteErr
		}
	}
	return n, err
}

func (nc *netConn) Close() error {
	nc.mu.Lock()
	nc.closed = true
	nc.mu.Unlock()
	nc.update()
	nc.wg.Wait()
	return nc.c.Close()
}

func (nc *netConn) LocalAddr() net.Addr  { return nc.c.LocalAddr() }
func (nc *netConn) RemoteAddr() net.Addr { return nc.c.RemoteAddr() }

func (nc *netConn) SetReadContext(ctx context.Context) {
	nc.mu.Lock()
	nc.readCtx = ctx
	nc.readCtxVer++
	if nc.lastReadErr != nil {
		nc.lastReadErr = nil
		_ = nc.c.SetReadDeadline(time.Time{})
	}
	nc.mu.Unlock()
	nc.update()
}

func (nc *netConn) SetWriteContext(ctx context.Context) {
	nc.mu.Lock()
	nc.writeCtx = ctx
	nc.writeCtxVer++
	if nc.lastWriteErr != nil {
		nc.lastWriteErr = nil
		_ = nc.c.SetWriteDeadline(time.Time{})
	}
	nc.mu.Unlock()
	nc.update()
}

func (nc *netConn) update() {
	select {
	case nc.updated <- struct{}{}:
	default:
	}
}
