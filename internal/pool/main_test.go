package pool_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pool")
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

func dummyDialer(context.Context) (net.Conn, error) {
	return newDummyConn(), nil
}

func newDummyConn() net.Conn {
	return &dummyConn{
		rawConn: new(dummyRawConn),
	}
}

var (
	_ net.Conn     = (*dummyConn)(nil)
	_ syscall.Conn = (*dummyConn)(nil)
)

type dummyConn struct {
	rawConn *dummyRawConn
}

func (d *dummyConn) SyscallConn() (syscall.RawConn, error) {
	return d.rawConn, nil
}

var errDummy = fmt.Errorf("dummyConn err")

func (d *dummyConn) Read(b []byte) (n int, err error) {
	return 0, errDummy
}

func (d *dummyConn) Write(b []byte) (n int, err error) {
	return 0, errDummy
}

func (d *dummyConn) Close() error {
	d.rawConn.Close()
	return nil
}

func (d *dummyConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (d *dummyConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (d *dummyConn) SetDeadline(t time.Time) error {
	return nil
}

func (d *dummyConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (d *dummyConn) SetWriteDeadline(t time.Time) error {
	return nil
}

var _ syscall.RawConn = (*dummyRawConn)(nil)

type dummyRawConn struct {
	mu     sync.Mutex
	closed bool
}

func (d *dummyRawConn) Control(f func(fd uintptr)) error {
	return nil
}

func (d *dummyRawConn) Read(f func(fd uintptr) (done bool)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("dummyRawConn closed")
	}
	return nil
}

func (d *dummyRawConn) Write(f func(fd uintptr) (done bool)) error {
	return nil
}

func (d *dummyRawConn) Close() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
}
