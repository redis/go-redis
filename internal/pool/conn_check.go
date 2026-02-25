//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package pool

import (
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

var errUnexpectedRead = errors.New("unexpected read from socket")

// connChecker is a reusable struct to avoid closure allocation in connCheck
type connChecker struct {
	mu     sync.Mutex
	sysErr error
	buf    [1]byte
}

// Global pre-allocated checker to avoid sync.Pool overhead
var globalChecker = &connChecker{}

// checkFn is the reusable function for rawConn.Read
// It's a method on connChecker to avoid capturing variables
func (c *connChecker) checkFn(fd uintptr) bool {
	// Use MSG_PEEK to peek at data without consuming it
	n, _, err := syscall.Recvfrom(int(fd), c.buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)

	switch {
	case n == 0 && err == nil:
		c.sysErr = io.EOF
	case n > 0:
		c.sysErr = errUnexpectedRead
	case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
		c.sysErr = nil
	default:
		c.sysErr = err
	}
	return true
}

// connCheck checks if the connection is still alive and if there is data in the socket
// it will try to peek at the next byte without consuming it since we may want to work with it
// later on (e.g. push notifications)
func connCheck(conn net.Conn) error {
	// Reset previous timeout.
	_ = conn.SetDeadline(time.Time{})

	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}

	// Use global checker with mutex to avoid allocation
	globalChecker.mu.Lock()
	globalChecker.sysErr = nil // Reset

	// Use the method as the callback - no closure allocation!
	err = rawConn.Read(globalChecker.checkFn)
	result := globalChecker.sysErr
	globalChecker.mu.Unlock()

	if err != nil {
		return err
	}

	return result
}

// maybeHasData checks if there is data in the socket without consuming it
func maybeHasData(conn net.Conn) bool {
	return connCheck(conn) == errUnexpectedRead
}
