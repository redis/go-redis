//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package pool

import (
	"errors"
	"io"
	"net"
	"syscall"
	"time"
)

var errUnexpectedRead = errors.New("unexpected read from socket")

// connChecker is a per-connection checker that avoids closure allocations.
// Each Conn has its own connChecker instance, eliminating the need for a global mutex.
type connChecker struct {
	rawConn syscall.RawConn // Cached RawConn to avoid allocation on every check
	buf     [1]byte         // Reusable buffer for peeking
	sysErr  error           // Result of the check (set by checkFn method)
}

// checkFn is a method that implements the check logic without closure allocation.
// It's called by rawConn.Read() and sets c.sysErr with the result.
func (c *connChecker) checkFn(fd uintptr) bool {
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

// check performs the connection health check using the cached RawConn.
// Returns nil if connection is healthy, io.EOF if closed, errUnexpectedRead if data available.
func (c *connChecker) check(conn net.Conn) error {
	// Reset previous timeout.
	_ = conn.SetDeadline(time.Time{})

	// Initialize rawConn lazily if not cached
	if c.rawConn == nil {
		sysConn, ok := conn.(syscall.Conn)
		if !ok {
			return nil
		}
		rawConn, err := sysConn.SyscallConn()
		if err != nil {
			return err
		}
		c.rawConn = rawConn
	}

	// Reset sysErr before check
	c.sysErr = nil

	// Use method reference instead of closure - no allocation!
	if err := c.rawConn.Read(c.checkFn); err != nil {
		return err
	}

	return c.sysErr
}

// resetRawConn clears the cached RawConn.
// This should be called when the underlying net.Conn is replaced (e.g., during handoff).
func (c *connChecker) resetRawConn() {
	c.rawConn = nil
}

// connCheck checks if the connection is still alive and if there is data in the socket
// it will try to peek at the next byte without consuming it since we may want to work with it
// later on (e.g. push notifications)
//
// Deprecated: Use Conn.connChecker.check() for better performance.
// This function is kept for backwards compatibility with code that doesn't have access to Conn.
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

	var sysErr error

	if err := rawConn.Read(func(fd uintptr) bool {
		var buf [1]byte
		// Use MSG_PEEK to peek at data without consuming it
		n, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)

		switch {
		case n == 0 && err == nil:
			sysErr = io.EOF
		case n > 0:
			sysErr = errUnexpectedRead
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			sysErr = nil
		default:
			sysErr = err
		}
		return true
	}); err != nil {
		return err
	}

	return sysErr
}

// maybeHasData checks if there is data in the socket without consuming it
func maybeHasData(conn net.Conn) bool {
	return connCheck(conn) == errUnexpectedRead
}
