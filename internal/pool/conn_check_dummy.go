//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !solaris && !illumos

package pool

import (
	"errors"
	"net"
)

// errUnexpectedRead is placeholder error variable for non-unix build constraints
var errUnexpectedRead = errors.New("unexpected read from socket")

// connChecker is a per-connection checker stub for non-Unix platforms.
// On these platforms, we can't perform the syscall-based connection check.
type connChecker struct{}

// check always returns nil on non-Unix platforms since we can't check the socket.
func (c *connChecker) check(_ net.Conn) error {
	return nil
}

// resetRawConn is a no-op on non-Unix platforms.
func (c *connChecker) resetRawConn() {}

func connCheck(_ net.Conn) error {
	return nil
}

// since we can't check for data on the socket, we just assume there is some
func maybeHasData(_ net.Conn) bool {
	return true
}
