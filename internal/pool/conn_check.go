//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package pool

import (
	"errors"
	"io"
	"net"
	"syscall"
	"time"
	"unsafe"
)

var errUnexpectedRead = errors.New("unexpected read from socket")

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

	var sysErr error

	if err := rawConn.Read(func(fd uintptr) bool {
		var buf [1]byte
		// Use MSG_PEEK to peek at data without consuming it
		n, _, errno := syscall.Syscall6(
			syscall.SYS_RECVFROM,
			fd,
			uintptr(unsafe.Pointer(&buf[0])),
			1,
			syscall.MSG_PEEK, // This ensures the byte stays in the socket buffer
			0, 0,
		)

		switch {
		case n == 0 && errno == 0:
			sysErr = io.EOF
		case n > 0:
			sysErr = errUnexpectedRead
		case errno == syscall.EAGAIN || errno == syscall.EWOULDBLOCK:
			sysErr = nil
		default:
			sysErr = errno
		}
		return true
	}); err != nil {
		return err
	}

	return sysErr
}
