//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !solaris && !illumos

package pool

import "net"

func connCheck(conn net.Conn) error {
	return nil
}

// since we can't check for data on the socket, we just assume there is some
func maybeHasData(conn net.Conn) bool {
	return true
}
