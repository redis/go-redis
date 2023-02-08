//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos
// +build linux darwin dragonfly freebsd netbsd openbsd solaris illumos

package pool

import (
	"net"
	"net/http/httptest"
	"testing"
	"time"
)

func Test_connCheck(t *testing.T) {
	// tests with real conns
	ts := httptest.NewServer(nil)
	defer ts.Close()

	t.Run("good conn", func(t *testing.T) {
		conn, err := net.DialTimeout(ts.Listener.Addr().Network(), ts.Listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer conn.Close()
		if err = connCheck(conn); err != nil {
			t.Fatalf(err.Error())
		}
		conn.Close()

		if err = connCheck(conn); err == nil {
			t.Fatalf("expect has error")
		}
	})

	t.Run("bad conn 2", func(t *testing.T) {
		conn, err := net.DialTimeout(ts.Listener.Addr().Network(), ts.Listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatalf(err.Error())
		}
		defer conn.Close()

		ts.Close()

		if err = connCheck(conn); err == nil {
			t.Fatalf("expect has err")
		}
	})
}
