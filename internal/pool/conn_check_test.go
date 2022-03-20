//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos
// +build linux darwin dragonfly freebsd netbsd openbsd solaris illumos

package pool

import (
	"net"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("tests conn_check with real conns", func() {
	var ts *httptest.Server
	var conn net.Conn
	var err error

	BeforeEach(func() {
		ts = httptest.NewServer(nil)
		conn, err = net.DialTimeout(ts.Listener.Addr().Network(), ts.Listener.Addr().String(), time.Second)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ts.Close()
	})

	It("good conn check", func() {
		Expect(connCheck(conn)).NotTo(HaveOccurred())

		Expect(conn.Close()).NotTo(HaveOccurred())
		Expect(connCheck(conn)).To(HaveOccurred())
	})

	It("bad conn check", func() {
		Expect(conn.Close()).NotTo(HaveOccurred())
		Expect(connCheck(conn)).To(HaveOccurred())
	})

	It("check conn deadline", func() {
		Expect(conn.SetDeadline(time.Now())).NotTo(HaveOccurred())
		time.Sleep(time.Millisecond * 10)
		Expect(connCheck(conn)).NotTo(HaveOccurred())
		Expect(conn.Close()).NotTo(HaveOccurred())
	})
})
