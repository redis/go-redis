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

	BeforeEach(func() {
		ts = httptest.NewServer(nil)
	})

	AfterEach(func() {
		ts.Close()
	})

	It("good conn check", func() {
		conn, err := net.DialTimeout(ts.Listener.Addr().Network(), ts.Listener.Addr().String(), time.Second)
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()

		err = connCheck(conn)
		Expect(err).NotTo(HaveOccurred())

		_ = conn.Close()
		err = connCheck(conn)
		Expect(err).To(HaveOccurred())
	})

	It("bad conn check", func() {
		conn, err := net.DialTimeout(ts.Listener.Addr().Network(), ts.Listener.Addr().String(), time.Second)
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()

		ts.Close()
		err = connCheck(conn)
		Expect(err).To(HaveOccurred())
	})
})