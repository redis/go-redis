//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package pool

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

type blockingReadUnwrapper struct {
	net.Conn
	readStarted chan struct{}
	releaseRead chan struct{}
}

func (c *blockingReadUnwrapper) NetConn() net.Conn {
	return c.Conn
}

func (c *blockingReadUnwrapper) Read([]byte) (int, error) {
	select {
	case c.readStarted <- struct{}{}:
	default:
	}
	<-c.releaseRead
	return 0, io.EOF
}

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

	It("does not unwrap buffered transports during a pool health check", func() {
		ln, listenErr := net.Listen("tcp", "127.0.0.1:0")
		Expect(listenErr).NotTo(HaveOccurred())
		defer ln.Close()

		accepted := make(chan net.Conn, 1)
		go func() {
			server, acceptErr := ln.Accept()
			if acceptErr == nil {
				accepted <- server
			}
		}()

		rawClient, dialErr := net.Dial("tcp", ln.Addr().String())
		Expect(dialErr).NotTo(HaveOccurred())
		defer rawClient.Close()
		server := <-accepted
		defer server.Close()

		wrapped := &blockingReadUnwrapper{
			Conn:        rawClient,
			readStarted: make(chan struct{}, 1),
			releaseRead: make(chan struct{}),
		}
		defer close(wrapped.releaseRead)

		_, writeErr := server.Write([]byte{1})
		Expect(writeErr).NotTo(HaveOccurred())
		Eventually(func() bool { return maybeHasData(wrapped) }).Should(BeTrue())

		p := NewConnPool(&Options{
			Dialer:                   func(context.Context) (net.Conn, error) { return nil, nil },
			PoolSize:                 1,
			MaxConcurrentDials:       1,
			PushNotificationsEnabled: true,
		})
		defer p.Close()

		healthy := make(chan bool, 1)
		go func() {
			healthy <- p.isHealthyConn(NewConn(wrapped), time.Now().UnixNano())
		}()

		select {
		case ok := <-healthy:
			Expect(ok).To(BeTrue())
		case <-wrapped.readStarted:
			Fail("pool health check read through the buffered wrapper")
		case <-time.After(time.Second):
			Fail("pool health check blocked")
		}
	})

	It("bad conn check", func() {
		Expect(conn.Close()).NotTo(HaveOccurred())
		Expect(connCheck(conn)).To(HaveOccurred())
	})

	It("reports a remotely closed socket to CSC", func() {
		ln, listenErr := net.Listen("tcp", "127.0.0.1:0")
		Expect(listenErr).NotTo(HaveOccurred())
		defer ln.Close()

		accepted := make(chan net.Conn, 1)
		go func() {
			server, acceptErr := ln.Accept()
			if acceptErr == nil {
				accepted <- server
			}
		}()

		client, dialErr := net.Dial("tcp", ln.Addr().String())
		Expect(dialErr).NotTo(HaveOccurred())
		defer client.Close()
		server := <-accepted
		Expect(server.Close()).NotTo(HaveOccurred())

		cn := NewConn(client)
		Eventually(func() bool {
			_, checkErr := cn.CheckForData()
			return checkErr != nil
		}).Should(BeTrue())
	})

	It("check conn deadline", func() {
		Expect(conn.SetDeadline(time.Now())).NotTo(HaveOccurred())
		time.Sleep(time.Millisecond * 10)
		Expect(connCheck(conn)).NotTo(HaveOccurred())
		Expect(conn.Close()).NotTo(HaveOccurred())
	})
})
