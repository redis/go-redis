package pool

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("netConn", func() {
	var nc *netConn
	var peer net.Conn

	BeforeEach(func() {
		c1, c2, err := testMakeSocketPair()
		Expect(err).NotTo(HaveOccurred())
		nc = newNetConn(c1)
		peer = c2
	})

	It("respect read context set", func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_ = cancel
			nc.SetReadContext(ctx)
		}()
		_, err := nc.Read(make([]byte, 1024))
		Expect(err.(net.Error).Timeout()).To(BeTrue())
		_, err = nc.Read(make([]byte, 1024))
		Expect(err.(net.Error).Timeout()).To(BeTrue())
	})

	It("respect write context set", func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_ = cancel
			nc.SetWriteContext(ctx)
		}()
		buf := make([]byte, 10*1024*1024)
		n, err := nc.Write(buf)
		Expect(err.(net.Error).Timeout()).To(BeTrue())
		Expect(n).NotTo(Equal(0))
		n, err = nc.Write(buf)
		Expect(err.(net.Error).Timeout()).To(BeTrue())
		Expect(n).To(Equal(0))
	})

	It("read context can be reset", func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_ = cancel
			nc.SetReadContext(ctx)
			time.Sleep(100 * time.Millisecond)
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
			nc.SetReadContext(ctx)
		}()
		_, err := nc.Read(make([]byte, 1024))
		Expect(err).To(Equal(context.Canceled))
		_, err = nc.Read(make([]byte, 1024))
		Expect(err).To(Equal(context.Canceled))

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_ = cancel
		nc.SetReadContext(ctx)
		go func() {
			time.Sleep(100 * time.Millisecond)
			peer.Write([]byte("hello"))
			peer.Close()
		}()
		b, err := ioutil.ReadAll(nc)
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(Equal([]byte("hello")))
	})

	It("write context can be reset", func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_ = cancel
			nc.SetWriteContext(ctx)
			time.Sleep(100 * time.Millisecond)
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
			nc.SetWriteContext(ctx)
		}()
		buf := make([]byte, 10*1024*1024)
		n, err := nc.Write(buf)
		Expect(err).To(Equal(context.Canceled))
		Expect(n).NotTo(Equal(0))

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_ = cancel
		nc.SetWriteContext(ctx)
		go func() {
			time.Sleep(100 * time.Millisecond)
			_, _ = io.Copy(ioutil.Discard, peer)
		}()
		n, err = nc.Write([]byte("hello"))
		nc.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).NotTo(Equal(0))
	})

	AfterEach(func() {
		nc.Close()
		peer.Close()
	})
})

func testMakeSocketPair() (_ net.Conn, _ net.Conn, returnedErr error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	defer l.Close()
	type AcceptResult struct {
		C   net.Conn
		Err error
	}
	ch := make(chan AcceptResult, 1)
	go func() {
		c, err := l.Accept()
		ch <- AcceptResult{c, err}
	}()
	c1, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		return nil, nil, err
	}
	ar := <-ch
	if ar.Err != nil {
		c1.Close()
		return nil, nil, err
	}
	c2 := ar.C
	return c1, c2, nil
}
