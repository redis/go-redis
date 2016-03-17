package pool_test

import (
	"errors"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3/internal/pool"
)

var _ = Describe("ConnPool", func() {
	var connPool *pool.ConnPool

	BeforeEach(func() {
		connPool = pool.NewConnPool(
			dummyDialer, 10, time.Hour, time.Millisecond, time.Millisecond)
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("rate limits dial", func() {
		var rateErr error
		for i := 0; i < 1000; i++ {
			cn, err := connPool.Get()
			if err != nil {
				rateErr = err
				break
			}

			_ = connPool.Remove(cn, errors.New("test"))
		}

		Expect(rateErr).To(MatchError(`redis: you open connections too fast (last_error="test")`))
	})

	It("should unblock client when conn is removed", func() {
		// Reserve one connection.
		cn, err := connPool.Get()
		Expect(err).NotTo(HaveOccurred())

		// Reserve all other connections.
		var cns []*pool.Conn
		for i := 0; i < 9; i++ {
			cn, err := connPool.Get()
			Expect(err).NotTo(HaveOccurred())
			cns = append(cns, cn)
		}

		started := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			defer GinkgoRecover()

			started <- true
			_, err := connPool.Get()
			Expect(err).NotTo(HaveOccurred())
			done <- true

			err = connPool.Put(cn)
			Expect(err).NotTo(HaveOccurred())
		}()
		<-started

		// Check that Get is blocked.
		select {
		case <-done:
			Fail("Get is not blocked")
		default:
			// ok
		}

		err = connPool.Remove(cn, errors.New("test"))
		Expect(err).NotTo(HaveOccurred())

		// Check that Ping is unblocked.
		select {
		case <-done:
			// ok
		case <-time.After(time.Second):
			Fail("Get is not unblocked")
		}

		for _, cn := range cns {
			err = connPool.Put(cn)
			Expect(err).NotTo(HaveOccurred())
		}
	})
})

var _ = Describe("conns reaper", func() {
	var connPool *pool.ConnPool

	BeforeEach(func() {
		connPool = pool.NewConnPool(
			dummyDialer, 10, time.Second, time.Millisecond, time.Hour)

		var cns []*pool.Conn

		// add stale connections
		for i := 0; i < 3; i++ {
			cn, err := connPool.Get()
			Expect(err).NotTo(HaveOccurred())
			cn.UsedAt = time.Now().Add(-2 * time.Minute)
			cns = append(cns, cn)
		}

		// add fresh connections
		for i := 0; i < 3; i++ {
			cn := pool.NewConn(&net.TCPConn{})
			cn, err := connPool.Get()
			Expect(err).NotTo(HaveOccurred())
			cns = append(cns, cn)
		}

		for _, cn := range cns {
			Expect(connPool.Put(cn)).NotTo(HaveOccurred())
		}

		Expect(connPool.Len()).To(Equal(6))
		Expect(connPool.FreeLen()).To(Equal(6))

		n, err := connPool.ReapStaleConns()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(3))
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("reaps stale connections", func() {
		Expect(connPool.Len()).To(Equal(3))
		Expect(connPool.FreeLen()).To(Equal(3))
	})

	It("pool is functional", func() {
		for j := 0; j < 3; j++ {
			var freeCns []*pool.Conn
			for i := 0; i < 3; i++ {
				cn, err := connPool.Get()
				Expect(err).NotTo(HaveOccurred())
				Expect(cn).NotTo(BeNil())
				freeCns = append(freeCns, cn)
			}

			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.FreeLen()).To(Equal(0))

			cn, err := connPool.Get()
			Expect(err).NotTo(HaveOccurred())
			Expect(cn).NotTo(BeNil())

			Expect(connPool.Len()).To(Equal(4))
			Expect(connPool.FreeLen()).To(Equal(0))

			err = connPool.Remove(cn, errors.New("test"))
			Expect(err).NotTo(HaveOccurred())

			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.FreeLen()).To(Equal(0))

			for _, cn := range freeCns {
				err := connPool.Put(cn)
				Expect(err).NotTo(HaveOccurred())
			}

			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.FreeLen()).To(Equal(3))
		}
	})
})

var _ = Describe("race", func() {
	var connPool *pool.ConnPool
	var C, N int

	BeforeEach(func() {
		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("does not happen on Get, Put, and Remove", func() {
		connPool = pool.NewConnPool(
			dummyDialer, 10, time.Minute, time.Millisecond, time.Millisecond)
		connPool.DialLimiter = nil

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get()
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					Expect(connPool.Put(cn)).NotTo(HaveOccurred())
				}
			}
		}, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get()
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					Expect(connPool.Remove(cn, errors.New("test"))).NotTo(HaveOccurred())
				}
			}
		})
	})

	It("does not happen on Get and PopFree", func() {
		connPool = pool.NewConnPool(
			dummyDialer, 10, time.Minute, time.Second, time.Millisecond)
		connPool.DialLimiter = nil

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get()
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					Expect(connPool.Put(cn)).NotTo(HaveOccurred())
				}

				cn = connPool.PopFree()
				if cn != nil {
					Expect(connPool.Put(cn)).NotTo(HaveOccurred())
				}
			}
		})
	})
})
