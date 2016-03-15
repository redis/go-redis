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

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pool")
}

var _ = Describe("conns reapser", func() {
	var connPool *pool.ConnPool

	BeforeEach(func() {
		dial := func() (net.Conn, error) {
			return &net.TCPConn{}, nil
		}
		connPool = pool.NewConnPool(dial, 10, 0, time.Minute)

		// add stale connections
		for i := 0; i < 3; i++ {
			cn := pool.NewConn(&net.TCPConn{})
			cn.UsedAt = time.Now().Add(-2 * time.Minute)
			Expect(connPool.Add(cn)).To(BeTrue())
			Expect(cn.Index()).To(Equal(i))
		}

		// add fresh connections
		for i := 0; i < 3; i++ {
			cn := pool.NewConn(&net.TCPConn{})
			Expect(connPool.Add(cn)).To(BeTrue())
			Expect(cn.Index()).To(Equal(3 + i))
		}

		Expect(connPool.Len()).To(Equal(6))
		Expect(connPool.FreeLen()).To(Equal(6))

		n, err := connPool.ReapStaleConns()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(3))
	})

	It("reaps stale connections", func() {
		Expect(connPool.Len()).To(Equal(3))
		Expect(connPool.FreeLen()).To(Equal(3))
	})

	It("pool is functional", func() {
		for j := 0; j < 3; j++ {
			var freeCns []*pool.Conn
			for i := 0; i < 3; i++ {
				cn := connPool.First()
				Expect(cn).NotTo(BeNil())
				freeCns = append(freeCns, cn)
			}

			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.FreeLen()).To(Equal(0))

			cn := connPool.First()
			Expect(cn).To(BeNil())

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
