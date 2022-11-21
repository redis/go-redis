package redis

import (
	"context"

	"github.com/go-redis/redis/v9/internal/pool"
	"github.com/go-redis/redis/v9/internal/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type timeoutErr struct {
	error
}

func (e timeoutErr) Timeout() bool {
	return true
}

func (e timeoutErr) Temporary() bool {
	return true
}

func (e timeoutErr) Error() string {
	return "i/o timeout"
}

var _ = Describe("withConn", func() {
	var client *Client

	BeforeEach(func() {
		client = NewClient(&Options{
			PoolSize: 1,
		})
	})

	AfterEach(func() {
		client.Close()
	})

	It("should replace the connection in the pool when there is no error", func() {
		var conn *pool.Conn

		client.withConn(ctx, func(ctx context.Context, c *pool.Conn) error {
			conn = c
			return nil
		})

		newConn, err := client.connPool.Get(ctx)
		Expect(err).To(BeNil())
		Expect(newConn).To(Equal(conn))
	})

	It("should replace the connection in the pool when there is an error not related to a bad connection", func() {
		var conn *pool.Conn

		client.withConn(ctx, func(ctx context.Context, c *pool.Conn) error {
			conn = c
			return proto.RedisError("LOADING")
		})

		newConn, err := client.connPool.Get(ctx)
		Expect(err).To(BeNil())
		Expect(newConn).To(Equal(conn))
	})

	It("should remove the connection from the pool when it times out", func() {
		var conn *pool.Conn

		client.withConn(ctx, func(ctx context.Context, c *pool.Conn) error {
			conn = c
			return timeoutErr{}
		})

		newConn, err := client.connPool.Get(ctx)
		Expect(err).To(BeNil())
		Expect(newConn).NotTo(Equal(conn))
		Expect(client.connPool.Len()).To(Equal(1))
	})
})
