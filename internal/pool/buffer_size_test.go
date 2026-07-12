package pool_test

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

var _ = Describe("Buffer Size Configuration", func() {
	var connPool *pool.ConnPool
	ctx := context.Background()

	AfterEach(func() {
		if connPool != nil {
			connPool.Close()
		}
	})

	It("should use default buffer sizes when not specified", func() {
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(1),
			MaxConcurrentDials: 1,
			PoolTimeout:        1000,
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(ctx, cn, pool.CloseReasonTest, pool.MetricStateIdle)

		// Check that default buffer sizes are used (32KiB)
		writerBufSize := cn.WriterBufSize()
		readerBufSize := cn.ReaderBufSize()

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
	})

	It("should use custom buffer sizes when specified", func() {
		customReadSize := 32 * 1024  // 32KB
		customWriteSize := 64 * 1024 // 64KB

		connPool = pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(1),
			MaxConcurrentDials: 1,
			PoolTimeout:        1000,
			ReadBufferSize:     customReadSize,
			WriteBufferSize:    customWriteSize,
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(ctx, cn, pool.CloseReasonTest, pool.MetricStateIdle)

		// Check that custom buffer sizes are used
		writerBufSize := cn.WriterBufSize()
		readerBufSize := cn.ReaderBufSize()

		Expect(writerBufSize).To(Equal(customWriteSize))
		Expect(readerBufSize).To(Equal(customReadSize))
	})

	It("should handle zero buffer sizes by using defaults", func() {
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(1),
			MaxConcurrentDials: 1,
			PoolTimeout:        1000,
			ReadBufferSize:     0, // Should use default
			WriteBufferSize:    0, // Should use default
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(ctx, cn, pool.CloseReasonTest, pool.MetricStateIdle)

		// Check that default buffer sizes are used (32KiB)
		writerBufSize := cn.WriterBufSize()
		readerBufSize := cn.ReaderBufSize()

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
	})

	It("should use 32KiB default buffer sizes for standalone NewConn", func() {
		// Test that NewConn (without pool) also uses 32KiB buffers
		netConn := newDummyConn()
		cn := pool.NewConn(netConn)
		defer cn.Close()

		writerBufSize := cn.WriterBufSize()
		readerBufSize := cn.ReaderBufSize()

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
	})

	It("should use 32KiB defaults even when pool is created directly without buffer sizes", func() {
		// Test the scenario where someone creates a pool directly (like in tests)
		// without setting ReadBufferSize and WriteBufferSize
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(1),
			MaxConcurrentDials: 1,
			PoolTimeout:        1000,
			// ReadBufferSize and WriteBufferSize are not set (will be 0)
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(ctx, cn, pool.CloseReasonTest, pool.MetricStateIdle)

		// Should still get 32KiB defaults because NewConnPool sets them
		writerBufSize := cn.WriterBufSize()
		readerBufSize := cn.ReaderBufSize()

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 32KiB buffer size
	})
})
