package pool_test

import (
	"bufio"
	"context"
	"net"
	"unsafe"

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
			Dialer:      dummyDialer,
			PoolSize:    1,
			PoolTimeout: 1000,
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Check that default buffer sizes are used (256KiB)
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
	})

	It("should use custom buffer sizes when specified", func() {
		customReadSize := 32 * 1024  // 32KB
		customWriteSize := 64 * 1024 // 64KB

		connPool = pool.NewConnPool(&pool.Options{
			Dialer:          dummyDialer,
			PoolSize:        1,
			PoolTimeout:     1000,
			ReadBufferSize:  customReadSize,
			WriteBufferSize: customWriteSize,
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Check that custom buffer sizes are used
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(customWriteSize))
		Expect(readerBufSize).To(Equal(customReadSize))
	})

	It("should handle zero buffer sizes by using defaults", func() {
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:          dummyDialer,
			PoolSize:        1,
			PoolTimeout:     1000,
			ReadBufferSize:  0, // Should use default
			WriteBufferSize: 0, // Should use default
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Check that default buffer sizes are used (256KiB)
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
	})

	It("should use 256KiB default buffer sizes for standalone NewConn", func() {
		// Test that NewConn (without pool) also uses 256KiB buffers
		netConn := newDummyConn()
		cn := pool.NewConn(netConn)
		defer cn.Close()

		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
	})

	It("should use 256KiB defaults even when pool is created directly without buffer sizes", func() {
		// Test the scenario where someone creates a pool directly (like in tests)
		// without setting ReadBufferSize and WriteBufferSize
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:      dummyDialer,
			PoolSize:    1,
			PoolTimeout: 1000,
			// ReadBufferSize and WriteBufferSize are not set (will be 0)
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Should still get 256KiB defaults because NewConnPool sets them
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 256KiB buffer size
	})
})

// Helper functions to extract buffer sizes using unsafe pointers
func getWriterBufSizeUnsafe(cn *pool.Conn) int {
	cnPtr := (*struct {
		usedAt  int64
		netConn net.Conn
		rd      *proto.Reader
		bw      *bufio.Writer
		wr      *proto.Writer
		// ... other fields
	})(unsafe.Pointer(cn))

	if cnPtr.bw == nil {
		return -1
	}

	bwPtr := (*struct {
		err error
		buf []byte
		n   int
		wr  interface{}
	})(unsafe.Pointer(cnPtr.bw))

	return len(bwPtr.buf)
}

func getReaderBufSizeUnsafe(cn *pool.Conn) int {
	cnPtr := (*struct {
		usedAt  int64
		netConn net.Conn
		rd      *proto.Reader
		bw      *bufio.Writer
		wr      *proto.Writer
		// ... other fields
	})(unsafe.Pointer(cn))

	if cnPtr.rd == nil {
		return -1
	}

	rdPtr := (*struct {
		rd *bufio.Reader
	})(unsafe.Pointer(cnPtr.rd))

	if rdPtr.rd == nil {
		return -1
	}

	bufReaderPtr := (*struct {
		buf          []byte
		rd           interface{}
		r, w         int
		err          error
		lastByte     int
		lastRuneSize int
	})(unsafe.Pointer(rdPtr.rd))

	return len(bufReaderPtr.buf)
}
