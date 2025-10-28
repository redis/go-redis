package pool_test

import (
	"bufio"
	"context"
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
			PoolSize:    int32(1),
			PoolTimeout: 1000,
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Check that default buffer sizes are used (64KiB)
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
	})

	It("should use custom buffer sizes when specified", func() {
		customReadSize := 32 * 1024  // 32KB
		customWriteSize := 64 * 1024 // 64KB

		connPool = pool.NewConnPool(&pool.Options{
			Dialer:          dummyDialer,
			PoolSize:        int32(1),
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
			PoolSize:        int32(1),
			PoolTimeout:     1000,
			ReadBufferSize:  0, // Should use default
			WriteBufferSize: 0, // Should use default
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Check that default buffer sizes are used (64KiB)
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
	})

	It("should use 64KiB default buffer sizes for standalone NewConn", func() {
		// Test that NewConn (without pool) also uses 64KiB buffers
		netConn := newDummyConn()
		cn := pool.NewConn(netConn)
		defer cn.Close()

		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
	})

	It("should use 64KiB defaults even when pool is created directly without buffer sizes", func() {
		// Test the scenario where someone creates a pool directly (like in tests)
		// without setting ReadBufferSize and WriteBufferSize
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:      dummyDialer,
			PoolSize:    int32(1),
			PoolTimeout: 1000,
			// ReadBufferSize and WriteBufferSize are not set (will be 0)
		})

		cn, err := connPool.NewConn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer connPool.CloseConn(cn)

		// Should still get 64KiB defaults because NewConnPool sets them
		writerBufSize := getWriterBufSizeUnsafe(cn)
		readerBufSize := getReaderBufSizeUnsafe(cn)

		Expect(writerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
		Expect(readerBufSize).To(Equal(proto.DefaultBufferSize)) // Default 64KiB buffer size
	})
})

// Helper functions to extract buffer sizes using unsafe pointers
// The struct layout must match pool.Conn exactly to avoid checkptr violations.
// checkptr is Go's pointer safety checker, which ensures that unsafe pointer
// conversions are valid. If the struct layouts do not match exactly, this can
// cause runtime panics or incorrect memory access due to invalid pointer dereferencing.
func getWriterBufSizeUnsafe(cn *pool.Conn) int {
	cnPtr := (*struct {
		id            uint64      // First field in pool.Conn
		usedAt        int64       // Second field (atomic)
		netConnAtomic interface{} // atomic.Value (interface{} has same size)
		rd            *proto.Reader
		bw            *bufio.Writer
		wr            *proto.Writer
		// We only need fields up to bw, so we can stop here
	})(unsafe.Pointer(cn))

	if cnPtr.bw == nil {
		return -1
	}

	// bufio.Writer internal structure
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
		id            uint64      // First field in pool.Conn
		usedAt        int64       // Second field (atomic)
		netConnAtomic interface{} // atomic.Value (interface{} has same size)
		rd            *proto.Reader
		bw            *bufio.Writer
		wr            *proto.Writer
		// We only need fields up to rd, so we can stop here
	})(unsafe.Pointer(cn))

	if cnPtr.rd == nil {
		return -1
	}

	// proto.Reader internal structure
	rdPtr := (*struct {
		rd *bufio.Reader
	})(unsafe.Pointer(cnPtr.rd))

	if rdPtr.rd == nil {
		return -1
	}

	// bufio.Reader internal structure
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
