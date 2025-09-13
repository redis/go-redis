package pool_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/logging"
)

var _ = Describe("ConnPool", func() {
	ctx := context.Background()
	var connPool *pool.ConnPool

	BeforeEach(func() {
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(10),
			MaxConcurrentDials: 10,
			PoolTimeout:        time.Hour,
			DialTimeout:        1 * time.Second,
			ConnMaxIdleTime:    time.Millisecond,
		})
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("should safe close", func() {
		const minIdleConns = 10

		var (
			wg         sync.WaitGroup
			closedChan = make(chan struct{})
		)
		wg.Add(minIdleConns)
		connPool = pool.NewConnPool(&pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				wg.Done()
				<-closedChan
				return &net.TCPConn{}, nil
			},
			PoolSize:           int32(10),
			MaxConcurrentDials: 10,
			PoolTimeout:        time.Hour,
			DialTimeout:        1 * time.Second,
			ConnMaxIdleTime:    time.Millisecond,
			MinIdleConns:       int32(minIdleConns),
		})
		wg.Wait()
		Expect(connPool.Close()).NotTo(HaveOccurred())
		close(closedChan)

		// We wait for 1 second and believe that checkIdleConns has been executed.
		time.Sleep(time.Second)

		Expect(connPool.Stats()).To(Equal(&pool.Stats{
			Hits:           0,
			Misses:         0,
			Timeouts:       0,
			WaitCount:      0,
			WaitDurationNs: 0,
			TotalConns:     0,
			IdleConns:      0,
			StaleConns:     0,
		}))
	})

	It("should unblock client when conn is removed", func() {
		// Reserve one connection.
		cn, err := connPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Reserve all other connections.
		var cns []*pool.Conn
		for i := 0; i < 9; i++ {
			cn, err := connPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			cns = append(cns, cn)
		}

		started := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			defer GinkgoRecover()

			started <- true
			_, err := connPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			done <- true

			connPool.Put(ctx, cn)
		}()
		<-started

		// Check that Get is blocked.
		select {
		case <-done:
			Fail("Get is not blocked")
		case <-time.After(time.Millisecond):
			// ok
		}

		connPool.Remove(ctx, cn, errors.New("test"))

		// Check that Get is unblocked.
		select {
		case <-done:
			// ok
		case <-time.After(time.Second):
			Fail("Get is not unblocked")
		}

		for _, cn := range cns {
			connPool.Put(ctx, cn)
		}
	})
})

var _ = Describe("MinIdleConns", func() {
	const poolSize = 100
	ctx := context.Background()
	var minIdleConns int
	var connPool *pool.ConnPool

	newConnPool := func() *pool.ConnPool {
		connPool := pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(poolSize),
			MaxConcurrentDials: poolSize,
			MinIdleConns:       int32(minIdleConns),
			PoolTimeout:        100 * time.Millisecond,
			DialTimeout:        1 * time.Second,
			ConnMaxIdleTime:    -1,
		})
		Eventually(func() int {
			return connPool.Len()
		}).Should(Equal(minIdleConns))
		return connPool
	}

	assert := func() {
		It("has idle connections when created", func() {
			Expect(connPool.Len()).To(Equal(minIdleConns))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))
		})

		Context("after Get", func() {
			var cn *pool.Conn

			BeforeEach(func() {
				var err error
				cn, err = connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return connPool.Len()
				}).Should(Equal(minIdleConns + 1))
			})

			It("has idle connections", func() {
				Expect(connPool.Len()).To(Equal(minIdleConns + 1))
				Expect(connPool.IdleLen()).To(Equal(minIdleConns))
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					connPool.Remove(ctx, cn, errors.New("test"))
				})

				It("has idle connections", func() {
					Expect(connPool.Len()).To(Equal(minIdleConns))
					Expect(connPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})

		Describe("Get does not exceed pool size", func() {
			var mu sync.RWMutex
			var cns []*pool.Conn

			BeforeEach(func() {
				cns = make([]*pool.Conn, 0)

				perform(poolSize, func(_ int) {
					defer GinkgoRecover()

					cn, err := connPool.Get(ctx)
					Expect(err).NotTo(HaveOccurred())
					mu.Lock()
					cns = append(cns, cn)
					mu.Unlock()
				})

				Eventually(func() int {
					return connPool.Len()
				}).Should(BeNumerically(">=", poolSize))
			})

			It("Get is blocked", func() {
				done := make(chan struct{})
				go func() {
					connPool.Get(ctx)
					close(done)
				}()

				select {
				case <-done:
					Fail("Get is not blocked")
				case <-time.After(time.Millisecond):
					// ok
				}

				select {
				case <-done:
					// ok
				case <-time.After(time.Second):
					Fail("Get is not unblocked")
				}
			})

			Context("after Put", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						connPool.Put(ctx, cns[i])
						mu.RUnlock()
					})

					Eventually(func() int {
						return connPool.Len()
					}).Should(Equal(poolSize))
				})

				It("pool.Len is back to normal", func() {
					Expect(connPool.Len()).To(Equal(poolSize))
					Expect(connPool.IdleLen()).To(Equal(poolSize))
				})
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						connPool.Remove(ctx, cns[i], errors.New("test"))
						mu.RUnlock()
					})

					Eventually(func() int {
						return connPool.Len()
					}).Should(Equal(minIdleConns))
				})

				It("has idle connections", func() {
					Expect(connPool.Len()).To(Equal(minIdleConns))
					Expect(connPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})
	}

	Context("minIdleConns = 1", func() {
		BeforeEach(func() {
			minIdleConns = 1
			connPool = newConnPool()
		})

		AfterEach(func() {
			connPool.Close()
		})

		assert()
	})

	Context("minIdleConns = 32", func() {
		BeforeEach(func() {
			minIdleConns = 32
			connPool = newConnPool()
		})

		AfterEach(func() {
			connPool.Close()
		})

		assert()
	})
})

var _ = Describe("race", func() {
	ctx := context.Background()
	var connPool *pool.ConnPool
	var C, N int

	BeforeEach(func() {
		C, N = 10, 1000
		if testing.Short() {
			C = 2
			N = 50
		}
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("does not happen on Get, Put, and Remove", func() {
		connPool = pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           int32(10),
			MaxConcurrentDials: 10,
			PoolTimeout:        time.Minute,
			DialTimeout:        1 * time.Second,
			ConnMaxIdleTime:    time.Millisecond,
		})

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					connPool.Put(ctx, cn)
				}
			}
		}, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					connPool.Remove(ctx, cn, errors.New("test"))
				}
			}
		})
	})

	It("limit the number of connections", func() {
		opt := &pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				return &net.TCPConn{}, nil
			},
			PoolSize:           int32(1000),
			MaxConcurrentDials: 1000,
			MinIdleConns:       int32(50),
			PoolTimeout:        3 * time.Second,
			DialTimeout:        1 * time.Second,
		}
		p := pool.NewConnPool(opt)

		var wg sync.WaitGroup
		for i := int32(0); i < opt.PoolSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = p.Get(ctx)
			}()
		}
		wg.Wait()

		stats := p.Stats()
		Expect(stats.IdleConns).To(Equal(uint32(0)))
		Expect(stats.TotalConns).To(Equal(uint32(opt.PoolSize)))
	})

	It("recover addIdleConn panic", func() {
		opt := &pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				panic("test panic")
			},
			PoolSize:           int32(100),
			MaxConcurrentDials: 100,
			MinIdleConns:       int32(30),
		}
		p := pool.NewConnPool(opt)

		p.CheckMinIdleConns()

		Eventually(func() bool {
			state := p.Stats()
			return state.TotalConns == 0 && state.IdleConns == 0 && p.QueueLen() == 0
		}, "3s", "50ms").Should(BeTrue())
	})

	It("wait", func() {
		opt := &pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				return &net.TCPConn{}, nil
			},
			PoolSize:           int32(1),
			MaxConcurrentDials: 1,
			PoolTimeout:        3 * time.Second,
		}
		p := pool.NewConnPool(opt)

		wait := make(chan struct{})
		conn, _ := p.Get(ctx)
		go func() {
			_, _ = p.Get(ctx)
			wait <- struct{}{}
		}()
		time.Sleep(time.Second)
		p.Put(ctx, conn)
		<-wait

		stats := p.Stats()
		Expect(stats.IdleConns).To(Equal(uint32(0)))
		Expect(stats.TotalConns).To(Equal(uint32(1)))
		Expect(stats.WaitCount).To(Equal(uint32(1)))
		Expect(stats.WaitDurationNs).To(BeNumerically("~", time.Second.Nanoseconds(), 100*time.Millisecond.Nanoseconds()))
	})

	It("timeout", func() {
		testPoolTimeout := 1 * time.Second
		opt := &pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				// Artificial delay to force pool timeout
				time.Sleep(3 * testPoolTimeout)

				return &net.TCPConn{}, nil
			},
			PoolSize:           int32(1),
			MaxConcurrentDials: 1,
			PoolTimeout:        testPoolTimeout,
		}
		p := pool.NewConnPool(opt)

		stats := p.Stats()
		Expect(stats.Timeouts).To(Equal(uint32(0)))

		conn, err := p.Get(ctx)
		Expect(err).NotTo(HaveOccurred())
		_, err = p.Get(ctx)
		Expect(err).To(MatchError(pool.ErrPoolTimeout))
		p.Put(ctx, conn)
		_, err = p.Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		stats = p.Stats()
		Expect(stats.Timeouts).To(Equal(uint32(1)))
	})
})

// TestDialerRetryConfiguration tests the new DialerRetries and DialerRetryTimeout options
func TestDialerRetryConfiguration(t *testing.T) {
	ctx := context.Background()

	t.Run("CustomDialerRetries", func(t *testing.T) {
		var attempts int64
		failingDialer := func(ctx context.Context) (net.Conn, error) {
			atomic.AddInt64(&attempts, 1)
			return nil, errors.New("dial failed")
		}

		connPool := pool.NewConnPool(&pool.Options{
			Dialer:             failingDialer,
			PoolSize:           1,
			MaxConcurrentDials: 1,
			PoolTimeout:        time.Second,
			DialTimeout:        time.Second,
			DialerRetries:      3,                     // Custom retry count
			DialerRetryTimeout: 10 * time.Millisecond, // Fast retries for testing
		})
		defer connPool.Close()

		_, err := connPool.Get(ctx)
		if err == nil {
			t.Error("Expected error from failing dialer")
		}

		// Should have attempted at least 3 times (DialerRetries = 3)
		// There might be additional attempts due to pool logic
		finalAttempts := atomic.LoadInt64(&attempts)
		if finalAttempts < 3 {
			t.Errorf("Expected at least 3 dial attempts, got %d", finalAttempts)
		}
		if finalAttempts > 6 {
			t.Errorf("Expected around 3 dial attempts, got %d (too many)", finalAttempts)
		}
	})

	t.Run("DefaultDialerRetries", func(t *testing.T) {
		var attempts int64
		failingDialer := func(ctx context.Context) (net.Conn, error) {
			atomic.AddInt64(&attempts, 1)
			return nil, errors.New("dial failed")
		}

		connPool := pool.NewConnPool(&pool.Options{
			Dialer:             failingDialer,
			PoolSize:           1,
			MaxConcurrentDials: 1,
			PoolTimeout:        time.Second,
			DialTimeout:        time.Second,
			// DialerRetries and DialerRetryTimeout not set - should use defaults
		})
		defer connPool.Close()

		_, err := connPool.Get(ctx)
		if err == nil {
			t.Error("Expected error from failing dialer")
		}

		// Should have attempted 5 times (default DialerRetries = 5)
		// There might be 1 additional attempt due to tryDial() recovery mechanism
		finalAttempts := atomic.LoadInt64(&attempts)
		if finalAttempts < 5 {
			t.Errorf("Expected at least 5 dial attempts (default), got %d", finalAttempts)
		}
		if finalAttempts > 6 {
			t.Errorf("Expected around 5 dial attempts, got %d (too many)", finalAttempts)
		}
	})
}

var _ = Describe("queuedNewConn", func() {
	ctx := context.Background()

	It("should successfully create connection when pool is exhausted", func() {
		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           1,
			MaxConcurrentDials: 2,
			DialTimeout:        1 * time.Second,
			PoolTimeout:        2 * time.Second,
		})
		defer testPool.Close()

		// Fill the pool
		conn1, err := testPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(conn1).NotTo(BeNil())

		// Get second connection in another goroutine
		done := make(chan struct{})
		var conn2 *pool.Conn
		var err2 error

		go func() {
			defer GinkgoRecover()
			conn2, err2 = testPool.Get(ctx)
			close(done)
		}()

		// Wait a bit to let the second Get start waiting
		time.Sleep(100 * time.Millisecond)

		// Release first connection to let second Get acquire Turn
		testPool.Put(ctx, conn1)

		// Wait for second Get to complete
		<-done
		Expect(err2).NotTo(HaveOccurred())
		Expect(conn2).NotTo(BeNil())

		// Clean up second connection
		testPool.Put(ctx, conn2)
	})

	It("should handle context cancellation before acquiring dialsInProgress", func() {
		slowDialer := func(ctx context.Context) (net.Conn, error) {
			// Simulate slow dialing to let first connection creation occupy dialsInProgress
			time.Sleep(200 * time.Millisecond)
			return newDummyConn(), nil
		}

		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             slowDialer,
			PoolSize:           2,
			MaxConcurrentDials: 1, // Limit to 1 so second request cannot get dialsInProgress permission
			DialTimeout:        1 * time.Second,
			PoolTimeout:        1 * time.Second,
		})
		defer testPool.Close()

		// Start first connection creation, this will occupy dialsInProgress
		done1 := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			conn1, err := testPool.Get(ctx)
			if err == nil {
				defer testPool.Put(ctx, conn1)
			}
			close(done1)
		}()

		// Wait a bit to ensure first request starts and occupies dialsInProgress
		time.Sleep(50 * time.Millisecond)

		// Create a context that will be cancelled quickly
		cancelCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		// Second request should timeout while waiting for dialsInProgress
		_, err := testPool.Get(cancelCtx)
		Expect(err).To(Equal(context.DeadlineExceeded))

		// Wait for first request to complete
		<-done1
	})

	It("should handle context cancellation while waiting for connection result", func() {
		// This test focuses on proper error handling when context is cancelled
		// during queuedNewConn execution (not testing connection reuse)

		slowDialer := func(ctx context.Context) (net.Conn, error) {
			// Simulate slow dialing
			time.Sleep(500 * time.Millisecond)
			return newDummyConn(), nil
		}

		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             slowDialer,
			PoolSize:           1,
			MaxConcurrentDials: 2,
			DialTimeout:        2 * time.Second,
			PoolTimeout:        2 * time.Second,
		})
		defer testPool.Close()

		// Get first connection to fill the pool
		conn1, err := testPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Create a context that will be cancelled during connection creation
		cancelCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()

		// This request should timeout while waiting for connection creation result
		// Testing the error handling path in queuedNewConn select statement
		done := make(chan struct{})
		var err2 error
		go func() {
			defer GinkgoRecover()
			_, err2 = testPool.Get(cancelCtx)
			close(done)
		}()

		<-done
		Expect(err2).To(Equal(context.DeadlineExceeded))

		// Clean up - release the first connection
		testPool.Put(ctx, conn1)
	})

	It("should handle dial failures gracefully", func() {
		alwaysFailDialer := func(ctx context.Context) (net.Conn, error) {
			return nil, fmt.Errorf("dial failed")
		}

		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             alwaysFailDialer,
			PoolSize:           1,
			MaxConcurrentDials: 1,
			DialTimeout:        1 * time.Second,
			PoolTimeout:        1 * time.Second,
		})
		defer testPool.Close()

		// This call should fail, testing error handling branch in goroutine
		_, err := testPool.Get(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dial failed"))
	})

	It("should handle connection creation success with normal delivery", func() {
		// This test verifies normal case where connection creation and delivery both succeed
		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           1,
			MaxConcurrentDials: 2,
			DialTimeout:        1 * time.Second,
			PoolTimeout:        2 * time.Second,
		})
		defer testPool.Close()

		// Get first connection
		conn1, err := testPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Get second connection in another goroutine
		done := make(chan struct{})
		var conn2 *pool.Conn
		var err2 error

		go func() {
			defer GinkgoRecover()
			conn2, err2 = testPool.Get(ctx)
			close(done)
		}()

		// Wait a bit to let second Get start waiting
		time.Sleep(100 * time.Millisecond)

		// Release first connection
		testPool.Put(ctx, conn1)

		// Wait for second Get to complete
		<-done
		Expect(err2).NotTo(HaveOccurred())
		Expect(conn2).NotTo(BeNil())

		// Clean up second connection
		testPool.Put(ctx, conn2)
	})

	It("should handle MaxConcurrentDials limit", func() {
		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           3,
			MaxConcurrentDials: 1, // Only allow 1 concurrent dial
			DialTimeout:        1 * time.Second,
			PoolTimeout:        1 * time.Second,
		})
		defer testPool.Close()

		// Get all connections to fill the pool
		var conns []*pool.Conn
		for i := 0; i < 3; i++ {
			conn, err := testPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			conns = append(conns, conn)
		}

		// Now pool is full, next request needs to create new connection
		// But due to MaxConcurrentDials=1, only one concurrent dial is allowed
		done := make(chan struct{})
		var err4 error
		go func() {
			defer GinkgoRecover()
			_, err4 = testPool.Get(ctx)
			close(done)
		}()

		// Release one connection to let the request complete
		time.Sleep(100 * time.Millisecond)
		testPool.Put(ctx, conns[0])

		<-done
		Expect(err4).NotTo(HaveOccurred())

		// Clean up remaining connections
		for i := 1; i < len(conns); i++ {
			testPool.Put(ctx, conns[i])
		}
	})

	It("should reuse connections created in background after request timeout", func() {
		// This test focuses on connection reuse mechanism:
		// When a request times out but background connection creation succeeds,
		// the created connection should be added to pool for future reuse

		slowDialer := func(ctx context.Context) (net.Conn, error) {
			// Simulate delay for connection creation
			time.Sleep(100 * time.Millisecond)
			return newDummyConn(), nil
		}

		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             slowDialer,
			PoolSize:           1,
			MaxConcurrentDials: 1,
			DialTimeout:        1 * time.Second,
			PoolTimeout:        150 * time.Millisecond, // Short timeout for waiting Turn
		})
		defer testPool.Close()

		// Fill the pool with one connection
		conn1, err := testPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())
		// Don't put it back yet, so pool is full

		// Start a goroutine that will create a new connection but take time
		done1 := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done1)
			// This will trigger queuedNewConn since pool is full
			conn, err := testPool.Get(ctx)
			if err == nil {
				// Put connection back to pool after creation
				time.Sleep(50 * time.Millisecond)
				testPool.Put(ctx, conn)
			}
		}()

		// Wait a bit to let the goroutine start and begin connection creation
		time.Sleep(50 * time.Millisecond)

		// Now make a request that should timeout waiting for Turn
		start := time.Now()
		_, err = testPool.Get(ctx)
		duration := time.Since(start)

		Expect(err).To(Equal(pool.ErrPoolTimeout))
		// Should timeout around PoolTimeout
		Expect(duration).To(BeNumerically("~", 150*time.Millisecond, 50*time.Millisecond))

		// Release the first connection to allow the background creation to complete
		testPool.Put(ctx, conn1)

		// Wait for background connection creation to complete
		<-done1
		time.Sleep(100 * time.Millisecond)

		// CORE TEST: Verify connection reuse mechanism
		// The connection created in background should now be available in pool
		start = time.Now()
		conn3, err := testPool.Get(ctx)
		duration = time.Since(start)

		Expect(err).NotTo(HaveOccurred())
		Expect(conn3).NotTo(BeNil())
		// Should be fast since connection is from pool (not newly created)
		Expect(duration).To(BeNumerically("<", 50*time.Millisecond))

		testPool.Put(ctx, conn3)
	})

	It("recover queuedNewConn panic", func() {
		opt := &pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				panic("test panic in queuedNewConn")
			},
			PoolSize:           int32(10),
			MaxConcurrentDials: 10,
			DialTimeout:        1 * time.Second,
			PoolTimeout:        1 * time.Second,
		}
		testPool := pool.NewConnPool(opt)
		defer testPool.Close()

		// Trigger queuedNewConn - calling Get() on empty pool will trigger it
		// Since dialer will panic, it should be handled by recover
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Try to get connections multiple times, each will trigger panic but should be properly recovered
		for i := 0; i < 3; i++ {
			conn, err := testPool.Get(ctx)
			// Connection should be nil, error should exist (panic converted to error)
			Expect(conn).To(BeNil())
			Expect(err).To(HaveOccurred())
		}

		// Verify state after panic recovery:
		// - turn should be properly released (QueueLen() == 0)
		// - connection counts should be correct (TotalConns == 0, IdleConns == 0)
		Eventually(func() bool {
			stats := testPool.Stats()
			queueLen := testPool.QueueLen()
			return stats.TotalConns == 0 && stats.IdleConns == 0 && queueLen == 0
		}, "3s", "50ms").Should(BeTrue())
	})

	It("should handle connection creation success but delivery failure (putIdleConn path)", func() {
		// This test covers the most important untested branch in queuedNewConn:
		// cnErr == nil && !delivered -> putIdleConn()

		// Use slow dialer to ensure request times out before connection is ready
		slowDialer := func(ctx context.Context) (net.Conn, error) {
			// Delay long enough for client request to timeout first
			time.Sleep(300 * time.Millisecond)
			return newDummyConn(), nil
		}

		testPool := pool.NewConnPool(&pool.Options{
			Dialer:             slowDialer,
			PoolSize:           1,
			MaxConcurrentDials: 2,
			DialTimeout:        500 * time.Millisecond, // Long enough for dialer to complete
			PoolTimeout:        100 * time.Millisecond, // Client requests will timeout quickly
		})
		defer testPool.Close()

		// Record initial idle connection count
		initialIdleConns := testPool.Stats().IdleConns

		// Make a request that will timeout
		// This request will start queuedNewConn, create connection, but fail to deliver due to timeout
		shortCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		conn, err := testPool.Get(shortCtx)

		// Request should fail due to timeout
		Expect(err).To(HaveOccurred())
		Expect(conn).To(BeNil())

		// However, background queuedNewConn should continue and complete connection creation
		// Since it cannot deliver (request timed out), it should call putIdleConn to add connection to idle pool
		Eventually(func() bool {
			stats := testPool.Stats()
			return stats.IdleConns > initialIdleConns
		}, "1s", "50ms").Should(BeTrue())

		// Verify the connection can indeed be used by subsequent requests
		conn2, err2 := testPool.Get(context.Background())
		Expect(err2).NotTo(HaveOccurred())
		Expect(conn2).NotTo(BeNil())
		Expect(conn2.IsUsable()).To(BeTrue())

		// Cleanup
		testPool.Put(context.Background(), conn2)
	})
})

func init() {
	logging.Disable()
}
