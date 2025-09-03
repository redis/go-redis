package pool_test

import (
	"context"
	"errors"
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
			Dialer:          dummyDialer,
			PoolSize:        int32(10),
			PoolTimeout:     time.Hour,
			DialTimeout:     1 * time.Second,
			ConnMaxIdleTime: time.Millisecond,
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
			PoolSize:        int32(10),
			PoolTimeout:     time.Hour,
			DialTimeout:     1 * time.Second,
			ConnMaxIdleTime: time.Millisecond,
			MinIdleConns:    int32(minIdleConns),
		})
		wg.Wait()
		Expect(connPool.Close()).NotTo(HaveOccurred())
		close(closedChan)

		// We wait for 1 second and believe that checkMinIdleConns has been executed.
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
			Dialer:          dummyDialer,
			PoolSize:        int32(poolSize),
			MinIdleConns:    int32(minIdleConns),
			PoolTimeout:     100 * time.Millisecond,
			DialTimeout:     1 * time.Second,
			ConnMaxIdleTime: -1,
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
			Dialer:          dummyDialer,
			PoolSize:        int32(10),
			PoolTimeout:     time.Minute,
			DialTimeout:     1 * time.Second,
			ConnMaxIdleTime: time.Millisecond,
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
			PoolSize:     int32(1000),
			MinIdleConns: int32(50),
			PoolTimeout:  3 * time.Second,
			DialTimeout:  1 * time.Second,
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
			PoolSize:     int32(100),
			MinIdleConns: int32(30),
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
			PoolSize:    int32(1),
			PoolTimeout: 3 * time.Second,
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
			PoolSize:    int32(1),
			PoolTimeout: testPoolTimeout,
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
			PoolTimeout:        time.Second,
			DialTimeout:        time.Second,
			DialerRetries:      3, // Custom retry count
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
			Dialer:      failingDialer,
			PoolSize:    1,
			PoolTimeout: time.Second,
			DialTimeout: time.Second,
			// DialerRetries and DialerRetryTimeout not set - should use defaults
		})
		defer connPool.Close()

		_, err := connPool.Get(ctx)
		if err == nil {
			t.Error("Expected error from failing dialer")
		}

		// Should have attempted 5 times (default DialerRetries = 5)
		finalAttempts := atomic.LoadInt64(&attempts)
		if finalAttempts != 5 {
			t.Errorf("Expected 5 dial attempts (default), got %d", finalAttempts)
		}
	})
}

func init() {
	logging.Disable()
}
