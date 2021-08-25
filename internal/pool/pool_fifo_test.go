package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8/internal/pool"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FifoConnPool", func() {
	ctx := context.Background()
	var fifoConnPool *pool.FifoConnPool

	BeforeEach(func() {
		fifoConnPool = pool.NewFifoConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           10,
			PoolTimeout:        time.Hour,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond,
		})
	})

	AfterEach(func() {
		fifoConnPool.Close()
	})

	It("should unblock client when conn is removed", func() {
		// Reserve one connection.
		cn, err := fifoConnPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Reserve all other connections.
		var cns []*pool.Conn
		for i := 0; i < 9; i++ {
			cn, err := fifoConnPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			cns = append(cns, cn)
		}

		started := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			defer GinkgoRecover()

			started <- true
			_, err := fifoConnPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			done <- true

			fifoConnPool.Put(ctx, cn)
		}()
		<-started

		// Check that Get is blocked.
		select {
		case <-done:
			Fail("Get is not blocked")
		case <-time.After(time.Millisecond):
			// ok
		}

		fifoConnPool.Remove(ctx, cn, nil)

		// Check that Get is unblocked.
		select {
		case <-done:
			// ok
		case <-time.After(time.Second):
			Fail("Get is not unblocked")
		}

		for _, cn := range cns {
			fifoConnPool.Put(ctx, cn)
		}
	})
})

var _ = Describe("FifoPoolMinIdleConns", func() {
	const poolSize = 100
	ctx := context.Background()
	var minIdleConns int
	var fifoConnPool *pool.FifoConnPool

	newConnPool := func() *pool.FifoConnPool {
		fifoConnPool := pool.NewFifoConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           poolSize,
			MinIdleConns:       minIdleConns,
			PoolTimeout:        100 * time.Millisecond,
			IdleTimeout:        -1,
			IdleCheckFrequency: -1,
		})
		Eventually(func() int {
			return fifoConnPool.Len()
		}).Should(Equal(minIdleConns))
		return fifoConnPool
	}

	assert := func() {
		It("has idle connections when created", func() {
			Expect(fifoConnPool.Len()).To(Equal(minIdleConns))
			Expect(fifoConnPool.IdleLen()).To(Equal(minIdleConns))
		})

		Context("after Get", func() {
			var cn *pool.Conn

			BeforeEach(func() {
				var err error
				cn, err = fifoConnPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return fifoConnPool.Len()
				}).Should(Equal(minIdleConns + 1))
			})

			It("has idle connections", func() {
				Expect(fifoConnPool.Len()).To(Equal(minIdleConns + 1))
				Expect(fifoConnPool.IdleLen()).To(Equal(minIdleConns))
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					fifoConnPool.Remove(ctx, cn, nil)
				})

				It("has idle connections", func() {
					Expect(fifoConnPool.Len()).To(Equal(minIdleConns))
					Expect(fifoConnPool.IdleLen()).To(Equal(minIdleConns))
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

					cn, err := fifoConnPool.Get(ctx)
					Expect(err).NotTo(HaveOccurred())
					mu.Lock()
					cns = append(cns, cn)
					mu.Unlock()
				})

				Eventually(func() int {
					return fifoConnPool.Len()
				}).Should(BeNumerically(">=", poolSize))
			})

			It("Get is blocked", func() {
				done := make(chan struct{})
				go func() {
					fifoConnPool.Get(ctx)
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
						fifoConnPool.Put(ctx, cns[i])
						mu.RUnlock()
					})

					Eventually(func() int {
						return fifoConnPool.Len()
					}).Should(Equal(poolSize))
				})

				It("pool.Len is back to normal", func() {
					Expect(fifoConnPool.Len()).To(Equal(poolSize))
					Expect(fifoConnPool.IdleLen()).To(Equal(poolSize))
				})
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						fifoConnPool.Remove(ctx, cns[i], nil)
						mu.RUnlock()
					})

					Eventually(func() int {
						return fifoConnPool.Len()
					}).Should(Equal(minIdleConns))
				})

				It("has idle connections", func() {
					Expect(fifoConnPool.Len()).To(Equal(minIdleConns))
					Expect(fifoConnPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})
	}

	Context("minIdleConns = 1", func() {
		BeforeEach(func() {
			minIdleConns = 1
			fifoConnPool = newConnPool()
		})

		AfterEach(func() {
			fifoConnPool.Close()
		})

		assert()
	})

	Context("minIdleConns = 32", func() {
		BeforeEach(func() {
			minIdleConns = 32
			fifoConnPool = newConnPool()
		})

		AfterEach(func() {
			fifoConnPool.Close()
		})

		assert()
	})
})

var _ = Describe("conns reaper in fifo", func() {
	const idleTimeout = time.Second
	const maxAge = time.Hour

	ctx := context.Background()
	var fifoConnPool *pool.FifoConnPool
	var conns, staleConns, closedConns []*pool.Conn

	assert := func(typ string) {
		BeforeEach(func() {
			closedConns = nil
			fifoConnPool = pool.NewFifoConnPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           10,
				IdleTimeout:        idleTimeout,
				MaxConnAge:         maxAge,
				PoolTimeout:        time.Second,
				IdleCheckFrequency: time.Hour,
				OnClose: func(cn *pool.Conn) error {
					closedConns = append(closedConns, cn)
					return nil
				},
			})
			time.Sleep(100 * time.Millisecond)

			conns = nil

			// add stale connections
			staleConns = nil
			for i := 0; i < 3; i++ {
				cn, err := fifoConnPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				switch typ {
				case "aged":
					cn.SetCreatedAt(time.Now().Add(-2 * maxAge))
				}
				conns = append(conns, cn)
				staleConns = append(staleConns, cn)
			}

			// add fresh connections
			for i := 0; i < 3; i++ {
				cn, err := fifoConnPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				// make them cannot be recycled after 1 idle timeout
				cn.SetCreatedAt(time.Now().Add(2 * idleTimeout))
				conns = append(conns, cn)
			}

			for _, cn := range conns {
				fifoConnPool.Put(ctx, cn)
			}

			if typ == "idle" {
				// make them idle
				time.Sleep(idleTimeout * 12 / 10)
			}

			Expect(fifoConnPool.Len()).To(Equal(6))
			Expect(fifoConnPool.IdleLen()).To(Equal(6))

			n, err := fifoConnPool.ReapStaleConns()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(3))
		})

		AfterEach(func() {
			_ = fifoConnPool.Close()
			Expect(fifoConnPool.Len()).To(Equal(0))
			Expect(fifoConnPool.IdleLen()).To(Equal(0))
			Expect(len(closedConns)).To(Equal(len(conns)))
			Expect(closedConns).To(ConsistOf(conns))
		})

		It("reaps stale connections", func() {
			Expect(fifoConnPool.Len()).To(Equal(3))
			Expect(fifoConnPool.IdleLen()).To(Equal(3))
		})

		It("does not reap fresh connections", func() {
			n, err := fifoConnPool.ReapStaleConns()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(0))
		})

		It("stale connections are closed", func() {
			Expect(len(closedConns)).To(Equal(len(staleConns)))
			Expect(closedConns).To(ConsistOf(staleConns))
		})

		It("pool is functional", func() {
			for j := 0; j < 3; j++ {
				var freeCns []*pool.Conn
				for i := 0; i < 3; i++ {
					cn, err := fifoConnPool.Get(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(cn).NotTo(BeNil())
					freeCns = append(freeCns, cn)
				}

				Expect(fifoConnPool.Len()).To(Equal(3))
				Expect(fifoConnPool.IdleLen()).To(Equal(0))

				cn, err := fifoConnPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cn).NotTo(BeNil())
				conns = append(conns, cn)

				Expect(fifoConnPool.Len()).To(Equal(4))
				Expect(fifoConnPool.IdleLen()).To(Equal(0))

				fifoConnPool.Remove(ctx, cn, nil)

				Expect(fifoConnPool.Len()).To(Equal(3))
				Expect(fifoConnPool.IdleLen()).To(Equal(0))

				for _, cn := range freeCns {
					fifoConnPool.Put(ctx, cn)
				}

				Expect(fifoConnPool.Len()).To(Equal(3))
				Expect(fifoConnPool.IdleLen()).To(Equal(3))
			}
		})
	}

	assert("idle")
	assert("aged")
})

var _ = Describe("race in fifo", func() {
	ctx := context.Background()
	var fifoConnPool *pool.FifoConnPool
	var C, N int

	BeforeEach(func() {
		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		fifoConnPool.Close()
	})

	It("does not happen on Get, Put, and Remove", func() {
		fifoConnPool = pool.NewFifoConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           10,
			PoolTimeout:        time.Minute,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond,
		})

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := fifoConnPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					fifoConnPool.Put(ctx, cn)
				}
			}
		}, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := fifoConnPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					fifoConnPool.Remove(ctx, cn, nil)
				}
			}
		})
	})
})
