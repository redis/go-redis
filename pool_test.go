package redis_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("pool", func() {
	var client *redis.Client

	var perform = func(n int, cb func()) {
		wg := &sync.WaitGroup{}
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				cb()
			}()
		}
		wg.Wait()
	}

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			PoolSize: 10,
		})
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should respect max size", func() {
		perform(1000, func() {
			val, err := client.Ping().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		pool := client.Pool()
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.FreeLen()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(Equal(pool.FreeLen()))
	})

	It("should respect max on multi", func() {
		perform(1000, func() {
			var ping *redis.StatusCmd

			multi := client.Multi()
			cmds, err := multi.Exec(func() error {
				ping = multi.Ping()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
			Expect(multi.Close()).NotTo(HaveOccurred())
		})

		pool := client.Pool()
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.FreeLen()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(Equal(pool.FreeLen()))
	})

	It("should respect max on pipelines", func() {
		perform(1000, func() {
			pipe := client.Pipeline()
			ping := pipe.Ping()
			cmds, err := pipe.Exec()
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
			Expect(pipe.Close()).NotTo(HaveOccurred())
		})

		pool := client.Pool()
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.FreeLen()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(Equal(pool.FreeLen()))
	})

	It("should respect max on pubsub", func() {
		perform(10, func() {
			pubsub := client.PubSub()
			Expect(pubsub.Subscribe()).NotTo(HaveOccurred())
			Expect(pubsub.Close()).NotTo(HaveOccurred())
		})

		pool := client.Pool()
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.FreeLen()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(Equal(pool.FreeLen()))
	})

	It("should remove broken connections", func() {
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.Close()).NotTo(HaveOccurred())
		Expect(client.Pool().Put(cn)).NotTo(HaveOccurred())

		err = client.Ping().Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("use of closed network connection"))

		val, err := client.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))

		pool := client.Pool()
		Expect(pool.Len()).To(Equal(1))
		Expect(pool.FreeLen()).To(Equal(1))
	})

	It("should reuse connections", func() {
		for i := 0; i < 100; i++ {
			val, err := client.Ping().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		}

		pool := client.Pool()
		Expect(pool.Len()).To(Equal(1))
		Expect(pool.FreeLen()).To(Equal(1))
	})

	It("should unblock client when connection is removed", func() {
		pool := client.Pool()

		// Reserve one connection.
		cn, _, err := pool.Get()
		Expect(err).NotTo(HaveOccurred())

		// Reserve the rest of connections.
		for i := 0; i < 9; i++ {
			_, _, err := pool.Get()
			Expect(err).NotTo(HaveOccurred())
		}

		var ping *redis.StatusCmd
		started := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			started <- true
			ping = client.Ping()
			done <- true
		}()
		<-started

		// Check that Ping is blocked.
		select {
		case <-done:
			panic("Ping is not blocked")
		default:
			// ok
		}

		err = pool.Remove(cn, errors.New("test"))
		Expect(err).NotTo(HaveOccurred())

		// Check that Ping is unblocked.
		select {
		case <-done:
			// ok
		case <-time.After(time.Second):
			panic("Ping is not unblocked")
		}
		Expect(ping.Err()).NotTo(HaveOccurred())
	})

	It("should rate limit dial", func() {
		pool := client.Pool()

		var rateErr error
		for i := 0; i < 1000; i++ {
			cn, _, err := pool.Get()
			if err != nil {
				rateErr = err
				break
			}

			_ = pool.Remove(cn, errors.New("test"))
		}

		Expect(rateErr).To(MatchError(`redis: you open connections too fast (last_error="test")`))
	})
})

func BenchmarkPool(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	pool := client.Pool()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, _, err := pool.Get()
			if err != nil {
				b.Fatalf("no error expected on pool.Get but received: %s", err.Error())
			}
			if err = pool.Put(conn); err != nil {
				b.Fatalf("no error expected on pool.Put but received: %s", err.Error())
			}
		}
	})
}
