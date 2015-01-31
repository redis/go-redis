package redis_test

import (
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Pool", func() {
	var client *redis.Client
	var perform = func(n int, cb func()) {
		wg := &sync.WaitGroup{}
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cb()
			}()
		}
		wg.Wait()
	}

	BeforeEach(func() {
		client = redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		client.FlushDb()
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should respect max size", func() {
		perform(1000, func() {
			val, err := client.Ping().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		})

		pool := client.Pool()
		Expect(pool.Size()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.Size()).To(Equal(pool.Len()))
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
		Expect(pool.Size()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.Size()).To(Equal(pool.Len()))
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
		Expect(pool.Size()).To(BeNumerically("<=", 10))
		Expect(pool.Len()).To(BeNumerically("<=", 10))
		Expect(pool.Size()).To(Equal(pool.Len()))
	})

	It("should respect max on pubsub", func() {
		perform(10, func() {
			pubsub := client.PubSub()
			Expect(pubsub.Subscribe()).NotTo(HaveOccurred())
			Expect(pubsub.Close()).NotTo(HaveOccurred())
		})

		pool := client.Pool()
		Expect(pool.Size()).To(Equal(0))
		Expect(pool.Len()).To(Equal(0))
	})

	It("should remove broken connections", func() {
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.Close()).NotTo(HaveOccurred())
		Expect(client.Pool().Put(cn)).NotTo(HaveOccurred())

		err = client.Ping().Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal("use of closed network connection"))

		val, err := client.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))

		pool := client.Pool()
		Expect(pool.Size()).To(Equal(1))
		Expect(pool.Len()).To(Equal(1))
	})

	It("should reuse connections", func() {
		for i := 0; i < 100; i++ {
			val, err := client.Ping().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("PONG"))
		}

		pool := client.Pool()
		Expect(pool.Size()).To(Equal(1))
		Expect(pool.Len()).To(Equal(1))
	})

})

func BenchmarkPool(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr:        redisAddr,
		IdleTimeout: 100 * time.Millisecond,
	})
	defer client.Close()

	pool := client.Pool()
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
