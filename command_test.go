package redis_test

import (
	"bytes"
	"strconv"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("Command", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should implement Stringer", func() {
		set := client.Set("foo", "bar", 0)
		Expect(set.String()).To(Equal("SET foo bar: OK"))

		get := client.Get("foo")
		Expect(get.String()).To(Equal("GET foo: bar"))
	})

	It("should have correct val/err states", func() {
		set := client.Set("key", "hello", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello"))

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))
	})

	It("should escape special chars", func() {
		set := client.Set("key", "hello1\r\nhello2\r\n", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello1\r\nhello2\r\n"))
	})

	It("should handle big vals", func() {
		val := string(bytes.Repeat([]byte{'*'}, 1<<16))
		set := client.Set("key", val, 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal(val))
	})

	It("should handle many keys #1", func() {
		const n = 100000
		for i := 0; i < n; i++ {
			client.Set("keys.key"+strconv.Itoa(i), "hello"+strconv.Itoa(i), 0)
		}
		keys := client.Keys("keys.*")
		Expect(keys.Err()).NotTo(HaveOccurred())
		Expect(len(keys.Val())).To(Equal(n))
	})

	It("should handle many keys #2", func() {
		const n = 100000

		keys := []string{"non-existent-key"}
		for i := 0; i < n; i++ {
			key := "keys.key" + strconv.Itoa(i)
			client.Set(key, "hello"+strconv.Itoa(i), 0)
			keys = append(keys, key)
		}
		keys = append(keys, "non-existent-key")

		mget := client.MGet(keys...)
		Expect(mget.Err()).NotTo(HaveOccurred())
		Expect(len(mget.Val())).To(Equal(n + 2))
		vals := mget.Val()
		for i := 0; i < n; i++ {
			Expect(vals[i+1]).To(Equal("hello" + strconv.Itoa(i)))
		}
		Expect(vals[0]).To(BeNil())
		Expect(vals[n+1]).To(BeNil())
	})

	It("should convert strings via helpers", func() {
		set := client.Set("key", "10", 0)
		Expect(set.Err()).NotTo(HaveOccurred())

		n, err := client.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(10)))

		un, err := client.Get("key").Uint64()
		Expect(err).NotTo(HaveOccurred())
		Expect(un).To(Equal(uint64(10)))

		f, err := client.Get("key").Float64()
		Expect(err).NotTo(HaveOccurred())
		Expect(f).To(Equal(float64(10)))
	})

	It("Cmd should return string", func() {
		cmd := redis.NewCmd("PING")
		client.Process(cmd)
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Val()).To(Equal("PONG"))
	})

	Describe("races", func() {
		var C, N = 10, 1000
		if testing.Short() {
			N = 100
		}

		It("should echo", func() {
			wg := &sync.WaitGroup{}
			for i := 0; i < C; i++ {
				wg.Add(1)

				go func(i int) {
					defer GinkgoRecover()
					defer wg.Done()

					for j := 0; j < N; j++ {
						msg := "echo" + strconv.Itoa(i)
						echo := client.Echo(msg)
						Expect(echo.Err()).NotTo(HaveOccurred())
						Expect(echo.Val()).To(Equal(msg))
					}
				}(i)
			}
			wg.Wait()
		})

		It("should incr", func() {
			key := "TestIncrFromGoroutines"
			wg := &sync.WaitGroup{}
			for i := 0; i < C; i++ {
				wg.Add(1)

				go func() {
					defer GinkgoRecover()
					defer wg.Done()

					for j := 0; j < N; j++ {
						err := client.Incr(key).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				}()
			}
			wg.Wait()

			val, err := client.Get(key).Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(C * N)))
		})

	})

})
