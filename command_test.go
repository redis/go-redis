package redis_test

import (
	"bytes"
	"strconv"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Command", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		Expect(client.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should have a plain string result", func() {
		set := client.Set("foo", "bar")
		Expect(set.String()).To(Equal("SET foo bar: OK"))

		get := client.Get("foo")
		Expect(get.String()).To(Equal("GET foo: bar"))
	})

	It("should have correct val/err states", func() {
		set := client.Set("key", "hello")
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello"))

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))
	})

	It("should escape special chars", func() {
		set := client.Set("key", "hello1\r\nhello2\r\n")
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello1\r\nhello2\r\n"))
	})

	It("should handle big vals", func() {
		val := string(bytes.Repeat([]byte{'*'}, 1<<16))
		set := client.Set("key", val)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal(val))
	})

	It("should handle many keys #1", func() {
		for i := 0; i < 100000; i++ {
			client.Set("keys.key"+strconv.Itoa(i), "hello"+strconv.Itoa(i))
		}
		keys := client.Keys("keys.*")
		Expect(keys.Err()).NotTo(HaveOccurred())
		Expect(len(keys.Val())).To(Equal(100000))
	})

	It("should handle many keys #2", func() {
		keys := []string{"non-existent-key"}
		for i := 0; i < 100000; i++ {
			key := "keys.key" + strconv.Itoa(i)
			client.Set(key, "hello"+strconv.Itoa(i))
			keys = append(keys, key)
		}
		keys = append(keys, "non-existent-key")

		mget := client.MGet(keys...)
		Expect(mget.Err()).NotTo(HaveOccurred())
		Expect(len(mget.Val())).To(Equal(100002))
		vals := mget.Val()
		for i := 0; i < 100000; i++ {
			Expect(vals[i+1]).To(Equal("hello" + strconv.Itoa(i)))
		}
		Expect(vals[0]).To(BeNil())
		Expect(vals[100001]).To(BeNil())
	})

	It("should convert strings via helpers", func() {
		set := client.Set("key", "10")
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

	Describe("races", func() {

		It("should echo", func() {
			const N = 10000

			wg := &sync.WaitGroup{}
			wg.Add(N)
			for i := 0; i < N; i++ {
				go func(i int) {
					defer wg.Done()

					msg := "echo" + strconv.Itoa(i)
					echo := client.Echo(msg)
					Expect(echo.Err()).NotTo(HaveOccurred())
					Expect(echo.Val()).To(Equal(msg))
				}(i)
			}
			wg.Wait()
		})

		It("should incr", func() {
			const N = 10000
			key := "TestIncrFromGoroutines"

			wg := &sync.WaitGroup{}
			wg.Add(N)
			for i := int64(0); i < N; i++ {
				go func() {
					defer wg.Done()
					err := client.Incr(key).Err()
					Expect(err).NotTo(HaveOccurred())
				}()
			}
			wg.Wait()

			val, err := client.Get(key).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(strconv.Itoa(N)))
		})

	})

})
