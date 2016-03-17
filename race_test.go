package redis_test

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
	"gopkg.in/redis.v3/internal/pool"
)

var _ = Describe("races", func() {
	var client *redis.Client
	var C, N int

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDb().Err()).To(BeNil())

		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		err := client.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should echo", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				msg := fmt.Sprintf("echo %d %d", id, i)
				echo, err := client.Echo(msg).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(echo).To(Equal(msg))
			}
		})
	})

	It("should incr", func() {
		key := "TestIncrFromGoroutines"

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Incr(key).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		val, err := client.Get(key).Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(int64(C * N)))
	})

	It("should handle many keys", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Set(
					fmt.Sprintf("keys.key-%d-%d", id, i),
					fmt.Sprintf("hello-%d-%d", id, i),
					0,
				).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		keys := client.Keys("keys.*")
		Expect(keys.Err()).NotTo(HaveOccurred())
		Expect(len(keys.Val())).To(Equal(C * N))
	})

	It("should handle many keys 2", func() {
		perform(C, func(id int) {
			keys := []string{"non-existent-key"}
			for i := 0; i < N; i++ {
				key := fmt.Sprintf("keys.key-%d", i)
				keys = append(keys, key)

				err := client.Set(key, fmt.Sprintf("hello-%d", i), 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}
			keys = append(keys, "non-existent-key")

			vals, err := client.MGet(keys...).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vals)).To(Equal(N + 2))

			for i := 0; i < N; i++ {
				Expect(vals[i+1]).To(Equal(fmt.Sprintf("hello-%d", i)))
			}

			Expect(vals[0]).To(BeNil())
			Expect(vals[N+1]).To(BeNil())
		})
	})

	It("should handle big vals in Get", func() {
		bigVal := string(bytes.Repeat([]byte{'*'}, 1<<17)) // 128kb

		err := client.Set("key", bigVal, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Reconnect to get new connection.
		Expect(client.Close()).To(BeNil())
		client = redis.NewClient(redisOptions())

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				got, err := client.Get("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(bigVal))
			}
		})

	})

	It("should handle big vals in Set", func() {
		C, N = 4, 100
		bigVal := string(bytes.Repeat([]byte{'*'}, 1<<17)) // 128kb

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Set("key", bigVal, 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	It("should PubSub", func() {
		connPool := client.Pool()
		connPool.(*pool.ConnPool).DialLimiter = nil

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				pubsub, err := client.Subscribe(fmt.Sprintf("mychannel%d", id))
				Expect(err).NotTo(HaveOccurred())

				go func() {
					defer GinkgoRecover()

					time.Sleep(time.Millisecond)
					err := pubsub.Close()
					Expect(err).NotTo(HaveOccurred())
				}()

				_, err = pubsub.ReceiveMessage()
				Expect(err.Error()).To(ContainSubstring("closed"))

				val := "echo" + strconv.Itoa(i)
				echo, err := client.Echo(val).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(echo).To(Equal(val))
			}
		})

		Expect(connPool.Len()).To(Equal(connPool.FreeLen()))
		Expect(connPool.Len()).To(BeNumerically("<=", 10))
	})

	It("should select db", func() {
		err := client.Set("db", 1, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		perform(C, func(id int) {
			opt := redisOptions()
			opt.DB = int64(id)
			client := redis.NewClient(opt)
			for i := 0; i < N; i++ {
				err := client.Set("db", id, 0).Err()
				Expect(err).NotTo(HaveOccurred())

				n, err := client.Get("db").Int64()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(id)))
			}
			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		n, err := client.Get("db").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))
	})

	It("should select DB with read timeout", func() {
		perform(C, func(id int) {
			opt := redisOptions()
			opt.DB = int64(id)
			opt.ReadTimeout = time.Nanosecond
			client := redis.NewClient(opt)

			perform(C, func(id int) {
				err := client.Ping().Err()
				Expect(err).To(HaveOccurred())
				Expect(err.(net.Error).Timeout()).To(BeTrue())
			})

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
