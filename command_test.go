package redis_test

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
	"gopkg.in/redis.v3/internal/pool"
)

var _ = Describe("Command", func() {
	var client *redis.Client

	connect := func() *redis.Client {
		return redis.NewClient(&redis.Options{
			Addr:        redisAddr,
			PoolTimeout: time.Minute,
		})
	}

	BeforeEach(func() {
		client = connect()
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
		bigVal := string(bytes.Repeat([]byte{'*'}, 1<<16))

		err := client.Set("key", bigVal, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Reconnect to get new connection.
		Expect(client.Close()).To(BeNil())
		client = connect()

		got, err := client.Get("key").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(got)).To(Equal(len(bigVal)))
		Expect(got).To(Equal(bigVal))
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
			C = 3
			N = 100
		}

		It("should echo", func() {
			perform(C, func() {
				for i := 0; i < N; i++ {
					msg := "echo" + strconv.Itoa(i)
					echo, err := client.Echo(msg).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(echo).To(Equal(msg))
				}
			})
		})

		It("should incr", func() {
			key := "TestIncrFromGoroutines"

			perform(C, func() {
				for i := 0; i < N; i++ {
					err := client.Incr(key).Err()
					Expect(err).NotTo(HaveOccurred())
				}
			})

			val, err := client.Get(key).Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(C * N)))
		})

		It("should handle big vals", func() {
			client2 := connect()
			defer client2.Close()

			bigVal := string(bytes.Repeat([]byte{'*'}, 1<<16))

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				perform(C, func() {
					for i := 0; i < N; i++ {
						got, err := client.Get("key").Result()
						if err == redis.Nil {
							continue
						}
						Expect(got).To(Equal(bigVal))
					}
				})
			}()

			go func() {
				defer wg.Done()
				perform(C, func() {
					for i := 0; i < N; i++ {
						err := client2.Set("key", bigVal, 0).Err()
						Expect(err).NotTo(HaveOccurred())
					}
				})
			}()

			wg.Wait()
		})

		It("should PubSub", func() {
			connPool := client.Pool()
			connPool.(*pool.ConnPool).DialLimiter = nil

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				perform(C, func() {
					for i := 0; i < N; i++ {
						pubsub, err := client.Subscribe("mychannel")
						Expect(err).NotTo(HaveOccurred())

						go func() {
							defer GinkgoRecover()

							time.Sleep(time.Millisecond)
							err := pubsub.Close()
							Expect(err).NotTo(HaveOccurred())
						}()

						_, err = pubsub.ReceiveMessage()
						Expect(err.Error()).To(ContainSubstring("closed"))
					}
				})
			}()

			go func() {
				defer wg.Done()
				perform(C, func() {
					for i := 0; i < N; i++ {
						val := "echo" + strconv.Itoa(i)
						echo, err := client.Echo(val).Result()
						Expect(err).NotTo(HaveOccurred())
						Expect(echo).To(Equal(val))
					}
				})
			}()

			wg.Wait()

			Expect(connPool.Len()).To(Equal(connPool.FreeLen()))
			Expect(connPool.Len()).To(BeNumerically("<=", 10))
		})
	})

})
