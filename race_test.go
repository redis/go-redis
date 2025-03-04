package redis_test

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("races", func() {
	var client *redis.Client
	var C, N int

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).To(BeNil())

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
				echo, err := client.Echo(ctx, msg).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(echo).To(Equal(msg))
			}
		})
	})

	It("should incr", func() {
		key := "TestIncrFromGoroutines"

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Incr(ctx, key).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		val, err := client.Get(ctx, key).Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(int64(C * N)))
	})

	It("should handle many keys", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Set(
					ctx,
					fmt.Sprintf("keys.key-%d-%d", id, i),
					fmt.Sprintf("hello-%d-%d", id, i),
					0,
				).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		keys := client.Keys(ctx, "keys.*")
		Expect(keys.Err()).NotTo(HaveOccurred())
		Expect(len(keys.Val())).To(Equal(C * N))
	})

	It("should handle many keys 2", func() {
		perform(C, func(id int) {
			keys := []string{"non-existent-key"}
			for i := 0; i < N; i++ {
				key := fmt.Sprintf("keys.key-%d", i)
				keys = append(keys, key)

				err := client.Set(ctx, key, fmt.Sprintf("hello-%d", i), 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}
			keys = append(keys, "non-existent-key")

			vals, err := client.MGet(ctx, keys...).Result()
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
		C, N := 4, 100

		bigVal := bigVal()

		err := client.Set(ctx, "key", bigVal, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Reconnect to get new connection.
		Expect(client.Close()).To(BeNil())
		client = redis.NewClient(redisOptions())

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				got, err := client.Get(ctx, "key").Bytes()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(bigVal))
			}
		})
	})

	It("should handle big vals in Set", func() {
		C, N := 4, 100

		bigVal := bigVal()
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Set(ctx, "key", bigVal, 0).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	It("should select db", Label("NonRedisEnterprise"), func() {
		err := client.Set(ctx, "db", 0, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		perform(C, func(id int) {
			opt := redisOptions()
			opt.DB = id
			client := redis.NewClient(opt)
			for i := 0; i < N; i++ {
				err := client.Set(ctx, "db", id, 0).Err()
				Expect(err).NotTo(HaveOccurred())

				n, err := client.Get(ctx, "db").Int64()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(id)))
			}
			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		n, err := client.Get(ctx, "db").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(0)))
	})

	It("should select DB with read timeout", func() {
		perform(C, func(id int) {
			opt := redisOptions()
			opt.DB = id
			opt.ReadTimeout = time.Nanosecond
			client := redis.NewClient(opt)

			perform(C, func(id int) {
				err := client.Ping(ctx).Err()
				Expect(err).To(HaveOccurred())
				Expect(err.(net.Error).Timeout()).To(BeTrue())
			})

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("should Watch/Unwatch", func() {
		err := client.Set(ctx, "key", "0", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Watch(ctx, func(tx *redis.Tx) error {
					val, err := tx.Get(ctx, "key").Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(val).NotTo(Equal(redis.Nil))

					num, err := strconv.ParseInt(val, 10, 64)
					Expect(err).NotTo(HaveOccurred())

					cmds, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Set(ctx, "key", strconv.FormatInt(num+1, 10), 0)
						return nil
					})
					Expect(cmds).To(HaveLen(1))
					return err
				}, "key")
				if err == redis.TxFailedErr {
					i--
					continue
				}
				Expect(err).NotTo(HaveOccurred())
			}
		})

		val, err := client.Get(ctx, "key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(int64(C * N)))
	})

	It("should BLPop", func() {
		C := 5
		N := 5
		var received uint32

		wg := performAsync(C, func(id int) {
			for {
				v, err := client.BLPop(ctx, time.Second, "list").Result()
				if err != nil {
					if err == redis.Nil {
						break
					}
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(v).To(Equal([]string{"list", "hello"}))
				atomic.AddUint32(&received, 1)
			}
		})

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.LPush(ctx, "list", "hello").Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		wg.Wait()
		Expect(atomic.LoadUint32(&received)).To(Equal(uint32(C * N)))
	})
})

var _ = Describe("cluster races", Label("NonRedisEnterprise"), func() {
	var client *redis.ClusterClient
	var C, N int

	BeforeEach(func() {
		opt := redisClusterOptions()
		client = cluster.newClusterClient(ctx, opt)

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
				echo, err := client.Echo(ctx, msg).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(echo).To(Equal(msg))
			}
		})
	})

	It("should get", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				key := fmt.Sprintf("key_%d_%d", id, i)
				_, err := client.Get(ctx, key).Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})
	})

	It("should incr", func() {
		key := "TestIncrFromGoroutines"

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				err := client.Incr(ctx, key).Err()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		val, err := client.Get(ctx, key).Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(int64(C * N)))
	})

	It("write cmd data-race", func() {
		pubsub := client.Subscribe(ctx)
		defer pubsub.Close()

		pubsub.Channel(redis.WithChannelHealthCheckInterval(time.Millisecond))
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("channel_%d", i)
			pubsub.Subscribe(ctx, key)
			pubsub.Unsubscribe(ctx, key)
		}
	})
})

func bigVal() []byte {
	return bytes.Repeat([]byte{'*'}, 1<<17) // 128kb
}
