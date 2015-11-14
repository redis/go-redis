package redis_test

import (
	"bytes"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v3"
)

var _ = Describe("Client", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		client.Close()
	})

	It("should Stringer", func() {
		Expect(client.String()).To(Equal("Redis<:6380 db:0>"))
	})

	It("should ping", func() {
		val, err := client.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))
	})

	It("should support custom dialers", func() {
		custom := redis.NewClient(&redis.Options{
			Dialer: func() (net.Conn, error) {
				return net.Dial("tcp", redisAddr)
			},
		})

		val, err := custom.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))
		Expect(custom.Close()).NotTo(HaveOccurred())
	})

	It("should close", func() {
		Expect(client.Close()).NotTo(HaveOccurred())
		err := client.Ping().Err()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
	})

	It("should close pubsub without closing the connection", func() {
		pubsub := client.PubSub()
		Expect(pubsub.Close()).NotTo(HaveOccurred())

		_, err := pubsub.Receive()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close multi without closing the connection", func() {
		multi := client.Multi()
		Expect(multi.Close()).NotTo(HaveOccurred())

		_, err := multi.Exec(func() error {
			multi.Ping()
			return nil
		})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close pipeline without closing the connection", func() {
		pipeline := client.Pipeline()
		Expect(pipeline.Close()).NotTo(HaveOccurred())

		pipeline.Ping()
		_, err := pipeline.Exec()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close pubsub when client is closed", func() {
		pubsub := client.PubSub()
		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(pubsub.Close()).NotTo(HaveOccurred())
	})

	It("should close multi when client is closed", func() {
		multi := client.Multi()
		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(multi.Close()).NotTo(HaveOccurred())
	})

	It("should close pipeline when client is closed", func() {
		pipeline := client.Pipeline()
		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(pipeline.Close()).NotTo(HaveOccurred())
	})

	It("should support idle-timeouts", func() {
		idle := redis.NewClient(&redis.Options{
			Addr:        redisAddr,
			IdleTimeout: 100 * time.Microsecond,
		})
		defer idle.Close()

		Expect(idle.Ping().Err()).NotTo(HaveOccurred())
		time.Sleep(time.Millisecond)
		Expect(idle.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should support DB selection", func() {
		db1 := redis.NewClient(&redis.Options{
			Addr: redisAddr,
			DB:   1,
		})
		defer db1.Close()

		Expect(db1.Get("key").Err()).To(Equal(redis.Nil))
		Expect(db1.Set("key", "value", 0).Err()).NotTo(HaveOccurred())

		Expect(client.Get("key").Err()).To(Equal(redis.Nil))
		Expect(db1.Get("key").Val()).To(Equal("value"))
		Expect(db1.FlushDb().Err()).NotTo(HaveOccurred())
	})

	It("should support DB selection with read timeout (issue #135)", func() {
		for i := 0; i < 100; i++ {
			db1 := redis.NewClient(&redis.Options{
				Addr:        redisAddr,
				DB:          1,
				ReadTimeout: time.Nanosecond,
			})

			err := db1.Ping().Err()
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		}
	})

	It("should retry command on network error", func() {
		Expect(client.Close()).NotTo(HaveOccurred())

		client = redis.NewClient(&redis.Options{
			Addr:       redisAddr,
			MaxRetries: 1,
		})

		// Put bad connection in the pool.
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.SetNetConn(&badConn{})
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		err = client.Ping().Err()
		Expect(err).NotTo(HaveOccurred())
	})
})

//------------------------------------------------------------------------------

func benchRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	if err := client.FlushDb().Err(); err != nil {
		panic(err)
	}
	return client
}

func BenchmarkRedisPing(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Ping().Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisSet(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	value := string(bytes.Repeat([]byte{'1'}, 10000))

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisGetNil(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Get("key").Err(); err != redis.Nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkRedisSetGet(b *testing.B, size int) {
	client := benchRedisClient()
	defer client.Close()

	value := string(bytes.Repeat([]byte{'1'}, size))

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
			}

			got, err := client.Get("key").Result()
			if err != nil {
				b.Fatal(err)
			}
			if got != value {
				b.Fatalf("got != value")
			}
		}
	})
}

func BenchmarkRedisSetGet64Bytes(b *testing.B) {
	benchmarkRedisSetGet(b, 64)
}

func BenchmarkRedisSetGet1KB(b *testing.B) {
	benchmarkRedisSetGet(b, 1024)
}

func BenchmarkRedisSetGet10KB(b *testing.B) {
	benchmarkRedisSetGet(b, 10*1024)
}

func BenchmarkRedisSetGet1MB(b *testing.B) {
	benchmarkRedisSetGet(b, 1024*1024)
}

func BenchmarkRedisSetGetBytes(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	value := bytes.Repeat([]byte{'1'}, 10000)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
			}

			got, err := client.Get("key").Bytes()
			if err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(got, value) {
				b.Fatalf("got != value")
			}
		}
	})
}

func BenchmarkRedisMGet(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	if err := client.MSet("key1", "hello1", "key2", "hello2").Err(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.MGet("key1", "key2").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSetExpire(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Set("key", "hello", 0).Err(); err != nil {
				b.Fatal(err)
			}
			if err := client.Expire("key", time.Second).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPipeline(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
				pipe.Set("key", "hello", 0)
				pipe.Expire("key", time.Second)
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkZAdd(b *testing.B) {
	client := benchRedisClient()
	defer client.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.ZAdd("key", redis.Z{float64(1), "hello"}).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
