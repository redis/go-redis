package redis_test

import (
	"sort"
	"testing"
	"time"

	"gopkg.in/redis.v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const redisAddr = ":6379"

var _ = Describe("Client", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
		})
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should ping", func() {
		val, err := client.Ping().Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("PONG"))
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

	It("should support idle-timeouts", func() {
		idle := redis.NewTCPClient(&redis.Options{
			Addr:        redisAddr,
			IdleTimeout: time.Nanosecond,
		})
		defer idle.Close()

		Expect(idle.Ping().Err()).NotTo(HaveOccurred())
		time.Sleep(time.Millisecond)
		Expect(idle.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should support DB selection", func() {
		db1 := redis.NewTCPClient(&redis.Options{
			Addr: redisAddr,
			DB:   1,
		})
		defer db1.Close()

		Expect(db1.Get("key").Err()).To(Equal(redis.Nil))
		Expect(db1.Set("key", "value").Err()).NotTo(HaveOccurred())

		Expect(client.Get("key").Err()).To(Equal(redis.Nil))
		Expect(db1.Get("key").Val()).To(Equal("value"))
		Expect(db1.FlushDb().Err()).NotTo(HaveOccurred())
	})

})

//------------------------------------------------------------------------------

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gopkg.in/redis.v2")
}

func sortStrings(slice []string) []string {
	sort.Strings(slice)
	return slice
}

//------------------------------------------------------------------------------

func BenchmarkRedisPing(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Ping().Err(); err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisSet(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Set("key", "hello").Err(); err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisGetNil(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	if err := client.FlushDb().Err(); err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Get("key").Err(); err != redis.Nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisGet(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	if err := client.Set("key", "hello").Err(); err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Get("key").Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisMGet(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	if err := client.MSet("key1", "hello1", "key2", "hello2").Err(); err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := client.MGet("key1", "key2").Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetExpire(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Set("key", "hello").Err(); err != nil {
			b.Fatal(err)
		}
		if err := client.Expire("key", time.Second).Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	b.StopTimer()
	client := redis.NewTCPClient(&redis.Options{
		Addr: redisAddr,
	})
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.Pipelined(func(pipe *redis.Pipeline) error {
			pipe.Set("key", "hello")
			pipe.Expire("key", time.Second)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
