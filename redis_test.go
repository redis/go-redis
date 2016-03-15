package redis_test

import (
	"net"
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

	It("should return pool stats", func() {
		Expect(client.PoolStats()).To(BeAssignableToTypeOf(&redis.PoolStats{}))
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
		Expect(err).To(MatchError("redis: client is closed"))
	})

	It("should close pubsub without closing the client", func() {
		pubsub := client.PubSub()
		Expect(pubsub.Close()).NotTo(HaveOccurred())

		_, err := pubsub.Receive()
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close multi without closing the client", func() {
		multi := client.Multi()
		Expect(multi.Close()).NotTo(HaveOccurred())

		_, err := multi.Exec(func() error {
			multi.Ping()
			return nil
		})
		Expect(err).To(MatchError("redis: client is closed"))

		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close pipeline without closing the client", func() {
		pipeline := client.Pipeline()
		Expect(pipeline.Close()).NotTo(HaveOccurred())

		pipeline.Ping()
		_, err := pipeline.Exec()
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
		cn, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())

		cn.NetConn = &badConn{}
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		err = client.Ping().Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should maintain conn.UsedAt", func() {
		cn, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.UsedAt).NotTo(BeZero())
		createdAt := cn.UsedAt

		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.UsedAt.Equal(createdAt)).To(BeTrue())

		err = client.Ping().Err()
		Expect(err).NotTo(HaveOccurred())

		cn = client.Pool().First()
		Expect(cn).NotTo(BeNil())
		Expect(cn.UsedAt.After(createdAt)).To(BeTrue())
	})
})
