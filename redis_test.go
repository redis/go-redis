package redis_test

import (
	"bytes"
	"net"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/redis.v4"
)

var _ = Describe("Client", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDb().Err()).To(BeNil())
	})

	AfterEach(func() {
		client.Close()
	})

	It("should Stringer", func() {
		Expect(client.String()).To(Equal("Redis<:6380 db:15>"))
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
			Addr: ":1234",
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
		pubsub, err := client.Subscribe()
		Expect(pubsub.Close()).NotTo(HaveOccurred())

		_, err = pubsub.Receive()
		Expect(err).To(MatchError("redis: client is closed"))
		Expect(client.Ping().Err()).NotTo(HaveOccurred())
	})

	It("should close Tx without closing the client", func() {
		err := client.Watch(func(tx *redis.Tx) error {
			_, err := tx.MultiExec(func() error {
				tx.Ping()
				return nil
			})
			return err
		})
		Expect(err).NotTo(HaveOccurred())

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
		pubsub, err := client.Subscribe()
		Expect(client.Close()).NotTo(HaveOccurred())

		_, err = pubsub.Receive()
		Expect(err).To(HaveOccurred())

		Expect(pubsub.Close()).NotTo(HaveOccurred())
	})

	It("should close pipeline when client is closed", func() {
		pipeline := client.Pipeline()
		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(pipeline.Close()).NotTo(HaveOccurred())
	})

	It("should select DB", func() {
		db2 := redis.NewClient(&redis.Options{
			Addr: redisAddr,
			DB:   2,
		})
		Expect(db2.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(db2.Get("db").Err()).To(Equal(redis.Nil))
		Expect(db2.Set("db", 2, 0).Err()).NotTo(HaveOccurred())

		n, err := db2.Get("db").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(2)))

		Expect(client.Get("db").Err()).To(Equal(redis.Nil))

		Expect(db2.FlushDb().Err()).NotTo(HaveOccurred())
		Expect(db2.Close()).NotTo(HaveOccurred())
	})

	It("processes custom commands", func() {
		cmd := redis.NewCmd("PING")
		client.Process(cmd)

		// Flush buffers.
		Expect(client.Echo("hello").Err()).NotTo(HaveOccurred())

		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Val()).To(Equal("PONG"))
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

		cn.NetConn = &badConn{}
		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())

		err = client.Ping().Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should update conn.UsedAt on read/write", func() {
		cn, _, err := client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.UsedAt).NotTo(BeZero())
		createdAt := cn.UsedAt

		err = client.Pool().Put(cn)
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.UsedAt.Equal(createdAt)).To(BeTrue())

		err = client.Ping().Err()
		Expect(err).NotTo(HaveOccurred())

		cn, _, err = client.Pool().Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(cn).NotTo(BeNil())
		Expect(cn.UsedAt.After(createdAt)).To(BeTrue())
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
		bigVal := string(bytes.Repeat([]byte{'*'}, 1<<17)) // 128kb

		err := client.Set("key", bigVal, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Reconnect to get new connection.
		Expect(client.Close()).To(BeNil())
		client = redis.NewClient(redisOptions())

		got, err := client.Get("key").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(got)).To(Equal(len(bigVal)))
		Expect(got).To(Equal(bigVal))
	})

})
