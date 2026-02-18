package redis_test

import (
	"bytes"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Cmd", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("implements Stringer", func() {
		set := client.Set(ctx, "foo", "bar", 0)
		Expect(set.String()).To(Equal("set foo bar: OK"))

		get := client.Get(ctx, "foo")
		Expect(get.String()).To(Equal("get foo: bar"))
	})

	It("has val/err", func() {
		set := client.Set(ctx, "key", "hello", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get(ctx, "key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello"))

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))
	})

	It("has helpers", func() {
		set := client.Set(ctx, "key", "10", 0)
		Expect(set.Err()).NotTo(HaveOccurred())

		n, err := client.Get(ctx, "key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(10)))

		un, err := client.Get(ctx, "key").Uint64()
		Expect(err).NotTo(HaveOccurred())
		Expect(un).To(Equal(uint64(10)))

		f, err := client.Get(ctx, "key").Float64()
		Expect(err).NotTo(HaveOccurred())
		Expect(f).To(Equal(float64(10)))
	})

	It("supports float32", func() {
		f := float32(66.97)

		err := client.Set(ctx, "float_key", f, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := client.Get(ctx, "float_key").Float32()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(f))
	})

	It("supports time.Time", func() {
		tm := time.Date(2019, 1, 1, 9, 45, 10, 222125, time.UTC)

		err := client.Set(ctx, "time_key", tm, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		s, err := client.Get(ctx, "time_key").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(s).To(Equal("2019-01-01T09:45:10.000222125Z"))

		tm2, err := client.Get(ctx, "time_key").Time()
		Expect(err).NotTo(HaveOccurred())
		Expect(tm2).To(BeTemporally("==", tm))
	})

	It("allows to set custom error", func() {
		e := errors.New("custom error")
		cmd := redis.Cmd{}
		cmd.SetErr(e)
		_, err := cmd.Result()
		Expect(err).To(Equal(e))
	})
})

var _ = Describe("RawCmd", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("returns raw RESP bytes for simple string (PING)", func() {
		cmd := client.DoRaw(ctx, "PING")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(Equal("+PONG\r\n"))
	})

	It("returns raw RESP bytes for bulk string (GET)", func() {
		err := client.Set(ctx, "rawtest", "hello", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "GET", "rawtest")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(Equal("$5\r\nhello\r\n"))
	})

	It("returns raw RESP bytes for integer (INCR)", func() {
		err := client.Set(ctx, "counter", "10", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "INCR", "counter")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(Equal(":11\r\n"))
	})

	It("returns raw RESP bytes for array (KEYS)", func() {
		err := client.Set(ctx, "key1", "val1", 0).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.Set(ctx, "key2", "val2", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "KEYS", "key*")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(HavePrefix("*2\r\n"))
	})

	It("returns raw RESP bytes for HGETALL", func() {
		err := client.HSet(ctx, "myhash", "field1", "value1", "field2", "value2").Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "HGETALL", "myhash")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		rawStr := string(rawBytes)
		Expect(rawStr).To(ContainSubstring("field1"))
		Expect(rawStr).To(ContainSubstring("value1"))
		Expect(rawStr).To(ContainSubstring("field2"))
		Expect(rawStr).To(ContainSubstring("value2"))
	})

	It("returns raw RESP bytes for nil value", func() {
		cmd := client.DoRaw(ctx, "GET", "nonexistent")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		rawStr := string(rawBytes)
		Expect(rawStr == "$-1\r\n" || rawStr == "_\r\n").To(BeTrue())
	})

	It("returns raw RESP bytes for empty array", func() {
		cmd := client.DoRaw(ctx, "KEYS", "nonexistent*")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(Equal("*0\r\n"))
	})

	It("has Val and Bytes methods", func() {
		cmd := client.DoRaw(ctx, "PING")
		Expect(cmd.Err()).NotTo(HaveOccurred())

		val := cmd.Val()
		Expect(string(val)).To(Equal("+PONG\r\n"))

		bytes, err := cmd.Bytes()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(bytes)).To(Equal("+PONG\r\n"))
	})

	It("implements Stringer", func() {
		cmd := client.DoRaw(ctx, "PING")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.String()).To(ContainSubstring("PING"))
	})

	It("allows to set custom error", func() {
		e := errors.New("custom error")
		cmd := redis.RawCmd{}
		cmd.SetErr(e)
		_, err := cmd.Result()
		Expect(err).To(Equal(e))
	})

	It("can be cloned", func() {
		cmd := client.DoRaw(ctx, "PING")
		Expect(cmd.Err()).NotTo(HaveOccurred())

		cloned := cmd.Clone().(*redis.RawCmd)
		Expect(cloned.Val()).To(Equal(cmd.Val()))
		originalVal := cmd.Val()
		clonedVal := cloned.Val()
		Expect(&originalVal[0]).NotTo(BeIdenticalTo(&clonedVal[0]))
	})

	It("returns raw RESP bytes for error response", func() {
		cmd := client.DoRaw(ctx, "INVALIDCMD")
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(HavePrefix("-"))
		Expect(string(rawBytes)).To(ContainSubstring("ERR"))
	})
})

var _ = Describe("RawWriteToCmd", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("streams raw RESP bytes for PING", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "PING")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(7)))
		Expect(buf.String()).To(Equal("+PONG\r\n"))
	})

	It("streams raw RESP bytes for bulk string (GET)", func() {
		client.Set(ctx, "rawkey", "rawvalue", 0)
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "GET", "rawkey")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(BeNumerically(">", 0))
		Expect(buf.String()).To(Equal("$8\r\nrawvalue\r\n"))
	})

	It("streams raw RESP bytes for HGETALL", func() {
		client.HSet(ctx, "rawhash", "f1", "v1", "f2", "v2")
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "HGETALL", "rawhash")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(BeNumerically(">", 0))
		raw := buf.String()
		Expect(raw).To(ContainSubstring("f1"))
		Expect(raw).To(ContainSubstring("v1"))
	})

	It("returns correct byte count", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "PING")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(buf.Len())))
	})

	It("implements Stringer", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "PING")
		Expect(cmd.String()).To(ContainSubstring("PING"))
	})

	It("can be cloned", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "PING")
		clone := cmd.Clone().(*redis.RawWriteToCmd)
		Expect(clone.Val()).To(Equal(cmd.Val()))
	})
})
