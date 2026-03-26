package redis_test

import (
	"bytes"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

// parseRESPType returns the RESP type character and validates basic structure
func parseRESPType(data []byte) (byte, bool) {
	if len(data) < 3 {
		return 0, false
	}
	// Check for CRLF at the end
	if !bytes.HasSuffix(data, []byte("\r\n")) {
		return 0, false
	}
	return data[0], true
}

// countRESPElements counts the number of elements in a RESP array/map response
func countRESPElements(data []byte) (int, bool) {
	if len(data) < 4 {
		return 0, false
	}
	// Find first CRLF
	idx := bytes.Index(data, []byte("\r\n"))
	if idx < 2 {
		return 0, false
	}
	count, err := strconv.Atoi(string(data[1:idx]))
	if err != nil {
		return 0, false
	}
	return count, true
}

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

	It("returns raw RESP bytes for array (KEYS) with validation", func() {
		err := client.Set(ctx, "key1", "val1", 0).Err()
		Expect(err).NotTo(HaveOccurred())
		err = client.Set(ctx, "key2", "val2", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "KEYS", "key*")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		// Validate RESP structure
		respType, valid := parseRESPType(rawBytes)
		Expect(valid).To(BeTrue())
		Expect(respType).To(Equal(byte('*')), "Should be array type")

		// Verify element count
		count, ok := countRESPElements(rawBytes)
		Expect(ok).To(BeTrue())
		Expect(count).To(Equal(2))

		// Verify keys are present in the raw bytes
		rawStr := string(rawBytes)
		Expect(rawStr).To(ContainSubstring("key1"))
		Expect(rawStr).To(ContainSubstring("key2"))
	})

	It("returns raw RESP bytes for HGETALL with complete validation", func() {
		err := client.HSet(ctx, "myhash", "field1", "value1", "field2", "value2").Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "HGETALL", "myhash")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		// Validate RESP structure
		respType, valid := parseRESPType(rawBytes)
		Expect(valid).To(BeTrue(), "Response should be valid RESP")

		// HGETALL returns array (RESP2) or map (RESP3)
		Expect(respType).To(BeElementOf(byte('*'), byte('%')), "Should be array or map type")

		// Count elements: array has 4 elements (2 key-value pairs), map has 2 pairs
		count, ok := countRESPElements(rawBytes)
		Expect(ok).To(BeTrue())
		if respType == '*' {
			Expect(count).To(Equal(4), "Array should have 4 elements (2 key-value pairs)")
		} else {
			Expect(count).To(Equal(2), "Map should have 2 pairs")
		}

		rawStr := string(rawBytes)
		Expect(rawStr).To(ContainSubstring("field1"))
		Expect(rawStr).To(ContainSubstring("value1"))
		Expect(rawStr).To(ContainSubstring("field2"))
		Expect(rawStr).To(ContainSubstring("value2"))

		// Verify response ends with CRLF
		Expect(rawBytes[len(rawBytes)-2:]).To(Equal([]byte("\r\n")))
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

	It("returns raw RESP bytes for empty hash (nil map)", func() {
		// HGETALL on non-existent hash returns empty array/map
		cmd := client.DoRaw(ctx, "HGETALL", "nonexistent_hash")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		respType, valid := parseRESPType(rawBytes)
		Expect(valid).To(BeTrue())
		// Should be empty array or empty map
		Expect(respType).To(BeElementOf(byte('*'), byte('%')))

		count, ok := countRESPElements(rawBytes)
		Expect(ok).To(BeTrue())
		Expect(count).To(Equal(0), "Empty hash should return 0 elements")
	})

	It("returns raw RESP bytes for nested array structures (SMEMBERS)", func() {
		// Create a set with multiple members
		err := client.SAdd(ctx, "myset", "member1", "member2", "member3").Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "SMEMBERS", "myset")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		// Validate basic RESP structure
		respType, valid := parseRESPType(rawBytes)
		Expect(valid).To(BeTrue())
		// SMEMBERS returns array (RESP2) or set (RESP3)
		Expect(respType).To(BeElementOf(byte('*'), byte('~')))

		// Verify count
		count, ok := countRESPElements(rawBytes)
		Expect(ok).To(BeTrue())
		Expect(count).To(Equal(3))

		// Verify members are present
		rawStr := string(rawBytes)
		Expect(rawStr).To(ContainSubstring("member1"))
		Expect(rawStr).To(ContainSubstring("member2"))
		Expect(rawStr).To(ContainSubstring("member3"))
	})

	It("returns raw RESP bytes for LRANGE with exact byte validation", func() {
		// Create a list with multiple elements
		err := client.RPush(ctx, "mylist", "elem1", "elem2", "elem3").Err()
		Expect(err).NotTo(HaveOccurred())

		cmd := client.DoRaw(ctx, "LRANGE", "mylist", "0", "-1")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		// LRANGE returns elements in order, so we can validate exact bytes
		expectedResp := "*3\r\n$5\r\nelem1\r\n$5\r\nelem2\r\n$5\r\nelem3\r\n"
		Expect(string(rawBytes)).To(Equal(expectedResp))
	})

	It("returns raw RESP bytes for LPUSH with exact integer response", func() {
		// LPUSH returns the length of the list after push
		cmd := client.DoRaw(ctx, "LPUSH", "newlist", "item")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(Equal(":1\r\n"))
	})

	It("returns raw RESP bytes for ECHO with exact bulk string", func() {
		cmd := client.DoRaw(ctx, "ECHO", "testmsg")
		Expect(cmd.Err()).NotTo(HaveOccurred())
		rawBytes, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawBytes)).To(Equal("$7\r\ntestmsg\r\n"))
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

	It("streams raw RESP bytes for integer (INCR)", func() {
		client.Set(ctx, "writeto_counter", "10", 0)
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "INCR", "writeto_counter")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(5)))
		Expect(buf.String()).To(Equal(":11\r\n"))
	})

	It("streams raw RESP bytes for nil value (GET non-existent)", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "GET", "nonexistent_key")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(buf.Len())))
		rawStr := buf.String()
		// RESP2 returns $-1\r\n, RESP3 returns _\r\n
		Expect(rawStr == "$-1\r\n" || rawStr == "_\r\n").To(BeTrue())
	})

	It("streams raw RESP bytes for array (LRANGE) with exact validation", func() {
		client.RPush(ctx, "writeto_list", "a", "b", "c")
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "LRANGE", "writeto_list", "0", "-1")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(buf.Len())))
		// LRANGE returns elements in order
		expectedResp := "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
		Expect(buf.String()).To(Equal(expectedResp))
	})

	It("streams raw RESP bytes for empty array (LRANGE empty list)", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "LRANGE", "nonexistent_list", "0", "-1")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(buf.Len())))
		Expect(buf.String()).To(Equal("*0\r\n"))
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

	It("streams HGETALL with complete validation", func() {
		client.HSet(ctx, "rawhash2", "key1", "val1", "key2", "val2")
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "HGETALL", "rawhash2")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		// Verify written bytes match buffer length
		Expect(written).To(Equal(int64(buf.Len())))

		// Validate RESP structure
		rawBytes := buf.Bytes()
		respType, valid := parseRESPType(rawBytes)
		Expect(valid).To(BeTrue())
		Expect(respType).To(BeElementOf(byte('*'), byte('%')))

		// Verify all data present
		rawStr := buf.String()
		Expect(rawStr).To(ContainSubstring("key1"))
		Expect(rawStr).To(ContainSubstring("val1"))
		Expect(rawStr).To(ContainSubstring("key2"))
		Expect(rawStr).To(ContainSubstring("val2"))
	})

	It("streams empty hash correctly", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "HGETALL", "nonexistent_hash")
		written, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(Equal(int64(buf.Len())))

		rawBytes := buf.Bytes()
		respType, valid := parseRESPType(rawBytes)
		Expect(valid).To(BeTrue())
		Expect(respType).To(BeElementOf(byte('*'), byte('%')))

		count, ok := countRESPElements(rawBytes)
		Expect(ok).To(BeTrue())
		Expect(count).To(Equal(0))
	})

	It("streams error response correctly", func() {
		var buf bytes.Buffer
		cmd := client.DoRawWriteTo(ctx, &buf, "INVALIDCMD")
		_, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())

		rawStr := buf.String()
		Expect(rawStr).To(HavePrefix("-"))
		Expect(rawStr).To(ContainSubstring("ERR"))
	})
})

var _ = Describe("NoRetry behavior", func() {
	It("RawCmd does not have NoRetry set", func() {
		cmd := redis.NewRawCmd(ctx, "PING")
		Expect(cmd.NoRetry()).To(BeFalse())
	})

	It("RawWriteToCmd has NoRetry set to prevent data corruption", func() {
		var buf bytes.Buffer
		cmd := redis.NewRawWriteToCmd(ctx, &buf, "PING")
		Expect(cmd.NoRetry()).To(BeTrue())
	})
})

var _ = Describe("ZeroCopyStringCmd", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("reads GET response directly into buffer", func() {
		// Set a value
		err := client.Set(ctx, "zerocopy_test", "hello world", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Read into buffer using zero-copy
		buf := make([]byte, 100)
		cmd := client.GetToBuffer(ctx, "zerocopy_test", buf)
		Expect(cmd.Err()).NotTo(HaveOccurred())

		n, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(11))
		Expect(string(cmd.Bytes())).To(Equal("hello world"))
	})

	It("handles non-existent key (nil)", func() {
		buf := make([]byte, 100)
		cmd := client.GetToBuffer(ctx, "nonexistent_key", buf)
		Expect(cmd.Err()).To(Equal(redis.Nil))
	})

	It("handles large values", func() {
		// Create a large value
		largeValue := make([]byte, 10000)
		for i := range largeValue {
			largeValue[i] = byte('a' + (i % 26))
		}

		err := client.Set(ctx, "large_key", largeValue, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Read into buffer
		buf := make([]byte, 15000)
		cmd := client.GetToBuffer(ctx, "large_key", buf)
		Expect(cmd.Err()).NotTo(HaveOccurred())

		n, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(10000))
		Expect(cmd.Bytes()).To(Equal(largeValue))
	})

	It("returns error when buffer is too small", func() {
		err := client.Set(ctx, "big_value", "this is a longer string", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Buffer too small
		buf := make([]byte, 5)
		cmd := client.GetToBuffer(ctx, "big_value", buf)
		Expect(cmd.Err()).To(HaveOccurred())
		Expect(cmd.Err().Error()).To(ContainSubstring("buffer too small"))
	})

	It("handles empty string value", func() {
		err := client.Set(ctx, "empty_key", "", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, 100)
		cmd := client.GetToBuffer(ctx, "empty_key", buf)
		Expect(cmd.Err()).NotTo(HaveOccurred())

		n, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(0))
		Expect(cmd.Bytes()).To(BeEmpty())
	})

	It("handles binary data", func() {
		// Binary data with null bytes and special characters
		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x10}

		err := client.Set(ctx, "binary_key", binaryData, 0).Err()
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, 100)
		cmd := client.GetToBuffer(ctx, "binary_key", buf)
		Expect(cmd.Err()).NotTo(HaveOccurred())

		n, err := cmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(binaryData)))
		Expect(cmd.Bytes()).To(Equal(binaryData))
	})

	It("NoRetry returns true", func() {
		buf := make([]byte, 100)
		cmd := redis.NewZeroCopyStringCmd(ctx, buf, "GET", "key")
		Expect(cmd.NoRetry()).To(BeTrue())
	})
})

var _ = Describe("SetFromBuffer", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("writes data directly from buffer", func() {
		buf := []byte("hello world from buffer")
		err := client.SetFromBuffer(ctx, "zerocopy_set_test", buf).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify the data was written correctly
		val, err := client.Get(ctx, "zerocopy_set_test").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("hello world from buffer"))
	})

	It("handles large values", func() {
		// Create a large value
		largeValue := make([]byte, 10000)
		for i := range largeValue {
			largeValue[i] = byte('a' + (i % 26))
		}

		err := client.SetFromBuffer(ctx, "large_set_key", largeValue).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify using GetToBuffer for symmetry
		readBuf := make([]byte, 15000)
		cmd := client.GetToBuffer(ctx, "large_set_key", readBuf)
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Val()).To(Equal(10000))
		Expect(cmd.Bytes()).To(Equal(largeValue))
	})

	It("handles empty buffer", func() {
		buf := []byte{}
		err := client.SetFromBuffer(ctx, "empty_set_key", buf).Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := client.Get(ctx, "empty_set_key").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(""))
	})

	It("handles binary data", func() {
		// Binary data with null bytes and special characters
		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x10}

		err := client.SetFromBuffer(ctx, "binary_set_key", binaryData).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify using GetToBuffer
		readBuf := make([]byte, 100)
		cmd := client.GetToBuffer(ctx, "binary_set_key", readBuf)
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Bytes()).To(Equal(binaryData))
	})

	// Note: SetFromBuffer with expiration is not supported in zero-copy mode
	// because the RESP protocol requires all arguments to be written in order,
	// but zero-copy writes the value data after flushing the buffer.
	// Use SetFromBuffer without expiration, then call Expire separately if needed.

	It("round-trip with GetToBuffer", func() {
		// Test complete round-trip: SetFromBuffer -> GetToBuffer
		originalData := []byte("round trip test data with special chars: \x00\xFF\r\n")

		err := client.SetFromBuffer(ctx, "roundtrip_key", originalData).Err()
		Expect(err).NotTo(HaveOccurred())

		readBuf := make([]byte, 100)
		cmd := client.GetToBuffer(ctx, "roundtrip_key", readBuf)
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Bytes()).To(Equal(originalData))
	})

	It("handles huge data (50MB) round-trip", func() {
		// Test with 50MB of data to verify zero-copy works with huge payloads
		const dataSize = 50 * 1024 * 1024 // 50MB

		// Create deterministic test data
		hugeData := make([]byte, dataSize)
		for i := range hugeData {
			hugeData[i] = byte(i % 256)
		}

		// Set using SetFromBuffer
		err := client.SetFromBuffer(ctx, "huge_zerocopy_key", hugeData).Err()
		Expect(err).NotTo(HaveOccurred())

		// Get using GetToBuffer
		readBuf := make([]byte, dataSize+1000) // Slightly larger to be safe
		cmd := client.GetToBuffer(ctx, "huge_zerocopy_key", readBuf)
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Val()).To(Equal(dataSize))

		// Verify data integrity
		retrievedData := cmd.Bytes()
		Expect(len(retrievedData)).To(Equal(dataSize))

		// Check first, middle, and last portions
		Expect(retrievedData[:1000]).To(Equal(hugeData[:1000]))
		midPoint := dataSize / 2
		Expect(retrievedData[midPoint : midPoint+1000]).To(Equal(hugeData[midPoint : midPoint+1000]))
		Expect(retrievedData[dataSize-1000:]).To(Equal(hugeData[dataSize-1000:]))

		// Full comparison
		Expect(retrievedData).To(Equal(hugeData))
	})

	It("handles 500MB data round-trip", func() {
		// Test with 500MB - under default Redis proto-max-bulk-len (512MB)
		// Note: Redis internally uses int32 for bulk string lengths,
		// so the absolute max is 2147483646 bytes (~2GB - 1 byte)
		const dataSize = 500 * 1024 * 1024 // 500MB

		// Create deterministic test data
		hugeData := make([]byte, dataSize)
		for i := range hugeData {
			hugeData[i] = byte(i % 256)
		}

		// Set using SetFromBuffer
		err := client.SetFromBuffer(ctx, "huge_500mb_key", hugeData).Err()
		Expect(err).NotTo(HaveOccurred())

		// Get using GetToBuffer
		readBuf := make([]byte, dataSize+1000) // Slightly larger to be safe
		cmd := client.GetToBuffer(ctx, "huge_500mb_key", readBuf)
		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Val()).To(Equal(dataSize))

		// Verify data integrity
		retrievedData := cmd.Bytes()
		Expect(len(retrievedData)).To(Equal(dataSize))

		// Check first, middle, and last portions
		Expect(retrievedData[:1000]).To(Equal(hugeData[:1000]))
		midPoint := dataSize / 2
		Expect(retrievedData[midPoint : midPoint+1000]).To(Equal(hugeData[midPoint : midPoint+1000]))
		Expect(retrievedData[dataSize-1000:]).To(Equal(hugeData[dataSize-1000:]))

		// Sample verification at 100MB intervals
		for i := 0; i < 5; i++ {
			offset := i * 100 * 1024 * 1024
			Expect(retrievedData[offset : offset+1000]).To(Equal(hugeData[offset : offset+1000]))
		}
	})
})
