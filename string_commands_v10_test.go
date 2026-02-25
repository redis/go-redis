package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("String Commands V10", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("GetV10/SetV10", func() {
		It("should set and get a value", func() {
			// Set a value
			status, err := client.SetV10(ctx, "key", "hello", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal("OK"))

			// Get the value
			val, err := client.GetV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should return error for non-existent key", func() {
			val, err := client.GetV10(ctx, "nonexistent")
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))
		})

		It("should set with expiration", func() {
			status, err := client.SetV10(ctx, "key", "value", 1*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal("OK"))

			val, err := client.GetV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value"))

			// Wait for expiration
			time.Sleep(2 * time.Second)
			val, err = client.GetV10(ctx, "key")
			Expect(err).To(Equal(redis.Nil))
		})
	})

	Describe("IncrV10/DecrV10", func() {
		It("should increment a counter", func() {
			val, err := client.IncrV10(ctx, "counter")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(1)))

			val, err = client.IncrV10(ctx, "counter")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(2)))
		})

		It("should increment by value", func() {
			val, err := client.IncrByV10(ctx, "counter", 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(5)))

			val, err = client.IncrByV10(ctx, "counter", 3)
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(8)))
		})

		It("should decrement a counter", func() {
			// Set initial value
			_, err := client.SetV10(ctx, "counter", "10", 0)
			Expect(err).NotTo(HaveOccurred())

			val, err := client.DecrV10(ctx, "counter")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(9)))

			val, err = client.DecrV10(ctx, "counter")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(8)))
		})

		It("should decrement by value", func() {
			// Set initial value
			_, err := client.SetV10(ctx, "counter", "10", 0)
			Expect(err).NotTo(HaveOccurred())

			val, err := client.DecrByV10(ctx, "counter", 3)
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(7)))
		})
	})

	Describe("SetNXV10/SetXXV10", func() {
		It("should set only if not exists", func() {
			// First set should succeed
			wasSet, err := client.SetNXV10(ctx, "key", "value1", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(wasSet).To(BeTrue())

			// Second set should fail (key exists)
			wasSet, err = client.SetNXV10(ctx, "key", "value2", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(wasSet).To(BeFalse())

			// Value should still be value1
			val, err := client.GetV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value1"))
		})

		It("should set only if exists", func() {
			// First set should fail (key doesn't exist)
			wasSet, err := client.SetXXV10(ctx, "key", "value1", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(wasSet).To(BeFalse())

			// Create the key
			_, err = client.SetV10(ctx, "key", "initial", 0)
			Expect(err).NotTo(HaveOccurred())

			// Now SetXX should succeed
			wasSet, err = client.SetXXV10(ctx, "key", "value2", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(wasSet).To(BeTrue())

			// Value should be value2
			val, err := client.GetV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value2"))
		})
	})

	Describe("MGetV10/MSetV10", func() {
		It("should set and get multiple values", func() {
			// Set multiple values
			status, err := client.MSetV10(ctx, "key1", "value1", "key2", "value2", "key3", "value3")
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal("OK"))

			// Get multiple values
			vals, err := client.MGetV10(ctx, "key1", "key2", "key3")
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"value1", "value2", "value3"}))
		})

		It("should return nil for non-existent keys", func() {
			vals, err := client.MGetV10(ctx, "key1", "nonexistent", "key3")
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(HaveLen(3))
			Expect(vals[1]).To(BeNil())
		})
	})

	Describe("AppendV10", func() {
		It("should append to a string", func() {
			// Append to non-existent key
			length, err := client.AppendV10(ctx, "key", "hello")
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(int64(5)))

			// Append more
			length, err = client.AppendV10(ctx, "key", " world")
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(int64(11)))

			// Verify value
			val, err := client.GetV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello world"))
		})
	})

	Describe("GetRangeV10/SetRangeV10", func() {
		It("should get and set range", func() {
			// Set initial value
			_, err := client.SetV10(ctx, "key", "hello world", 0)
			Expect(err).NotTo(HaveOccurred())

			// Get range
			val, err := client.GetRangeV10(ctx, "key", 0, 4)
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			// Set range
			length, err := client.SetRangeV10(ctx, "key", 6, "Redis")
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(int64(11)))

			// Verify
			val, err = client.GetV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello Redis"))
		})
	})

	Describe("StrLenV10", func() {
		It("should return string length", func() {
			_, err := client.SetV10(ctx, "key", "hello", 0)
			Expect(err).NotTo(HaveOccurred())

			length, err := client.StrLenV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(int64(5)))
		})

		It("should return 0 for non-existent key", func() {
			length, err := client.StrLenV10(ctx, "nonexistent")
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(int64(0)))
		})
	})

	Describe("GetDelV10/GetExV10", func() {
		It("should get and delete", func() {
			_, err := client.SetV10(ctx, "key", "value", 0)
			Expect(err).NotTo(HaveOccurred())

			val, err := client.GetDelV10(ctx, "key")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value"))

			// Key should be deleted
			_, err = client.GetV10(ctx, "key")
			Expect(err).To(Equal(redis.Nil))
		})

		It("should get and set expiration", func() {
			_, err := client.SetV10(ctx, "key", "value", 0)
			Expect(err).NotTo(HaveOccurred())

			val, err := client.GetExV10(ctx, "key", 1*time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("value"))

			// Key should expire
			time.Sleep(2 * time.Second)
			_, err = client.GetV10(ctx, "key")
			Expect(err).To(Equal(redis.Nil))
		})
	})
})

