package redis_test

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("GCRA rate limiting", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		SkipBeforeRedisVersion(8.8, "GCRA command requires Redis 8.8+")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("GCRA", func() {
		It("should allow requests within rate limit", func() {
			// Allow 10 requests per second with a burst of 5
			result, err := client.GCRA(ctx, "user:123", 5, 10, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Limited).To(Equal(int64(0)))     // Not limited
			Expect(result.MaxRequests).To(Equal(int64(6))) // MaxBurst + 1
			Expect(result.AvailableRequests).To(BeNumerically(">=", 0))
			Expect(result.RetryAfter).To(Equal(float64(-1))) // Not limited, so -1
		})

		It("should rate limit when exceeding burst", func() {
			// Allow 1 request per second with no burst
			key := "user:456"

			// First request should succeed
			result, err := client.GCRA(ctx, key, 0, 1, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))

			// Second immediate request should be limited
			result, err = client.GCRA(ctx, key, 0, 1, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(1))) // Limited
			Expect(result.RetryAfter).To(BeNumerically(">", 0))
		})

		It("should handle burst correctly", func() {
			// Allow 10 requests per second with a burst of 5
			key := "user:789"

			// Should be able to make 6 requests (burst + 1) immediately
			for i := 0; i < 6; i++ {
				result, err := client.GCRA(ctx, key, 5, 10, time.Second).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Limited).To(Equal(int64(0)), "Request %d should not be limited", i+1)
			}

			// 7th request should be limited
			result, err := client.GCRA(ctx, key, 5, 10, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(1)))
			Expect(result.RetryAfter).To(BeNumerically(">", 0))
		})

		It("should return correct max requests", func() {
			result, err := client.GCRA(ctx, "user:max", 10, 5, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.MaxRequests).To(Equal(int64(11))) // MaxBurst + 1
		})

		It("should handle different periods", func() {
			// 60 requests per minute
			result, err := client.GCRA(ctx, "user:minute", 10, 60, time.Minute).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))
			Expect(result.MaxRequests).To(Equal(int64(11)))
		})

		It("should handle fractional periods", func() {
			// 10 requests per 500ms
			result, err := client.GCRA(ctx, "user:fractional", 5, 10, 500*time.Millisecond).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))
		})

		It("should track separate keys independently", func() {
			key1 := "user:key1"
			key2 := "user:key2"

			// Exhaust key1
			for i := 0; i < 2; i++ {
				client.GCRA(ctx, key1, 0, 1, time.Second)
			}

			// key1 should be limited
			result1, err := client.GCRA(ctx, key1, 0, 1, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result1.Limited).To(Equal(int64(1)))

			// key2 should still be allowed
			result2, err := client.GCRA(ctx, key2, 0, 1, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result2.Limited).To(Equal(int64(0)))
		})
	})

	Describe("GCRAWithArgs", func() {
		It("should work with default num requests", func() {
			args := &redis.GCRAArgs{
				MaxBurst:          5,
				RequestsPerPeriod: 10,
				Period:            time.Second,
				NumRequests:       1,
			}
			result, err := client.GCRAWithArgs(ctx, "user:args1", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))
		})

		It("should handle custom num requests", func() {
			key := "user:weighted"
			args := &redis.GCRAArgs{
				MaxBurst:          10,
				RequestsPerPeriod: 10,
				Period:            time.Second,
				NumRequests:       5, // Each request costs 5 tokens
			}

			// First request costs 5 tokens
			result, err := client.GCRAWithArgs(ctx, key, args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))

			// Second request costs 5 tokens (total 10)
			result, err = client.GCRAWithArgs(ctx, key, args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))

			// Third request would cost 5 more tokens (total 15), should be limited
			result, err = client.GCRAWithArgs(ctx, key, args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(1)))
		})

		It("should handle large num requests", func() {
			key := "user:heavy"
			args := &redis.GCRAArgs{
				MaxBurst:          5,
				RequestsPerPeriod: 10,
				Period:            time.Second,
				NumRequests:       10, // Request costs all available tokens
			}

			// First heavy request should succeed but consume all tokens
			result, err := client.GCRAWithArgs(ctx, key, args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))

			// Second request should be limited
			result, err = client.GCRAWithArgs(ctx, key, args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(1)))
		})

		It("should work with different period units", func() {
			// Test with milliseconds
			args := &redis.GCRAArgs{
				MaxBurst:          2,
				RequestsPerPeriod: 100,
				Period:            100 * time.Millisecond,
				NumRequests:       1,
			}
			result, err := client.GCRAWithArgs(ctx, "user:millis", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))

			// Test with minutes
			args = &redis.GCRAArgs{
				MaxBurst:          10,
				RequestsPerPeriod: 60,
				Period:            time.Minute,
				NumRequests:       1,
			}
			result, err = client.GCRAWithArgs(ctx, "user:minutes", args).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Limited).To(Equal(int64(0)))
		})
	})

	Describe("GCRA result fields", func() {
		It("should return all fields correctly", func() {
			result, err := client.GCRA(ctx, "user:fields", 5, 10, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())

			// Check all fields are present
			Expect(result.Limited).To(BeNumerically(">=", 0))
			Expect(result.MaxRequests).To(Equal(int64(6)))
			Expect(result.AvailableRequests).To(BeNumerically(">=", 0))
			Expect(result.RetryAfter).To(BeNumerically(">=", -1))
			Expect(result.FullBurstAfter).To(BeNumerically(">=", 0))
		})

		It("should show decreasing available requests", func() {
			key := "user:decreasing"

			result1, err := client.GCRA(ctx, key, 5, 10, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			available1 := result1.AvailableRequests

			result2, err := client.GCRA(ctx, key, 5, 10, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			available2 := result2.AvailableRequests

			// Available requests should decrease
			Expect(available2).To(BeNumerically("<", available1))
		})
	})

	Describe("GCRA edge cases", func() {
		It("should handle zero burst", func() {
			result, err := client.GCRA(ctx, "user:noburst", 0, 10, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.MaxRequests).To(Equal(int64(1))) // 0 + 1
		})

		It("should handle high rate limits", func() {
			result, err := client.GCRA(ctx, "user:high", 1000, 10000, time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.MaxRequests).To(Equal(int64(1001)))
		})
	})
})
