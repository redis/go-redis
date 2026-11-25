package redis_test

import (
	"context"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// BITRANGEINTARRAY (Redis 8.12). The expected replies come from the client HLD
// tables: the client passes arguments through and returns the reply as sent.
var _ = Describe("BitRangeIntArray", Label("bitmap"), func() {
	ctx := context.TODO()
	var client *redis.Client

	limit := func(n int64) *int64 { return &n }
	ints := func(v ...int64) []int64 { return v }

	setBits := func(key string, offsets ...int64) {
		for _, off := range offsets {
			Expect(client.SetBit(ctx, key, off, 1).Err()).NotTo(HaveOccurred())
		}
	}
	rng := func(key string, start, end int64, opts *redis.BitRangeIntArrayOptions) []int64 {
		offs, err := client.BitRangeIntArrayWithOptions(ctx, key, start, end, opts).Result()
		Expect(err).NotTo(HaveOccurred())
		return offs
	}

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.12", "BITRANGEINTARRAY needs Redis 8.12")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(client.Close()).NotTo(HaveOccurred()) })
	})

	It("should read ranges in bytes and bits", func() {
		// Four bytes, the first and last bit of each byte set.
		setBits("b", 0, 7, 8, 15, 16, 23, 24, 31)
		bit := &redis.BitRangeIntArrayOptions{Unit: redis.BitCountIndexBit}

		Expect(rng("b", 0, -1, nil)).To(Equal(ints(0, 7, 8, 15, 16, 23, 24, 31)))
		Expect(rng("b", 1, 1, nil)).To(Equal(ints(8, 15)))
		Expect(rng("b", 1, 2, nil)).To(Equal(ints(8, 15, 16, 23)))
		Expect(rng("b", -1, -1, nil)).To(Equal(ints(24, 31)))
		Expect(rng("b", -2, -1, nil)).To(Equal(ints(16, 23, 24, 31)))
		Expect(rng("b", 2, 99, nil)).To(Equal(ints(16, 23, 24, 31)))
		Expect(rng("b", -99, 0, nil)).To(Equal(ints(0, 7)))
		Expect(rng("b", 3, 1, nil)).To(BeEmpty())
		Expect(rng("b", 4, 9, nil)).To(BeEmpty())
		Expect(rng("b", -1, -2, nil)).To(BeEmpty())

		Expect(rng("b", 0, 0, bit)).To(Equal(ints(0)))
		Expect(rng("b", 7, 8, bit)).To(Equal(ints(7, 8)))
		Expect(rng("b", 1, 6, bit)).To(BeEmpty())
		Expect(rng("b", -8, -1, bit)).To(Equal(ints(24, 31)))
		Expect(rng("b", 30, 1000, bit)).To(Equal(ints(31)))
		Expect(rng("b", 32, 40, bit)).To(BeEmpty())
	})

	It("should match BITCOUNT over the same range", func() {
		setBits("b", 0, 7, 8, 15, 16, 23, 24, 31, 40)
		for _, r := range [][2]int64{{0, -1}, {1, 2}, {-2, -1}, {2, 99}, {3, 1}} {
			count, err := client.BitCount(ctx, "b", &redis.BitCount{Start: r[0], End: r[1]}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(int64(len(rng("b", r[0], r[1], nil)))).To(Equal(count), "range %v", r)
		}
	})

	It("should return the clear bits inside the value only", func() {
		setBits("b", 0, 7, 8, 15, 16, 23, 24, 31)
		bit := func() *redis.BitRangeIntArrayOptions {
			return &redis.BitRangeIntArrayOptions{Unit: redis.BitCountIndexBit, Zeros: true}
		}

		Expect(rng("b", 0, 0, bit())).To(BeEmpty())
		Expect(rng("b", 1, 6, bit())).To(Equal(ints(1, 2, 3, 4, 5, 6)))

		// 0xff 0x7e: ZEROS covers bits 8 and 15 only, never bits past the end.
		Expect(client.Set(ctx, "s", "\xff\x7e", 0).Err()).NotTo(HaveOccurred())
		Expect(rng("s", 0, -1, &redis.BitRangeIntArrayOptions{Zeros: true})).To(Equal(ints(8, 15)))
		Expect(rng("s", 0, 99, &redis.BitRangeIntArrayOptions{Zeros: true})).To(Equal(ints(8, 15)))

		// BITPOS looks one bit past an all-ones value; this command does not.
		Expect(client.Set(ctx, "ones", "\xff\xff", 0).Err()).NotTo(HaveOccurred())
		Expect(rng("ones", 0, -1, &redis.BitRangeIntArrayOptions{Zeros: true})).To(BeEmpty())
	})

	Describe("LIMIT", func() {
		BeforeEach(func() {
			// Bytes 0, 7 and 8 full; byte 1 holds bits 8 and 15; byte 9 holds bit 73.
			offs := []int64{8, 15, 73}
			for i := int64(0); i < 8; i++ {
				offs = append(offs, i, 56+i, 64+i)
			}
			setBits("p", offs...)
		})

		It("should complete the last byte in BYTE units", func() {
			Expect(rng("p", 0, -1, &redis.BitRangeIntArrayOptions{Limit: limit(1)})).
				To(Equal(ints(0, 1, 2, 3, 4, 5, 6, 7)))
			Expect(rng("p", 0, -1, &redis.BitRangeIntArrayOptions{Limit: limit(9)})).
				To(Equal(ints(0, 1, 2, 3, 4, 5, 6, 7, 8, 15)))
			Expect(rng("p", 1, -1, &redis.BitRangeIntArrayOptions{Limit: limit(1)})).
				To(Equal(ints(8, 15)))
		})

		It("should stop at the count in BIT units", func() {
			bit := redis.BitCountIndexBit
			Expect(rng("p", 0, -1, &redis.BitRangeIntArrayOptions{Unit: bit, Limit: limit(9)})).
				To(Equal(ints(0, 1, 2, 3, 4, 5, 6, 7, 8)))
			Expect(rng("p", 57, -1, &redis.BitRangeIntArrayOptions{Unit: bit, Limit: limit(2)})).
				To(Equal(ints(57, 58)))
		})

		It("should send LIMIT 0 and get an empty array", func() {
			offs, err := client.BitRangeIntArrayWithOptions(ctx, "p", 0, -1,
				&redis.BitRangeIntArrayOptions{Limit: limit(0)}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(offs).NotTo(BeNil())
			Expect(offs).To(BeEmpty())
		})

		It("should page in BIT units without gaps or repeats", func() {
			var all []int64
			start := int64(0)
			for {
				page := rng("p", start, -1, &redis.BitRangeIntArrayOptions{
					Unit: redis.BitCountIndexBit, Limit: limit(9),
				})
				all = append(all, page...)
				if len(page) < 9 {
					break
				}
				start = page[len(page)-1] + 1
			}
			Expect(all).To(Equal(rng("p", 0, -1, nil)))
			Expect(all).To(HaveLen(27))
		})

		It("should page in BYTE units, accepting pages longer than the count", func() {
			var all []int64
			start := int64(0)
			for {
				page := rng("p", start, -1, &redis.BitRangeIntArrayOptions{Limit: limit(9)})
				all = append(all, page...)
				if len(page) < 9 {
					break
				}
				start = page[len(page)-1]/8 + 1
			}
			Expect(all).To(Equal(rng("p", 0, -1, nil)))
			Expect(all).To(HaveLen(27))
		})
	})

	It("should return an empty array for a missing key", func() {
		offs, err := client.BitRangeIntArray(ctx, "missing", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(BeEmpty())
	})

	It("should pass invalid arguments through to the server", func() {
		setBits("b", 1)

		err := client.BitRangeIntArrayWithOptions(ctx, "b", 0, -1, &redis.BitRangeIntArrayOptions{Unit: "nibble"}).Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("syntax error"))

		err = client.BitRangeIntArrayWithOptions(ctx, "b", 0, -1, &redis.BitRangeIntArrayOptions{Limit: limit(-1)}).Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("LIMIT can't be negative"))

		Expect(client.RPush(ctx, "list", "x").Err()).NotTo(HaveOccurred())
		err = client.BitRangeIntArray(ctx, "list", 0, -1).Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
	})
})
