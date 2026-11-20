package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// Native Roaring bitmaps (Redis 8.12, redis/redis#15331): with
// bitmap-default-roaring on, bit writes create a distinct "bitmap" type with
// encoding "bitmap-roaring". The bit commands work on it unchanged; string
// commands return WRONGTYPE. These specs pin that the client needs no special
// handling.
//
// NonRedisEnterprise: the specs flip a server config with CONFIG SET.
var _ = Describe("Native bitmap", Label("bitmap", "NonRedisEnterprise"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.12", "native bitmaps were added in Redis 8.12")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		prev, err := client.ConfigGet(ctx, "bitmap-default-roaring").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(client.ConfigSet(ctx, "bitmap-default-roaring", "yes").Err()).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(client.ConfigSet(ctx, "bitmap-default-roaring", prev["bitmap-default-roaring"]).Err()).
				NotTo(HaveOccurred())
			Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			Expect(client.Close()).NotTo(HaveOccurred())
		})
	})

	It("should report type and encoding", func() {
		Expect(client.SetBit(ctx, "b", 100, 1).Err()).NotTo(HaveOccurred())

		Expect(client.Type(ctx, "b").Val()).To(Equal("bitmap"))
		Expect(client.ObjectEncoding(ctx, "b").Val()).To(Equal("bitmap-roaring"))
	})

	It("should keep plain strings when the config is off", func() {
		Expect(client.ConfigSet(ctx, "bitmap-default-roaring", "no").Err()).NotTo(HaveOccurred())
		Expect(client.SetBit(ctx, "s", 100, 1).Err()).NotTo(HaveOccurred())

		Expect(client.Type(ctx, "s").Val()).To(Equal("string"))
	})

	It("should run the bit commands on a native bitmap", func() {
		Expect(client.SetBit(ctx, "b", 7, 1).Err()).NotTo(HaveOccurred())
		Expect(client.SetBit(ctx, "b", 100, 1).Err()).NotTo(HaveOccurred())

		Expect(client.GetBit(ctx, "b", 100).Val()).To(Equal(int64(1)))
		Expect(client.GetBit(ctx, "b", 8).Val()).To(Equal(int64(0)))
		Expect(client.BitCount(ctx, "b", nil).Val()).To(Equal(int64(2)))
		Expect(client.BitPos(ctx, "b", 1).Val()).To(Equal(int64(7)))

		res, err := client.BitField(ctx, "b", "GET", "u8", "0").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal([]int64{1}))
	})

	It("should store a native bitmap as the BITOP destination", func() {
		Expect(client.SetBit(ctx, "a", 1, 1).Err()).NotTo(HaveOccurred())
		Expect(client.SetBit(ctx, "c", 9, 1).Err()).NotTo(HaveOccurred())

		Expect(client.BitOpOr(ctx, "dest", "a", "c").Err()).NotTo(HaveOccurred())

		Expect(client.Type(ctx, "dest").Val()).To(Equal("bitmap"))
		Expect(client.BitCount(ctx, "dest", nil).Val()).To(Equal(int64(2)))
	})

	It("should return WRONGTYPE for string commands", func() {
		Expect(client.SetBit(ctx, "b", 100, 1).Err()).NotTo(HaveOccurred())

		err := client.Get(ctx, "b").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))

		err = client.StrLen(ctx, "b").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
	})

	It("should convert a string bitmap in place and keep its TTL", func() {
		Expect(client.ConfigSet(ctx, "bitmap-default-roaring", "no").Err()).NotTo(HaveOccurred())
		Expect(client.SetBit(ctx, "k", 5, 1).Err()).NotTo(HaveOccurred())
		Expect(client.Expire(ctx, "k", time.Hour).Err()).NotTo(HaveOccurred())
		Expect(client.Type(ctx, "k").Val()).To(Equal("string"))

		Expect(client.ConfigSet(ctx, "bitmap-default-roaring", "yes").Err()).NotTo(HaveOccurred())
		Expect(client.SetBit(ctx, "k", 6, 1).Err()).NotTo(HaveOccurred())

		Expect(client.Type(ctx, "k").Val()).To(Equal("bitmap"))
		Expect(client.TTL(ctx, "k").Val()).To(BeNumerically(">", 0))
		Expect(client.BitCount(ctx, "k", nil).Val()).To(Equal(int64(2)))
	})

	It("should round-trip through DUMP and RESTORE", func() {
		Expect(client.SetBit(ctx, "b", 100, 1).Err()).NotTo(HaveOccurred())

		dump, err := client.Dump(ctx, "b").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Restore(ctx, "copy", 0, dump).Err()).NotTo(HaveOccurred())

		Expect(client.Type(ctx, "copy").Val()).To(Equal("bitmap"))
		Expect(client.GetBit(ctx, "copy", 100).Val()).To(Equal(int64(1)))
	})

	It("should run BITRANGEINTARRAY like it does on a string bitmap", func() {
		bits := []int64{0, 7, 8, 15, 56, 57, 63, 64, 73}
		for _, off := range bits {
			Expect(client.SetBit(ctx, "native", off, 1).Err()).NotTo(HaveOccurred())
		}
		Expect(client.Type(ctx, "native").Val()).To(Equal("bitmap"))

		Expect(client.ConfigSet(ctx, "bitmap-default-roaring", "no").Err()).NotTo(HaveOccurred())
		for _, off := range bits {
			Expect(client.SetBit(ctx, "plain", off, 1).Err()).NotTo(HaveOccurred())
		}
		Expect(client.Type(ctx, "plain").Val()).To(Equal("string"))

		limit := int64(3)
		for _, opts := range []*redis.BitRangeIntArrayOptions{
			nil,
			{Unit: redis.BitCountIndexBit},
			{Unit: redis.BitCountIndexBit, Limit: &limit},
			{Limit: &limit},
			{Unit: redis.BitCountIndexBit, Zeros: true},
		} {
			native, err := client.BitRangeIntArrayWithOptions(ctx, "native", 0, -1, opts).Result()
			Expect(err).NotTo(HaveOccurred())
			plain, err := client.BitRangeIntArrayWithOptions(ctx, "plain", 0, -1, opts).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(native).To(Equal(plain), "opts %+v", opts)
		}
	})

	It("should return BITRANGEINTARRAY offsets above 32 bits", func() {
		// Bit offsets are capped at proto-max-bulk-len * 8; raise it past 2^33.
		prev, err := client.ConfigGet(ctx, "proto-max-bulk-len").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(client.ConfigSet(ctx, "proto-max-bulk-len", "2gb").Err()).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(client.ConfigSet(ctx, "proto-max-bulk-len", prev["proto-max-bulk-len"]).Err()).
				NotTo(HaveOccurred())
		})

		const off = int64(1)<<33 + 5
		Expect(client.SetBit(ctx, "big", off, 1).Err()).NotTo(HaveOccurred())
		Expect(client.Type(ctx, "big").Val()).To(Equal("bitmap"))

		limit := int64(1)
		offs, err := client.BitRangeIntArrayWithOptions(ctx, "big", 0, -1,
			&redis.BitRangeIntArrayOptions{Unit: redis.BitCountIndexBit, Limit: &limit}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(Equal([]int64{off}))
	})

	It("should SCAN by the bitmap type", func() {
		Expect(client.SetBit(ctx, "b", 100, 1).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "str", "x", 0).Err()).NotTo(HaveOccurred())

		keys, _, err := client.ScanType(ctx, 0, "", 100, "bitmap").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(ConsistOf("b"))
	})
})
