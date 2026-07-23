package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

type bitCountExpected struct {
	Start    int64
	End      int64
	Expected int64
}

var _ = Describe("BitCountBite", func() {
	var client redis.UniversalClient
	var rawClient *redis.Client
	var closeSubject func() error
	key := "bit_count_test"

	BeforeEach(func() {
		rawClient = redis.NewClient(redisOptions())
		Expect(rawClient.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		client, closeSubject = newUniversalSubject(rawClient)
		values := []int{0, 1, 0, 0, 1, 0, 1, 0, 1, 1}
		for i, v := range values {
			cmd := client.SetBit(ctx, key, int64(i), v)
			Expect(cmd.Err()).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		Expect(closeSubject()).NotTo(HaveOccurred())
	})

	It("bit count bite", func() {
		var expected = []bitCountExpected{
			{0, 0, 0},
			{0, 1, 1},
			{0, 2, 1},
			{0, 3, 1},
			{0, 4, 2},
			{0, 5, 2},
			{0, 6, 3},
			{0, 7, 3},
			{0, 8, 4},
			{0, 9, 5},
		}

		for _, e := range expected {
			cmd := client.BitCount(ctx, key, &redis.BitCount{Start: e.Start, End: e.End, Unit: redis.BitCountIndexBit})
			Expect(cmd.Err()).NotTo(HaveOccurred())
			Expect(cmd.Val()).To(Equal(e.Expected))
		}
	})
})

var _ = Describe("BitCountByte", func() {
	var client redis.UniversalClient
	var rawClient *redis.Client
	var closeSubject func() error
	key := "bit_count_test"

	BeforeEach(func() {
		rawClient = redis.NewClient(redisOptions())
		Expect(rawClient.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		client, closeSubject = newUniversalSubject(rawClient)
		values := []int{0, 0, 0, 0, 0, 0, 0, 1, 1, 1}
		for i, v := range values {
			cmd := client.SetBit(ctx, key, int64(i), v)
			Expect(cmd.Err()).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		Expect(closeSubject()).NotTo(HaveOccurred())
	})

	It("bit count byte", func() {
		var expected = []bitCountExpected{
			{0, 0, 1},
			{0, 1, 3},
		}

		for _, e := range expected {
			cmd := client.BitCount(ctx, key, &redis.BitCount{Start: e.Start, End: e.End, Unit: redis.BitCountIndexByte})
			Expect(cmd.Err()).NotTo(HaveOccurred())
			Expect(cmd.Val()).To(Equal(e.Expected))
		}
	})

	It("bit count byte with no unit specified", func() {
		var expected = []bitCountExpected{
			{0, 0, 1},
			{0, 1, 3},
		}

		for _, e := range expected {
			cmd := client.BitCount(ctx, key, &redis.BitCount{Start: e.Start, End: e.End})
			Expect(cmd.Err()).NotTo(HaveOccurred())
			Expect(cmd.Val()).To(Equal(e.Expected))
		}
	})
})
