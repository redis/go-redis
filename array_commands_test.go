package redis_test

import (
	"context"
	"fmt"
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Array Commands", Label("array"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		SkipBeforeRedisVersion(8.8, "Redis 8.8.0 introduces support for Array")
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})


	Describe("ARSet and ARGet", func() {
		It("should ARSet and ARGet basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			n, err := client.ARSet(ctx, "myarray", 0, "hello").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			_, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).To(Equal(redis.Nil))
		})

		It("should ARSet overwrite existing value", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			n, err := client.ARSet(ctx, "myarray", 0, "hello").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.ARSet(ctx, "myarray", 0, "world").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("world"))
		})

		It("should ARGet non-existing key", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			_, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).To(Equal(redis.Nil))
		})

		It("should ARGet validate index on non-existing key", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			err := client.ARGet(ctx, "myarray", 0).Err()
			// Non-existing key returns redis.Nil, not an index error
			Expect(err).To(Equal(redis.Nil))
		})

		It("should ARSet and ARGet with integer values", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "12345").Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("12345"))
		})

		It("should ARSet and ARGet with float values", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "3.14159").Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("3.14159"))
		})

		It("should ARSet and ARGet with small strings", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "abc").Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("abc"))
		})

		It("should ARSet and ARGet with large string", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			longstr := strings.Repeat("x", 100)
			Expect(client.ARSet(ctx, "myarray", 0, longstr).Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(longstr))
		})

		It("should ARSet and ARGet with empty string", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "").Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(""))
		})
	})


	Describe("ARLen and ARCount", func() {
		It("should ARLen and ARCount basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			length, err := client.ARLen(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(uint64(0)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(0)))

			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			length, err = client.ARLen(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(uint64(1)))
			count, err = client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(1)))

			Expect(client.ARSet(ctx, "myarray", 5, "b").Err()).NotTo(HaveOccurred())
			length, err = client.ARLen(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(uint64(6)))
			count, err = client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(2)))

			Expect(client.ARSet(ctx, "myarray", 100, "c").Err()).NotTo(HaveOccurred())
			length, err = client.ARLen(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(uint64(101)))
			count, err = client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(3)))
		})
	})


	Describe("ARDel", func() {
		It("should ARDel basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 2, "c").Err()).NotTo(HaveOccurred())

			n, err := client.ARDel(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			_, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).To(Equal(redis.Nil))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(2)))

			// Delete non-existing index returns 0
			n, err = client.ARDel(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should ARDel multiple indices", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 2, "c").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 3, "d").Err()).NotTo(HaveOccurred())

			n, err := client.ARDel(ctx, "myarray", 0, 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(1)))
		})

		It("should ARDel last element delete key", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())

			_, err := client.ARDel(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())

			exists, err := client.Exists(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(int64(0)))
		})
	})


	Describe("ARDelRange", func() {
		It("should ARDelRange basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 10; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("%d", i*10)).Err()).NotTo(HaveOccurred())
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(10)))

			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 2, End: 6}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(5)))

			count, err = client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})

		It("should ARDelRange reverse order", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 10; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("%d", i*10)).Err()).NotTo(HaveOccurred())
			}

			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 6, End: 2}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(5)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})

		It("should ARDelRange with multiple ranges in single call", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 20; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("val%d", i)).Err()).NotTo(HaveOccurred())
			}

			n, err := client.ARDelRange(ctx, "myarray",
				redis.ARRange{Start: 2, End: 4},
				redis.ARRange{Start: 10, End: 14},
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(8)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(12)))

			// Verify correct elements deleted
			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("val0"))

			_, err = client.ARGet(ctx, "myarray", 2).Result()
			Expect(err).To(Equal(redis.Nil))

			_, err = client.ARGet(ctx, "myarray", 4).Result()
			Expect(err).To(Equal(redis.Nil))

			val, err = client.ARGet(ctx, "myarray", 15).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("val15"))
		})
	})


	Describe("ARMSet and ARMGet", func() {
		It("should ARMSet basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			n, err := client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "a"},
				redis.AREntry{Index: 1, Value: "b"},
				redis.AREntry{Index: 2, Value: "c"},
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("a"))

			val, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("b"))

			val, err = client.ARGet(ctx, "myarray", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("c"))
		})

		It("should ARMSet return only newly filled slots", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())

			n, err := client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "aa"},
				redis.AREntry{Index: 1, Value: "b"},
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("aa"))

			val, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("b"))
		})

		It("should ARMGet basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 5, "c").Err()).NotTo(HaveOccurred())

			result, err := client.ARMGet(ctx, "myarray", 0, 1, 5, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(4))
			Expect(result[0]).To(Equal("a"))
			Expect(result[1]).To(Equal("b"))
			Expect(result[2]).To(Equal("c"))
			Expect(result[3]).To(BeNil())
		})
	})


	Describe("ARGetRange and contiguous ARSet", func() {
		It("should ARGetRange basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "a"},
				redis.AREntry{Index: 1, Value: "b"},
				redis.AREntry{Index: 2, Value: "c"},
				redis.AREntry{Index: 3, Value: "d"},
				redis.AREntry{Index: 4, Value: "e"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGetRange(ctx, "myarray", 1, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"b", "c", "d"}))
		})

		It("should ARGetRange reverse", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "a"},
				redis.AREntry{Index: 1, Value: "b"},
				redis.AREntry{Index: 2, Value: "c"},
				redis.AREntry{Index: 3, Value: "d"},
				redis.AREntry{Index: 4, Value: "e"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGetRange(ctx, "myarray", 3, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"d", "c", "b"}))
		})

		It("should ARGetRange error when range exceeds hard limit", func() {
			err := client.ARGetRange(ctx, "myarray", 0, 1000000).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("range exceeds maximum"))
		})

		It("should ARGetRange reverse error when range exceeds hard limit", func() {
			err := client.ARGetRange(ctx, "myarray", 1000000, 0).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("range exceeds maximum"))
		})

		It("should ARSet contiguous write basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			n, err := client.ARSet(ctx, "myarray", 0, "a", "b", "c").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(3)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("a"))

			val, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("b"))

			val, err = client.ARGet(ctx, "myarray", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("c"))
		})
	})


	Describe("ARScan", func() {
		It("should ARScan return only existing elements with indices", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 5, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 9, "c").Err()).NotTo(HaveOccurred())

			result, err := client.ARScan(ctx, "myarray", 0, 10, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(3))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 0, Value: "a"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 5, Value: "b"}))
			Expect(result[2]).To(Equal(redis.AREntry{Index: 9, Value: "c"}))
		})

		It("should ARScan on empty range return empty array", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 500, "x").Err()).NotTo(HaveOccurred())

			result, err := client.ARScan(ctx, "myarray", 0, 100, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})

		It("should ARScan reversed range", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 5, "b").Err()).NotTo(HaveOccurred())

			result, err := client.ARScan(ctx, "myarray", 5, 0, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 5, Value: "b"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 0, Value: "a"}))
		})

		It("should ARScan on non-existent key return empty array", func() {
			Expect(client.Del(ctx, "nokey").Err()).NotTo(HaveOccurred())

			result, err := client.ARScan(ctx, "nokey", 0, 100, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})

		It("should ARScan with mixed value types", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "string").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1, "12345").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 2, "3.14").Err()).NotTo(HaveOccurred())

			result, err := client.ARScan(ctx, "myarray", 0, 10, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(3))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 0, Value: "string"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 1, Value: "12345"}))
			Expect(result[2]).To(Equal(redis.AREntry{Index: 2, Value: "3.14"}))
		})

		It("should ARScan with LIMIT", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 20; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Delete some in the middle
			_, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 5, End: 14}).Result()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.ARScan(ctx, "myarray", 0, 100, &redis.ARScanArgs{Limit: 3}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(3))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 0, Value: "0"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 1, Value: "1"}))
			Expect(result[2]).To(Equal(redis.AREntry{Index: 2, Value: "2"}))
		})
	})


	Describe("ARInsert", func() {
		It("should ARInsert basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			idx, err := client.ARInsert(ctx, "myarray", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(Equal(uint64(0)))

			idx, err = client.ARInsert(ctx, "myarray", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(Equal(uint64(1)))

			idx, err = client.ARInsert(ctx, "myarray", "c").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(Equal(uint64(2)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("a"))

			val, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("b"))

			val, err = client.ARGet(ctx, "myarray", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("c"))
		})

		It("should ARInsert with multiple values", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			idx, err := client.ARInsert(ctx, "myarray", "x", "y", "z").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(Equal(uint64(2)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("x"))

			val, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("y"))

			val, err = client.ARGet(ctx, "myarray", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("z"))
		})
	})


	Describe("ARGrep", func() {
		It("should ARGREP MATCH return matching indexes", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "alpha"},
				redis.AREntry{Index: 1, Value: "beta"},
				redis.AREntry{Index: 2, Value: "alphabet"},
				redis.AREntry{Index: 5, Value: "gamma"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGrep(ctx, "myarray", "-", "+", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepMatch, Value: "alpha"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]uint64{0, 2}))
		})

		It("should ARGREP support WITHVALUES and reverse ranges", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "alpha"},
				redis.AREntry{Index: 1, Value: "beta"},
				redis.AREntry{Index: 2, Value: "alphabet"},
				redis.AREntry{Index: 3, Value: "delta"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGrepWithValues(ctx, "myarray", "3", "0", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepMatch, Value: "alpha"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(2))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 2, Value: "alphabet"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 0, Value: "alpha"}))
		})

		It("should ARGREP support AND, GLOB, and NOCASE", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "RedisArray"},
				redis.AREntry{Index: 1, Value: "redis-match"},
				redis.AREntry{Index: 2, Value: "array-only"},
				redis.AREntry{Index: 3, Value: "plain"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGrep(ctx, "myarray", "-", "+", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepMatch, Value: "redis"},
					{Type: redis.ARGrepGlob, Value: "*array*"},
				},
				CombineAnd: true,
				NoCase:     true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]uint64{0}))
		})

		It("should ARGREP support RE predicates", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "foo123"},
				redis.AREntry{Index: 1, Value: "bar"},
				redis.AREntry{Index: 2, Value: "zoo999"},
				redis.AREntry{Index: 3, Value: "Foo777"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGrep(ctx, "myarray", "-", "+", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepRegex, Value: `^.*[0-9]{3}$`},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]uint64{0, 2, 3}))

			result, err = client.ARGrep(ctx, "myarray", "-", "+", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepRegex, Value: `^foo[0-9]+$`},
				},
				NoCase: true,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]uint64{0, 3}))
		})

		It("should ARGREP LIMIT stop after enough matches", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "hit-1"},
				redis.AREntry{Index: 1, Value: "hit-2"},
				redis.AREntry{Index: 2, Value: "miss"},
				redis.AREntry{Index: 3, Value: "hit-3"},
			).Err()).NotTo(HaveOccurred())

			result, err := client.ARGrep(ctx, "myarray", "-", "+", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepMatch, Value: "hit"},
				},
				Limit: 2,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]uint64{0, 1}))
		})

		It("should ARGREP handle missing keys", func() {
			Expect(client.Del(ctx, "nokey").Err()).NotTo(HaveOccurred())

			result, err := client.ARGrep(ctx, "nokey", "-", "+", &redis.ARGrepArgs{
				Predicates: []redis.ARGrepPredicate{
					{Type: redis.ARGrepMatch, Value: "foo"},
				},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())
		})
	})


	Describe("ARRing", func() {
		It("should ARRING create ring buffer", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			for i := 0; i < 10; i++ {
				_, err := client.ARRing(ctx, "myarray", 5, fmt.Sprintf("%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			// After wrap, indices 0-4 should have values 5-9
			for i := 0; i < 5; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("%d", i+5)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})

		It("should ARRING basic wraparound", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			for i := 0; i < 20; i++ {
				idx, err := client.ARRing(ctx, "myarray", 10, fmt.Sprintf("val%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(idx).To(Equal(uint64(i % 10)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(10)))

			// Values should be the last 10 inserted (val10-val19)
			for i := 0; i < 10; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("val%d", i+10)))
			}
		})

		It("should ARRING with size 1", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			for i := 0; i < 100; i++ {
				_, err := client.ARRing(ctx, "myarray", 1, fmt.Sprintf("val%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(1)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("val99"))
		})
	})


	Describe("ARNext and ARSeek", func() {
		It("should ARNext track insert position", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			next, err := client.ARNext(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(Equal(uint64(0)))

			_, err = client.ARInsert(ctx, "myarray", "a").Result()
			Expect(err).NotTo(HaveOccurred())

			next, err = client.ARNext(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(Equal(uint64(1)))

			_, err = client.ARInsert(ctx, "myarray", "b").Result()
			Expect(err).NotTo(HaveOccurred())

			next, err = client.ARNext(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(Equal(uint64(2)))
		})

		It("should ARSeek work", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			_, err := client.ARInsert(ctx, "myarray", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARInsert(ctx, "myarray", "b").Result()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.ARSeek(ctx, "myarray", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			idx, err := client.ARInsert(ctx, "myarray", "c").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(Equal(uint64(10)))

			next, err := client.ARNext(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(Equal(uint64(11)))

			val, err := client.ARGet(ctx, "myarray", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("c"))
		})

		It("should ARNext return nil when insert cursor is exhausted", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			_, err := client.ARInsert(ctx, "myarray", "a").Result()
			Expect(err).NotTo(HaveOccurred())

			// Move to terminal cursor state
			_, err = client.ARSeek(ctx, "myarray", 18446744073709551615).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err = client.ARNext(ctx, "myarray").Result()
			Expect(err).To(HaveOccurred())
		})

		It("should ARSeek on non-existent key not create it", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			n, err := client.ARSeek(ctx, "myarray", 100).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			exists, err := client.Exists(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(int64(0)))

			// Now create the array and verify ARSeek works
			_, err = client.ARInsert(ctx, "myarray", "first").Result()
			Expect(err).NotTo(HaveOccurred())

			exists, err = client.Exists(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(int64(1)))

			n, err = client.ARSeek(ctx, "myarray", 50).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			_, err = client.ARInsert(ctx, "myarray", "second").Result()
			Expect(err).NotTo(HaveOccurred())

			next, err := client.ARNext(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(Equal(uint64(51)))
		})
	})


	Describe("ARLastItems", func() {
		It("should ARLASTITEMS basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 5; i++ {
				_, err := client.ARInsert(ctx, "myarray", fmt.Sprintf("%d", i*10)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			result, err := client.ARLastItems(ctx, "myarray", 3, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"20", "30", "40"}))

			result, err = client.ARLastItems(ctx, "myarray", 3, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"40", "30", "20"}))
		})

		It("should ARLASTITEMS handle empty and partial cases", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Empty array
			result, err := client.ARLastItems(ctx, "myarray", 5, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())

			// Fewer items than requested
			_, err = client.ARRing(ctx, "myarray", 10, "a").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARRing(ctx, "myarray", 10, "b").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARRing(ctx, "myarray", 10, "c").Result()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.ARLastItems(ctx, "myarray", 5, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"a", "b", "c"}))
		})

		It("should ARLASTITEMS with various counts and REV", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 20; i++ {
				_, err := client.ARRing(ctx, "myarray", 10, fmt.Sprintf("item%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			// Get exactly 1 item
			result, err := client.ARLastItems(ctx, "myarray", 1, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"item19"}))

			result, err = client.ARLastItems(ctx, "myarray", 1, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"item19"}))

			// Get 3 items
			result, err = client.ARLastItems(ctx, "myarray", 3, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"item17", "item18", "item19"}))

			result, err = client.ARLastItems(ctx, "myarray", 3, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"item19", "item18", "item17"}))
		})

		It("should ARLASTITEMS edge cases", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Empty array
			result, err := client.ARLastItems(ctx, "myarray", 5, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())

			result, err = client.ARLastItems(ctx, "myarray", 5, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())

			// Single element
			_, err = client.ARInsert(ctx, "myarray", "only").Result()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.ARLastItems(ctx, "myarray", 1, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"only"}))

			result, err = client.ARLastItems(ctx, "myarray", 10, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"only"}))

			// Two elements
			_, err = client.ARInsert(ctx, "myarray", "second").Result()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.ARLastItems(ctx, "myarray", 5, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"only", "second"}))

			result, err = client.ARLastItems(ctx, "myarray", 5, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"second", "only"}))
		})
	})


	Describe("AROp", func() {
		It("should AROP SUM", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "10"},
				redis.AREntry{Index: 1, Value: "20"},
				redis.AREntry{Index: 2, Value: "30"},
			).Err()).NotTo(HaveOccurred())

			cmd := client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpSum)
			Expect(cmd.Err()).NotTo(HaveOccurred())

			val, err := cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(60)))
		})

		It("should AROP MIN", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "30"},
				redis.AREntry{Index: 1, Value: "10"},
				redis.AREntry{Index: 2, Value: "20"},
			).Err()).NotTo(HaveOccurred())

			cmd := client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpMin)
			Expect(cmd.Err()).NotTo(HaveOccurred())

			val, err := cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(10)))
		})

		It("should AROP MAX", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "30"},
				redis.AREntry{Index: 1, Value: "10"},
				redis.AREntry{Index: 2, Value: "20"},
			).Err()).NotTo(HaveOccurred())

			cmd := client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpMax)
			Expect(cmd.Err()).NotTo(HaveOccurred())

			val, err := cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(30)))
		})

		It("should AROP MATCH", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "hello"},
				redis.AREntry{Index: 1, Value: "world"},
				redis.AREntry{Index: 2, Value: "hello"},
				redis.AREntry{Index: 3, Value: "foo"},
			).Err()).NotTo(HaveOccurred())

			cmd := client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, "hello")
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(2)))

			cmd = client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, "world")
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(1)))

			cmd = client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, "bar")
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(0)))
		})

		It("should AROP USED", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "a"},
				redis.AREntry{Index: 2, Value: "b"},
				redis.AREntry{Index: 5, Value: "c"},
			).Err()).NotTo(HaveOccurred())

			cmd := client.AROp(ctx, "myarray", 0, 10, redis.ArrayOpUsed)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(3)))
		})

		It("should AROP AND/OR/XOR", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "255"},
				redis.AREntry{Index: 1, Value: "15"},
				redis.AREntry{Index: 2, Value: "240"},
			).Err()).NotTo(HaveOccurred())

			// AND: 255 & 15 & 240 = 0
			cmd := client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpAnd)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(0)))

			// OR: 255 | 15 | 240 = 255
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpOr)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(255)))

			// XOR: 255 ^ 15 ^ 240 = 0
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpXor)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(0)))
		})

		It("should AROP AND/OR/XOR truncate floats toward zero", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "7.9"},
				redis.AREntry{Index: 1, Value: "3.2"},
				redis.AREntry{Index: 2, Value: "1.8"},
			).Err()).NotTo(HaveOccurred())

			// Truncated: 7, 3, 1 → AND = 1
			cmd := client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpAnd)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(1)))

			// OR = 7
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpOr)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(7)))

			// XOR: 7 ^ 3 ^ 1 = 5
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpXor)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(5)))
		})

		It("should AROP MATCH with large strings", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			largestr := strings.Repeat("x", 300)
			largestr2 := strings.Repeat("y", 300)

			Expect(client.ARSet(ctx, "myarray", 0, largestr).Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1, "small").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 2, largestr).Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 3, largestr2).Err()).NotTo(HaveOccurred())

			cmd := client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, largestr)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(2)))

			cmd = client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, largestr2)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(1)))

			cmd = client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, "small")
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(1)))

			cmd = client.AROp(ctx, "myarray", 0, 3, redis.ArrayOpMatch, "notfound")
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int64(0)))
		})
	})


	Describe("ARInfo", func() {
		It("should ARInfo basics", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARMSet(ctx, "myarray",
				redis.AREntry{Index: 0, Value: "a"},
				redis.AREntry{Index: 1, Value: "b"},
				redis.AREntry{Index: 100, Value: "c"},
			).Err()).NotTo(HaveOccurred())

			info, err := client.ARInfo(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info["count"]).To(Equal(int64(3)))
			Expect(info["len"]).To(Equal(int64(101)))
		})
	})


	Describe("Type checks", func() {
		It("should array commands return WRONGTYPE on wrong type", func() {
			Expect(client.Del(ctx, "mykey").Err()).NotTo(HaveOccurred())
			Expect(client.Set(ctx, "mykey", "value", 0).Err()).NotTo(HaveOccurred())

			err := client.ARGet(ctx, "mykey", 0).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))

			err = client.ARSet(ctx, "mykey", 0, "foo").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))

			err = client.ARLen(ctx, "mykey").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))

			err = client.ARCount(ctx, "mykey").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
		})

		It("should ARMGET return WRONGTYPE on wrong type", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.Set(ctx, "myarray", "string_value", 0).Err()).NotTo(HaveOccurred())

			err := client.ARMGet(ctx, "myarray", 0, 1, 2).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
		})

		It("should ARGETRANGE return WRONGTYPE on wrong type", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.Set(ctx, "myarray", "string_value", 0).Err()).NotTo(HaveOccurred())

			err := client.ARGetRange(ctx, "myarray", 0, 10).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
		})
	})


	Describe("TYPE and OBJECT ENCODING", func() {
		It("should TYPE return array", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "hello").Err()).NotTo(HaveOccurred())

			typ, err := client.Type(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(typ).To(Equal("array"))
		})

		It("should OBJECT ENCODING return sliced-array", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 0, "hello").Err()).NotTo(HaveOccurred())

			encoding, err := client.ObjectEncoding(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(encoding).To(Equal("sliced-array"))
		})
	})


	Describe("Sparse arrays", func() {
		It("should sparse array with large gaps work", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 10000, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1000000, "c").Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("a"))

			val, err = client.ARGet(ctx, "myarray", 10000).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("b"))

			val, err = client.ARGet(ctx, "myarray", 1000000).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("c"))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(3)))

			length, err := client.ARLen(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(length).To(Equal(uint64(1000001)))
		})
	})


	Describe("Directory resizing", func() {
		It("should handle many slices", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			sliceSize := 4096
			for i := 0; i < 20; i++ {
				idx := uint64(i * sliceSize)
				Expect(client.ARSet(ctx, "myarray", idx, fmt.Sprintf("slice%d", i)).Err()).NotTo(HaveOccurred())
			}

			for i := 0; i < 20; i++ {
				idx := uint64(i * sliceSize)
				val, err := client.ARGet(ctx, "myarray", idx).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("slice%d", i)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(20)))
		})

		It("should handle very large index jump", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "start").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1000000, "middle").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 10000000, "end").Err()).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("start"))

			val, err = client.ARGet(ctx, "myarray", 1000000).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("middle"))

			val, err = client.ARGet(ctx, "myarray", 10000000).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("end"))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(3)))
		})
	})


	Describe("Dense window growth", func() {
		It("should handle right expansion", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			for i := 0; i < 100; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("val%d", i)).Err()).NotTo(HaveOccurred())
			}

			for i := 0; i < 100; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("val%d", i)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(100)))
		})

		It("should handle left expansion", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 500, "anchor").Err()).NotTo(HaveOccurred())
			for i := 499; i >= 400; i-- {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("val%d", i)).Err()).NotTo(HaveOccurred())
			}

			val, err := client.ARGet(ctx, "myarray", 500).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("anchor"))

			for i := 400; i < 500; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("val%d", i)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(101)))
		})

		It("should handle bidirectional expansion", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 500, "center").Err()).NotTo(HaveOccurred())
			for i := 1; i <= 50; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(500-i), fmt.Sprintf("left%d", i)).Err()).NotTo(HaveOccurred())
				Expect(client.ARSet(ctx, "myarray", uint64(500+i), fmt.Sprintf("right%d", i)).Err()).NotTo(HaveOccurred())
			}

			val, err := client.ARGet(ctx, "myarray", 500).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("center"))

			for i := 1; i <= 50; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(500-i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("left%d", i)))

				val, err = client.ARGet(ctx, "myarray", uint64(500+i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("right%d", i)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(101)))
		})
	})


	Describe("Sparse to dense promotion", func() {
		It("should promote sparse to dense when exceeding kmax threshold", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Add 15 elements at scattered offsets within one slice
			for i := 0; i < 15; i++ {
				idx := uint64(i * 100)
				Expect(client.ARSet(ctx, "myarray", idx, fmt.Sprintf("sparse%d", i)).Err()).NotTo(HaveOccurred())
			}

			for i := 0; i < 15; i++ {
				idx := uint64(i * 100)
				val, err := client.ARGet(ctx, "myarray", idx).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("sparse%d", i)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(15)))
		})

		It("should promote then continue adding", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Phase 1: sparse
			for i := 0; i < 5; i++ {
				idx := uint64(i * 200)
				Expect(client.ARSet(ctx, "myarray", idx, fmt.Sprintf("phase1_%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Phase 2: more to trigger promotion
			for i := 5; i < 20; i++ {
				idx := uint64(i * 200)
				Expect(client.ARSet(ctx, "myarray", idx, fmt.Sprintf("phase2_%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Verify all
			for i := 0; i < 5; i++ {
				idx := uint64(i * 200)
				val, err := client.ARGet(ctx, "myarray", idx).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("phase1_%d", i)))
			}
			for i := 5; i < 20; i++ {
				idx := uint64(i * 200)
				val, err := client.ARGet(ctx, "myarray", idx).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("phase2_%d", i)))
			}
		})
	})


	Describe("Dense to sparse demotion", func() {
		It("should demote dense to sparse when deleting below kmin", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create dense slice with 50 elements
			for i := 0; i < 50; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("val%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Delete most elements, leaving only 3
			for i := 3; i < 50; i++ {
				_, err := client.ARDel(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify remaining elements
			for i := 0; i < 3; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("val%d", i)))
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(3)))
		})

		It("should demote then add again", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create dense
			for i := 0; i < 30; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("initial%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Delete to demote
			for i := 4; i < 30; i++ {
				_, err := client.ARDel(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(4)))

			// Add new elements
			for i := 100; i < 105; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("new%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Verify old and new
			for i := 0; i < 4; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("initial%d", i)))
			}
			for i := 100; i < 105; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("new%d", i)))
			}
		})
	})


	Describe("ARDelRange detailed", func() {
		It("should ARDELRANGE trigger dense to sparse demotion", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create dense slice with 50 elements
			for i := 0; i < 50; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("val%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Delete most elements with ARDELRANGE
			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 3, End: 49}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(47)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(3)))

			// Verify remaining elements
			for i := 0; i < 3; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("val%d", i)))
			}
		})

		It("should ARDELRANGE with overlapping ranges", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			for i := 0; i < 20; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("val%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Overlapping ranges: [5,12] and [8,15]
			n, err := client.ARDelRange(ctx, "myarray",
				redis.ARRange{Start: 5, End: 12},
				redis.ARRange{Start: 8, End: 15},
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(11)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(9)))

			val, err := client.ARGet(ctx, "myarray", 4).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("val4"))

			_, err = client.ARGet(ctx, "myarray", 5).Result()
			Expect(err).To(Equal(redis.Nil))

			_, err = client.ARGet(ctx, "myarray", 15).Result()
			Expect(err).To(Equal(redis.Nil))

			val, err = client.ARGet(ctx, "myarray", 16).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("val16"))
		})

		It("should ARDELRANGE sparse slice middle-span deletion", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 10, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 20, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 30, "c").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 40, "d").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 50, "e").Err()).NotTo(HaveOccurred())

			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 20, End: 40}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(3)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(2)))

			val, err := client.ARGet(ctx, "myarray", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("a"))

			_, err = client.ARGet(ctx, "myarray", 20).Result()
			Expect(err).To(Equal(redis.Nil))

			val, err = client.ARGet(ctx, "myarray", 50).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("e"))
		})

		It("should ARDELRANGE sparse whole-slice deletion", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 10, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 20, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 30, "c").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 40, "d").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 50, "e").Err()).NotTo(HaveOccurred())

			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 0, End: 100}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(5)))

			exists, err := client.Exists(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(int64(0)))
		})

		It("should ARDELRANGE sparse no-hit range", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 10, "a").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 20, "b").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 30, "c").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 40, "d").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 50, "e").Err()).NotTo(HaveOccurred())

			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 11, End: 19}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(0)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})

		It("should ARDELRANGE delete entire slice then verify iteration", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Two slices
			for i := 0; i < 10; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("slice0_%d", i)).Err()).NotTo(HaveOccurred())
			}
			for i := 4096; i < 4106; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("slice1_%d", i)).Err()).NotTo(HaveOccurred())
			}

			// Delete entire first slice
			n, err := client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 0, End: 4095}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(uint64(10)))

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(10)))

			// ARSCAN should only find second slice elements
			result, err := client.ARScan(ctx, "myarray", 0, 5000, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(10))
			Expect(result[0].Index).To(Equal(uint64(4096)))
			Expect(result[0].Value).To(Equal("slice1_4096"))
		})
	})


	Describe("Circular buffer", func() {
		It("should ARRING wraparound with ARLASTITEMS", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create ring with 8 items, MOD 5
			for i := 0; i < 8; i++ {
				_, err := client.ARRing(ctx, "myarray", 5, fmt.Sprintf("%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			// Values: 0->3, 1->4, 2->5, 3->6, 4->7
			result, err := client.ARLastItems(ctx, "myarray", 3, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"5", "6", "7"}))

			result, err = client.ARLastItems(ctx, "myarray", 3, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"7", "6", "5"}))

			// Request more items than exist
			result, err = client.ARLastItems(ctx, "myarray", 10, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(5))
		})

		It("should ARLASTITEMS handle empty and partial cases", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Empty array
			result, err := client.ARLastItems(ctx, "myarray", 5, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty())

			// Fewer items than requested
			_, err = client.ARRing(ctx, "myarray", 10, "a").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARRing(ctx, "myarray", 10, "b").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARRing(ctx, "myarray", 10, "c").Result()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.ARLastItems(ctx, "myarray", 5, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"a", "b", "c"}))
		})

		It("should ARNEXT track correctly with ARRING", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			for i := 0; i < 7; i++ {
				expectedIdx := uint64(i % 4)
				idx, err := client.ARRing(ctx, "myarray", 4, fmt.Sprintf("%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(idx).To(Equal(expectedIdx))

				next, err := client.ARNext(ctx, "myarray").Result()
				Expect(err).NotTo(HaveOccurred())
				if i < 4 {
					Expect(next).To(Equal(uint64(i + 1)))
				} else {
					Expect(next).To(Equal(expectedIdx + 1))
				}
			}
		})

		It("should ARRING truncation when size decreases", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create ring buffer with MOD 10
			for i := 0; i < 15; i++ {
				_, err := client.ARRing(ctx, "myarray", 10, fmt.Sprintf("v%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(10)))

			// Use smaller MOD - truncates and inserts new value
			_, err = client.ARRing(ctx, "myarray", 5, "truncated").Result()
			Expect(err).NotTo(HaveOccurred())

			count, err = client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))

			// Verify values
			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("truncated"))

			val, err = client.ARGet(ctx, "myarray", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("v11"))

			// Positions 5-9 should be empty
			_, err = client.ARGet(ctx, "myarray", 5).Result()
			Expect(err).To(Equal(redis.Nil))
		})

		It("should ARRING shrink stop at first hole", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			for i := 0; i < 5; i++ {
				_, err := client.ARRing(ctx, "myarray", 5, fmt.Sprintf("v%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			_, err := client.ARDel(ctx, "myarray", 3).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err = client.ARRing(ctx, "myarray", 3, "new").Result()
			Expect(err).NotTo(HaveOccurred())

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(2)))

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("v4"))

			_, err = client.ARGet(ctx, "myarray", 2).Result()
			Expect(err).To(Equal(redis.Nil))
		})

		It("should ARSEEK followed by ARRING work", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			_, err := client.ARInsert(ctx, "myarray", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARInsert(ctx, "myarray", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ARInsert(ctx, "myarray", "c").Result()
			Expect(err).NotTo(HaveOccurred())

			// Seek to position 10
			_, err = client.ARSeek(ctx, "myarray", 10).Result()
			Expect(err).NotTo(HaveOccurred())

			next, err := client.ARNext(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(next).To(Equal(uint64(10)))

			// Now use MOD - should insert at index 0 (10 % 5 = 0)
			_, err = client.ARRing(ctx, "myarray", 5, "x").Result()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.ARGet(ctx, "myarray", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("x"))
		})

		It("should ARLASTITEMS reverse order work", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create ring with wraparound
			for i := 0; i < 12; i++ {
				_, err := client.ARRing(ctx, "myarray", 8, fmt.Sprintf("v%d", i)).Result()
				Expect(err).NotTo(HaveOccurred())
			}

			result, err := client.ARLastItems(ctx, "myarray", 4, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"v8", "v9", "v10", "v11"}))

			result, err = client.ARLastItems(ctx, "myarray", 4, true).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"v11", "v10", "v9", "v8"}))

			// Request all items
			result, err = client.ARLastItems(ctx, "myarray", 100, false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(8))
		})
	})


	Describe("Regression tests", func() {
		It("should AROP work on whole-number floats", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 0, "10.0").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 1, "20.0").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 2, "30.0").Err()).NotTo(HaveOccurred())

			// SUM
			cmd := client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpSum)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(60)))

			// MIN
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpMin)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(10)))

			// MAX
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpMax)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err = cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(30)))

			// MATCH
			cmd = client.AROp(ctx, "myarray", 0, 2, redis.ArrayOpMatch, "10.0")
			Expect(cmd.Err()).NotTo(HaveOccurred())
			intVal, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(intVal).To(Equal(int64(1)))
		})

		It("should ARDELRANGE with multiple ranges across slices", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Create elements across slice boundaries
			for i := 0; i < 10; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("%d", i)).Err()).NotTo(HaveOccurred())
			}
			for i := 4096; i < 4106; i++ {
				Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("%d", i)).Err()).NotTo(HaveOccurred())
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(20)))

			// Delete first slice partially
			_, err = client.ARDelRange(ctx, "myarray", redis.ARRange{Start: 5, End: 9}).Result()
			Expect(err).NotTo(HaveOccurred())

			// AROP SUM should work across slices
			cmd := client.AROp(ctx, "myarray", 0, 5000, redis.ArrayOpSum)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			val, err := cmd.Float64()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(float64(41015)))

			// AROP USED
			cmd = client.AROp(ctx, "myarray", 0, 5000, redis.ArrayOpUsed)
			Expect(cmd.Err()).NotTo(HaveOccurred())
			intVal, err := cmd.Int64()
			Expect(err).NotTo(HaveOccurred())
			Expect(intVal).To(Equal(int64(15)))
		})

		It("should ARSCAN work over superdir blocks", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Elements in different superdir blocks
			Expect(client.ARSet(ctx, "myarray", 0, "first").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 8388608, "second").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 16777216, "third").Err()).NotTo(HaveOccurred())

			// Scan entire range
			result, err := client.ARScan(ctx, "myarray", 0, 20000000, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(3))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 0, Value: "first"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 8388608, Value: "second"}))
			Expect(result[2]).To(Equal(redis.AREntry{Index: 16777216, Value: "third"}))

			// Reverse scan
			result, err = client.ARScan(ctx, "myarray", 20000000, 0, nil).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(3))
			Expect(result[0]).To(Equal(redis.AREntry{Index: 16777216, Value: "third"}))
			Expect(result[1]).To(Equal(redis.AREntry{Index: 8388608, Value: "second"}))
			Expect(result[2]).To(Equal(redis.AREntry{Index: 0, Value: "first"}))
		})

		It("should ARGETRANGE work across superdir slice boundary", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			Expect(client.ARSet(ctx, "myarray", 8388607, "left").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 8388608, "mid").Err()).NotTo(HaveOccurred())
			Expect(client.ARSet(ctx, "myarray", 8388609, "right").Err()).NotTo(HaveOccurred())

			result, err := client.ARGetRange(ctx, "myarray", 8388607, 8388609).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"left", "mid", "right"}))

			result, err = client.ARGetRange(ctx, "myarray", 8388609, 8388607).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal([]any{"right", "mid", "left"}))
		})

		It("should stress test mixed operations across multiple slices", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())
			sliceSize := 4096

			// Create elements across 5 slices
			for slice := 0; slice < 5; slice++ {
				base := slice * sliceSize
				for i := 0; i < 20; i++ {
					idx := uint64(base + i*50)
					Expect(client.ARSet(ctx, "myarray", idx, fmt.Sprintf("s%d_e%d", slice, i)).Err()).NotTo(HaveOccurred())
				}
			}

			count, err := client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(100)))

			// Delete half from each slice
			for slice := 0; slice < 5; slice++ {
				base := slice * sliceSize
				for i := 10; i < 20; i++ {
					_, err := client.ARDel(ctx, "myarray", uint64(base+i*50)).Result()
					Expect(err).NotTo(HaveOccurred())
				}
			}

			count, err = client.ARCount(ctx, "myarray").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(50)))

			// Verify remaining elements
			for slice := 0; slice < 5; slice++ {
				base := slice * sliceSize
				for i := 0; i < 10; i++ {
					idx := uint64(base + i*50)
					val, err := client.ARGet(ctx, "myarray", idx).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(val).To(Equal(fmt.Sprintf("s%d_e%d", slice, i)))
				}
			}
		})

		It("should stress test rapid insert/delete cycles", func() {
			Expect(client.Del(ctx, "myarray").Err()).NotTo(HaveOccurred())

			// Multiple cycles of growth and shrinkage
			for cycle := 0; cycle < 3; cycle++ {
				// Grow
				for i := 0; i < 100; i++ {
					Expect(client.ARSet(ctx, "myarray", uint64(i), fmt.Sprintf("cycle%d_%d", cycle, i)).Err()).NotTo(HaveOccurred())
				}

				count, err := client.ARCount(ctx, "myarray").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(uint64(100)))

				// Shrink (but leave some)
				for i := 10; i < 100; i++ {
					_, err := client.ARDel(ctx, "myarray", uint64(i)).Result()
					Expect(err).NotTo(HaveOccurred())
				}

				count, err = client.ARCount(ctx, "myarray").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(uint64(10)))
			}

			// Verify final state
			for i := 0; i < 10; i++ {
				val, err := client.ARGet(ctx, "myarray", uint64(i)).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(fmt.Sprintf("cycle2_%d", i)))
			}
		})
	})
})
