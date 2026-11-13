package redis_test

import (
	"context"
	"strconv"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

// ZMOVE, BZMOVE and ZMOVEMEMBER (Redis 8.12). Expected replies follow the
// client HLD: a list of pairs, or redis.Nil when nothing moved.
var _ = Describe("ZMove", Label("zset"), func() {
	ctx := context.TODO()
	// client is the subject under test (raw client or an autopipeliner, per
	// GOREDIS_TEST_SUBJECT); rawClient does setup.
	var client redis.UniversalClient
	var rawClient *redis.Client

	zs := func(pairs ...interface{}) []redis.Z {
		out := make([]redis.Z, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, redis.Z{Member: pairs[i], Score: float64(pairs[i+1].(int))})
		}
		return out
	}
	seed := func(key string, z ...redis.Z) {
		Expect(client.ZAdd(ctx, key, z...).Err()).NotTo(HaveOccurred())
	}
	members := func(key string) []redis.Z {
		v, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		return v
	}

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.12", "ZMOVE needs Redis 8.12")
		rawClient = redis.NewClient(redisOptions())
		var closeSubject func() error
		client, closeSubject = newUniversalSubject(rawClient)
		Expect(rawClient.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(closeSubject()).NotTo(HaveOccurred()) })
		seed("{z}src", zs("a", 1, "b", 2, "c", 3, "d", 4)...)
	})

	Describe("ZMove and ZMoveCount", func() {
		It("should move one member as a list of one pair", func() {
			res, err := client.ZMove(ctx, "{z}src", "{z}dst", "MIN").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("a", 1)))
			Expect(members("{z}dst")).To(Equal(zs("a", 1)))
		})

		It("should move from the high end in descending order", func() {
			res, err := client.ZMoveCount(ctx, "{z}src", "{z}dst", "MAX", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("d", 4, "c", 3)))
			Expect(members("{z}src")).To(Equal(zs("a", 1, "b", 2)))
		})

		It("should move all members for a large count and delete the source", func() {
			res, err := client.ZMoveCount(ctx, "{z}src", "{z}dst", "MIN", 100).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("a", 1, "b", 2, "c", 3, "d", 4)))
			Expect(client.Exists(ctx, "{z}src").Val()).To(Equal(int64(0)))
		})

		It("should overwrite the destination score", func() {
			seed("{z}dst", redis.Z{Member: "a", Score: 50})
			Expect(client.ZMove(ctx, "{z}src", "{z}dst", "MIN").Err()).NotTo(HaveOccurred())
			Expect(members("{z}dst")).To(Equal(zs("a", 1)))
		})

		It("should return redis.Nil when nothing moved", func() {
			Expect(client.ZMove(ctx, "{z}missing", "{z}dst", "MIN").Err()).To(Equal(redis.Nil))
			Expect(client.ZMove(ctx, "{z}src", "{z}src", "MIN").Err()).To(Equal(redis.Nil))
			Expect(members("{z}src")).To(HaveLen(4))
		})

		It("should pass the count through to the server", func() {
			err := client.ZMoveCount(ctx, "{z}src", "{z}dst", "MIN", 0).Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("count should be greater than 0"))
			Expect(members("{z}src")).To(HaveLen(4))
		})
	})

	Describe("ZMoveByScore", func() {
		It("should move the whole range without a count", func() {
			res, err := client.ZMoveByScore(ctx, "{z}src", "{z}dst", "2", "+inf").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("b", 2, "c", 3, "d", 4)))
		})

		It("should accept exclusive and infinite bounds", func() {
			res, err := client.ZMoveByScore(ctx, "{z}src", "{z}dst", "-inf", "(3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("a", 1, "b", 2)))
		})

		It("should take the lowest scores of the range with a count", func() {
			res, err := client.ZMoveByScoreCount(ctx, "{z}src", "{z}dst", "-inf", "+inf", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("a", 1, "b", 2)))
		})

		It("should return redis.Nil for an empty range", func() {
			Expect(client.ZMoveByScore(ctx, "{z}src", "{z}dst", "10", "20").Err()).To(Equal(redis.Nil))
		})

		It("should pass a bad bound through to the server", func() {
			err := client.ZMoveByScore(ctx, "{z}src", "{z}dst", "nan", "2").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("min or max is not a float"))
		})
	})

	Describe("BZMove", func() {
		It("should move at once when the source has members", func() {
			res, err := client.BZMoveCount(ctx, "{z}src", "{z}dst", "MIN", time.Second, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("a", 1, "b", 2)))
		})

		It("should keep the fraction of a second in the timeout", func() {
			start := time.Now()
			err := client.BZMove(ctx, "{z}empty", "{z}dst", "MIN", 100*time.Millisecond).Err()
			Expect(err).To(Equal(redis.Nil))
			Expect(time.Since(start)).To(BeNumerically("<", 900*time.Millisecond))
		})

		It("should wake on the first member and not wait for a full batch", func() {
			done := make(chan []redis.Z, 1)
			errs := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				res, err := client.BZMoveCount(ctx, "{z}empty", "{z}dst", "MIN", 5*time.Second, 10).Result()
				errs <- err
				done <- res
			}()

			pusher := redis.NewClient(redisOptions())
			defer pusher.Close()
			Eventually(func() int64 { return blockedClients(ctx, pusher) }, "3s", "20ms").
				Should(BeNumerically(">=", 1))
			Expect(pusher.ZAdd(ctx, "{z}empty", redis.Z{Member: "x", Score: 7}).Err()).NotTo(HaveOccurred())

			Eventually(errs, "5s").Should(Receive(BeNil()))
			Expect(<-done).To(Equal(zs("x", 7)))
		})
	})

	Describe("ZMoveMember", func() {
		It("should reply in argument order, skip missing members and move a duplicate once", func() {
			res, err := client.ZMoveMember(ctx, "{z}src", "{z}dst", "c", "missing", "a", "c").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("c", 3, "a", 1)))
			Expect(members("{z}src")).To(Equal(zs("b", 2, "d", 4)))
		})

		It("should treat keyword-named members as data", func() {
			seed("{z}src", zs("NX", 10, "MEMBERS", 11)...)
			res, err := client.ZMoveMember(ctx, "{z}src", "{z}dst", "NX", "MEMBERS").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("NX", 10, "MEMBERS", 11)))
		})

		It("should move only members the destination lacks with NX", func() {
			seed("{z}dst", redis.Z{Member: "a", Score: 50})
			res, err := client.ZMoveMemberWithArgs(ctx, "{z}src", "{z}dst", redis.ZMoveMemberNX, "a", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("b", 2)))
			Expect(client.ZScore(ctx, "{z}dst", "a").Val()).To(Equal(float64(50)))
			Expect(client.ZScore(ctx, "{z}src", "a").Val()).To(Equal(float64(1)))
		})

		It("should move only members the destination has with XX", func() {
			seed("{z}dst", redis.Z{Member: "a", Score: 50})
			res, err := client.ZMoveMemberWithArgs(ctx, "{z}src", "{z}dst", redis.ZMoveMemberXX, "a", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(zs("a", 1)))
		})

		It("should return redis.Nil when no member moved", func() {
			err := client.ZMoveMemberWithArgs(ctx, "{z}src", "{z}new", redis.ZMoveMemberXX, "a").Err()
			Expect(err).To(Equal(redis.Nil))
			Expect(client.Exists(ctx, "{z}new").Val()).To(Equal(int64(0)))
		})

		It("should send an empty member list to the server", func() {
			Expect(client.ZMoveMember(ctx, "{z}src", "{z}dst").Err()).To(HaveOccurred())
		})
	})

	It("should return WRONGTYPE for a non-sorted-set destination", func() {
		Expect(client.Set(ctx, "{z}str", "x", 0).Err()).NotTo(HaveOccurred())
		err := client.ZMove(ctx, "{z}src", "{z}str", "MIN").Err()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("WRONGTYPE"))
		Expect(members("{z}src")).To(HaveLen(4))
	})

	It("should return the same result on RESP2 and RESP3", func() {
		for _, proto := range []int{2, 3} {
			opts := redisOptions()
			opts.Protocol = proto
			c := redis.NewClient(opts)
			Expect(c.Del(ctx, "{z}p", "{z}q").Err()).NotTo(HaveOccurred())
			Expect(c.ZAdd(ctx, "{z}p", zs("a", 1, "b", 2)...).Err()).NotTo(HaveOccurred())

			res, err := c.ZMoveCount(ctx, "{z}p", "{z}q", "MIN", 2).Result()
			Expect(err).NotTo(HaveOccurred(), "RESP%d", proto)
			Expect(res).To(Equal(zs("a", 1, "b", 2)), "RESP%d", proto)
			Expect(c.ZMove(ctx, "{z}p", "{z}q", "MIN").Err()).To(Equal(redis.Nil), "RESP%d", proto)
			Expect(c.Close()).NotTo(HaveOccurred())
		}
	})

	It("should work in pipelines and transactions", func() {
		var one, nothing *redis.ZSliceCmd
		_, err := client.TxPipelined(ctx, func(p redis.Pipeliner) error {
			one = p.ZMove(ctx, "{z}src", "{z}dst", "MIN")
			p.ZMoveMember(ctx, "{z}src", "{z}dst", "c")
			nothing = p.ZMove(ctx, "{z}missing", "{z}dst", "MIN")
			return nil
		})
		Expect(err).To(Equal(redis.Nil))
		Expect(one.Val()).To(Equal(zs("a", 1)))
		Expect(nothing.Err()).To(Equal(redis.Nil))
		Expect(members("{z}dst")).To(Equal(zs("a", 1, "c", 3)))
	})
})

// blockedClients reads blocked_clients from INFO clients.
func blockedClients(ctx context.Context, c *redis.Client) int64 {
	info, err := c.InfoMap(ctx, "clients").Result()
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(info["Clients"]["blocked_clients"], 10, 64)
	return v
}
