package redis_test

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
)

func expectNil(err error) {
	Expect(err).NotTo(HaveOccurred())
}

func expectTrue(t bool) {
	expectEqual(t, true)
}

func expectEqual[T any, U any](a T, b U) {
	Expect(a).To(BeEquivalentTo(b))
}

func generateRandomVector(dim int) redis.VectorValues {
	rand.Seed(time.Now().UnixNano())
	v := make([]float64, dim)
	for i := range v {
		v[i] = float64(rand.Intn(1000)) + rand.Float64()
	}
	return redis.VectorValues{Val: v}
}

var _ = Describe("Redis VectorSet commands", Label("vectorset"), func() {
	ctx := context.TODO()

	setupRedisClient := func(protocolVersion int) *redis.Client {
		return redis.NewClient(&redis.Options{
			Addr:          "localhost:6379",
			DB:            0,
			Protocol:      protocolVersion,
			UnstableResp3: true,
		})
	}

	protocols := []int{2, 3}
	for _, protocol := range protocols {
		protocol := protocol

		Context(fmt.Sprintf("with protocol version %d", protocol), func() {
			var client *redis.Client

			BeforeEach(func() {
				client = setupRedisClient(protocol)
				Expect(client.FlushAll(ctx).Err()).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				if client != nil {
					client.FlushDB(ctx)
					client.Close()
				}
			})

			It("basic", func() {
				SkipBeforeRedisVersion(8.0, "Redis 8.0 introduces support for VectorSet")
				vecName := "basic"
				val := &redis.VectorValues{
					Val: []float64{1.5, 2.4, 3.3, 4.2},
				}
				ok, err := client.VAdd(ctx, vecName, "k1", val).Result()
				expectNil(err)
				expectTrue(ok)

				fp32 := "\x8f\xc2\xf9\x3e\xcb\xbe\xe9\xbe\xb0\x1e\xca\x3f\x5e\x06\x9e\x3f"
				val2 := &redis.VectorFP32{
					Val: []byte(fp32),
				}
				ok, err = client.VAdd(ctx, vecName, "k2", val2).Result()
				expectNil(err)
				expectTrue(ok)

				dim, err := client.VDim(ctx, vecName).Result()
				expectNil(err)
				expectEqual(dim, 4)

				count, err := client.VCard(ctx, vecName).Result()
				expectNil(err)
				expectEqual(count, 2)

				ok, err = client.VRem(ctx, vecName, "k1").Result()
				expectNil(err)
				expectTrue(ok)

				count, err = client.VCard(ctx, vecName).Result()
				expectNil(err)
				expectEqual(count, 1)
			})

			It("basic similarity", func() {
				SkipBeforeRedisVersion(8.0, "Redis 8.0 introduces support for VectorSet")
				vecName := "basic_similarity"

				ok, err := client.VAdd(ctx, vecName, "k1", &redis.VectorValues{
					Val: []float64{1, 0, 0, 0},
				}).Result()
				expectNil(err)
				expectTrue(ok)
				ok, err = client.VAdd(ctx, vecName, "k2", &redis.VectorValues{
					Val: []float64{0.99, 0.01, 0, 0},
				}).Result()
				expectNil(err)
				expectTrue(ok)
				ok, err = client.VAdd(ctx, vecName, "k3", &redis.VectorValues{
					Val: []float64{0.1, 1, -1, 0.5},
				}).Result()
				expectNil(err)
				expectTrue(ok)

				sim, err := client.VSimWithScores(ctx, vecName, &redis.VectorValues{
					Val: []float64{1, 0, 0, 0},
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 3)
				simMap := make(map[string]float64)
				for _, vi := range sim {
					simMap[vi.Name] = vi.Score
				}
				expectTrue(simMap["k1"] > 0.99)
				expectTrue(simMap["k2"] > 0.99)
				expectTrue(simMap["k3"] < 0.8)
			})

			It("dimension operation", func() {
				SkipBeforeRedisVersion(8.0, "Redis 8.0 introduces support for VectorSet")
				vecName := "dimension_op"
				originalDim := 100
				reducedDim := 50

				v1 := generateRandomVector(originalDim)
				ok, err := client.VAddWithArgs(ctx, vecName, "k1", &v1, &redis.VAddArgs{
					Reduce: int64(reducedDim),
				}).Result()
				expectNil(err)
				expectTrue(ok)

				info, err := client.VInfo(ctx, vecName).Result()
				expectNil(err)
				dim := info["vector-dim"].(int64)
				oriDim := info["projection-input-dim"].(int64)
				expectEqual(dim, reducedDim)
				expectEqual(oriDim, originalDim)

				wrongDim := 80
				wrongV := generateRandomVector(wrongDim)
				_, err = client.VAddWithArgs(ctx, vecName, "kw", &wrongV, &redis.VAddArgs{
					Reduce: int64(reducedDim),
				}).Result()
				expectTrue(err != nil)

				v2 := generateRandomVector(originalDim)
				ok, err = client.VAddWithArgs(ctx, vecName, "k2", &v2, &redis.VAddArgs{
					Reduce: int64(reducedDim),
				}).Result()
				expectNil(err)
				expectTrue(ok)
			})

			It("remove", func() {
				SkipBeforeRedisVersion(8.0, "Redis 8.0 introduces support for VectorSet")
				vecName := "remove"
				v1 := generateRandomVector(5)
				ok, err := client.VAdd(ctx, vecName, "k1", &v1).Result()
				expectNil(err)
				expectTrue(ok)

				exist, err := client.Exists(ctx, vecName).Result()
				expectNil(err)
				expectEqual(exist, 1)

				ok, err = client.VRem(ctx, vecName, "k1").Result()
				expectNil(err)
				expectTrue(ok)

				exist, err = client.Exists(ctx, vecName).Result()
				expectNil(err)
				expectEqual(exist, 0)
			})

			It("all operations", func() {
				SkipBeforeRedisVersion(8.0, "Redis 8.0 introduces support for VectorSet")
				vecName := "commands"
				vals := []struct {
					name string
					v    redis.VectorValues
					attr string
				}{
					{
						name: "k0",
						v:    redis.VectorValues{Val: []float64{1, 0, 0, 0}},
						attr: `{"age": 25, "name": "Alice", "active": true, "scores": [85, 90, 95], "city": "New York"}`,
					},
					{
						name: "k1",
						v:    redis.VectorValues{Val: []float64{0, 1, 0, 0}},
						attr: `{"age": 30, "name": "Bob", "active": false, "scores": [70, 75, 80], "city": "Boston"}`,
					},
					{
						name: "k2",
						v:    redis.VectorValues{Val: []float64{0, 0, 1, 0}},
						attr: `{"age": 35, "name": "Charlie", "scores": [60, 65, 70], "city": "Seattle"}`,
					},
					{
						name: "k3",
						v:    redis.VectorValues{Val: []float64{0, 0, 0, 1}},
					},
					{
						name: "k4",
						v:    redis.VectorValues{Val: []float64{0.5, 0.5, 0, 0}},
						attr: `invalid json`,
					},
				}

				// If the key doesn't exist, return null error
				_, err := client.VRandMember(ctx, vecName).Result()
				expectEqual(err.Error(), proto.Nil.Error())

				// If the key doesn't exist, return an empty array
				res, err := client.VRandMemberCount(ctx, vecName, 3).Result()
				expectNil(err)
				expectEqual(len(res), 0)

				for _, v := range vals {
					ok, err := client.VAdd(ctx, vecName, v.name, &v.v).Result()
					expectNil(err)
					expectTrue(ok)
					if len(v.attr) > 0 {
						ok, err = client.VSetAttr(ctx, vecName, v.name, v.attr).Result()
						expectNil(err)
						expectTrue(ok)
					}
				}

				// VGetAttr
				attr, err := client.VGetAttr(ctx, vecName, vals[1].name).Result()
				expectNil(err)
				expectEqual(attr, vals[1].attr)

				// VRandMember
				_, err = client.VRandMember(ctx, vecName).Result()
				expectNil(err)

				res, err = client.VRandMemberCount(ctx, vecName, 3).Result()
				expectNil(err)
				expectEqual(len(res), 3)

				res, err = client.VRandMemberCount(ctx, vecName, 10).Result()
				expectNil(err)
				expectEqual(len(res), len(vals))

				// test equality
				sim, err := client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.age == 25`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 1)
				expectEqual(sim[0], vals[0].name)

				// test greater than
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.age > 25`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 2)

				// test less than or equal
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.age <= 30`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 2)

				// test string equality
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.name == "Alice"`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 1)
				expectEqual(sim[0], vals[0].name)

				// test string inequality
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.name != "Alice"`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 2)

				// test bool
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.active`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 1)
				expectEqual(sim[0], vals[0].name)

				// test logical add
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.age > 20 and .age < 30`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 1)
				expectEqual(sim[0], vals[0].name)

				// test logical or
				sim, err = client.VSimWithArgs(ctx, vecName, &vals[0].v, &redis.VSimArgs{
					Filter: `.age < 30 or .age > 35`,
				}).Result()
				expectNil(err)
				expectEqual(len(sim), 1)
				expectEqual(sim[0], vals[0].name)
			})
		})
	}
})
