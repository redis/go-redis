package redis_test

import (
	"context"
	"fmt"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Probabilistic commands", Label("probabilistic"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("bloom", Label("bloom"), func() {
		It("should BFAdd", Label("bloom", "bfadd"), func() {
			resultAdd, err := client.BFAdd(ctx, "testbf1", 1).Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultAdd).To(Equal(int64(1)))

			resultInfo, err := client.BFInfo(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(resultInfo["Number of items inserted"]).To(BeEquivalentTo(int64(1)))
		})

		It("should BFCard", Label("bloom", "bfcard"), func() {
			// This is a probabilistic data structure, and it's not always guaranteed that we will get back
			// the exact number of inserted items, during hash collisions
			// But with such a low number of items (only 3),
			// the probability of a collision is very low, so we can expect to get back the exact number of items
			_, err := client.BFAdd(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.BFAdd(ctx, "testbf1", "item2").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.BFAdd(ctx, "testbf1", 3).Result()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFCard(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEquivalentTo(int64(3)))
		})

		It("should BFExists", Label("bloom", "bfexists"), func() {
			exists, err := client.BFExists(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(int64(0)))

			_, err = client.BFAdd(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())

			exists, err = client.BFExists(ctx, "testbf1", "item1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeEquivalentTo(int64(1)))
		})

		It("should BFInfo and BFReserve", Label("bloom", "bfinfo", "bfreserve"), func() {
			err := client.BFReserve(ctx, "testbf1", 0.001, 2000).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFInfo(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(int64(2000)))
		})

		It("should BFInsert", Label("bloom", "bfinsert"), func() {
			options := &redis.BFReserveOptions{
				Capacity:   2000,
				Error:      0.001,
				Expansion:  3,
				NonScaling: false,
			}
			err := client.BFInsert(ctx, "testbf1", options, "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			exists, err := client.BFExists(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeEquivalentTo(int64(1)))

			result, err := client.BFInfo(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(int64(2000)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(int64(3)))
		})

		It("should BFMAdd", Label("bloom", "bfmadd"), func() {
			resultAdd, err := client.BFMAdd(ctx, "testbf1", "item1", "item2", "item3").Result()
			fmt.Printf("resultAdd: %v\n", resultAdd)

			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultAdd)).To(Equal(int(3)))

			resultInfo, err := client.BFInfo(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(resultInfo["Number of items inserted"]).To(BeEquivalentTo(int64(3)))
		})

		It("should BFMExists", Label("bloom", "bfmexists"), func() {
			exist, err := client.BFMExists(ctx, "testbf1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(exist)).To(Equal(int(3)))
			Expect(exist[0]).To(Equal(int64(0)))
			Expect(exist[1]).To(Equal(int64(0)))
			Expect(exist[2]).To(Equal(int64(0)))

			_, err = client.BFMAdd(ctx, "testbf1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())

			exist, err = client.BFMExists(ctx, "testbf1", "item1", "item2", "item3", "item4").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(len(exist)).To(Equal(int(4)))
			Expect(exist[0]).To(Equal(int64(1)))
			Expect(exist[1]).To(Equal(int64(1)))
			Expect(exist[2]).To(Equal(int64(1)))
			Expect(exist[3]).To(Equal(int64(0)))
		})

		It("should BFReserveExpansion", Label("bloom", "bfreserveexpansion"), func() {
			err := client.BFReserveExpansion(ctx, "testbf1", 0.001, 2000, 3).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFInfo(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(int64(2000)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(int64(3)))
		})

		It("should BFReserveArgs", Label("bloom", "bfreserveargs"), func() {
			options := &redis.BFReserveOptions{
				Capacity:   2000,
				Error:      0.001,
				Expansion:  3,
				NonScaling: false,
			}
			err := client.BFReserveArgs(ctx, "testbf", options).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFInfo(ctx, "testbf").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(int64(2000)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(int64(3)))
		})
	})

	Describe("cuckoo", Label("cuckoo"), func() {
		It("should CFAdd", Label("cuckoo", "cfadd"), func() {
			err := client.CFAdd(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			exists, err := client.CFExists(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeEquivalentTo(int64(1)))

			info, err := client.CFInfo(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(info["Number of items inserted"]).To(BeEquivalentTo(int64(1)))
		})

		It("should CFAddNX", Label("cuckoo", "cfaddnx"), func() {
			err := client.CFAddNX(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			exists, err := client.CFExists(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeEquivalentTo(int64(1)))

			result, err := client.CFAddNX(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEquivalentTo(int64(0)))

			info, err := client.CFInfo(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(info["Number of items inserted"]).To(BeEquivalentTo(int64(1)))
		})

		It("should CFInCFCountsert", Label("cuckoo", "cfinsert"), func() {
			err := client.CFAdd(ctx, "testcf1", "item1").Err()
			cnt, err := client.CFCount(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(BeEquivalentTo(int64(1)))

			err = client.CFAdd(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			cnt, err = client.CFCount(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(BeEquivalentTo(int64(2)))
		})

		It("should CFDel and CFExists", Label("cuckoo", "cfdel", "cfexists"), func() {
			err := client.CFAdd(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			exists, err := client.CFExists(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeEquivalentTo(int64(1)))

			err = client.CFDel(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			exists, err = client.CFExists(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeEquivalentTo(int64(0)))
		})

		It("should CFInfo and CFReserve", Label("cuckoo", "cfinfo", "cfreserve"), func() {
			err := client.CFReserve(ctx, "testcf1", 1000).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CFInfo(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]int64{}))
		})

		It("should CFInfo and CFReserveArgs", Label("cuckoo", "cfinfo", "cfreserveargs"), func() {
			args := &redis.CFReserveOptions{
				Capacity:      2048,
				BucketSize:    3,
				MaxIterations: 15,
				Expansion:     2,
			}

			err := client.CFReserveArgs(ctx, "testcf1", args).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CFInfo(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(result["Bucket size"]).To(BeEquivalentTo(int64(3)))
			Expect(result["Max iterations"]).To(BeEquivalentTo(int64(15)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(int64(2)))
		})

		It("should CFInsert", Label("cuckoo", "cfinsert"), func() {
			args := &redis.CFInsertOptions{
				Capacity: 3000,
				NoCreate: true,
			}

			result, err := client.CFInsert(ctx, "testcf1", args, "item1", "item2", "item3").Result()
			Expect(err).To(HaveOccurred())

			args = &redis.CFInsertOptions{
				Capacity: 3000,
				NoCreate: false,
			}

			result, err = client.CFInsert(ctx, "testcf1", args, "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
		})

		It("should CFInsertNx", Label("cuckoo", "cfinsertnx"), func() {
			args := &redis.CFInsertOptions{
				Capacity: 3000,
				NoCreate: true,
			}

			result, err := client.CFInsertNx(ctx, "testcf1", args, "item1", "item2", "item2").Result()
			Expect(err).To(HaveOccurred())

			args = &redis.CFInsertOptions{
				Capacity: 3000,
				NoCreate: false,
			}

			result, err = client.CFInsertNx(ctx, "testcf2", args, "item1", "item2", "item2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(1)))
			Expect(result[2]).To(BeEquivalentTo(int64(0)))
		})

		It("should CFMexists", Label("cuckoo", "cfmexists"), func() {
			err := client.CFInsert(ctx, "testcf1", nil, "item1", "item2", "item3").Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CFMExists(ctx, "testcf1", "item1", "item2", "item3", "item4").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(4))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(1)))
			Expect(result[2]).To(BeEquivalentTo(int64(1)))
			Expect(result[3]).To(BeEquivalentTo(int64(0)))
		})

	})

	FDescribe("CMS", Label("cms"), func() {
		It("should CMSIncrBy", Label("cms", "cmsincrby"), func() {
			err := client.CMSInitByDim(ctx, "testcms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CMSIncrBy(ctx, "testcms1", "item1", 1, "item2", 2, "item3", 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(2)))
			Expect(result[2]).To(BeEquivalentTo(int64(3)))

		})

		It("should CMSInitByDim and CMSInfo", Label("cms", "cmsinitbydim", "cmsinfo"), func() {
			err := client.CMSInitByDim(ctx, "testcms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.CMSInfo(ctx, "testcms1").Result()
			Expect(err).NotTo(HaveOccurred())

			fmt.Println()
			fmt.Println()
			fmt.Println(info)

			Expect(info).To(BeAssignableToTypeOf(map[string]int64{}))
			Expect(info["width"]).To(BeEquivalentTo(int64(5)))
			Expect(info["depth"]).To(BeEquivalentTo(int64(10)))
		})

		It("should CMSInitByProb", Label("cms", "cmsinitbyprob"), func() {
			err := client.CMSInitByProb(ctx, "testcms1", 0.002, 0.01).Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.CMSInfo(ctx, "testcms1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(BeAssignableToTypeOf(map[string]int64{}))
		})

		It("should CMSMerge, CMSMergeWithWeight and CMSQuery", Label("cms", "cmsmerge", "cmsquery"), func() {
			err := client.CMSMerge(ctx, "destCms1", "testcms2", "testcms3").Err()
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("CMS: key does not exist"))

			err = client.CMSInitByDim(ctx, "destCms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSInitByDim(ctx, "destCms2", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSInitByDim(ctx, "cms1", 2, 20).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSInitByDim(ctx, "cms2", 3, 20).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.CMSMerge(ctx, "destCms1", "cms1", "cms2").Err()
			Expect(err).To(MatchError("CMS: width/depth is not equal"))

			client.Del(ctx, "cms1", "cms2")

			err = client.CMSInitByDim(ctx, "cms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSInitByDim(ctx, "cms2", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			client.CMSIncrBy(ctx, "cms1", "item1", 1, "item2", 2)
			client.CMSIncrBy(ctx, "cms2", "item2", 2, "item3", 3)

			err = client.CMSMerge(ctx, "destCms1", "cms1", "cms2").Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CMSQuery(ctx, "destCms1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(4)))
			Expect(result[2]).To(BeEquivalentTo(int64(3)))

			sourceSketches := map[string]int{
				"cms1": 1,
				"cms2": 2,
			}
			err = client.CMSMergeWithWeight(ctx, "destCms2", sourceSketches).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.CMSQuery(ctx, "destCms2", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(6)))
			Expect(result[2]).To(BeEquivalentTo(int64(6)))

		})

	})
})
