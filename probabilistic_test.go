package redis_test

import (
	"context"
	"fmt"
	"math"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Probabilistic commands", Label("probabilistic"), func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379"})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("bloom", Label("bloom"), func() {
		It("should BFADD", Label("bloom", "bfadd"), func() {
			resultAdd, err := client.BFADD(ctx, "testbf1", 1).Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultAdd).To(BeTrue())

			resultInfo, err := client.BFINFO(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo).To(BeAssignableToTypeOf(redis.BFINFO{}))
			Expect(resultInfo.ItemsInserted).To(BeEquivalentTo(int64(1)))
		})

		It("should BFCARD", Label("bloom", "bfcard"), func() {
			// This is a probabilistic data structure, and it's not always guaranteed that we will get back
			// the exact number of inserted items, during hash collisions
			// But with such a low number of items (only 3),
			// the probability of a collision is very low, so we can expect to get back the exact number of items
			_, err := client.BFADD(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.BFADD(ctx, "testbf1", "item2").Result()
			Expect(err).NotTo(HaveOccurred())
			_, err = client.BFADD(ctx, "testbf1", 3).Result()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFCARD(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEquivalentTo(int64(3)))
		})

		It("should BFEXISTS", Label("bloom", "bfexists"), func() {
			exists, err := client.BFEXISTS(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())

			_, err = client.BFADD(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())

			exists, err = client.BFEXISTS(ctx, "testbf1", "item1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("should BFINFO and BFRESERVE", Label("bloom", "bfinfo", "bfreserve"), func() {
			err := client.BFRESERVE(ctx, "testbf1", 0.001, 2000).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFINFO(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(redis.BFINFO{}))
			Expect(result.Capacity).To(BeEquivalentTo(int64(2000)))
		})

		It("should BFINFOCAPACITY, BFINFOSIZE, BFINFOFILTERS, BFINFOITEMS, BFINFOEXPANSION, ", Label("bloom", "bfinfocapacity", "bfinfosize", "bfinfofilters", "bfinfoitems", "bfinfoexpansion"), func() {
			err := client.BFRESERVE(ctx, "testbf1", 0.001, 2000).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFINFOCAPACITY(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Capacity).To(BeEquivalentTo(int64(2000)))

			result, err = client.BFINFOITEMS(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.ItemsInserted).To(BeEquivalentTo(int64(0)))

			result, err = client.BFINFOSIZE(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size).To(BeEquivalentTo(int64(4056)))

			err = client.BFRESERVEEXPANSION(ctx, "testbf2", 0.001, 2000, 3).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.BFINFOFILTERS(ctx, "testbf2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Filters).To(BeEquivalentTo(int64(1)))

			result, err = client.BFINFOEXPANSION(ctx, "testbf2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.ExpansionRate).To(BeEquivalentTo(int64(3)))
		})

		It("should BFINSERT", Label("bloom", "bfinsert"), func() {
			options := &redis.BFINSERTOptions{
				Capacity:   2000,
				Error:      0.001,
				Expansion:  3,
				NonScaling: false,
				NoCreate:   true,
			}

			resultInsert, err := client.BFINSERT(ctx, "testbf1", options, "item1").Result()
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("ERR not found"))

			options = &redis.BFINSERTOptions{
				Capacity:   2000,
				Error:      0.001,
				Expansion:  3,
				NonScaling: false,
				NoCreate:   false,
			}

			resultInsert, err = client.BFINSERT(ctx, "testbf1", options, "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultInsert)).To(BeEquivalentTo(1))

			exists, err := client.BFEXISTS(ctx, "testbf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			result, err := client.BFINFO(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(redis.BFINFO{}))
			Expect(result.Capacity).To(BeEquivalentTo(int64(2000)))
			Expect(result.ExpansionRate).To(BeEquivalentTo(int64(3)))
		})

		It("should BFMADD", Label("bloom", "bfmadd"), func() {
			resultAdd, err := client.BFMADD(ctx, "testbf1", "item1", "item2", "item3").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultAdd)).To(Equal(3))

			resultInfo, err := client.BFINFO(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo).To(BeAssignableToTypeOf(redis.BFINFO{}))
			Expect(resultInfo.ItemsInserted).To(BeEquivalentTo(int64(3)))
			resultAdd2, err := client.BFMADD(ctx, "testbf1", "item1", "item2", "item4").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resultAdd2[0]).To(BeFalse())
			Expect(resultAdd2[1]).To(BeFalse())
			Expect(resultAdd2[2]).To(BeTrue())

		})

		It("should BFMEXISTS", Label("bloom", "bfmexists"), func() {
			exist, err := client.BFMEXISTS(ctx, "testbf1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(exist)).To(Equal(3))
			Expect(exist[0]).To(BeFalse())
			Expect(exist[1]).To(BeFalse())
			Expect(exist[2]).To(BeFalse())

			_, err = client.BFMADD(ctx, "testbf1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())

			exist, err = client.BFMEXISTS(ctx, "testbf1", "item1", "item2", "item3", "item4").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(len(exist)).To(Equal(4))
			Expect(exist[0]).To(BeTrue())
			Expect(exist[1]).To(BeTrue())
			Expect(exist[2]).To(BeTrue())
			Expect(exist[3]).To(BeFalse())
		})

		It("should BFRESERVEEXPANSION", Label("bloom", "bfreserveexpansion"), func() {
			err := client.BFRESERVEEXPANSION(ctx, "testbf1", 0.001, 2000, 3).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFINFO(ctx, "testbf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(redis.BFINFO{}))
			Expect(result.Capacity).To(BeEquivalentTo(int64(2000)))
			Expect(result.ExpansionRate).To(BeEquivalentTo(int64(3)))
		})

		It("should BFRESERVENONSCALING", Label("bloom", "bfreservenonscaling"), func() {
			err := client.BFRESERVENONSCALING(ctx, "testbfns1", 0.001, 1000).Err()
			Expect(err).NotTo(HaveOccurred())

			_, err = client.BFINFO(ctx, "testbfns1").Result()
			Expect(err).To(HaveOccurred())
		})

		It("should BFSCANDUMP and BFLOADCHUNK", Label("bloom", "bfscandump", "bfloadchunk"), func() {
			err := client.BFRESERVE(ctx, "testbfsd1", 0.001, 3000).Err()
			Expect(err).NotTo(HaveOccurred())
			for i := 0; i < 1000; i++ {
				client.BFADD(ctx, "testbfsd1", i)
			}
			infBefore := client.BFINFOSIZE(ctx, "testbfsd1")
			fd := []redis.ScanDump{}
			sd, err := client.BFSCANDUMP(ctx, "testbfsd1", 0).Result()
			for {
				if sd.Iter == 0 {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				fd = append(fd, sd)
				sd, err = client.BFSCANDUMP(ctx, "testbfsd1", sd.Iter).Result()
			}
			client.Del(ctx, "testbfsd1")
			for _, e := range fd {
				client.BFLOADCHUNK(ctx, "testbfsd1", e.Iter, e.Data)
			}
			infAfter := client.BFINFOSIZE(ctx, "testbfsd1")
			Expect(infBefore).To(BeEquivalentTo(infAfter))
		})

		It("should BFRESERVEARGS", Label("bloom", "bfreserveargs"), func() {
			options := &redis.BFRESERVEOptions{
				Capacity:   2000,
				Error:      0.001,
				Expansion:  3,
				NonScaling: false,
			}
			err := client.BFRESERVEARGS(ctx, "testbf", options).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.BFINFO(ctx, "testbf").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(redis.BFINFO{}))
			Expect(result.Capacity).To(BeEquivalentTo(int64(2000)))
			Expect(result.ExpansionRate).To(BeEquivalentTo(int64(3)))
		})
	})

	Describe("cuckoo", Label("cuckoo"), func() {
		It("should CFADD", Label("cuckoo", "cfadd"), func() {
			add, err := client.CFADD(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(add).To(BeTrue())

			exists, err := client.CFEXISTS(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			info, err := client.CFINFO(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(BeAssignableToTypeOf(redis.CFINFO{}))
			Expect(info.NumItemsInserted).To(BeEquivalentTo(int64(1)))
		})

		It("should CFADDNX", Label("cuckoo", "cfaddnx"), func() {
			add, err := client.CFADDNX(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(add).To(BeTrue())

			exists, err := client.CFEXISTS(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			result, err := client.CFADDNX(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeFalse())

			info, err := client.CFINFO(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(BeAssignableToTypeOf(redis.CFINFO{}))
			Expect(info.NumItemsInserted).To(BeEquivalentTo(int64(1)))
		})

		It("should CFCOUNT", Label("cuckoo", "cfcount"), func() {
			err := client.CFADD(ctx, "testcf1", "item1").Err()
			cnt, err := client.CFCOUNT(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(BeEquivalentTo(int64(1)))

			err = client.CFADD(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			cnt, err = client.CFCOUNT(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(BeEquivalentTo(int64(2)))
		})

		It("should CFDEL and CFEXISTS", Label("cuckoo", "cfdel", "cfexists"), func() {
			err := client.CFADD(ctx, "testcf1", "item1").Err()
			Expect(err).NotTo(HaveOccurred())

			exists, err := client.CFEXISTS(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			del, err := client.CFDEL(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(del).To(BeTrue())

			exists, err = client.CFEXISTS(ctx, "testcf1", "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})

		It("should CFINFO and CFRESERVE", Label("cuckoo", "cfinfo", "cfreserve"), func() {
			err := client.CFRESERVE(ctx, "testcf1", 1000).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CFRESERVEEXPANSION(ctx, "testcfe1", 1000, 1).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CFRESERVEBUCKETSIZE(ctx, "testcfbs1", 1000, 4).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CFRESERVEMAXITERATIONS(ctx, "testcfmi1", 1000, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CFINFO(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(redis.CFINFO{}))
		})

		It("should CFSCANDUMP and CFLOADCHUNK", Label("bloom", "cfscandump", "cfloadchunk"), func() {
			err := client.CFRESERVE(ctx, "testcfsd1", 1000).Err()
			Expect(err).NotTo(HaveOccurred())
			for i := 0; i < 1000; i++ {
				Item := fmt.Sprintf("item%d", i)
				client.CFADD(ctx, "testcfsd1", Item)
			}
			infBefore := client.CFINFO(ctx, "testcfsd1")
			fd := []redis.ScanDump{}
			sd, err := client.CFSCANDUMP(ctx, "testcfsd1", 0).Result()
			for {
				if sd.Iter == 0 {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				fd = append(fd, sd)
				sd, err = client.CFSCANDUMP(ctx, "testcfsd1", sd.Iter).Result()
			}
			client.Del(ctx, "testcfsd1")
			for _, e := range fd {
				client.CFLOADCHUNK(ctx, "testcfsd1", e.Iter, e.Data)
			}
			infAfter := client.CFINFO(ctx, "testcfsd1")
			Expect(infBefore).To(BeEquivalentTo(infAfter))
		})

		It("should CFINFO and CFRESERVEARGS", Label("cuckoo", "cfinfo", "cfreserveargs"), func() {
			args := &redis.CFRESERVEOptions{
				Capacity:      2048,
				BucketSize:    3,
				MaxIterations: 15,
				Expansion:     2,
			}

			err := client.CFRESERVEARGS(ctx, "testcf1", args).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CFINFO(ctx, "testcf1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeAssignableToTypeOf(redis.CFINFO{}))
			Expect(result.BucketSize).To(BeEquivalentTo(int64(3)))
			Expect(result.MaxIteration).To(BeEquivalentTo(int64(15)))
			Expect(result.ExpansionRate).To(BeEquivalentTo(int64(2)))
		})

		It("should CFINSERT", Label("cuckoo", "cfinsert"), func() {
			args := &redis.CFINSERTOptions{
				Capacity: 3000,
				NoCreate: true,
			}

			result, err := client.CFINSERT(ctx, "testcf1", args, "item1", "item2", "item3").Result()
			Expect(err).To(HaveOccurred())

			args = &redis.CFINSERTOptions{
				Capacity: 3000,
				NoCreate: false,
			}

			result, err = client.CFINSERT(ctx, "testcf1", args, "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
		})

		It("should CFINSERTNX", Label("cuckoo", "cfinsertnx"), func() {
			args := &redis.CFINSERTOptions{
				Capacity: 3000,
				NoCreate: true,
			}

			result, err := client.CFINSERTNX(ctx, "testcf1", args, "item1", "item2", "item2").Result()
			Expect(err).To(HaveOccurred())

			args = &redis.CFINSERTOptions{
				Capacity: 3000,
				NoCreate: false,
			}

			result, err = client.CFINSERTNX(ctx, "testcf2", args, "item1", "item2", "item2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(1)))
			Expect(result[2]).To(BeEquivalentTo(int64(0)))
		})

		It("should CFMexists", Label("cuckoo", "cfmexists"), func() {
			err := client.CFINSERT(ctx, "testcf1", nil, "item1", "item2", "item3").Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CFMEXISTS(ctx, "testcf1", "item1", "item2", "item3", "item4").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(4))
			Expect(result[0]).To(BeTrue())
			Expect(result[1]).To(BeTrue())
			Expect(result[2]).To(BeTrue())
			Expect(result[3]).To(BeFalse())
		})

	})

	Describe("CMS", Label("cms"), func() {
		It("should CMSINCRBY", Label("cms", "cmsincrby"), func() {
			err := client.CMSINITBYDIM(ctx, "testcms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CMSINCRBY(ctx, "testcms1", "item1", 1, "item2", 2, "item3", 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(2)))
			Expect(result[2]).To(BeEquivalentTo(int64(3)))

		})

		It("should CMSINITBYDIM and CMSINFO", Label("cms", "cmsinitbydim", "cmsinfo"), func() {
			err := client.CMSINITBYDIM(ctx, "testcms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.CMSINFO(ctx, "testcms1").Result()
			Expect(err).NotTo(HaveOccurred())

			Expect(info).To(BeAssignableToTypeOf(redis.CMSINFO{}))
			Expect(info.Width).To(BeEquivalentTo(int64(5)))
			Expect(info.Depth).To(BeEquivalentTo(int64(10)))
		})

		It("should CMSINITBYPROB", Label("cms", "cmsinitbyprob"), func() {
			err := client.CMSINITBYPROB(ctx, "testcms1", 0.002, 0.01).Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.CMSINFO(ctx, "testcms1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info).To(BeAssignableToTypeOf(redis.CMSINFO{}))
		})

		It("should CMSMERGE, CMSMERGEWITHWEIGHT and CMSQUERY", Label("cms", "cmsmerge", "cmsquery"), func() {
			err := client.CMSMERGE(ctx, "destCms1", "testcms2", "testcms3").Err()
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("CMS: key does not exist"))

			err = client.CMSINITBYDIM(ctx, "destCms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSINITBYDIM(ctx, "destCms2", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSINITBYDIM(ctx, "cms1", 2, 20).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSINITBYDIM(ctx, "cms2", 3, 20).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.CMSMERGE(ctx, "destCms1", "cms1", "cms2").Err()
			Expect(err).To(MatchError("CMS: width/depth is not equal"))

			client.Del(ctx, "cms1", "cms2")

			err = client.CMSINITBYDIM(ctx, "cms1", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.CMSINITBYDIM(ctx, "cms2", 5, 10).Err()
			Expect(err).NotTo(HaveOccurred())

			client.CMSINCRBY(ctx, "cms1", "item1", 1, "item2", 2)
			client.CMSINCRBY(ctx, "cms2", "item2", 2, "item3", 3)

			err = client.CMSMERGE(ctx, "destCms1", "cms1", "cms2").Err()
			Expect(err).NotTo(HaveOccurred())

			result, err := client.CMSQUERY(ctx, "destCms1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(4)))
			Expect(result[2]).To(BeEquivalentTo(int64(3)))

			sourceSketches := map[string]int64{
				"cms1": 1,
				"cms2": 2,
			}
			err = client.CMSMERGEWITHWEIGHT(ctx, "destCms2", sourceSketches).Err()
			Expect(err).NotTo(HaveOccurred())

			result, err = client.CMSQUERY(ctx, "destCms2", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(result)).To(BeEquivalentTo(3))
			Expect(result[0]).To(BeEquivalentTo(int64(1)))
			Expect(result[1]).To(BeEquivalentTo(int64(6)))
			Expect(result[2]).To(BeEquivalentTo(int64(6)))

		})

	})

	Describe("TopK", Label("topk"), func() {
		It("should TOPKRESERVE, TOPKINFO, TOPKADD, TOPKQUERY, TOPKCOUNT, TOPKINCRBY, TOPKLIST, TOPKLISTWITHCOUNT", Label("topk", "topkreserve", "topkinfo", "topkadd", "topkquery", "topkcount", "topkincrby", "topklist", "topklistwithcount"), func() {
			err := client.TOPKRESERVE(ctx, "topk1", 3).Err()
			Expect(err).NotTo(HaveOccurred())

			resultInfo, err := client.TOPKINFO(ctx, "topk1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo.K).To(BeEquivalentTo(int64(3)))

			resultAdd, err := client.TOPKADD(ctx, "topk1", "item1", "item2", 3, "item1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultAdd)).To(BeEquivalentTo(int64(4)))

			resultQuery, err := client.TOPKQUERY(ctx, "topk1", "item1", "item2", 4, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultQuery)).To(BeEquivalentTo(4))
			Expect(resultQuery[0]).To(BeTrue())
			Expect(resultQuery[1]).To(BeTrue())
			Expect(resultQuery[2]).To(BeFalse())
			Expect(resultQuery[3]).To(BeTrue())

			resultCount, err := client.TOPKCOUNT(ctx, "topk1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultCount)).To(BeEquivalentTo(3))
			Expect(resultCount[0]).To(BeEquivalentTo(int64(2)))
			Expect(resultCount[1]).To(BeEquivalentTo(int64(1)))
			Expect(resultCount[2]).To(BeEquivalentTo(int64(0)))

			resultIncr, err := client.TOPKINCRBY(ctx, "topk1", "item1", 5, "item2", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultIncr)).To(BeEquivalentTo(2))

			resultCount, err = client.TOPKCOUNT(ctx, "topk1", "item1", "item2", "item3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultCount)).To(BeEquivalentTo(3))
			Expect(resultCount[0]).To(BeEquivalentTo(int64(7)))
			Expect(resultCount[1]).To(BeEquivalentTo(int64(11)))
			Expect(resultCount[2]).To(BeEquivalentTo(int64(0)))

			resultList, err := client.TOPKLIST(ctx, "topk1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultList)).To(BeEquivalentTo(3))
			Expect(resultList).To(ContainElements("item2", "item1", "3"))

			resultListWithCount, err := client.TOPKLISTWITHCOUNT(ctx, "topk1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultListWithCount)).To(BeEquivalentTo(3))
			Expect(resultListWithCount["3"]).To(BeEquivalentTo(int64(1)))
			Expect(resultListWithCount["item1"]).To(BeEquivalentTo(int64(7)))
			Expect(resultListWithCount["item2"]).To(BeEquivalentTo(int64(11)))
		})

		It("should TOPKRESERVEWITHOPTIONS", Label("topk", "topkreservewithoptions"), func() {
			err := client.TOPKRESERVEWITHOPTIONS(ctx, "topk1", 3, 1500, 8, 0.5).Err()
			Expect(err).NotTo(HaveOccurred())

			resultInfo, err := client.TOPKINFO(ctx, "topk1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo.K).To(BeEquivalentTo(int64(3)))
			Expect(resultInfo.Width).To(BeEquivalentTo(int64(1500)))
			Expect(resultInfo.Depth).To(BeEquivalentTo(int64(8)))
			Expect(resultInfo.Decay).To(BeEquivalentTo(0.5))
		})

	})

	Describe("t-digest", Label("tdigest"), func() {
		It("should TDIGESTADD, TDIGESTCREATE, TDIGESTINFO, TDIGESTBYRANK, TDIGESTBYREVRANK, TDIGESTCDF, TDIGESTMAX, TDIGESTMIN, TDIGESTQUANTILE, TDIGESTRANK, TDIGESTREVRANK, TDIGESTTRIMMEDMEAN, TDIGESTRESET, ", Label("tdigest", "tdigestadd", "tdigestcreate", "tdigestinfo", "tdigestbyrank", "tdigestbyrevrank", "tdigestcdf", "tdigestmax", "tdigestmin", "tdigestquantile", "tdigestrank", "tdigestrevrank", "tdigesttrimmedmean", "tdigestreset"), func() {
			err := client.TDIGESTCREATE(ctx, "tdigest1").Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.TDIGESTINFO(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Observations).To(BeEquivalentTo(int64(0)))

			// Test with empty sketch
			byRank, err := client.TDIGESTBYRANK(ctx, "tdigest1", 0, 1, 2, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(byRank)).To(BeEquivalentTo(4))

			byRevRank, err := client.TDIGESTBYREVRANK(ctx, "tdigest1", 0, 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(byRevRank)).To(BeEquivalentTo(3))

			cdf, err := client.TDIGESTCDF(ctx, "tdigest1", 15, 35, 70).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cdf)).To(BeEquivalentTo(3))

			max, err := client.TDIGESTMAX(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(math.IsNaN(max)).To(BeTrue())

			min, err := client.TDIGESTMIN(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(math.IsNaN(min)).To(BeTrue())

			quantile, err := client.TDIGESTQUANTILE(ctx, "tdigest1", 0.1, 0.2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(quantile)).To(BeEquivalentTo(2))

			rank, err := client.TDIGESTRANK(ctx, "tdigest1", 10, 20).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(rank)).To(BeEquivalentTo(2))

			revRank, err := client.TDIGESTREVRANK(ctx, "tdigest1", 10, 20).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(revRank)).To(BeEquivalentTo(2))

			trimmedMean, err := client.TDIGESTTRIMMEDMEAN(ctx, "tdigest1", 0.1, 0.6).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(math.IsNaN(trimmedMean)).To(BeTrue())

			// Add elements
			err = client.TDIGESTADD(ctx, "tdigest1", 10, 20, 30, 40, 50, 60, 70, 80, 90, 100).Err()
			Expect(err).NotTo(HaveOccurred())

			info, err = client.TDIGESTINFO(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Observations).To(BeEquivalentTo(int64(10)))

			byRank, err = client.TDIGESTBYRANK(ctx, "tdigest1", 0, 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(byRank)).To(BeEquivalentTo(3))
			Expect(byRank[0]).To(BeEquivalentTo(float64(10)))
			Expect(byRank[1]).To(BeEquivalentTo(float64(20)))
			Expect(byRank[2]).To(BeEquivalentTo(float64(30)))

			byRevRank, err = client.TDIGESTBYREVRANK(ctx, "tdigest1", 0, 1, 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(byRevRank)).To(BeEquivalentTo(3))
			Expect(byRevRank[0]).To(BeEquivalentTo(float64(100)))
			Expect(byRevRank[1]).To(BeEquivalentTo(float64(90)))
			Expect(byRevRank[2]).To(BeEquivalentTo(float64(80)))

			cdf, err = client.TDIGESTCDF(ctx, "tdigest1", 15, 35, 70).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cdf)).To(BeEquivalentTo(3))
			Expect(cdf[0]).To(BeEquivalentTo(0.1))
			Expect(cdf[1]).To(BeEquivalentTo(0.3))
			Expect(cdf[2]).To(BeEquivalentTo(0.65))

			max, err = client.TDIGESTMAX(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(max).To(BeEquivalentTo(float64(100)))

			min, err = client.TDIGESTMIN(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(min).To(BeEquivalentTo(float64(10)))

			quantile, err = client.TDIGESTQUANTILE(ctx, "tdigest1", 0.1, 0.2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(quantile)).To(BeEquivalentTo(2))
			Expect(quantile[0]).To(BeEquivalentTo(float64(20)))
			Expect(quantile[1]).To(BeEquivalentTo(float64(30)))

			rank, err = client.TDIGESTRANK(ctx, "tdigest1", 10, 20).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(rank)).To(BeEquivalentTo(2))
			Expect(rank[0]).To(BeEquivalentTo(int64(0)))
			Expect(rank[1]).To(BeEquivalentTo(int64(1)))

			revRank, err = client.TDIGESTREVRANK(ctx, "tdigest1", 10, 20).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(revRank)).To(BeEquivalentTo(2))
			Expect(revRank[0]).To(BeEquivalentTo(int64(9)))
			Expect(revRank[1]).To(BeEquivalentTo(int64(8)))

			trimmedMean, err = client.TDIGESTTRIMMEDMEAN(ctx, "tdigest1", 0.1, 0.6).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(trimmedMean).To(BeEquivalentTo(float64(40)))

			reset, err := client.TDIGESTRESET(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(reset).To(BeEquivalentTo("OK"))

		})

		It("should TDIGESTCREATEWITHCOMPRESSION", Label("tdigest", "tcreatewithcompression"), func() {
			err := client.TDIGESTCREATEWITHCOMPRESSION(ctx, "tdigest1", 2000).Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.TDIGESTINFO(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Compression).To(BeEquivalentTo(int64(2000)))
		})

		It("should TDIGESTMERGE", Label("tdigest", "tmerge"), func() {
			err := client.TDIGESTCREATE(ctx, "tdigest1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.TDIGESTADD(ctx, "tdigest1", 10, 20, 30, 40, 50, 60, 70, 80, 90, 100).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.TDIGESTCREATE(ctx, "tdigest2").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.TDIGESTADD(ctx, "tdigest2", 15, 25, 35, 45, 55, 65, 75, 85, 95, 105).Err()
			Expect(err).NotTo(HaveOccurred())

			err = client.TDIGESTCREATE(ctx, "tdigest3").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.TDIGESTADD(ctx, "tdigest3", 50, 60, 70, 80, 90, 100, 110, 120, 130, 140).Err()
			Expect(err).NotTo(HaveOccurred())

			options := &redis.TDIGESTMERGEOptions{
				Compression: 1000,
				Override:    false,
			}
			err = client.TDIGESTMERGE(ctx, "tdigest1", options, "tdigest2", "tdigest3").Err()
			Expect(err).NotTo(HaveOccurred())

			info, err := client.TDIGESTINFO(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Observations).To(BeEquivalentTo(int64(30)))
			Expect(info.Compression).To(BeEquivalentTo(int64(1000)))

			max, err := client.TDIGESTMAX(ctx, "tdigest1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(max).To(BeEquivalentTo(float64(140)))
		})
	})
})
