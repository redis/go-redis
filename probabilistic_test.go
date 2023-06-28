package redis_test

import (
	"context"
	"fmt"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"strconv"

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

	Describe("bloom", Label("bloom", "bfadd"), func() {
		It("should BFAdd", Label("bloom"), func() {
			resultAdd, err := client.BFAdd(ctx, "testbf1", "test1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultAdd).To(Equal(int64(1)))

			resultInfo, err := client.BFInfo(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo).To(BeAssignableToTypeOf(map[string]string{}))
			Expect(resultInfo["Number of items inserted"]).To(BeEquivalentTo(strconv.Itoa(1)))
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
			_, err = client.BFAdd(ctx, "testbf1", "item3").Result()
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
			Expect(result).To(BeAssignableToTypeOf(map[string]string{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(strconv.Itoa(2000)))
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
			Expect(result).To(BeAssignableToTypeOf(map[string]string{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(strconv.Itoa(2000)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(strconv.Itoa(3)))
		})

		It("should BFMAdd", Label("bloom", "bfmadd"), func() {
			resultAdd, err := client.BFMAdd(ctx, "testbf1", "item1", "item2", "item3").Result()
			fmt.Printf("resultAdd: %v\n", resultAdd)

			Expect(err).NotTo(HaveOccurred())
			Expect(len(resultAdd)).To(Equal(int(3)))

			resultInfo, err := client.BFInfo(ctx, "testbf1").Result()

			Expect(err).NotTo(HaveOccurred())
			Expect(resultInfo).To(BeAssignableToTypeOf(map[string]string{}))
			Expect(resultInfo["Number of items inserted"]).To(BeEquivalentTo(strconv.Itoa(3)))
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
			Expect(result).To(BeAssignableToTypeOf(map[string]string{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(strconv.Itoa(2000)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(strconv.Itoa(3)))
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
			Expect(result).To(BeAssignableToTypeOf(map[string]string{}))
			Expect(result["Capacity"]).To(BeEquivalentTo(strconv.Itoa(2000)))
			Expect(result["Expansion rate"]).To(BeEquivalentTo(strconv.Itoa(3)))
		})

	})
})
