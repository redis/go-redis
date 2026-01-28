package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("HotKeys Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("HOTKEYS", func() {
		It("should start, get, stop, and reset hotkeys tracking", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:      true,
				NET:      true,
				Count:    10,
				Duration: 0,
				Sample:   1,
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())
			Expect(start.Val()).To(Equal("OK"))

			for i := 0; i < 100; i++ {
				client.Set(ctx, "hotkey1", "value1", 0)
				client.Get(ctx, "hotkey1")
			}
			for i := 0; i < 50; i++ {
				client.Set(ctx, "hotkey2", "value2", 0)
				client.Get(ctx, "hotkey2")
			}
			for i := 0; i < 25; i++ {
				client.Set(ctx, "hotkey3", "value3", 0)
				client.Get(ctx, "hotkey3")
			}

			time.Sleep(100 * time.Millisecond)

			get := client.HotKeysGet(ctx)
			Expect(get.Err()).NotTo(HaveOccurred())
			result := get.Val()
			Expect(result).NotTo(BeNil())
			Expect(result.TrackingActive).To(BeTrue())
			Expect(result.SampleRatio).To(Equal(int64(1)))

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
			Expect(stop.Val()).To(Equal("OK"))

			get2 := client.HotKeysGet(ctx)
			Expect(get2.Err()).NotTo(HaveOccurred())
			result2 := get2.Val()
			Expect(result2).NotTo(BeNil())
			Expect(result2.TrackingActive).To(BeFalse())

			reset := client.HotKeysReset(ctx)
			Expect(reset.Err()).NotTo(HaveOccurred())
			Expect(reset.Val()).To(Equal("OK"))
		})

		It("should start hotkeys tracking with CPU metric only", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:   true,
				NET:   false,
				Count: 5,
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())
			Expect(start.Val()).To(Equal("OK"))

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
		})

		It("should start hotkeys tracking with NET metric only", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:   false,
				NET:   true,
				Count: 5,
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())
			Expect(start.Val()).To(Equal("OK"))

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
		})

		It("should start hotkeys tracking with duration", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:      true,
				NET:      true,
				Count:    10,
				Duration: 2,
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())
			Expect(start.Val()).To(Equal("OK"))

			time.Sleep(3 * time.Second)

			get := client.HotKeysGet(ctx)
			Expect(get.Err()).NotTo(HaveOccurred())
			result := get.Val()
			Expect(result).NotTo(BeNil())
			Expect(result.TrackingActive).To(BeFalse())
		})

		It("should start hotkeys tracking with sampling", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:    true,
				NET:    true,
				Count:  10,
				Sample: 10,
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())
			Expect(start.Val()).To(Equal("OK"))

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
		})

		It("should start hotkeys tracking with specific slots", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:   true,
				NET:   true,
				Count: 10,
				Slots: []int64{0, 1, 2, 100, 200},
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())
			Expect(start.Val()).To(Equal("OK"))

			get := client.HotKeysGet(ctx)
			Expect(get.Err()).NotTo(HaveOccurred())
			result := get.Val()
			Expect(result).NotTo(BeNil())
			Expect(result.SelectedSlots).To(Equal([]int64{0, 1, 2, 100, 200}))

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
		})

		It("should error when starting tracking while already active", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:   true,
				Count: 10,
			}
			start1 := client.HotKeysStart(ctx, startArgs)
			Expect(start1.Err()).NotTo(HaveOccurred())

			start2 := client.HotKeysStart(ctx, startArgs)
			Expect(start2.Err()).To(HaveOccurred())

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
		})

		It("should error when resetting while tracking is active", func() {
			SkipBeforeRedisVersion(8.6, "HOTKEYS commands require Redis >= 8.6")

			startArgs := &redis.HotKeysStartArgs{
				CPU:   true,
				Count: 10,
			}
			start := client.HotKeysStart(ctx, startArgs)
			Expect(start.Err()).NotTo(HaveOccurred())

			reset := client.HotKeysReset(ctx)
			Expect(reset.Err()).To(HaveOccurred())

			stop := client.HotKeysStop(ctx)
			Expect(stop.Err()).NotTo(HaveOccurred())
		})
	})
})
