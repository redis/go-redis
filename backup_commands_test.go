package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("Backup Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		SkipBeforeRedisVersion("8.10", "BACKUP commands require Redis >= 8.10")
		client = redis.NewClient(redisOptions())
		Expect(client.BackupCleanup(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if client == nil {
			return
		}
		_ = client.BackupAbort(ctx).Err()
		Expect(client.BackupCleanup(ctx).Err()).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
		client = nil
	})

	It("should start, seal, list and clean up a backup", func() {
		status, err := client.BackupStatus(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(status.State).To(Equal("idle"))
		Expect(status.StartTime.IsZero()).To(BeTrue())

		Expect(client.BackupStart(ctx).Val()).To(Equal("OK"))
		Eventually(func() string {
			return client.BackupStatus(ctx).Val().State
		}, 10*time.Second, 50*time.Millisecond).Should(Equal("incrementing"))

		files, err := client.BackupList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(files).NotTo(BeEmpty())

		Expect(client.BackupSeal(ctx).Val()).To(Equal("OK"))
		status, err = client.BackupStatus(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(status.State).To(Equal("sealed"))
		Expect(status.Error).To(BeEmpty())
		Expect(status.StartTime.IsZero()).To(BeFalse())
		Expect(status.EndTime.Before(status.StartTime)).To(BeFalse())

		Expect(client.BackupCleanup(ctx).Val()).To(Equal("OK"))
		Expect(client.BackupStatus(ctx).Val().State).To(Equal("idle"))
	})

	It("should abort a backup that hasn't been sealed", func() {
		Expect(client.BackupStart(ctx).Err()).NotTo(HaveOccurred())
		Expect(client.BackupAbort(ctx).Val()).To(Equal("OK"))

		status, err := client.BackupStatus(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(status.State).To(Equal("failed"))
		Expect(status.Error).NotTo(BeEmpty())
	})

	It("should refuse a state change the backup isn't in", func() {
		Expect(client.BackupSeal(ctx).Err()).To(HaveOccurred())
		Expect(client.BackupAbort(ctx).Err()).To(HaveOccurred())

		Expect(client.BackupStart(ctx).Err()).NotTo(HaveOccurred())
		Expect(client.BackupStart(ctx).Err()).To(HaveOccurred())
	})
})
