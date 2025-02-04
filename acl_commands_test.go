package redis_test

import (
	"context"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var TestUserName string = "goredis"

var _ = Describe("ACL Commands", func() {
	var client *redis.Client
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
		client = redis.NewClient(redisOptions())
	})

	AfterEach(func() {
		_, err := client.ACLDelUser(context.Background(), TestUserName).Result()
		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(err).NotTo(HaveOccurred())
	})

	It("list only default user", func() {
		res, err := client.ACLList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(HaveLen(1))
		Expect(res[0]).To(ContainSubstring("default"))
	})

	It("setuser and deluser", func() {
		res, err := client.ACLList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(HaveLen(1))
		Expect(res[0]).To(ContainSubstring("default"))

		add, err := client.ACLSetUser(ctx, TestUserName, "nopass", "on", "allkeys", "+set", "+get").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(add).To(Equal("OK"))

		resAfter, err := client.ACLList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resAfter).To(HaveLen(2))
		Expect(resAfter[1]).To(ContainSubstring(TestUserName))

		deletedN, err := client.ACLDelUser(ctx, TestUserName).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(deletedN).To(BeNumerically("==", 1))

		resAfterDeletion, err := client.ACLList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(resAfterDeletion).To(HaveLen(1))
		Expect(resAfterDeletion[0]).To(BeEquivalentTo(res[0]))
	})
})
