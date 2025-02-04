package redis_test

import (
	"context"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var TestUserName string = "goredis"
var _ = Describe("ACL Users Commands", Label("NonRedisEnterprise"), func() {
	var client *redis.Client
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
		client = redis.NewClient(redisOptions())
	})

	AfterEach(func() {
		_, err := client.ACLDelUser(context.Background(), TestUserName).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
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

var _ = Describe("ACL Permissions", Label("NonRedisEnterprise"), func() {
	var client *redis.Client
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
		client = redis.NewClient(redisOptions())
	})

	AfterEach(func() {
		_, err := client.ACLDelUser(context.Background(), TestUserName).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("reset permissions", func() {
		add, err := client.ACLSetUser(ctx,
			TestUserName,
			"reset",
			"nopass",
			"on",
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(add).To(Equal("OK"))

		connection := client.Conn()
		authed, err := connection.AuthACL(ctx, TestUserName, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(authed).To(Equal("OK"))

		_, err = connection.Get(ctx, "anykey").Result()
		Expect(err).To(HaveOccurred())
	})

	It("add write permissions", func() {
		add, err := client.ACLSetUser(ctx,
			TestUserName,
			"reset",
			"nopass",
			"on",
			"~*",
			"+SET",
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(add).To(Equal("OK"))

		connection := client.Conn()
		authed, err := connection.AuthACL(ctx, TestUserName, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(authed).To(Equal("OK"))

		// can write
		v, err := connection.Set(ctx, "anykey", "anyvalue", 0).Result()
		Expect(err).ToNot(HaveOccurred())
		Expect(v).To(Equal("OK"))

		// but can't read
		value, err := connection.Get(ctx, "anykey").Result()
		Expect(err).To(HaveOccurred())
		Expect(value).To(BeEmpty())
	})

	It("add read permissions", func() {
		add, err := client.ACLSetUser(ctx,
			TestUserName,
			"reset",
			"nopass",
			"on",
			"~*",
			"+GET",
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(add).To(Equal("OK"))

		connection := client.Conn()
		authed, err := connection.AuthACL(ctx, TestUserName, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(authed).To(Equal("OK"))

		// can read
		value, err := connection.Get(ctx, "anykey").Result()
		Expect(err).ToNot(HaveOccurred())
		Expect(value).To(Equal("anyvalue"))

		// but can't delete
		del, err := connection.Del(ctx, "anykey").Result()
		Expect(err).To(HaveOccurred())
		Expect(del).ToNot(Equal(1))
	})

	It("add del permissions", func() {
		add, err := client.ACLSetUser(ctx,
			TestUserName,
			"reset",
			"nopass",
			"on",
			"~*",
			"+DEL",
		).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(add).To(Equal("OK"))

		connection := client.Conn()
		authed, err := connection.AuthACL(ctx, TestUserName, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(authed).To(Equal("OK"))

		// can read
		del, err := connection.Del(ctx, "anykey").Result()
		Expect(err).ToNot(HaveOccurred())
		Expect(del).To(BeEquivalentTo(1))
	})
})

var _ = Describe("ACL Categories", func() {
	var client *redis.Client
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
		client = redis.NewClient(redisOptions())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("lists acl categories and subcategories", func() {
		res, err := client.ACLCat(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(res)).To(BeNumerically(">", 20))
		Expect(res).To(ContainElements(
			"read",
			"write",
			"keyspace",
			"dangerous",
			"slow",
			"set",
			"sortedset",
			"list",
			"hash",
		))

		res, err = client.ACLCatArgs(ctx, &redis.ACLCatArgs{Category: "read"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(ContainElement("get"))
	})

	It("lists acl categories and subcategories with Modules", func() {
		SkipBeforeRedisMajor(8, "modules are included in acl for redis version >= 8")
		aclTestCase := map[string]string{
			"search":     "FT.CREATE",
			"bloom":      "bf.add",
			"json":       "json.get",
			"cuckoo":     "cf.insert",
			"cms":        "cms.query",
			"topk":       "topk.list",
			"tdigest":    "tdigest.rank",
			"timeseries": "ts.range",
		}
		var cats []interface{}

		for cat, subitem := range aclTestCase {
			cats = append(cats, cat)

			res, err := client.ACLCatArgs(ctx, &redis.ACLCatArgs{
				Category: cat,
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainElement(subitem))
		}

		res, err := client.ACLCat(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(ContainElements(cats...))
	})
})
