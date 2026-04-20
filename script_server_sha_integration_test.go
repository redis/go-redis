package redis_test

import (
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("NewScriptServerSHA", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should reload script after SCRIPT FLUSH", func() {
		s := redis.NewScriptServerSHA("return 1")

		// First run should succeed (loads script via SCRIPT LOAD)
		err := s.Run(ctx, client, nil).Err()
		Expect(err).NotTo(HaveOccurred())

		// Flush all scripts from Redis
		err = client.Do(ctx, "SCRIPT", "FLUSH").Err()
		Expect(err).NotTo(HaveOccurred())

		// Second run should still succeed (reloads script after NOSCRIPT)
		err = s.Run(ctx, client, nil).Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should run script with RunRO", func() {
		s := redis.NewScriptServerSHA("return 1")

		err := s.RunRO(ctx, client, nil).Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should run script with EvalSha", func() {
		s := redis.NewScriptServerSHA("return KEYS[1]")

		result, err := s.EvalSha(ctx, client, []string{"mykey"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("mykey"))
	})

	It("should run script with EvalShaRO", func() {
		s := redis.NewScriptServerSHA("return ARGV[1]")

		result, err := s.EvalShaRO(ctx, client, nil, "myarg").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("myarg"))
	})

	It("should cache hash after first load", func() {
		s := redis.NewScriptServerSHA("return 42")

		// Hash should be empty before first run
		Expect(s.Hash()).To(Equal(""))

		// Run the script
		result, err := s.Run(ctx, client, nil).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(42)))

		// Hash should be populated after run
		Expect(s.Hash()).NotTo(Equal(""))
	})
})
