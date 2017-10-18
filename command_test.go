package redis_test

import (
	"context"

	"github.com/kirk91/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cmd", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(context.Background()).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("implements Stringer", func() {
		set := client.Set(context.Background(), "foo", "bar", 0)
		Expect(set.String()).To(Equal("set foo bar: OK"))

		get := client.Get(context.Background(), "foo")
		Expect(get.String()).To(Equal("get foo: bar"))
	})

	It("has val/err", func() {
		set := client.Set(context.Background(), "key", "hello", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get(context.Background(), "key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello"))

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))
	})

	It("has helpers", func() {
		set := client.Set(context.Background(), "key", "10", 0)
		Expect(set.Err()).NotTo(HaveOccurred())

		n, err := client.Get(context.Background(), "key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(10)))

		un, err := client.Get(context.Background(), "key").Uint64()
		Expect(err).NotTo(HaveOccurred())
		Expect(un).To(Equal(uint64(10)))

		f, err := client.Get(context.Background(), "key").Float64()
		Expect(err).NotTo(HaveOccurred())
		Expect(f).To(Equal(float64(10)))
	})

})
