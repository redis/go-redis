package redis_test

import (
	"github.com/go-redis/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cmd", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB().Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("implements Stringer", func() {
		set := client.Set("foo", "bar", 0)
		Expect(set.String()).To(Equal("set foo bar: OK"))

		get := client.Get("foo")
		Expect(get.String()).To(Equal("get foo: bar"))
	})

	It("has val/err", func() {
		set := client.Set("key", "hello", 0)
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))

		get := client.Get("key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello"))

		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal("OK"))
	})

	It("has helpers", func() {
		set := client.Set("key", "10", 0)
		Expect(set.Err()).NotTo(HaveOccurred())

		n, err := client.Get("key").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(10)))

		un, err := client.Get("key").Uint64()
		Expect(err).NotTo(HaveOccurred())
		Expect(un).To(Equal(uint64(10)))

		f, err := client.Get("key").Float64()
		Expect(err).NotTo(HaveOccurred())
		Expect(f).To(Equal(float64(10)))
	})

	It("safe float32 convert to float64", func() {
		var f float32
		f = 66.97

		set := client.Set("float_key", f, 0)
		Expect(set.Err()).NotTo(HaveOccurred())

		val, err := client.Get("float_key").Float64()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(float64(66.97)))

		member := "sam123"
		mycmd := []interface{}{"Zadd", "zset_float_key", f, member}
		cmd := redis.NewStatusCmd(mycmd...)

		client.Process(cmd)
		Expect(cmd.Err()).NotTo(HaveOccurred())

		fscore := client.ZScore("zset_float_key", member)
		Expect(fscore.Err()).NotTo(HaveOccurred())
		Expect(fscore.Val()).To(Equal(float64(66.97)))
	})

})
