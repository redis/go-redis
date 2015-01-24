package redis_test

import (
	"math/rand"

	"gopkg.in/redis.v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {

	Describe("HashSlot", func() {

		It("should calculate hash slots", func() {
			rand.Seed(100)

			Expect(redis.HashSlot("123456789")).To(Equal(12739))
			Expect(redis.HashSlot("{}foo")).To(Equal(9500))
			Expect(redis.HashSlot("foo{}")).To(Equal(5542))
			Expect(redis.HashSlot("foo{}{bar}")).To(Equal(8363))
			Expect(redis.HashSlot("")).To(Equal(10503))
			Expect(redis.HashSlot("")).To(Equal(5176))
		})

		It("should extract keys from tags", func() {
			tests := []struct {
				one, two string
			}{
				{"foo{bar}", "bar"},
				{"{foo}bar", "foo"},
				{"{user1000}.following", "{user1000}.followers"},
				{"foo{{bar}}zap", "{bar"},
				{"foo{bar}{zap}", "bar"},
			}

			for _, test := range tests {
				Expect(redis.HashSlot(test.one)).To(Equal(redis.HashSlot(test.two)), "for %s <-> %s", test.one, test.two)
			}
		})

	})

})
