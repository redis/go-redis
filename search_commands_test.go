package redis_test

import (
	"reflect"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("FTAggregateQuery", func() {
	It("returns only the base query when options is nil", func() {
		args := redis.FTAggregateQuery("testQuery", nil)
		Expect(args).To(Equal(redis.AggregateQuery{"testQuery"}))
	})

	It("includes VERBATIM and SCORER when options are set", func() {
		options := &redis.FTAggregateOptions{
			Verbatim: true,
			Scorer:   "BM25",
		}
		args := redis.FTAggregateQuery("testQuery", options)
		Expect(args[0]).To(Equal("testQuery"))
		Expect(args).To(ContainElement("VERBATIM"))
		Expect(args).To(ContainElement("SCORER"))
		Expect(args).To(ContainElement("BM25"))
	})

	It("includes ADDSCORES when AddScores is true", func() {
		options := &redis.FTAggregateOptions{
			AddScores: true,
		}
		args := redis.FTAggregateQuery("q", options)
		Expect(args).To(ContainElement("ADDSCORES"))
	})

	It("includes LOADALL when LoadAll is true", func() {
		options := &redis.FTAggregateOptions{
			LoadAll: true,
		}
		args := redis.FTAggregateQuery("q", options)
		Expect(args).To(ContainElement("LOAD"))
		Expect(args).To(ContainElement("*"))
	})

	It("includes LOAD when Load is provided", func() {
		options := &redis.FTAggregateOptions{
			Load: []redis.FTAggregateLoad{
				{Field: "field1", As: "alias1"},
				{Field: "field2"},
			},
		}
		args := redis.FTAggregateQuery("q", options)
		// Verify LOAD options related arguments
		Expect(args).To(ContainElement("LOAD"))
		// Check that field names and aliases are present
		Expect(args).To(ContainElement("field1"))
		Expect(args).To(ContainElement("alias1"))
		Expect(args).To(ContainElement("field2"))
	})

	It("includes TIMEOUT when Timeout > 0", func() {
		options := &redis.FTAggregateOptions{
			Timeout: 500,
		}
		args := redis.FTAggregateQuery("q", options)
		Expect(args).To(ContainElement("TIMEOUT"))
		found := false
		for _, a := range args {
			if reflect.DeepEqual(a, 500) {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue())
	})
})
