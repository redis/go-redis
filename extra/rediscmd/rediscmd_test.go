package rediscmd

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "redisext")
}

var _ = Describe("AppendArg", func() {
	DescribeTable("...",
		func(src, wanted string) {
			b := appendArg(nil, src)
			Expect(string(b)).To(Equal(wanted))
		},

		Entry("", "-inf", "-inf"),
		Entry("", "+inf", "+inf"),
		Entry("", "foo.bar", "foo.bar"),
		Entry("", "foo:bar", "foo:bar"),
		Entry("", "foo{bar}", "foo{bar}"),
		Entry("", "foo-123_BAR", "foo-123_BAR"),
		Entry("", "foo\nbar", "666f6f0a626172"),
		Entry("", "\000", "00"),
	)
})
