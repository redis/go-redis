package internal

import (
	"strings"
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func BenchmarkToLowerStd(b *testing.B) {
	str := "AaBbCcDdEeFfGgHhIiJjKk"
	for i := 0; i < b.N; i++ {
		_ = strings.ToLower(str)
	}
}

// util.ToLower is 3x faster than strings.ToLower.
func BenchmarkToLowerInternal(b *testing.B) {
	str := "AaBbCcDdEeFfGgHhIiJjKk"
	for i := 0; i < b.N; i++ {
		_ = ToLower(str)
	}
}

func TestToLower(t *testing.T) {
	It("toLower", func() {
		str := "AaBbCcDdEeFfGg"
		Expect(ToLower(str)).To(Equal(strings.ToLower(str)))

		str = "ABCDE"
		Expect(ToLower(str)).To(Equal(strings.ToLower(str)))

		str = "ABCDE"
		Expect(ToLower(str)).To(Equal(strings.ToLower(str)))

		str = "abced"
		Expect(ToLower(str)).To(Equal(strings.ToLower(str)))
	})
}

func TestIsLower(t *testing.T) {
	It("isLower", func() {
		str := "AaBbCcDdEeFfGg"
		Expect(isLower(str)).To(BeFalse())

		str = "ABCDE"
		Expect(isLower(str)).To(BeFalse())

		str = "abcdefg"
		Expect(isLower(str)).To(BeTrue())
	})
}
