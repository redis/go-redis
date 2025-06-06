package internal

import (
	"runtime"
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

func TestGetAddr(t *testing.T) {
	It("getAddr", func() {
		str := "127.0.0.1:1234"
		Expect(GetAddr(str)).To(Equal(str))

		str = "[::1]:1234"
		Expect(GetAddr(str)).To(Equal(str))

		str = "[fd01:abcd::7d03]:6379"
		Expect(GetAddr(str)).To(Equal(str))

		Expect(GetAddr("::1:1234")).To(Equal("[::1]:1234"))

		Expect(GetAddr("fd01:abcd::7d03:6379")).To(Equal("[fd01:abcd::7d03]:6379"))

		Expect(GetAddr("127.0.0.1")).To(Equal(""))

		Expect(GetAddr("127")).To(Equal(""))
	})
}

func BenchmarkReplaceSpaces(b *testing.B) {
	version := runtime.Version()
	for i := 0; i < b.N; i++ {
		_ = ReplaceSpaces(version)
	}
}

func ReplaceSpacesUseBuilder(s string) string {
	// Pre-allocate a builder with the same length as s to minimize allocations.
	// This is a basic optimization; adjust the initial size based on your use case.
	var builder strings.Builder
	builder.Grow(len(s))

	for _, char := range s {
		if char == ' ' {
			// Replace space with a hyphen.
			builder.WriteRune('-')
		} else {
			// Copy the character as-is.
			builder.WriteRune(char)
		}
	}

	return builder.String()
}

func BenchmarkReplaceSpacesUseBuilder(b *testing.B) {
	version := runtime.Version()
	for i := 0; i < b.N; i++ {
		_ = ReplaceSpacesUseBuilder(version)
	}
}
