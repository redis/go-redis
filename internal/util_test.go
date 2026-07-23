package internal

import (
	"math"
	"runtime"
	"strings"
	"testing"
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
	tests := []string{
		"AaBbCcDdEeFfGg",
		"ABCDE",
		"abced",
		"AbC",
		"already",
	}
	for _, s := range tests {
		if got, want := ToLower(s), strings.ToLower(s); got != want {
			t.Errorf("ToLower(%q) = %q, want %q", s, got, want)
		}
	}
}

func TestIsLower(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"AaBbCcDdEeFfGg", false},
		{"ABCDE", false},
		{"abcdefg", true},
	}
	for _, tt := range tests {
		if got := isLower(tt.in); got != tt.want {
			t.Errorf("isLower(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestGetAddr(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"127.0.0.1:1234", "127.0.0.1:1234"},
		{"[::1]:1234", "[::1]:1234"},
		{"[fd01:abcd::7d03]:6379", "[fd01:abcd::7d03]:6379"},
		{"::1:1234", "[::1]:1234"},
		{"fd01:abcd::7d03:6379", "[fd01:abcd::7d03]:6379"},
		{"127.0.0.1", ""},
		{"127", ""},
		{"localhost-without-colon", ""},
	}
	for _, tt := range tests {
		if got := GetAddr(tt.in); got != tt.want {
			t.Errorf("GetAddr(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
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

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"NaN", math.NaN(), NaN},
		{"positive infinity", math.Inf(1), Inf},
		{"negative infinity", math.Inf(-1), NInf},
		{"zero", 0, "0"},
		{"positive integer", 42, "42"},
		{"negative integer", -42, "-42"},
		{"positive float", 3.14159, "3.14159"},
		{"negative float", -3.14159, "-3.14159"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFloat(tt.input)
			if result != tt.expected {
				t.Errorf("FormatFloat(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
