package util

import (
	"math"
	"testing"
)

func TestParseStringToFloat(t *testing.T) {
	tests := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"1.23", 1.23, true},
		{"inf", math.Inf(1), true},
		{"-inf", math.Inf(-1), true},
		{"nan", math.NaN(), true},
		{"oops", 0, false},
	}

	for _, tc := range tests {
		got, err := ParseStringToFloat(tc.in)
		if tc.ok {
			if err != nil {
				t.Fatalf("ParseFloat(%q) error: %v", tc.in, err)
			}
			if math.IsNaN(tc.want) {
				if !math.IsNaN(got) {
					t.Errorf("ParseFloat(%q) = %v; want NaN", tc.in, got)
				}
			} else if got != tc.want {
				t.Errorf("ParseFloat(%q) = %v; want %v", tc.in, got, tc.want)
			}
		} else {
			if err == nil {
				t.Errorf("ParseFloat(%q) expected error, got nil", tc.in)
			}
		}
	}
}
