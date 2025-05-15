package util

import (
	"math"
	"testing"
)

func TestAtoi(t *testing.T) {
	tests := []struct {
		input    []byte
		expected int
		wantErr  bool
	}{
		{[]byte("123"), 123, false},
		{[]byte("-456"), -456, false},
		{[]byte("abc"), 0, true},
	}

	for _, tt := range tests {
		result, err := Atoi(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("Atoi(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if result != tt.expected && !tt.wantErr {
			t.Errorf("Atoi(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    []byte
		base     int
		bitSize  int
		expected int64
		wantErr  bool
	}{
		{[]byte("123"), 10, 64, 123, false},
		{[]byte("-7F"), 16, 64, -127, false},
		{[]byte("zzz"), 36, 64, 46655, false},
		{[]byte("invalid"), 10, 64, 0, true},
	}

	for _, tt := range tests {
		result, err := ParseInt(tt.input, tt.base, tt.bitSize)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseInt(%q, base=%d) error = %v, wantErr %v", tt.input, tt.base, err, tt.wantErr)
		}
		if result != tt.expected && !tt.wantErr {
			t.Errorf("ParseInt(%q, base=%d) = %d, want %d", tt.input, tt.base, result, tt.expected)
		}
	}
}

func TestParseUint(t *testing.T) {
	tests := []struct {
		input    []byte
		base     int
		bitSize  int
		expected uint64
		wantErr  bool
	}{
		{[]byte("255"), 10, 8, 255, false},
		{[]byte("FF"), 16, 16, 255, false},
		{[]byte("-1"), 10, 8, 0, true}, // negative should error for unsigned
	}

	for _, tt := range tests {
		result, err := ParseUint(tt.input, tt.base, tt.bitSize)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseUint(%q, base=%d) error = %v, wantErr %v", tt.input, tt.base, err, tt.wantErr)
		}
		if result != tt.expected && !tt.wantErr {
			t.Errorf("ParseUint(%q, base=%d) = %d, want %d", tt.input, tt.base, result, tt.expected)
		}
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		input    []byte
		bitSize  int
		expected float64
		wantErr  bool
	}{
		{[]byte("3.14"), 64, 3.14, false},
		{[]byte("-2.71"), 64, -2.71, false},
		{[]byte("NaN"), 64, math.NaN(), false},
		{[]byte("invalid"), 64, 0, true},
	}

	for _, tt := range tests {
		result, err := ParseFloat(tt.input, tt.bitSize)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseFloat(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if !tt.wantErr && !(math.IsNaN(tt.expected) && math.IsNaN(result)) && result != tt.expected {
			t.Errorf("ParseFloat(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}
