package util

import (
	"math"
	"testing"
)

func TestMustParseFloat(t *testing.T) {
	if got := MustParseFloat("1.5"); got != 1.5 {
		t.Errorf("MustParseFloat(1.5) = %v", got)
	}
	if got := MustParseFloat("inf"); !math.IsInf(got, 1) {
		t.Errorf("MustParseFloat(inf) = %v, want +Inf", got)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustParseFloat(bad) should panic")
		}
	}()
	MustParseFloat("not-a-float")
}

func TestSafeIntToInt32(t *testing.T) {
	tests := []struct {
		in      int
		want    int32
		wantErr bool
	}{
		{0, 0, false},
		{42, 42, false},
		{math.MaxInt32, math.MaxInt32, false},
		{math.MinInt32, math.MinInt32, false},
	}
	for _, tt := range tests {
		got, err := SafeIntToInt32(tt.in, "field")
		if (err != nil) != tt.wantErr {
			t.Errorf("SafeIntToInt32(%d) err = %v, wantErr %v", tt.in, err, tt.wantErr)
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("SafeIntToInt32(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}

	if _, err := SafeIntToInt32(math.MaxInt32+1, "field"); err == nil {
		t.Errorf("SafeIntToInt32(overflow) should error")
	}
	if _, err := SafeIntToInt32(math.MinInt32-1, "field"); err == nil {
		t.Errorf("SafeIntToInt32(underflow) should error")
	}
}

func TestToPtr(t *testing.T) {
	p := ToPtr(7)
	if p == nil || *p != 7 {
		t.Errorf("ToPtr(7) = %v", p)
	}
	s := ToPtr("x")
	if s == nil || *s != "x" {
		t.Errorf("ToPtr(\"x\") = %v", s)
	}
}

func TestAtomicMax(t *testing.T) {
	m := NewAtomicMax()
	if _, has := m.Max(); has {
		t.Errorf("new AtomicMax should have no value")
	}

	if !m.Value(5) {
		t.Errorf("first Value(5) should be a new max")
	}
	if m.Value(3) {
		t.Errorf("Value(3) should not be a new max")
	}
	if !m.Value(10) {
		t.Errorf("Value(10) should be a new max")
	}

	val, has := m.Max()
	if !has || val != 10 {
		t.Errorf("Max() = %v,%v want 10,true", val, has)
	}
	if m.Max1() != 10 {
		t.Errorf("Max1() = %v, want 10", m.Max1())
	}

	// Sentinel value path.
	m2 := NewAtomicMax()
	if !m2.Value(-math.MaxFloat64) {
		t.Errorf("first sentinel Value should report new max")
	}
	if m2.Value(-math.MaxFloat64) {
		t.Errorf("second sentinel Value should not report new max")
	}
}

func TestAtomicMin(t *testing.T) {
	m := NewAtomicMin()
	if _, has := m.Min(); has {
		t.Errorf("new AtomicMin should have no value")
	}

	if !m.Value(5) {
		t.Errorf("first Value(5) should be a new min")
	}
	if m.Value(8) {
		t.Errorf("Value(8) should not be a new min")
	}
	if !m.Value(2) {
		t.Errorf("Value(2) should be a new min")
	}

	val, has := m.Min()
	if !has || val != 2 {
		t.Errorf("Min() = %v,%v want 2,true", val, has)
	}
	if m.Min1() != 2 {
		t.Errorf("Min1() = %v, want 2", m.Min1())
	}

	// Sentinel value path.
	m2 := NewAtomicMin()
	if !m2.Value(math.MaxFloat64) {
		t.Errorf("first sentinel Value should report new min")
	}
	if m2.Value(math.MaxFloat64) {
		t.Errorf("second sentinel Value should not report new min")
	}
}
