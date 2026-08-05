package internal

import (
	"bytes"
	"context"
	"log"
	"testing"
	"time"
)

func TestToInteger(t *testing.T) {
	tests := []struct {
		in   interface{}
		want int
	}{
		{42, 42},
		{int64(7), 7},
		{"123", 123},
		{"not-a-number", 0},
		{3.14, 0},
		{nil, 0},
	}
	for _, tt := range tests {
		if got := ToInteger(tt.in); got != tt.want {
			t.Errorf("ToInteger(%v) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestToFloat(t *testing.T) {
	tests := []struct {
		in   interface{}
		want float64
	}{
		{3.5, 3.5},
		{"2.25", 2.25},
		{"bad", 0},
		{42, 0},
		{nil, 0},
	}
	for _, tt := range tests {
		if got := ToFloat(tt.in); got != tt.want {
			t.Errorf("ToFloat(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestToString(t *testing.T) {
	if got := ToString("hello"); got != "hello" {
		t.Errorf("ToString(string) = %q, want %q", got, "hello")
	}
	if got := ToString(42); got != "" {
		t.Errorf("ToString(int) = %q, want empty", got)
	}
	if got := ToString(nil); got != "" {
		t.Errorf("ToString(nil) = %q, want empty", got)
	}
}

func TestToStringSlice(t *testing.T) {
	in := []interface{}{"a", "b", 3}
	got := ToStringSlice(in)
	want := []string{"a", "b", ""}
	if len(got) != len(want) {
		t.Fatalf("ToStringSlice len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ToStringSlice[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if ToStringSlice("not-a-slice") != nil {
		t.Errorf("ToStringSlice(non-slice) should be nil")
	}
}

func TestReplaceSpaces(t *testing.T) {
	if got := ReplaceSpaces("go 1 24"); got != "go-1-24" {
		t.Errorf("ReplaceSpaces = %q, want %q", got, "go-1-24")
	}
	if got := ReplaceSpaces("nospaces"); got != "nospaces" {
		t.Errorf("ReplaceSpaces = %q, want %q", got, "nospaces")
	}
}

func TestSleep(t *testing.T) {
	if err := Sleep(context.Background(), time.Millisecond); err != nil {
		t.Errorf("Sleep returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Hour); err == nil {
		t.Errorf("Sleep with cancelled context should return error")
	}
}

func TestAppendArg(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		in   interface{}
		want string
	}{
		{nil, "<nil>"},
		{"str", "str"},
		{[]byte("bytes"), "bytes"},
		{int(-1), "-1"},
		{int8(2), "2"},
		{int16(3), "3"},
		{int32(4), "4"},
		{int64(5), "5"},
		{uint(6), "6"},
		{uint8(7), "7"},
		{uint16(8), "8"},
		{uint32(9), "9"},
		{uint64(10), "10"},
		{float32(1.5), "1.5"},
		{float64(2.5), "2.5"},
		{true, "true"},
		{false, "false"},
		{now, "2024-01-02T03:04:05Z"},
		{struct{ X int }{1}, "{1}"},
	}
	for _, tt := range tests {
		got := string(AppendArg(nil, tt.in))
		if got != tt.want {
			t.Errorf("AppendArg(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestDefaultLoggerPrintf(t *testing.T) {
	var buf bytes.Buffer
	l := &DefaultLogger{log: log.New(&buf, "", 0)}
	l.Printf(context.Background(), "hello %s", "world")
	if got := buf.String(); got != "hello world\n" {
		t.Errorf("DefaultLogger.Printf wrote %q", got)
	}
	if NewDefaultLogger() == nil {
		t.Errorf("NewDefaultLogger returned nil")
	}
}
