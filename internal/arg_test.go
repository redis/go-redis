package internal

import (
	"testing"
	"time"
)

func TestAppendArgPointers(t *testing.T) {
	s, n, f, ok := "value", 42, 1.5, true
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var nilStr *string

	tests := []struct {
		arg  interface{}
		want string
	}{
		{&s, "value"},
		{&n, "42"},
		{&f, "1.5"},
		{&ok, "true"},
		{&ts, "2026-01-02T03:04:05Z"},
		{nilStr, "<nil>"},
	}
	for _, tt := range tests {
		if got := string(AppendArg(nil, tt.arg)); got != tt.want {
			t.Errorf("AppendArg(%T) = %q, want %q", tt.arg, got, tt.want)
		}
	}
}
