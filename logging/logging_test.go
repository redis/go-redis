package logging

import "testing"

func TestLogLevel_String(t *testing.T) {
	tests := []struct {
		level    LogLevelT
		expected string
	}{
		{LogLevelError, "ERROR"},
		{LogLevelWarn, "WARN"},
		{LogLevelInfo, "INFO"},
		{LogLevelDebug, "DEBUG"},
		{LogLevelT(99), "UNKNOWN"},
	}

	for _, test := range tests {
		if got := test.level.String(); got != test.expected {
			t.Errorf("LogLevel(%d).String() = %q, want %q", test.level, got, test.expected)
		}
	}
}

func TestLogLevel_IsValid(t *testing.T) {
	tests := []struct {
		level    LogLevelT
		expected bool
	}{
		{LogLevelError, true},
		{LogLevelWarn, true},
		{LogLevelInfo, true},
		{LogLevelDebug, true},
		{LogLevelT(-1), false},
		{LogLevelT(4), false},
		{LogLevelT(99), false},
	}

	for _, test := range tests {
		if got := test.level.IsValid(); got != test.expected {
			t.Errorf("LogLevel(%d).IsValid() = %v, want %v", test.level, got, test.expected)
		}
	}
}

func TestLogLevelConstants(t *testing.T) {
	// Test that constants have expected values
	if LogLevelError != 0 {
		t.Errorf("LogLevelError = %d, want 0", LogLevelError)
	}
	if LogLevelWarn != 1 {
		t.Errorf("LogLevelWarn = %d, want 1", LogLevelWarn)
	}
	if LogLevelInfo != 2 {
		t.Errorf("LogLevelInfo = %d, want 2", LogLevelInfo)
	}
	if LogLevelDebug != 3 {
		t.Errorf("LogLevelDebug = %d, want 3", LogLevelDebug)
	}
}
