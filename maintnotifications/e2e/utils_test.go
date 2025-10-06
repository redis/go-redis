package e2e

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

func isTimeout(errMsg string) bool {
	return contains(errMsg, "i/o timeout") ||
		contains(errMsg, "deadline exceeded") ||
		contains(errMsg, "context deadline exceeded")
}

// isTimeoutError checks if an error is a timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for various timeout error types
	errStr := err.Error()
	return isTimeout(errStr)
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func printLog(group string, isError bool, format string, args ...interface{}) {
	_, filename, line, _ := runtime.Caller(2)
	filename = filepath.Base(filename)
	if isError {
		format = "%s:%d [%s][%s][ERROR] " + format + "\n"
	}
	format = "%s:%d [%s][%s] " + format + "\n"
	ts := time.Now().Format("15:04:05.000")
	args = append([]interface{}{filename, line, ts, group}, args...)
	fmt.Printf(format, args...)
}
