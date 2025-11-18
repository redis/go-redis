package redis_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestTypedErrorsWithHookWrapping demonstrates that typed errors work correctly
// even when wrapped by hooks, which is the main improvement of this change.
func TestTypedErrorsWithHookWrapping(t *testing.T) {
	tests := []struct {
		name      string
		errorMsg  string
		checkFunc func(error) bool
		testName  string
	}{
		{
			name:      "LOADING error wrapped in hook",
			errorMsg:  "LOADING Redis is loading the dataset in memory",
			checkFunc: redis.IsLoadingError,
			testName:  "IsLoadingError",
		},
		{
			name:      "READONLY error wrapped in hook",
			errorMsg:  "READONLY You can't write against a read only replica",
			checkFunc: redis.IsReadOnlyError,
			testName:  "IsReadOnlyError",
		},
		{
			name:      "CLUSTERDOWN error wrapped in hook",
			errorMsg:  "CLUSTERDOWN The cluster is down",
			checkFunc: redis.IsClusterDownError,
			testName:  "IsClusterDownError",
		},
		{
			name:      "TRYAGAIN error wrapped in hook",
			errorMsg:  "TRYAGAIN Multiple keys request during rehashing of slot",
			checkFunc: redis.IsTryAgainError,
			testName:  "IsTryAgainError",
		},
		{
			name:      "MASTERDOWN error wrapped in hook",
			errorMsg:  "MASTERDOWN Link with MASTER is down",
			checkFunc: redis.IsMasterDownError,
			testName:  "IsMasterDownError",
		},
		{
			name:      "Max clients error wrapped in hook",
			errorMsg:  "ERR max number of clients reached",
			checkFunc: redis.IsMaxClientsError,
			testName:  "IsMaxClientsError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate a Redis error being created
			parsedErr := proto.ParseErrorReply([]byte("-" + tt.errorMsg))

			// Simulate hook wrapping the error
			wrappedErr := fmt.Errorf("hook wrapper: %w", parsedErr)
			doubleWrappedErr := fmt.Errorf("another hook: %w", wrappedErr)

			// Test that the typed error check works with wrapped errors
			if !tt.checkFunc(doubleWrappedErr) {
				t.Errorf("%s failed to detect wrapped error: %v", tt.testName, doubleWrappedErr)
			}

			// Test that the error message is still accessible
			if !errors.Is(doubleWrappedErr, parsedErr) {
				t.Errorf("errors.Is failed to match wrapped error")
			}

			// Test that the original error message is preserved in the chain
			expectedMsg := tt.errorMsg
			if parsedErr.Error() != expectedMsg {
				t.Errorf("Error message changed: got %q, want %q", parsedErr.Error(), expectedMsg)
			}

			// Verify the generic RedisError interface still works
			var redisError redis.Error
			if !errors.As(doubleWrappedErr, &redisError) {
				t.Errorf("Failed to extract redis.Error from wrapped error")
			}
		})
	}
}

// TestMovedAndAskErrorsWithHookWrapping tests MOVED and ASK errors with wrapping
func TestMovedAndAskErrorsWithHookWrapping(t *testing.T) {
	tests := []struct {
		name         string
		errorMsg     string
		expectedAddr string
		isMoved      bool
	}{
		{
			name:         "MOVED error",
			errorMsg:     "MOVED 3999 127.0.0.1:6381",
			expectedAddr: "127.0.0.1:6381",
			isMoved:      true,
		},
		{
			name:         "ASK error",
			errorMsg:     "ASK 3999 192.168.1.100:6380",
			expectedAddr: "192.168.1.100:6380",
			isMoved:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the error
			parsedErr := proto.ParseErrorReply([]byte("-" + tt.errorMsg))

			// Wrap it in hooks
			wrappedErr := fmt.Errorf("hook wrapper: %w", parsedErr)
			doubleWrappedErr := fmt.Errorf("another hook: %w", wrappedErr)

			// Test address extraction from wrapped error
			if tt.isMoved {
				addr, ok := redis.IsMovedError(doubleWrappedErr)
				if !ok {
					t.Errorf("IsMovedError failed to detect wrapped MOVED error")
				}
				if addr != tt.expectedAddr {
					t.Errorf("Address mismatch: got %q, want %q", addr, tt.expectedAddr)
				}
			} else {
				addr, ok := redis.IsAskError(doubleWrappedErr)
				if !ok {
					t.Errorf("IsAskError failed to detect wrapped ASK error")
				}
				if addr != tt.expectedAddr {
					t.Errorf("Address mismatch: got %q, want %q", addr, tt.expectedAddr)
				}
			}
		})
	}
}

// TestBackwardCompatibilityWithStringChecks verifies that old string-based
// error checking still works for backward compatibility
func TestBackwardCompatibilityWithStringChecks(t *testing.T) {
	tests := []struct {
		name         string
		errorMsg     string
		stringPrefix string
	}{
		{
			name:         "LOADING error",
			errorMsg:     "LOADING Redis is loading the dataset in memory",
			stringPrefix: "LOADING ",
		},
		{
			name:         "READONLY error",
			errorMsg:     "READONLY You can't write against a read only replica",
			stringPrefix: "READONLY ",
		},
		{
			name:         "CLUSTERDOWN error",
			errorMsg:     "CLUSTERDOWN The cluster is down",
			stringPrefix: "CLUSTERDOWN ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedErr := proto.ParseErrorReply([]byte("-" + tt.errorMsg))

			// Old-style string checking should still work
			errMsg := parsedErr.Error()
			if errMsg != tt.errorMsg {
				t.Errorf("Error message mismatch: got %q, want %q", errMsg, tt.errorMsg)
			}

			// String prefix checking should still work
			if len(errMsg) < len(tt.stringPrefix) || errMsg[:len(tt.stringPrefix)] != tt.stringPrefix {
				t.Errorf("String prefix check failed: error %q doesn't start with %q", errMsg, tt.stringPrefix)
			}
		})
	}
}

// TestErrorWrappingInHookScenario simulates a real-world scenario where
// a hook wraps errors for logging or instrumentation
func TestErrorWrappingInHookScenario(t *testing.T) {
	// Simulate a hook that wraps errors for logging
	wrapErrorForLogging := func(err error) error {
		if err != nil {
			return fmt.Errorf("logged error at %s: %w", "2024-01-01T00:00:00Z", err)
		}
		return nil
	}

	// Simulate a hook that adds context
	addContextToError := func(err error, cmd string) error {
		if err != nil {
			return fmt.Errorf("command %s failed: %w", cmd, err)
		}
		return nil
	}

	// Create a LOADING error
	loadingErr := proto.ParseErrorReply([]byte("-LOADING Redis is loading the dataset in memory"))

	// Wrap it through multiple hooks
	err := loadingErr
	err = wrapErrorForLogging(err)
	err = addContextToError(err, "GET mykey")

	// The typed error check should still work
	if !redis.IsLoadingError(err) {
		t.Errorf("IsLoadingError failed to detect error through multiple hook wrappers")
	}

	// The error message should contain all the context
	errMsg := err.Error()
	expectedSubstrings := []string{
		"command GET mykey failed",
		"logged error at",
		"LOADING Redis is loading the dataset in memory",
	}

	for _, substr := range expectedSubstrings {
		if !contains(errMsg, substr) {
			t.Errorf("Error message missing expected substring %q: %s", substr, errMsg)
		}
	}
}

// TestShouldRetryWithTypedErrors tests that shouldRetry works with typed errors
func TestShouldRetryWithTypedErrors(t *testing.T) {
	tests := []struct {
		name          string
		errorMsg      string
		shouldRetry   bool
		retryTimeout  bool
	}{
		{
			name:         "LOADING error should retry",
			errorMsg:     "LOADING Redis is loading the dataset in memory",
			shouldRetry:  true,
			retryTimeout: false,
		},
		{
			name:         "READONLY error should retry",
			errorMsg:     "READONLY You can't write against a read only replica",
			shouldRetry:  true,
			retryTimeout: false,
		},
		{
			name:         "CLUSTERDOWN error should retry",
			errorMsg:     "CLUSTERDOWN The cluster is down",
			shouldRetry:  true,
			retryTimeout: false,
		},
		{
			name:         "TRYAGAIN error should retry",
			errorMsg:     "TRYAGAIN Multiple keys request during rehashing of slot",
			shouldRetry:  true,
			retryTimeout: false,
		},
		{
			name:         "MASTERDOWN error should retry",
			errorMsg:     "MASTERDOWN Link with MASTER is down",
			shouldRetry:  true,
			retryTimeout: false,
		},
		{
			name:         "Max clients error should retry",
			errorMsg:     "ERR max number of clients reached",
			shouldRetry:  true,
			retryTimeout: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := proto.ParseErrorReply([]byte("-" + tt.errorMsg))

			// Wrap the error
			wrappedErr := fmt.Errorf("hook wrapper: %w", err)

			// Test shouldRetry (using the exported ShouldRetry for testing)
			result := redis.ShouldRetry(wrappedErr, tt.retryTimeout)
			if result != tt.shouldRetry {
				t.Errorf("ShouldRetry returned %v, want %v for error: %v", result, tt.shouldRetry, wrappedErr)
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

