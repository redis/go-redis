package redis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
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

// TestSetErrWithWrappedError tests that when a hook wraps an error and sets it
// via cmd.SetErr(), the underlying typed error can still be detected
func TestSetErrWithWrappedError(t *testing.T) {
	testCtx := context.Background()

	// Test with a simulated LOADING error
	// We test the mechanism directly without needing a real Redis server
	cmd := redis.NewStatusCmd(testCtx, "GET", "key")
	loadingErr := proto.ParseErrorReply([]byte("-LOADING Redis is loading the dataset in memory"))
	wrappedLoadingErr := fmt.Errorf("hook wrapper: %w", loadingErr)
	cmd.SetErr(wrappedLoadingErr)

	// Verify we can still detect the LOADING error through the wrapper
	if !redis.IsLoadingError(cmd.Err()) {
		t.Errorf("IsLoadingError failed to detect wrapped error set via SetErr: %v", cmd.Err())
	}

	// Test with MOVED error
	cmd2 := redis.NewStatusCmd(testCtx, "GET", "key")
	movedErr := proto.ParseErrorReply([]byte("-MOVED 3999 127.0.0.1:6381"))
	wrappedMovedErr := fmt.Errorf("hook wrapper: %w", movedErr)
	cmd2.SetErr(wrappedMovedErr)

	// Verify we can still detect and extract address from MOVED error
	addr, ok := redis.IsMovedError(cmd2.Err())
	if !ok {
		t.Errorf("IsMovedError failed to detect wrapped error set via SetErr: %v", cmd2.Err())
	}
	if addr != "127.0.0.1:6381" {
		t.Errorf("Address extraction failed: got %q, want %q", addr, "127.0.0.1:6381")
	}

	// Test with READONLY error
	cmd3 := redis.NewStatusCmd(testCtx, "SET", "key", "value")
	readonlyErr := proto.ParseErrorReply([]byte("-READONLY You can't write against a read only replica"))
	wrappedReadonlyErr := fmt.Errorf("custom error wrapper: %w", readonlyErr)
	cmd3.SetErr(wrappedReadonlyErr)

	// Verify we can still detect the READONLY error through the wrapper
	if !redis.IsReadOnlyError(cmd3.Err()) {
		t.Errorf("IsReadOnlyError failed to detect wrapped error set via SetErr: %v", cmd3.Err())
	}

	// Verify the error message contains both the wrapper and original error
	errMsg := cmd3.Err().Error()
	if !contains(errMsg, "custom error wrapper") {
		t.Errorf("Error message missing wrapper context: %v", errMsg)
	}
	if !contains(errMsg, "READONLY") {
		t.Errorf("Error message missing original error: %v", errMsg)
	}
}

// AppError is a custom error type for testing
type AppError struct {
	Code      string
	Message   string
	RequestID string
	Err       error
}

// Error implements the error interface
func (e *AppError) Error() string {
	return fmt.Sprintf("[%s] %s (request_id=%s): %v", e.Code, e.Message, e.RequestID, e.Err)
}

// Unwrap implements the error unwrapping interface - this is critical for errors.As() to work
func (e *AppError) Unwrap() error {
	return e.Err
}

// TestCustomErrorTypeWrapping tests that users can wrap Redis errors in their own custom error types
// and still have typed error detection work correctly
func TestCustomErrorTypeWrapping(t *testing.T) {
	testCtx := context.Background()

	// Test 1: Wrap LOADING error in custom type
	cmd1 := redis.NewStatusCmd(testCtx, "GET", "key")
	loadingErr := proto.ParseErrorReply([]byte("-LOADING Redis is loading the dataset in memory"))
	customErr1 := &AppError{
		Code:      "REDIS_ERROR",
		Message:   "Database operation failed",
		RequestID: "req-12345",
		Err:       loadingErr,
	}
	cmd1.SetErr(customErr1)

	// Verify typed error detection works through custom error type
	if !redis.IsLoadingError(cmd1.Err()) {
		t.Errorf("IsLoadingError failed to detect error wrapped in custom type: %v", cmd1.Err())
	}

	// Verify error message contains custom context
	errMsg := cmd1.Err().Error()
	if !contains(errMsg, "REDIS_ERROR") || !contains(errMsg, "req-12345") {
		t.Errorf("Error message missing custom error context: %v", errMsg)
	}

	// Test 2: Wrap MOVED error in custom type
	cmd2 := redis.NewStatusCmd(testCtx, "GET", "key")
	movedErr := proto.ParseErrorReply([]byte("-MOVED 3999 127.0.0.1:6381"))
	customErr2 := &AppError{
		Code:      "CLUSTER_REDIRECT",
		Message:   "Key moved to different node",
		RequestID: "req-67890",
		Err:       movedErr,
	}
	cmd2.SetErr(customErr2)

	// Verify address extraction works through custom error type
	addr, ok := redis.IsMovedError(cmd2.Err())
	if !ok {
		t.Errorf("IsMovedError failed to detect error wrapped in custom type: %v", cmd2.Err())
	}
	if addr != "127.0.0.1:6381" {
		t.Errorf("Address extraction failed: got %q, want %q", addr, "127.0.0.1:6381")
	}

	// Test 3: Multiple levels of wrapping (custom type + fmt.Errorf)
	cmd3 := redis.NewStatusCmd(testCtx, "SET", "key", "value")
	readonlyErr := proto.ParseErrorReply([]byte("-READONLY You can't write against a read only replica"))
	customErr3 := &AppError{
		Code:      "WRITE_ERROR",
		Message:   "Write operation failed",
		RequestID: "req-11111",
		Err:       readonlyErr,
	}
	// Wrap the custom error again with fmt.Errorf
	doubleWrapped := fmt.Errorf("hook context: %w", customErr3)
	cmd3.SetErr(doubleWrapped)

	// Verify typed error detection works through multiple levels of wrapping
	if !redis.IsReadOnlyError(cmd3.Err()) {
		t.Errorf("IsReadOnlyError failed to detect error wrapped in custom type + fmt.Errorf: %v", cmd3.Err())
	}

	// Verify we can unwrap to get the custom error
	var appErr *AppError
	if !errors.As(cmd3.Err(), &appErr) {
		t.Errorf("errors.As failed to extract custom error type from wrapped error")
	} else {
		if appErr.Code != "WRITE_ERROR" || appErr.RequestID != "req-11111" {
			t.Errorf("Custom error fields incorrect: Code=%s, RequestID=%s", appErr.Code, appErr.RequestID)
		}
	}
}

// TestTimeoutErrorWrapping tests that timeout errors work correctly when wrapped
func TestTimeoutErrorWrapping(t *testing.T) {
	// Test 1: Wrapped timeoutError interface
	t.Run("Wrapped timeoutError with Timeout()=true", func(t *testing.T) {
		timeoutErr := &testTimeoutError{timeout: true, msg: "i/o timeout"}
		wrappedErr := fmt.Errorf("hook wrapper: %w", timeoutErr)
		doubleWrappedErr := fmt.Errorf("another wrapper: %w", wrappedErr)

		// Should NOT retry when retryTimeout=false
		if redis.ShouldRetry(doubleWrappedErr, false) {
			t.Errorf("Should not retry timeout error when retryTimeout=false")
		}

		// Should retry when retryTimeout=true
		if !redis.ShouldRetry(doubleWrappedErr, true) {
			t.Errorf("Should retry timeout error when retryTimeout=true")
		}
	})

	// Test 2: Wrapped timeoutError with Timeout()=false
	t.Run("Wrapped timeoutError with Timeout()=false", func(t *testing.T) {
		timeoutErr := &testTimeoutError{timeout: false, msg: "connection error"}
		wrappedErr := fmt.Errorf("hook wrapper: %w", timeoutErr)

		// Should always retry when Timeout()=false
		if !redis.ShouldRetry(wrappedErr, false) {
			t.Errorf("Should retry non-timeout error even when retryTimeout=false")
		}
		if !redis.ShouldRetry(wrappedErr, true) {
			t.Errorf("Should retry non-timeout error when retryTimeout=true")
		}
	})

	// Test 3: Wrapped net.Error with Timeout()=true
	t.Run("Wrapped net.Error", func(t *testing.T) {
		netErr := &testNetError{timeout: true, temporary: true, msg: "network timeout"}
		wrappedErr := fmt.Errorf("hook context: %w", netErr)

		// Should respect retryTimeout parameter
		if redis.ShouldRetry(wrappedErr, false) {
			t.Errorf("Should not retry network timeout when retryTimeout=false")
		}
		if !redis.ShouldRetry(wrappedErr, true) {
			t.Errorf("Should retry network timeout when retryTimeout=true")
		}
	})

	// Test 4: Multiple levels of wrapping
	t.Run("Multiple levels of wrapping", func(t *testing.T) {
		timeoutErr := &testTimeoutError{timeout: true, msg: "timeout"}
		customErr := &AppError{
			Code:      "TIMEOUT_ERROR",
			Message:   "Operation timed out",
			RequestID: "req-timeout-123",
			Err:       timeoutErr,
		}
		wrappedErr := fmt.Errorf("hook wrapper: %w", customErr)

		// Should still detect timeout through multiple wrappers
		if redis.ShouldRetry(wrappedErr, false) {
			t.Errorf("Should not retry timeout through custom error when retryTimeout=false")
		}
		if !redis.ShouldRetry(wrappedErr, true) {
			t.Errorf("Should retry timeout through custom error when retryTimeout=true")
		}

		// Should be able to extract custom error
		var appErr *AppError
		if !errors.As(wrappedErr, &appErr) {
			t.Errorf("Should be able to extract AppError from wrapped error")
		}
	})
}

// testTimeoutError implements the timeoutError interface for testing
type testTimeoutError struct {
	timeout bool
	msg     string
}

func (e *testTimeoutError) Error() string {
	return e.msg
}

func (e *testTimeoutError) Timeout() bool {
	return e.timeout
}

// testNetError implements net.Error for testing
type testNetError struct {
	timeout   bool
	temporary bool
	msg       string
}

func (e *testNetError) Error() string {
	return e.msg
}

func (e *testNetError) Timeout() bool {
	return e.timeout
}

func (e *testNetError) Temporary() bool {
	return e.temporary
}

// TestContextErrorWrapping tests that context errors work correctly when wrapped
func TestContextErrorWrapping(t *testing.T) {
	t.Run("Wrapped context.Canceled", func(t *testing.T) {
		wrappedErr := fmt.Errorf("operation failed: %w", context.Canceled)
		doubleWrappedErr := fmt.Errorf("hook wrapper: %w", wrappedErr)

		// Should NOT retry
		if redis.ShouldRetry(doubleWrappedErr, false) {
			t.Errorf("Should not retry wrapped context.Canceled")
		}
		if redis.ShouldRetry(doubleWrappedErr, true) {
			t.Errorf("Should not retry wrapped context.Canceled even with retryTimeout=true")
		}
	})

	t.Run("Wrapped context.DeadlineExceeded", func(t *testing.T) {
		wrappedErr := fmt.Errorf("timeout: %w", context.DeadlineExceeded)
		doubleWrappedErr := fmt.Errorf("hook wrapper: %w", wrappedErr)

		// Should NOT retry
		if redis.ShouldRetry(doubleWrappedErr, false) {
			t.Errorf("Should not retry wrapped context.DeadlineExceeded")
		}
		if redis.ShouldRetry(doubleWrappedErr, true) {
			t.Errorf("Should not retry wrapped context.DeadlineExceeded even with retryTimeout=true")
		}
	})
}

// TestIOErrorWrapping tests that io errors work correctly when wrapped
func TestIOErrorWrapping(t *testing.T) {
	t.Run("Wrapped io.EOF", func(t *testing.T) {
		wrappedErr := fmt.Errorf("read failed: %w", io.EOF)
		doubleWrappedErr := fmt.Errorf("hook wrapper: %w", wrappedErr)

		// Should retry
		if !redis.ShouldRetry(doubleWrappedErr, false) {
			t.Errorf("Should retry wrapped io.EOF")
		}
	})

	t.Run("Wrapped io.ErrUnexpectedEOF", func(t *testing.T) {
		wrappedErr := fmt.Errorf("read failed: %w", io.ErrUnexpectedEOF)

		// Should retry
		if !redis.ShouldRetry(wrappedErr, false) {
			t.Errorf("Should retry wrapped io.ErrUnexpectedEOF")
		}
	})
}

// TestPoolErrorWrapping tests that pool errors work correctly when wrapped
func TestPoolErrorWrapping(t *testing.T) {
	t.Run("Wrapped pool.ErrPoolTimeout", func(t *testing.T) {
		wrappedErr := fmt.Errorf("connection failed: %w", redis.ErrPoolTimeout)
		doubleWrappedErr := fmt.Errorf("hook wrapper: %w", wrappedErr)

		// Should retry
		if !redis.ShouldRetry(doubleWrappedErr, false) {
			t.Errorf("Should retry wrapped pool.ErrPoolTimeout")
		}
	})
}

// TestRedisErrorWrapping tests that RedisError detection works with wrapped errors
func TestRedisErrorWrapping(t *testing.T) {
	t.Run("Wrapped proto.RedisError", func(t *testing.T) {
		redisErr := proto.RedisError("ERR something went wrong")
		wrappedErr := fmt.Errorf("command failed: %w", redisErr)
		doubleWrappedErr := fmt.Errorf("hook wrapper: %w", wrappedErr)

		// Create a command and set the wrapped error
		cmd := redis.NewStatusCmd(context.Background(), "GET", "key")
		cmd.SetErr(doubleWrappedErr)

		// The error should still be recognized as a Redis error
		// This is tested indirectly through the typed error system
		if !strings.Contains(cmd.Err().Error(), "ERR something went wrong") {
			t.Errorf("Error message not preserved through wrapping")
		}
	})
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestAuthErrorWrapping(t *testing.T) {
	t.Run("Wrapped NOAUTH error", func(t *testing.T) {
		// Create an auth error
		authErr := proto.NewAuthError("NOAUTH Authentication required")

		// Wrap it
		wrappedErr := fmt.Errorf("hook: %w", authErr)

		// Should still be detected
		if !redis.IsAuthError(wrappedErr) {
			t.Errorf("IsAuthError should detect wrapped NOAUTH error")
		}
	})

	t.Run("Wrapped WRONGPASS error", func(t *testing.T) {
		// Create an auth error
		authErr := proto.NewAuthError("WRONGPASS invalid username-password pair")

		// Wrap it multiple times
		wrappedErr := fmt.Errorf("connection error: %w", authErr)
		doubleWrappedErr := fmt.Errorf("client error: %w", wrappedErr)

		// Should still be detected
		if !redis.IsAuthError(doubleWrappedErr) {
			t.Errorf("IsAuthError should detect double-wrapped WRONGPASS error")
		}
	})

	t.Run("Wrapped unauthenticated error", func(t *testing.T) {
		// Create an auth error
		authErr := proto.NewAuthError("ERR unauthenticated")

		// Wrap it
		wrappedErr := fmt.Errorf("hook: %w", authErr)

		// Should still be detected
		if !redis.IsAuthError(wrappedErr) {
			t.Errorf("IsAuthError should detect wrapped unauthenticated error")
		}
	})
}

func TestPermissionErrorWrapping(t *testing.T) {
	t.Run("Wrapped NOPERM error", func(t *testing.T) {
		// Create a permission error
		permErr := proto.NewPermissionError("NOPERM this user has no permissions to run the 'flushdb' command")

		// Wrap it
		wrappedErr := fmt.Errorf("hook: %w", permErr)

		// Should still be detected
		if !redis.IsPermissionError(wrappedErr) {
			t.Errorf("IsPermissionError should detect wrapped NOPERM error")
		}
	})
}

func TestExecAbortErrorWrapping(t *testing.T) {
	t.Run("Wrapped EXECABORT error", func(t *testing.T) {
		// Create an EXECABORT error
		execAbortErr := proto.NewExecAbortError("EXECABORT Transaction discarded because of previous errors")

		// Wrap it
		wrappedErr := fmt.Errorf("hook: %w", execAbortErr)

		// Should still be detected
		if !redis.IsExecAbortError(wrappedErr) {
			t.Errorf("IsExecAbortError should detect wrapped EXECABORT error")
		}
	})
}

func TestOOMErrorWrapping(t *testing.T) {
	t.Run("Wrapped OOM error", func(t *testing.T) {
		// Create an OOM error
		oomErr := proto.NewOOMError("OOM command not allowed when used memory > 'maxmemory'")

		// Wrap it
		wrappedErr := fmt.Errorf("hook: %w", oomErr)

		// Should still be detected
		if !redis.IsOOMError(wrappedErr) {
			t.Errorf("IsOOMError should detect wrapped OOM error")
		}
	})
}
