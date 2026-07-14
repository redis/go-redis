package proto

import (
	"errors"
	"fmt"
	"testing"
)

// TestTypedRedisErrors tests that typed Redis errors are created correctly
func TestTypedRedisErrors(t *testing.T) {
	tests := []struct {
		name         string
		errorMsg     string
		expectedType interface{}
		expectedMsg  string
		checkFunc    func(error) bool
		extractAddr  func(error) string
	}{
		{
			name:         "LOADING error",
			errorMsg:     "LOADING Redis is loading the dataset in memory",
			expectedType: &LoadingError{},
			expectedMsg:  "LOADING Redis is loading the dataset in memory",
			checkFunc:    IsLoadingError,
		},
		{
			name:         "READONLY error",
			errorMsg:     "READONLY You can't write against a read only replica",
			expectedType: &ReadOnlyError{},
			expectedMsg:  "READONLY You can't write against a read only replica",
			checkFunc:    IsReadOnlyError,
		},
		{
			name:         "MOVED error",
			errorMsg:     "MOVED 3999 127.0.0.1:6381",
			expectedType: &MovedError{},
			expectedMsg:  "MOVED 3999 127.0.0.1:6381",
			checkFunc: func(err error) bool {
				_, ok := IsMovedError(err)
				return ok
			},
			extractAddr: func(err error) string {
				if movedErr, ok := IsMovedError(err); ok {
					return movedErr.Addr()
				}
				return ""
			},
		},
		{
			name:         "ASK error",
			errorMsg:     "ASK 3999 127.0.0.1:6381",
			expectedType: &AskError{},
			expectedMsg:  "ASK 3999 127.0.0.1:6381",
			checkFunc: func(err error) bool {
				_, ok := IsAskError(err)
				return ok
			},
			extractAddr: func(err error) string {
				if askErr, ok := IsAskError(err); ok {
					return askErr.Addr()
				}
				return ""
			},
		},
		{
			name:         "CLUSTERDOWN error",
			errorMsg:     "CLUSTERDOWN The cluster is down",
			expectedType: &ClusterDownError{},
			expectedMsg:  "CLUSTERDOWN The cluster is down",
			checkFunc:    IsClusterDownError,
		},
		{
			name:         "TRYAGAIN error",
			errorMsg:     "TRYAGAIN Multiple keys request during rehashing of slot",
			expectedType: &TryAgainError{},
			expectedMsg:  "TRYAGAIN Multiple keys request during rehashing of slot",
			checkFunc:    IsTryAgainError,
		},
		{
			name:         "MASTERDOWN error",
			errorMsg:     "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'",
			expectedType: &MasterDownError{},
			expectedMsg:  "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'",
			checkFunc:    IsMasterDownError,
		},
		{
			name:         "Max clients error",
			errorMsg:     "ERR max number of clients reached",
			expectedType: &MaxClientsError{},
			expectedMsg:  "ERR max number of clients reached",
			checkFunc:    IsMaxClientsError,
		},
		{
			name:         "NOAUTH error",
			errorMsg:     "NOAUTH Authentication required",
			expectedType: &AuthError{},
			expectedMsg:  "NOAUTH Authentication required",
			checkFunc:    IsAuthError,
		},
		{
			name:         "WRONGPASS error",
			errorMsg:     "WRONGPASS invalid username-password pair",
			expectedType: &AuthError{},
			expectedMsg:  "WRONGPASS invalid username-password pair",
			checkFunc:    IsAuthError,
		},
		{
			name:         "unauthenticated error",
			errorMsg:     "ERR unauthenticated",
			expectedType: &AuthError{},
			expectedMsg:  "ERR unauthenticated",
			checkFunc:    IsAuthError,
		},
		{
			name:         "NOPERM error",
			errorMsg:     "NOPERM this user has no permissions to run the 'flushdb' command",
			expectedType: &PermissionError{},
			expectedMsg:  "NOPERM this user has no permissions to run the 'flushdb' command",
			checkFunc:    IsPermissionError,
		},
		{
			name:         "EXECABORT error",
			errorMsg:     "EXECABORT Transaction discarded because of previous errors",
			expectedType: &ExecAbortError{},
			expectedMsg:  "EXECABORT Transaction discarded because of previous errors",
			checkFunc:    IsExecAbortError,
		},
		{
			name:         "OOM error",
			errorMsg:     "OOM command not allowed when used memory > 'maxmemory'",
			expectedType: &OOMError{},
			expectedMsg:  "OOM command not allowed when used memory > 'maxmemory'",
			checkFunc:    IsOOMError,
		},
		{
			name:         "NOREPLICAS error",
			errorMsg:     "NOREPLICAS Not enough good replicas to write",
			expectedType: &NoReplicasError{},
			expectedMsg:  "NOREPLICAS Not enough good replicas to write",
			checkFunc:    IsNoReplicasError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseTypedRedisError(tt.errorMsg)

			// Check error message is preserved
			if err.Error() != tt.expectedMsg {
				t.Errorf("Error message mismatch: got %q, want %q", err.Error(), tt.expectedMsg)
			}

			// Check error type using errors.As
			if !errors.As(err, &tt.expectedType) {
				t.Errorf("Error type mismatch: expected %T, got %T", tt.expectedType, err)
			}

			// Check using the helper function
			if tt.checkFunc != nil && !tt.checkFunc(err) {
				t.Errorf("Helper function returned false for error: %v", err)
			}

			// Check address extraction for MOVED/ASK errors
			if tt.extractAddr != nil {
				addr := tt.extractAddr(err)
				if addr == "" {
					t.Errorf("Failed to extract address from error: %v", err)
				}
			}
		})
	}
}

// TestWrappedTypedErrors tests that typed errors work correctly when wrapped
func TestWrappedTypedErrors(t *testing.T) {
	tests := []struct {
		name      string
		errorMsg  string
		checkFunc func(error) bool
	}{
		{
			name:      "Wrapped LOADING error",
			errorMsg:  "LOADING Redis is loading the dataset in memory",
			checkFunc: IsLoadingError,
		},
		{
			name:      "Wrapped READONLY error",
			errorMsg:  "READONLY You can't write against a read only replica",
			checkFunc: IsReadOnlyError,
		},
		{
			name:      "Wrapped CLUSTERDOWN error",
			errorMsg:  "CLUSTERDOWN The cluster is down",
			checkFunc: IsClusterDownError,
		},
		{
			name:      "Wrapped TRYAGAIN error",
			errorMsg:  "TRYAGAIN Multiple keys request during rehashing of slot",
			checkFunc: IsTryAgainError,
		},
		{
			name:      "Wrapped MASTERDOWN error",
			errorMsg:  "MASTERDOWN Link with MASTER is down",
			checkFunc: IsMasterDownError,
		},
		{
			name:      "Wrapped Max clients error",
			errorMsg:  "ERR max number of clients reached",
			checkFunc: IsMaxClientsError,
		},
		{
			name:      "Wrapped NOAUTH error",
			errorMsg:  "NOAUTH Authentication required",
			checkFunc: IsAuthError,
		},
		{
			name:      "Wrapped WRONGPASS error",
			errorMsg:  "WRONGPASS invalid username-password pair",
			checkFunc: IsAuthError,
		},
		{
			name:      "Wrapped unauthenticated error",
			errorMsg:  "ERR unauthenticated",
			checkFunc: IsAuthError,
		},
		{
			name:      "Wrapped NOPERM error",
			errorMsg:  "NOPERM this user has no permissions to run the 'flushdb' command",
			checkFunc: IsPermissionError,
		},
		{
			name:      "Wrapped EXECABORT error",
			errorMsg:  "EXECABORT Transaction discarded because of previous errors",
			checkFunc: IsExecAbortError,
		},
		{
			name:      "Wrapped OOM error",
			errorMsg:  "OOM command not allowed when used memory > 'maxmemory'",
			checkFunc: IsOOMError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the typed error
			err := parseTypedRedisError(tt.errorMsg)

			// Wrap it multiple times (simulating hook wrapping)
			wrappedErr := fmt.Errorf("hook error: %w", err)
			doubleWrappedErr := fmt.Errorf("another wrapper: %w", wrappedErr)

			// Check that the helper function still works with wrapped errors
			if !tt.checkFunc(doubleWrappedErr) {
				t.Errorf("Helper function failed to detect wrapped error: %v", doubleWrappedErr)
			}

			// Verify the original error message is still accessible
			if !errors.Is(doubleWrappedErr, err) {
				t.Errorf("errors.Is failed to match wrapped error")
			}
		})
	}
}

// TestMovedAndAskErrorAddressExtraction tests address extraction from MOVED/ASK errors
func TestMovedAndAskErrorAddressExtraction(t *testing.T) {
	tests := []struct {
		name         string
		errorMsg     string
		expectedAddr string
	}{
		{
			name:         "MOVED with IP address",
			errorMsg:     "MOVED 3999 127.0.0.1:6381",
			expectedAddr: "127.0.0.1:6381",
		},
		{
			name:         "MOVED with hostname",
			errorMsg:     "MOVED 3999 redis-node-1:6379",
			expectedAddr: "redis-node-1:6379",
		},
		{
			name:         "ASK with IP address",
			errorMsg:     "ASK 3999 192.168.1.100:6380",
			expectedAddr: "192.168.1.100:6380",
		},
		{
			name:         "ASK with hostname",
			errorMsg:     "ASK 3999 redis-node-2:6379",
			expectedAddr: "redis-node-2:6379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseTypedRedisError(tt.errorMsg)

			var addr string
			if movedErr, ok := IsMovedError(err); ok {
				addr = movedErr.Addr()
			} else if askErr, ok := IsAskError(err); ok {
				addr = askErr.Addr()
			} else {
				t.Fatalf("Error is neither MOVED nor ASK: %v", err)
			}

			if addr != tt.expectedAddr {
				t.Errorf("Address mismatch: got %q, want %q", addr, tt.expectedAddr)
			}

			// Test with wrapped error
			wrappedErr := fmt.Errorf("wrapped: %w", err)
			if movedErr, ok := IsMovedError(wrappedErr); ok {
				addr = movedErr.Addr()
			} else if askErr, ok := IsAskError(wrappedErr); ok {
				addr = askErr.Addr()
			} else {
				t.Fatalf("Wrapped error is neither MOVED nor ASK: %v", wrappedErr)
			}

			if addr != tt.expectedAddr {
				t.Errorf("Address mismatch in wrapped error: got %q, want %q", addr, tt.expectedAddr)
			}
		})
	}
}

// TestGenericRedisError tests that unknown Redis errors fall back to generic RedisError
func TestGenericRedisError(t *testing.T) {
	tests := []struct {
		name     string
		errorMsg string
	}{
		{
			name:     "Generic error",
			errorMsg: "ERR unknown command",
		},
		{
			name:     "WRONGTYPE error",
			errorMsg: "WRONGTYPE Operation against a key holding the wrong kind of value",
		},
		{
			name:     "BUSYKEY error",
			errorMsg: "BUSYKEY Target key name already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseTypedRedisError(tt.errorMsg)

			// Should be a generic RedisError
			if _, ok := err.(RedisError); !ok {
				t.Errorf("Expected RedisError, got %T", err)
			}

			// Should preserve the error message
			if err.Error() != tt.errorMsg {
				t.Errorf("Error message mismatch: got %q, want %q", err.Error(), tt.errorMsg)
			}

			// Should not match any typed error checks
			if IsLoadingError(err) || IsReadOnlyError(err) || IsClusterDownError(err) ||
				IsTryAgainError(err) || IsMasterDownError(err) || IsMaxClientsError(err) ||
				IsAuthError(err) || IsPermissionError(err) || IsExecAbortError(err) || IsOOMError(err) {
				t.Errorf("Generic error incorrectly matched a typed error check")
			}
		})
	}
}

// TestIsReadOnlyErrorWithLuaScriptErrors tests that READONLY errors from Lua scripts are detected.
// When a Lua script executes a write command on a read-only replica, Redis wraps the error:
// "ERR Error running script (call to f_<sha>): @user_script:<line>: -READONLY ..."
func TestIsReadOnlyErrorWithLuaScriptErrors(t *testing.T) {
	tests := []struct {
		name     string
		errorMsg string
		expected bool
	}{
		{
			name:     "standard READONLY error",
			errorMsg: "READONLY You can't write against a read only replica",
			expected: true,
		},
		{
			name:     "Lua script READONLY error",
			errorMsg: "ERR Error running script (call to f_abc123): @user_script:1: -READONLY You can't write against a read only replica.",
			expected: true,
		},
		{
			name:     "Lua script READONLY error with different line number",
			errorMsg: "ERR Error running script (call to f_def456): @user_script:42: -READONLY You can't write against a read only replica.",
			expected: true,
		},
		{
			name:     "unrelated error should not match",
			errorMsg: "ERR unknown command",
			expected: false,
		},
		{
			name:     "error with key containing READONLY should not match",
			errorMsg: "WRONGTYPE Operation against a key holding the wrong kind of value",
			expected: false,
		},
		{
			name:     "non-script error with -READONLY should not match",
			errorMsg: "ERR something-READONLY-something",
			expected: false,
		},
		{
			name:     "script error without -READONLY should not match",
			errorMsg: "ERR Error running script (call to f_abc123): @user_script:1: some other error",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseTypedRedisError(tt.errorMsg)

			result := IsReadOnlyError(err)
			if result != tt.expected {
				t.Errorf("IsReadOnlyError(%q) = %v, want %v", tt.errorMsg, result, tt.expected)
			}

			// Also test with wrapped errors
			wrappedErr := fmt.Errorf("hook wrapper: %w", err)
			wrappedResult := IsReadOnlyError(wrappedErr)
			if wrappedResult != tt.expected {
				t.Errorf("IsReadOnlyError(wrapped %q) = %v, want %v", tt.errorMsg, wrappedResult, tt.expected)
			}
		})
	}
}

// TestNewErrorConstructors tests that the New*Error constructors preserve the
// message and produce errors that are detected by their matching helper.
func TestNewErrorConstructors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		msg       string
		checkFunc func(error) bool
	}{
		{"NewLoadingError", NewLoadingError("LOADING Redis is loading"), "LOADING Redis is loading", IsLoadingError},
		{"NewReadOnlyError", NewReadOnlyError("READONLY You can't write"), "READONLY You can't write", IsReadOnlyError},
		{"NewClusterDownError", NewClusterDownError("CLUSTERDOWN The cluster is down"), "CLUSTERDOWN The cluster is down", IsClusterDownError},
		{"NewTryAgainError", NewTryAgainError("TRYAGAIN try again"), "TRYAGAIN try again", IsTryAgainError},
		{"NewMasterDownError", NewMasterDownError("MASTERDOWN master down"), "MASTERDOWN master down", IsMasterDownError},
		{"NewMaxClientsError", NewMaxClientsError("ERR max number of clients reached"), "ERR max number of clients reached", IsMaxClientsError},
		{"NewAuthError", NewAuthError("NOAUTH Authentication required"), "NOAUTH Authentication required", IsAuthError},
		{"NewPermissionError", NewPermissionError("NOPERM no permission"), "NOPERM no permission", IsPermissionError},
		{"NewExecAbortError", NewExecAbortError("EXECABORT Transaction discarded"), "EXECABORT Transaction discarded", IsExecAbortError},
		{"NewOOMError", NewOOMError("OOM out of memory"), "OOM out of memory", IsOOMError},
		{"NewNoReplicasError", NewNoReplicasError("NOREPLICAS not enough replicas"), "NOREPLICAS not enough replicas", IsNoReplicasError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.msg {
				t.Errorf("Error() = %q, want %q", tt.err.Error(), tt.msg)
			}
			if !tt.checkFunc(tt.err) {
				t.Errorf("Helper function did not detect constructed error %q", tt.msg)
			}
		})
	}

	// MOVED/ASK constructors also carry the target address.
	m := NewMovedError("MOVED 3999 127.0.0.1:6381", "127.0.0.1:6381")
	if m.Error() != "MOVED 3999 127.0.0.1:6381" || m.Addr() != "127.0.0.1:6381" {
		t.Errorf("NewMovedError: Error() = %q, Addr() = %q", m.Error(), m.Addr())
	}
	a := NewAskError("ASK 3999 127.0.0.1:6382", "127.0.0.1:6382")
	if a.Error() != "ASK 3999 127.0.0.1:6382" || a.Addr() != "127.0.0.1:6382" {
		t.Errorf("NewAskError: Error() = %q, Addr() = %q", a.Error(), a.Addr())
	}
}

// TestExtractAddrWithoutSpace tests that extractAddr returns an empty string
// for a message without any space (nothing to extract).
func TestExtractAddrWithoutSpace(t *testing.T) {
	if got := extractAddr("MOVED"); got != "" {
		t.Errorf("extractAddr(%q) = %q, want empty string", "MOVED", got)
	}
}

// TestErrorCheckersWithNilError tests that all error check helpers return false for nil errors
func TestErrorCheckersWithNilError(t *testing.T) {
	predicates := []struct {
		name string
		fn   func(error) bool
	}{
		{"IsLoadingError", IsLoadingError},
		{"IsReadOnlyError", IsReadOnlyError},
		{"IsClusterDownError", IsClusterDownError},
		{"IsTryAgainError", IsTryAgainError},
		{"IsMasterDownError", IsMasterDownError},
		{"IsMaxClientsError", IsMaxClientsError},
		{"IsAuthError", IsAuthError},
		{"IsPermissionError", IsPermissionError},
		{"IsExecAbortError", IsExecAbortError},
		{"IsOOMError", IsOOMError},
		{"IsNoReplicasError", IsNoReplicasError},
	}

	for _, tt := range predicates {
		t.Run(tt.name, func(t *testing.T) {
			if tt.fn(nil) {
				t.Errorf("%s(nil) = true, want false", tt.name)
			}
		})
	}

	if _, ok := IsMovedError(nil); ok {
		t.Error("IsMovedError(nil) = true, want false")
	}
	if _, ok := IsAskError(nil); ok {
		t.Error("IsAskError(nil) = true, want false")
	}
}

// TestErrorCheckersWithPlainRedisError tests that the error check helpers also
// match plain RedisError string errors (not the typed errors).
func TestErrorCheckersWithPlainRedisError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		checkFunc func(error) bool
	}{
		{"LOADING", RedisError("LOADING Redis is loading"), IsLoadingError},
		{"READONLY", RedisError("READONLY You can't write"), IsReadOnlyError},
		{"CLUSTERDOWN", RedisError("CLUSTERDOWN The cluster is down"), IsClusterDownError},
		{"TRYAGAIN", RedisError("TRYAGAIN try again"), IsTryAgainError},
		{"MASTERDOWN", RedisError("MASTERDOWN master down"), IsMasterDownError},
		{"Max clients", RedisError("ERR max number of clients reached"), IsMaxClientsError},
		{"NOAUTH", RedisError("NOAUTH Authentication required"), IsAuthError},
		{"WRONGPASS", RedisError("WRONGPASS invalid password"), IsAuthError},
		{"NOPERM", RedisError("NOPERM no permission"), IsPermissionError},
		{"EXECABORT", RedisError("EXECABORT Transaction discarded"), IsExecAbortError},
		{"OOM", RedisError("OOM out of memory"), IsOOMError},
		{"NOREPLICAS", RedisError("NOREPLICAS not enough replicas"), IsNoReplicasError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.checkFunc(tt.err) {
				t.Errorf("Helper function did not match plain RedisError %q", tt.err.Error())
			}
		})
	}

	// MOVED/ASK plain string errors are parsed, including the address.
	if m, ok := IsMovedError(RedisError("MOVED 3999 127.0.0.1:6381")); !ok || m.Addr() != "127.0.0.1:6381" {
		t.Errorf("IsMovedError(plain string) = %v, %v", m, ok)
	}
	if _, ok := IsMovedError(RedisError("ERR something else")); ok {
		t.Error("IsMovedError(non-MOVED plain string) = true, want false")
	}
	if a, ok := IsAskError(RedisError("ASK 3999 127.0.0.1:6381")); !ok || a.Addr() != "127.0.0.1:6381" {
		t.Errorf("IsAskError(plain string) = %v, %v", a, ok)
	}
	if _, ok := IsAskError(RedisError("ERR something else")); ok {
		t.Error("IsAskError(non-ASK plain string) = true, want false")
	}
}

// TestBackwardCompatibility tests that error messages remain unchanged
func TestBackwardCompatibility(t *testing.T) {
	// This test ensures that the error messages are exactly the same as before
	// to maintain backward compatibility with code that checks error messages
	tests := []struct {
		input    string
		expected string
	}{
		{"LOADING Redis is loading the dataset in memory", "LOADING Redis is loading the dataset in memory"},
		{"READONLY You can't write against a read only replica", "READONLY You can't write against a read only replica"},
		{"MOVED 3999 127.0.0.1:6381", "MOVED 3999 127.0.0.1:6381"},
		{"ASK 3999 127.0.0.1:6381", "ASK 3999 127.0.0.1:6381"},
		{"CLUSTERDOWN The cluster is down", "CLUSTERDOWN The cluster is down"},
		{"TRYAGAIN Multiple keys request during rehashing of slot", "TRYAGAIN Multiple keys request during rehashing of slot"},
		{"MASTERDOWN Link with MASTER is down", "MASTERDOWN Link with MASTER is down"},
		{"ERR max number of clients reached", "ERR max number of clients reached"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			err := parseTypedRedisError(tt.input)
			if err.Error() != tt.expected {
				t.Errorf("Error message changed! Got %q, want %q", err.Error(), tt.expected)
			}
		})
	}
}
