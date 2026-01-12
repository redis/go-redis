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
		expectedType any
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
