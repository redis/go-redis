package logs

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// validateJSONInLogMessage extracts and validates JSON from a log message
func validateJSONInLogMessage(t *testing.T, logMessage string, expectedData map[string]interface{}) {
	t.Helper()

	// Extract JSON from the log message
	re := regexp.MustCompile(`(\{.*\})$`)
	matches := re.FindStringSubmatch(logMessage)
	if len(matches) < 2 {
		t.Errorf("No JSON found in log message: %s", logMessage)
		return
	}

	jsonStr := matches[1]
	var actualData map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &actualData); err != nil {
		t.Errorf("Invalid JSON in log message: %s, error: %v", jsonStr, err)
		return
	}

	// Compare the data
	if len(actualData) != len(expectedData) {
		t.Errorf("Expected %d keys, got %d. Expected: %v, Got: %v", len(expectedData), len(actualData), expectedData, actualData)
		return
	}

	for key, expectedValue := range expectedData {
		actualValue, exists := actualData[key]
		if !exists {
			t.Errorf("Expected key %q not found in JSON", key)
			continue
		}

		if actualValue != expectedValue {
			t.Errorf("For key %q: expected %v (%T), got %v (%T)", key, expectedValue, expectedValue, actualValue, actualValue)
		}
	}
}

func TestHandoffLogFunctions(t *testing.T) {
	t.Run("HandoffStarted", func(t *testing.T) {
		result := HandoffStarted(LogLevelDebug, 123, "localhost:6379")
		if !strings.Contains(result, "conn[123] handoff started to localhost:6379") {
			t.Errorf("Expected message to contain handoff started text, got: %s", result)
		}
		validateJSONInLogMessage(t, result, map[string]interface{}{
			"connID":   float64(123),
			"endpoint": "localhost:6379",
		})
	})

	t.Run("HandoffFailed", func(t *testing.T) {
		err := errors.New("connection refused")
		result := HandoffFailed(LogLevelDebug, 123, "localhost:6379", 2, 3, err)
		if !strings.Contains(result, "conn[123] handoff failed to localhost:6379 (attempt 2/3): connection refused") {
			t.Errorf("Expected message to contain handoff failed text, got: %s", result)
		}
		validateJSONInLogMessage(t, result, map[string]interface{}{
			"connID":      float64(123),
			"endpoint":    "localhost:6379",
			"attempt":     float64(2),
			"maxAttempts": float64(3),
			"error":       "connection refused",
		})
	})

	t.Run("HandoffSucceeded", func(t *testing.T) {
		result := HandoffSucceeded(LogLevelDebug, 123, "localhost:6379")
		if !strings.Contains(result, "conn[123] handoff succeeded to localhost:6379") {
			t.Errorf("Expected message to contain handoff succeeded text, got: %s", result)
		}
		validateJSONInLogMessage(t, result, map[string]interface{}{
			"connID":   float64(123),
			"endpoint": "localhost:6379",
		})
	})
}

func TestTimeoutLogFunctions(t *testing.T) {
	t.Run("RelaxedTimeoutDueToNotification", func(t *testing.T) {
		result := RelaxedTimeoutDueToNotification(LogLevelDebug, 123, "MIGRATING", "10s")
		expected := "conn[123] applying relaxed timeout due to notification MIGRATING (10s) {\"connID\":123,\"notificationType\":\"MIGRATING\",\"timeout\":\"10s\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("UnrelaxedTimeout", func(t *testing.T) {
		result := UnrelaxedTimeout(LogLevelDebug, 123)
		expected := "conn[123] clearing relaxed timeout {\"connID\":123}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("UnrelaxedTimeoutAfterDeadline", func(t *testing.T) {
		result := UnrelaxedTimeoutAfterDeadline(LogLevelDebug, 123)
		expected := "conn[123] clearing relaxed timeout after deadline {\"connID\":123}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestHandoffQueueLogFunctions(t *testing.T) {
	t.Run("HandoffQueueFull", func(t *testing.T) {
		result := HandoffQueueFull(10, 10)
		expected := "handoff queue is full (10/10), cannot queue new handoff requests - consider increasing HandoffQueueSize or MaxWorkers in configuration {queueLen:10,queueCap:10}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("FailedToQueueHandoff", func(t *testing.T) {
		err := errors.New("queue full")
		result := FailedToQueueHandoff(123, err)
		expected := "conn[123] failed to queue handoff: queue full {connID:123,error:\"queue full\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("ConnectionAlreadyMarkedForHandoff", func(t *testing.T) {
		result := ConnectionAlreadyMarkedForHandoff(123)
		expected := "conn[123] already marked for handoff {connID:123}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("ReachedMaxHandoffRetries", func(t *testing.T) {
		result := ReachedMaxHandoffRetries(123, "localhost:6379", 3)
		expected := "conn[123] reached max handoff retries to localhost:6379 (max retries: 3) {connID:123,endpoint:\"localhost:6379\",maxRetries:3}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestNotificationLogFunctions(t *testing.T) {
	t.Run("ProcessingNotification", func(t *testing.T) {
		result := ProcessingNotification(LogLevelDebug, 123, 1, "MOVING", []interface{}{"MOVING", "slot_data"})
		if !strings.Contains(result, "conn[123] seqId[1] processing notification MOVING:") {
			t.Errorf("Expected message to contain processing notification, got %q", result)
		}
	})

	t.Run("ProcessingNotificationFailed", func(t *testing.T) {
		err := errors.New("invalid notification")
		result := ProcessingNotificationFailed(LogLevelDebug, 123, "MOVING", err, []interface{}{"MOVING"})
		if !strings.Contains(result, "conn[123] failed to process notification MOVING: invalid notification") {
			t.Errorf("Expected message to contain failed processing, got %q", result)
		}
	})

	t.Run("ProcessingNotificationSucceeded", func(t *testing.T) {
		result := ProcessingNotificationSucceeded(LogLevelDebug, 123, "MOVING")
		expected := "conn[123] processed notification successfully MOVING {\"connID\":123,\"notificationType\":\"MOVING\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestMovingOperationLogFunctions(t *testing.T) {
	t.Run("DuplicateMovingOperation", func(t *testing.T) {
		result := DuplicateMovingOperation(LogLevelDebug, 123, "localhost:6379", 456)
		expected := "conn[123] duplicate MOVING operation ignored for localhost:6379 (seqID: 456) {\"connID\":123,\"endpoint\":\"localhost:6379\",\"seqID\":456}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("TrackingMovingOperation", func(t *testing.T) {
		result := TrackingMovingOperation(LogLevelDebug, 123, "localhost:6379", 456)
		expected := "conn[123] tracking MOVING operation for localhost:6379 (seqID: 456) {\"connID\":123,\"endpoint\":\"localhost:6379\",\"seqID\":456}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("UntrackingMovingOperation", func(t *testing.T) {
		result := UntrackingMovingOperation(LogLevelDebug, 123, 456)
		expected := "conn[123] untracking MOVING operation (seqID: 456) {\"connID\":123,\"seqID\":456}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("OperationNotTracked", func(t *testing.T) {
		result := OperationNotTracked(LogLevelDebug, 123, 456)
		expected := "conn[123] operation not tracked (seqID: 456) {\"connID\":123,\"seqID\":456}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestConditionalJSONLogging(t *testing.T) {
	t.Run("DebugLevel_IncludesJSON", func(t *testing.T) {
		result := HandoffStarted(LogLevelDebug, 123, "localhost:6379")
		if !strings.Contains(result, "{\"connID\":123,\"endpoint\":\"localhost:6379\"}") {
			t.Errorf("Expected JSON to be included at Debug level, got: %s", result)
		}
	})

	t.Run("ErrorLevel_ExcludesJSON", func(t *testing.T) {
		result := HandoffStarted(LogLevelError, 123, "localhost:6379")
		if strings.Contains(result, "{") {
			t.Errorf("Expected JSON to be excluded at Error level, got: %s", result)
		}
		expected := "conn[123] handoff started to localhost:6379"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("InfoLevel_ExcludesJSON", func(t *testing.T) {
		result := DuplicateMovingOperation(LogLevelInfo, 123, "localhost:6379", 456)
		if strings.Contains(result, "{") {
			t.Errorf("Expected JSON to be excluded at Info level, got: %s", result)
		}
		expected := "conn[123] duplicate MOVING operation ignored for localhost:6379 (seqID: 456)"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestConnectionPoolLogFunctions(t *testing.T) {
	t.Run("RemovingConnectionFromPool", func(t *testing.T) {
		err := errors.New("connection timeout")
		result := RemovingConnectionFromPool(123, err)
		expected := "conn[123] removing connection from pool due to: connection timeout {connID:123,reason:\"connection timeout\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("NoPoolProvidedCannotRemove", func(t *testing.T) {
		err := errors.New("handoff failed")
		result := NoPoolProvidedCannotRemove(123, err)
		expected := "conn[123] no pool provided, cannot remove connection, closing it due to: handoff failed {connID:123,reason:\"handoff failed\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestCircuitBreakerLogFunctions(t *testing.T) {
	t.Run("CircuitBreakerOpen", func(t *testing.T) {
		result := CircuitBreakerOpen(123, "localhost:6379")
		expected := "conn[123] circuit breaker is open, failing fast for localhost:6379 {connID:123,endpoint:\"localhost:6379\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("CircuitBreakerHalfOpen", func(t *testing.T) {
		result := CircuitBreakerHalfOpen("localhost:6379")
		expected := "circuit breaker is half-open, testing if endpoint recovered for localhost:6379 {endpoint:\"localhost:6379\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

	t.Run("FailingDueToCircuitBreaker", func(t *testing.T) {
		result := FailingDueToCircuitBreaker(123, "localhost:6379")
		expected := "conn[123] failing due to circuit breaker for localhost:6379 {connID:123,endpoint:\"localhost:6379\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestSystemLogFunctions(t *testing.T) {
	t.Run("ShuttingDown", func(t *testing.T) {
		result := ShuttingDown()
		expected := "shutting down {}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestConnectionStateLogFunctions(t *testing.T) {
	t.Run("ConnectionInInvalidStateForHandoff", func(t *testing.T) {
		result := ConnectionInInvalidStateForHandoff(123, "closed")
		expected := "conn[123] connection is in invalid state for handoff (state: closed) {connID:123,state:\"closed\"}"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})

}

// TestAllFunctionsHaveStructuredFormat ensures all log functions have structured format
func TestAllFunctionsHaveStructuredFormat(t *testing.T) {
	testCases := []struct {
		name     string
		function func() string
	}{
		{"HandoffStarted", func() string { return HandoffStarted(LogLevelDebug, 1, "test") }},
		{"HandoffFailed", func() string { return HandoffFailed(LogLevelDebug, 1, "test", 1, 3, errors.New("test")) }},
		{"HandoffSucceeded", func() string { return HandoffSucceeded(LogLevelDebug, 1, "test") }},
		{"RelaxedTimeoutDueToNotification", func() string { return RelaxedTimeoutDueToNotification(LogLevelDebug, 1, "TEST", "5s") }},

		{"UnrelaxedTimeout", func() string { return UnrelaxedTimeout(LogLevelDebug, 1) }},
		{"UnrelaxedTimeoutAfterDeadline", func() string { return UnrelaxedTimeoutAfterDeadline(LogLevelDebug, 1) }},
		{"HandoffQueueFull", func() string { return HandoffQueueFull(1, 10) }},
		{"FailedToQueueHandoff", func() string { return FailedToQueueHandoff(1, errors.New("test")) }},
		{"ConnectionAlreadyMarkedForHandoff", func() string { return ConnectionAlreadyMarkedForHandoff(1) }},
		{"ReachedMaxHandoffRetries", func() string { return ReachedMaxHandoffRetries(1, "test", 3) }},
		{"ProcessingNotification", func() string { return ProcessingNotification(LogLevelDebug, 1, 2, "TEST", "data") }},
		{"ProcessingNotificationFailed", func() string {
			return ProcessingNotificationFailed(LogLevelDebug, 1, "TEST", errors.New("test"), "data")
		}},
		{"ProcessingNotificationSucceeded", func() string { return ProcessingNotificationSucceeded(LogLevelDebug, 1, "TEST") }},
		{"DuplicateMovingOperation", func() string { return DuplicateMovingOperation(LogLevelDebug, 1, "test", 1) }},
		{"TrackingMovingOperation", func() string { return TrackingMovingOperation(LogLevelDebug, 1, "test", 1) }},
		{"UntrackingMovingOperation", func() string { return UntrackingMovingOperation(LogLevelDebug, 1, 1) }},
		{"OperationNotTracked", func() string { return OperationNotTracked(LogLevelDebug, 1, 1) }},
		{"RemovingConnectionFromPool", func() string { return RemovingConnectionFromPool(1, errors.New("test")) }},
		{"NoPoolProvidedCannotRemove", func() string { return NoPoolProvidedCannotRemove(1, errors.New("test")) }},
		{"CircuitBreakerOpen", func() string { return CircuitBreakerOpen(1, "test") }},
		{"CircuitBreakerHalfOpen", func() string { return CircuitBreakerHalfOpen("test") }},
		{"FailingDueToCircuitBreaker", func() string { return FailingDueToCircuitBreaker(1, "test") }},
		{"ShuttingDown", func() string { return ShuttingDown() }},
		{"ConnectionInInvalidStateForHandoff", func() string { return ConnectionInInvalidStateForHandoff(1, "test") }},

		{"HandoffStarted", func() string { return HandoffStarted(LogLevelDebug, 1, "localhost:6379") }},
		{"HandoffFailed", func() string { return HandoffFailed(LogLevelDebug, 1, "localhost:6379", 1, 3, errors.New("test")) }},
		{"ConnectionNotMarkedForHandoff", func() string { return ConnectionNotMarkedForHandoff(1) }},
		{"HandoffRetryAttempt", func() string { return HandoffRetryAttempt(1, 2, "new", "old") }},
		{"CannotQueueHandoffForRetry", func() string { return CannotQueueHandoffForRetry(errors.New("test")) }},

		// Error functions
		{"ConnectionMarkedForHandoffError", func() string { return ConnectionMarkedForHandoffError(1) }},
		{"ConnectionInvalidHandoffStateError", func() string { return ConnectionInvalidHandoffStateError(1, "invalid") }},
		{"ShutdownError", func() string { return ShutdownError() }},
		{"CircuitBreakerOpenError", func() string { return CircuitBreakerOpenError("localhost:6379") }},
		{"InvalidRelaxedTimeoutError", func() string { return InvalidRelaxedTimeoutError() }},
		{"InvalidHandoffTimeoutError", func() string { return InvalidHandoffTimeoutError() }},
		{"InvalidHandoffWorkersError", func() string { return InvalidHandoffWorkersError() }},
		{"InvalidHandoffQueueSizeError", func() string { return InvalidHandoffQueueSizeError() }},
		{"InvalidPostHandoffRelaxedDurationError", func() string { return InvalidPostHandoffRelaxedDurationError() }},
		{"InvalidEndpointTypeError", func() string { return InvalidEndpointTypeError() }},
		{"InvalidMaintNotificationsError", func() string { return InvalidMaintNotificationsError() }},
		{"InvalidHandoffRetriesError", func() string { return InvalidHandoffRetriesError() }},
		{"InvalidClientError", func() string { return InvalidClientError() }},
		{"InvalidNotificationError", func() string { return InvalidNotificationError() }},
		{"MaxHandoffRetriesReachedError", func() string { return MaxHandoffRetriesReachedError() }},
		{"HandoffQueueFullError", func() string { return HandoffQueueFullError() }},
		{"InvalidCircuitBreakerFailureThresholdError", func() string { return InvalidCircuitBreakerFailureThresholdError() }},
		{"InvalidCircuitBreakerResetTimeoutError", func() string { return InvalidCircuitBreakerResetTimeoutError() }},
		{"InvalidCircuitBreakerMaxRequestsError", func() string { return InvalidCircuitBreakerMaxRequestsError() }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.function()
			if !strings.Contains(result, "{") || !strings.Contains(result, "}") {
				t.Errorf("Function %s should return a message with structured data (containing { and }), got: %q", tc.name, result)
			}
		})
	}
}

func TestManagerAndHandlerLogFunctions(t *testing.T) {
	tests := []struct {
		name     string
		function func() string
		expected string
	}{
		{
			name:     "ManagerNotInitialized",
			function: func() string { return ManagerNotInitialized() },
			expected: "manager not initialized {}",
		},
		{
			name:     "FailedToRegisterHandler",
			function: func() string { return FailedToRegisterHandler("MOVING", fmt.Errorf("test error")) },
			expected: "failed to register handler for MOVING: test error {notificationType:\"MOVING\",error:\"test error\"}",
		},
		{
			name:     "ConnectionNotMarkedForHandoffError",
			function: func() string { return ConnectionNotMarkedForHandoffError(123) },
			expected: "conn[123] is not marked for handoff",
		},
		{
			name:     "WorkerPanicRecovered",
			function: func() string { return WorkerPanicRecovered("test panic") },
			expected: "worker panic recovered: test panic {panic:\"test panic\"}",
		},
		{
			name:     "WorkerExitingDueToInactivityTimeout",
			function: func() string { return WorkerExitingDueToInactivityTimeout("30s") },
			expected: "worker exiting due to inactivity timeout (30s) {timeout:\"30s\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.function()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestInvalidNotification(t *testing.T) {
	tests := []struct {
		name             string
		notificationType string
		notification     interface{}
		expected         string
	}{
		{
			name:             "MOVING notification",
			notificationType: "MOVING",
			notification:     []interface{}{"MOVING", 123},
			expected:         "invalid MOVING notification: [MOVING 123] {notificationType:\"MOVING\",notification:\"[MOVING 123]\"}",
		},
		{
			name:             "MIGRATING notification",
			notificationType: "MIGRATING",
			notification:     []interface{}{"MIGRATING"},
			expected:         "invalid MIGRATING notification: [MIGRATING] {notificationType:\"MIGRATING\",notification:\"[MIGRATING]\"}",
		},
		{
			name:             "FAILED_OVER notification",
			notificationType: "FAILED_OVER",
			notification:     "invalid",
			expected:         "invalid FAILED_OVER notification: invalid {notificationType:\"FAILED_OVER\",notification:\"invalid\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InvalidNotification(tt.notificationType, tt.notification)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestExtractDataFromLogMessage(t *testing.T) {
	tests := []struct {
		name     string
		message  string
		expected map[string]interface{}
	}{
		{
			name:    "Simple handoff message",
			message: "conn[123] handoff started to localhost:6379 {\"connID\":123,\"endpoint\":\"localhost:6379\"}",
			expected: map[string]interface{}{
				"connID":   float64(123), // JSON numbers are float64 by default
				"endpoint": "localhost:6379",
			},
		},
		{
			name:    "Complex handoff failed message",
			message: "conn[123] handoff failed to localhost:6379 (attempt 2/3): connection refused {\"connID\":123,\"endpoint\":\"localhost:6379\",\"attempt\":2,\"maxAttempts\":3,\"error\":\"connection refused\"}",
			expected: map[string]interface{}{
				"connID":      float64(123),
				"endpoint":    "localhost:6379",
				"attempt":     float64(2),
				"maxAttempts": float64(3),
				"error":       "connection refused",
			},
		},
		{
			name:    "Timeout message with duration",
			message: "conn[123] applying relaxed timeout due to notification MIGRATING (10s) {\"connID\":123,\"notificationType\":\"MIGRATING\",\"timeout\":\"10s\"}",
			expected: map[string]interface{}{
				"connID":           float64(123),
				"notificationType": "MIGRATING",
				"timeout":          "10s",
			},
		},
		{
			name:    "Circuit breaker with numeric values",
			message: "circuit breaker opened for endpoint localhost:6379 after 5 failures {\"endpoint\":\"localhost:6379\",\"failures\":5}",
			expected: map[string]interface{}{
				"endpoint": "localhost:6379",
				"failures": float64(5),
			},
		},
		{
			name:    "Queue message with capacity",
			message: "handoff queue is full (10/10), cannot queue new handoff requests {\"queueLen\":10,\"queueCap\":10}",
			expected: map[string]interface{}{
				"queueLen": float64(10),
				"queueCap": float64(10),
			},
		},
		{
			name:     "Empty data section",
			message:  "shutting down {}",
			expected: map[string]interface{}{},
		},
		{
			name:     "No data section",
			message:  "some message without structured data",
			expected: map[string]interface{}{},
		},
		{
			name:    "Message with float values",
			message: "conn[123] scheduling handoff in 2.5 seconds {\"connID\":123,\"seconds\":2.5}",
			expected: map[string]interface{}{
				"connID":  float64(123),
				"seconds": 2.5,
			},
		},
		{
			name:    "Message with boolean values",
			message: "connection state changed {\"connID\":123,\"usable\":true,\"marked\":false}",
			expected: map[string]interface{}{
				"connID": float64(123),
				"usable": true,
				"marked": false,
			},
		},
		{
			name:    "Message with quoted error containing commas",
			message: "operation failed {\"connID\":123,\"error\":\"connection failed, timeout occurred, retry needed\"}",
			expected: map[string]interface{}{
				"connID": float64(123),
				"error":  "connection failed, timeout occurred, retry needed",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractDataFromLogMessage(tt.message)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d keys, got %d. Expected: %v, Got: %v", len(tt.expected), len(result), tt.expected, result)
				return
			}

			for key, expectedValue := range tt.expected {
				actualValue, exists := result[key]
				if !exists {
					t.Errorf("Expected key %q not found in result", key)
					continue
				}

				if actualValue != expectedValue {
					t.Errorf("For key %q: expected %v (%T), got %v (%T)", key, expectedValue, expectedValue, actualValue, actualValue)
				}
			}
		})
	}
}
