package logs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/maintnotifications/logs"
)

// withLogLevel sets the global log level for the duration of fn and restores it.
func withLogLevel(level internal.LogLevelT, fn func()) {
	prev := internal.LogLevel
	internal.LogLevel = level
	defer func() { internal.LogLevel = prev }()
	fn()
}

func TestLogMessages_AllBuilders(t *testing.T) {
	err := errors.New("boom")

	// Each builder must return a non-empty string. We exercise them under both
	// debug (JSON appended) and error (plain) log levels.
	builders := []func() string{
		func() string { return logs.HandoffStarted(1, "ep") },
		func() string { return logs.HandoffFailed(1, "ep", 2, 3, err) },
		func() string { return logs.HandoffSucceeded(1, "ep") },
		func() string { return logs.RelaxedTimeoutDueToNotification(1, "MOVING", "5s") },
		func() string { return logs.UnrelaxedTimeout(1) },
		func() string { return logs.UnrelaxedTimeoutAfterDeadline(1) },
		func() string { return logs.HandoffQueueFull(5, 10) },
		func() string { return logs.FailedToQueueHandoff(1, err) },
		func() string { return logs.FailedToMarkForHandoff(1, err) },
		func() string { return logs.FailedToDialNewEndpoint(1, "ep", err) },
		func() string { return logs.ReachedMaxHandoffRetries(1, "ep", 3) },
		func() string { return logs.ProcessingNotification(1, 2, "MOVING", "n") },
		func() string { return logs.ProcessingNotificationFailed(1, "MOVING", err, "n") },
		func() string { return logs.ProcessingNotificationSucceeded(1, "MOVING") },
		func() string { return logs.DuplicateMovingOperation(1, "ep", 2) },
		func() string { return logs.TrackingMovingOperation(1, "ep", 2) },
		func() string { return logs.UntrackingMovingOperation(1, 2) },
		func() string { return logs.OperationNotTracked(1, 2) },
		func() string { return logs.RemovingConnectionFromPool(1, err) },
		func() string { return logs.RemovingConnectionFromPool(1, nil) },
		func() string { return logs.NoPoolProvidedCannotRemove(1, err) },
		func() string { return logs.NoPoolProvidedCannotRemove(1, nil) },
		func() string { return logs.CircuitBreakerOpen(1, "ep") },
		func() string { return logs.ConnectionNotMarkedForHandoff(1) },
		func() string { return logs.ConnectionNotMarkedForHandoffError(1) },
		func() string { return logs.HandoffRetryAttempt(1, 2, "new", "old") },
		func() string { return logs.CannotQueueHandoffForRetry(err) },
		func() string { return logs.InvalidNotificationFormat("n") },
		func() string { return logs.InvalidNotificationTypeFormat("t") },
		func() string { return logs.InvalidNotification("MOVING", "n") },
		func() string { return logs.InvalidSeqIDInMovingNotification("x") },
		func() string { return logs.InvalidTimeSInMovingNotification("x") },
		func() string { return logs.InvalidNewEndpointInMovingNotification("x") },
		func() string { return logs.NoConnectionInHandlerContext("MOVING") },
		func() string { return logs.InvalidConnectionTypeInHandlerContext("MOVING", 1, 2) },
		func() string { return logs.SchedulingHandoffToCurrentEndpoint(1, 1.5) },
		func() string { return logs.ManagerNotInitialized() },
		func() string { return logs.FailedToRegisterHandler("MOVING", err) },
		func() string { return logs.ShutdownError() },
		func() string { return logs.InvalidRelaxedTimeoutError() },
		func() string { return logs.InvalidHandoffTimeoutError() },
		func() string { return logs.InvalidHandoffWorkersError() },
		func() string { return logs.InvalidHandoffQueueSizeError() },
		func() string { return logs.InvalidPostHandoffRelaxedDurationError() },
		func() string { return logs.InvalidEndpointTypeError() },
		func() string { return logs.InvalidMaintNotificationsError() },
		func() string { return logs.InvalidHandoffRetriesError() },
		func() string { return logs.InvalidClientError() },
		func() string { return logs.InvalidNotificationError() },
		func() string { return logs.MaxHandoffRetriesReachedError() },
		func() string { return logs.HandoffQueueFullError() },
		func() string { return logs.InvalidCircuitBreakerFailureThresholdError() },
		func() string { return logs.InvalidCircuitBreakerResetTimeoutError() },
		func() string { return logs.InvalidCircuitBreakerMaxRequestsError() },
		func() string { return logs.DebugLoggingEnabled() },
		func() string { return logs.ConfigDebug(struct{ A int }{1}) },
		func() string { return logs.WorkerExitingDueToShutdown() },
		func() string { return logs.WorkerExitingDueToShutdownWhileProcessing() },
		func() string { return logs.WorkerPanicRecovered("panic") },
		func() string { return logs.WorkerExitingDueToInactivityTimeout("5s") },
		func() string { return logs.ApplyingRelaxedTimeoutDueToPostHandoff(1, "5s", "later") },
		func() string { return logs.MetricsHookProcessingNotification("MOVING", 1) },
		func() string { return logs.MetricsHookRecordedError("MOVING", 1, err) },
		func() string { return logs.MarkedForHandoff(1) },
		func() string { return logs.CircuitBreakerTransitioningToHalfOpen("ep") },
		func() string { return logs.CircuitBreakerOpened("ep", 5) },
		func() string { return logs.CircuitBreakerReopened("ep") },
		func() string { return logs.CircuitBreakerClosed("ep", 3) },
		func() string { return logs.CircuitBreakerCleanup(1, 2) },
		func() string { return logs.InvalidSeqIDInSMigratingNotification("x") },
		func() string { return logs.InvalidSeqIDInSMigratedNotification("x") },
		func() string { return logs.TriggeringClusterStateReload(1, "host:1", []string{"0-100"}) },
	}

	for _, level := range []internal.LogLevelT{internal.LogLevelDebug, internal.LogLevelError} {
		withLogLevel(level, func() {
			for i, b := range builders {
				if got := b(); got == "" {
					t.Errorf("builder[%d] returned empty string at level %s", i, level)
				}
			}
		})
	}
}

func TestExtractDataFromLogMessage(t *testing.T) {
	withLogLevel(internal.LogLevelDebug, func() {
		msg := logs.HandoffStarted(42, "localhost:6379")
		data := logs.ExtractDataFromLogMessage(msg)
		if data["endpoint"] != "localhost:6379" {
			t.Errorf("extracted endpoint = %v", data["endpoint"])
		}
		// connID is unmarshaled as float64 from JSON.
		if got, ok := data["connID"].(float64); !ok || got != 42 {
			t.Errorf("extracted connID = %v (%T)", data["connID"], data["connID"])
		}
	})

	// Message without JSON suffix yields an empty map.
	if data := logs.ExtractDataFromLogMessage("plain message"); len(data) != 0 {
		t.Errorf("expected empty map, got %v", data)
	}

	// Malformed JSON suffix yields an empty map.
	if data := logs.ExtractDataFromLogMessage("msg {not json}"); len(data) != 0 {
		t.Errorf("expected empty map for malformed JSON, got %v", data)
	}

	// Sanity: builder output contains the human-readable prefix.
	withLogLevel(internal.LogLevelError, func() {
		if !strings.Contains(logs.HandoffStarted(1, "ep"), "handoff started") {
			t.Error("expected human-readable message prefix")
		}
	})
}
