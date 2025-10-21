package push

import (
	"context"
	"testing"
)

// TestProcessorCreation tests processor creation and initialization
func TestProcessorCreation(t *testing.T) {
	t.Run("NewProcessor", func(t *testing.T) {
		processor := NewProcessor()
		if processor == nil {
			t.Fatal("NewProcessor should not return nil")
		}
		if processor.registry == nil {
			t.Error("Processor should have a registry")
		}
	})

	t.Run("NewVoidProcessor", func(t *testing.T) {
		voidProcessor := NewVoidProcessor()
		if voidProcessor == nil {
			t.Fatal("NewVoidProcessor should not return nil")
		}
	})
}

// TestProcessorHandlerManagement tests handler registration and retrieval
func TestProcessorHandlerManagement(t *testing.T) {
	processor := NewProcessor()
	handler := &UnitTestHandler{name: "test-handler"}

	t.Run("RegisterHandler", func(t *testing.T) {
		err := processor.RegisterHandler("TEST", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		// Verify handler is registered
		retrievedHandler := processor.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})

	t.Run("RegisterProtectedHandler", func(t *testing.T) {
		protectedHandler := &UnitTestHandler{name: "protected-handler"}
		err := processor.RegisterHandler("PROTECTED", protectedHandler, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error for protected handler: %v", err)
		}

		// Verify handler is registered
		retrievedHandler := processor.GetHandler("PROTECTED")
		if retrievedHandler != protectedHandler {
			t.Error("GetHandler should return the protected handler")
		}
	})

	t.Run("GetNonExistentHandler", func(t *testing.T) {
		handler := processor.GetHandler("NONEXISTENT")
		if handler != nil {
			t.Error("GetHandler should return nil for non-existent handler")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		err := processor.UnregisterHandler("TEST")
		if err != nil {
			t.Errorf("UnregisterHandler should not error: %v", err)
		}

		// Verify handler is removed
		retrievedHandler := processor.GetHandler("TEST")
		if retrievedHandler != nil {
			t.Error("GetHandler should return nil after unregistering")
		}
	})

	t.Run("UnregisterProtectedHandler", func(t *testing.T) {
		err := processor.UnregisterHandler("PROTECTED")
		if err == nil {
			t.Error("UnregisterHandler should error for protected handler")
		}

		// Verify handler is still there
		retrievedHandler := processor.GetHandler("PROTECTED")
		if retrievedHandler == nil {
			t.Error("Protected handler should not be removed")
		}
	})
}

// TestVoidProcessorBehavior tests void processor behavior
func TestVoidProcessorBehavior(t *testing.T) {
	voidProcessor := NewVoidProcessor()
	handler := &UnitTestHandler{name: "test-handler"}

	t.Run("GetHandler", func(t *testing.T) {
		retrievedHandler := voidProcessor.GetHandler("ANY")
		if retrievedHandler != nil {
			t.Error("VoidProcessor GetHandler should always return nil")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		err := voidProcessor.RegisterHandler("TEST", handler, false)
		if err == nil {
			t.Error("VoidProcessor RegisterHandler should return error")
		}

		// Check error type
		if !IsVoidProcessorError(err) {
			t.Error("Error should be a VoidProcessorError")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		err := voidProcessor.UnregisterHandler("TEST")
		if err == nil {
			t.Error("VoidProcessor UnregisterHandler should return error")
		}

		// Check error type
		if !IsVoidProcessorError(err) {
			t.Error("Error should be a VoidProcessorError")
		}
	})
}

// TestProcessPendingNotificationsNilReader tests handling of nil reader
func TestProcessPendingNotificationsNilReader(t *testing.T) {
	t.Run("ProcessorWithNilReader", func(t *testing.T) {
		processor := NewProcessor()
		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error with nil reader: %v", err)
		}
	})

	t.Run("VoidProcessorWithNilReader", func(t *testing.T) {
		voidProcessor := NewVoidProcessor()
		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{}

		err := voidProcessor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should not error with nil reader: %v", err)
		}
	})
}

// TestWillHandleNotificationInClient tests the notification filtering logic
func TestWillHandleNotificationInClient(t *testing.T) {
	testCases := []struct {
		name           string
		notificationType string
		shouldHandle   bool
	}{
		// Pub/Sub notifications (should be handled in client)
		{"message", "message", true},
		{"pmessage", "pmessage", true},
		{"subscribe", "subscribe", true},
		{"unsubscribe", "unsubscribe", true},
		{"psubscribe", "psubscribe", true},
		{"punsubscribe", "punsubscribe", true},
		{"smessage", "smessage", true},
		{"ssubscribe", "ssubscribe", true},
		{"sunsubscribe", "sunsubscribe", true},

		// Push notifications (should be handled by processor)
		{"MOVING", "MOVING", false},
		{"MIGRATING", "MIGRATING", false},
		{"MIGRATED", "MIGRATED", false},
		{"FAILING_OVER", "FAILING_OVER", false},
		{"FAILED_OVER", "FAILED_OVER", false},
		{"custom", "custom", false},
		{"unknown", "unknown", false},
		{"empty", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := willHandleNotificationInClient(tc.notificationType)
			if result != tc.shouldHandle {
				t.Errorf("willHandleNotificationInClient(%q) = %v, want %v", tc.notificationType, result, tc.shouldHandle)
			}
		})
	}
}

// TestProcessorErrorHandlingUnit tests error handling scenarios
func TestProcessorErrorHandlingUnit(t *testing.T) {
	processor := NewProcessor()

	t.Run("RegisterNilHandler", func(t *testing.T) {
		err := processor.RegisterHandler("TEST", nil, false)
		if err == nil {
			t.Error("RegisterHandler should error with nil handler")
		}

		// Check error type
		if !IsHandlerNilError(err) {
			t.Error("Error should be a HandlerNilError")
		}
	})

	t.Run("RegisterDuplicateHandler", func(t *testing.T) {
		handler1 := &UnitTestHandler{name: "handler1"}
		handler2 := &UnitTestHandler{name: "handler2"}

		// Register first handler
		err := processor.RegisterHandler("DUPLICATE", handler1, false)
		if err != nil {
			t.Errorf("First RegisterHandler should not error: %v", err)
		}

		// Try to register second handler with same name
		err = processor.RegisterHandler("DUPLICATE", handler2, false)
		if err == nil {
			t.Error("RegisterHandler should error when registering duplicate handler")
		}

		// Verify original handler is still there
		retrievedHandler := processor.GetHandler("DUPLICATE")
		if retrievedHandler != handler1 {
			t.Error("Original handler should remain after failed duplicate registration")
		}
	})

	t.Run("UnregisterNonExistentHandler", func(t *testing.T) {
		err := processor.UnregisterHandler("NONEXISTENT")
		if err != nil {
			t.Errorf("UnregisterHandler should not error for non-existent handler: %v", err)
		}
	})
}

// TestProcessorConcurrentAccess tests concurrent access to processor
func TestProcessorConcurrentAccess(t *testing.T) {
	processor := NewProcessor()
	
	t.Run("ConcurrentRegisterAndGet", func(t *testing.T) {
		done := make(chan bool, 2)

		// Goroutine 1: Register handlers
		go func() {
			defer func() { done <- true }()
			for i := 0; i < 100; i++ {
				handler := &UnitTestHandler{name: "concurrent-handler"}
				processor.RegisterHandler("CONCURRENT", handler, false)
				processor.UnregisterHandler("CONCURRENT")
			}
		}()

		// Goroutine 2: Get handlers
		go func() {
			defer func() { done <- true }()
			for i := 0; i < 100; i++ {
				processor.GetHandler("CONCURRENT")
			}
		}()

		// Wait for both goroutines to complete
		<-done
		<-done
	})
}

// TestProcessorInterfaceCompliance tests interface compliance
func TestProcessorInterfaceCompliance(t *testing.T) {
	t.Run("ProcessorImplementsInterface", func(t *testing.T) {
		var _ NotificationProcessor = (*Processor)(nil)
	})

	t.Run("VoidProcessorImplementsInterface", func(t *testing.T) {
		var _ NotificationProcessor = (*VoidProcessor)(nil)
	})
}

// UnitTestHandler is a test implementation of NotificationHandler
type UnitTestHandler struct {
	name           string
	lastNotification []interface{}
	errorToReturn  error
	callCount      int
}

func (h *UnitTestHandler) HandlePushNotification(ctx context.Context, handlerCtx NotificationHandlerContext, notification []interface{}) error {
	h.callCount++
	h.lastNotification = notification
	return h.errorToReturn
}

// Helper methods for UnitTestHandler
func (h *UnitTestHandler) GetCallCount() int {
	return h.callCount
}

func (h *UnitTestHandler) GetLastNotification() []interface{} {
	return h.lastNotification
}

func (h *UnitTestHandler) SetErrorToReturn(err error) {
	h.errorToReturn = err
}

func (h *UnitTestHandler) Reset() {
	h.callCount = 0
	h.lastNotification = nil
	h.errorToReturn = nil
}
