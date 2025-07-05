package push

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestHandler implements NotificationHandler interface for testing
type TestHandler struct {
	name        string
	handled     [][]interface{}
	returnError error
}

func NewTestHandler(name string) *TestHandler {
	return &TestHandler{
		name:    name,
		handled: make([][]interface{}, 0),
	}
}

func (h *TestHandler) HandlePushNotification(ctx context.Context, handlerCtx NotificationHandlerContext, notification []interface{}) error {
	h.handled = append(h.handled, notification)
	return h.returnError
}

func (h *TestHandler) GetHandledNotifications() [][]interface{} {
	return h.handled
}

func (h *TestHandler) SetReturnError(err error) {
	h.returnError = err
}

func (h *TestHandler) Reset() {
	h.handled = make([][]interface{}, 0)
	h.returnError = nil
}

// Mock client types for testing
type MockClient struct {
	name string
}

type MockConnPool struct {
	name string
}

type MockPubSub struct {
	name string
}

// TestNotificationHandlerContext tests the handler context implementation
func TestNotificationHandlerContext(t *testing.T) {
	t.Run("DirectObjectCreation", func(t *testing.T) {
		client := &MockClient{name: "test-client"}
		connPool := &MockConnPool{name: "test-pool"}
		pubSub := &MockPubSub{name: "test-pubsub"}
		conn := &pool.Conn{}

		ctx := NotificationHandlerContext{
			Client:     client,
			ConnPool:   connPool,
			PubSub:     pubSub,
			Conn:       conn,
			IsBlocking: true,
		}

		if ctx.Client != client {
			t.Error("Client field should contain the provided client")
		}

		if ctx.ConnPool != connPool {
			t.Error("ConnPool field should contain the provided connection pool")
		}

		if ctx.PubSub != pubSub {
			t.Error("PubSub field should contain the provided PubSub")
		}

		if ctx.Conn != conn {
			t.Error("Conn field should contain the provided connection")
		}

		if !ctx.IsBlocking {
			t.Error("IsBlocking field should be true")
		}
	})

	t.Run("NilValues", func(t *testing.T) {
		ctx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		if ctx.Client != nil {
			t.Error("Client field should be nil when client is nil")
		}

		if ctx.ConnPool != nil {
			t.Error("ConnPool field should be nil when connPool is nil")
		}

		if ctx.PubSub != nil {
			t.Error("PubSub field should be nil when pubSub is nil")
		}

		if ctx.Conn != nil {
			t.Error("Conn field should be nil when conn is nil")
		}

		if ctx.IsBlocking {
			t.Error("IsBlocking field should be false")
		}
	})
}

// TestRegistry tests the registry implementation
func TestRegistry(t *testing.T) {
	t.Run("NewRegistry", func(t *testing.T) {
		registry := NewRegistry()
		if registry == nil {
			t.Error("NewRegistry should not return nil")
		}

		if registry.handlers == nil {
			t.Error("Registry handlers map should be initialized")
		}

		if registry.protected == nil {
			t.Error("Registry protected map should be initialized")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test")

		err := registry.RegisterHandler("TEST", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})

	t.Run("RegisterNilHandler", func(t *testing.T) {
		registry := NewRegistry()

		err := registry.RegisterHandler("TEST", nil, false)
		if err == nil {
			t.Error("RegisterHandler should error when handler is nil")
		}

		if !strings.Contains(err.Error(), "handler cannot be nil") {
			t.Errorf("Error message should mention nil handler, got: %v", err)
		}
	})

	t.Run("RegisterProtectedHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test")

		// Register protected handler
		err := registry.RegisterHandler("TEST", handler, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		// Try to overwrite any existing handler (protected or not)
		newHandler := NewTestHandler("new")
		err = registry.RegisterHandler("TEST", newHandler, false)
		if err == nil {
			t.Error("RegisterHandler should error when trying to overwrite existing handler")
		}

		if !strings.Contains(err.Error(), "cannot overwrite existing handler") {
			t.Errorf("Error message should mention existing handler, got: %v", err)
		}

		// Original handler should still be there
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("Existing handler should not be overwritten")
		}
	})

	t.Run("CannotOverwriteExistingHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler1 := NewTestHandler("test1")
		handler2 := NewTestHandler("test2")

		// Register non-protected handler
		err := registry.RegisterHandler("TEST", handler1, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		// Try to overwrite with another handler (should fail)
		err = registry.RegisterHandler("TEST", handler2, false)
		if err == nil {
			t.Error("RegisterHandler should error when trying to overwrite existing handler")
		}

		if !strings.Contains(err.Error(), "cannot overwrite existing handler") {
			t.Errorf("Error message should mention existing handler, got: %v", err)
		}

		// Original handler should still be there
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler1 {
			t.Error("Existing handler should not be overwritten")
		}
	})

	t.Run("GetNonExistentHandler", func(t *testing.T) {
		registry := NewRegistry()

		handler := registry.GetHandler("NONEXISTENT")
		if handler != nil {
			t.Error("GetHandler should return nil for non-existent handler")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test")

		registry.RegisterHandler("TEST", handler, false)

		err := registry.UnregisterHandler("TEST")
		if err != nil {
			t.Errorf("UnregisterHandler should not error: %v", err)
		}

		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != nil {
			t.Error("GetHandler should return nil after unregistering")
		}
	})

	t.Run("UnregisterProtectedHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test")

		// Register protected handler
		registry.RegisterHandler("TEST", handler, true)

		// Try to unregister protected handler
		err := registry.UnregisterHandler("TEST")
		if err == nil {
			t.Error("UnregisterHandler should error for protected handler")
		}

		if !strings.Contains(err.Error(), "cannot unregister protected handler") {
			t.Errorf("Error message should mention protected handler, got: %v", err)
		}

		// Handler should still be there
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("Protected handler should still be registered")
		}
	})

	t.Run("UnregisterNonExistentHandler", func(t *testing.T) {
		registry := NewRegistry()

		err := registry.UnregisterHandler("NONEXISTENT")
		if err != nil {
			t.Errorf("UnregisterHandler should not error for non-existent handler: %v", err)
		}
	})

	t.Run("CannotOverwriteExistingHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler1 := NewTestHandler("handler1")
		handler2 := NewTestHandler("handler2")

		// Register first handler (non-protected)
		err := registry.RegisterHandler("TEST_NOTIFICATION", handler1, false)
		if err != nil {
			t.Errorf("First RegisterHandler should not error: %v", err)
		}

		// Verify first handler is registered
		retrievedHandler := registry.GetHandler("TEST_NOTIFICATION")
		if retrievedHandler != handler1 {
			t.Error("First handler should be registered correctly")
		}

		// Attempt to overwrite with second handler (should fail)
		err = registry.RegisterHandler("TEST_NOTIFICATION", handler2, false)
		if err == nil {
			t.Error("RegisterHandler should error when trying to overwrite existing handler")
		}

		// Verify error message mentions overwriting
		if !strings.Contains(err.Error(), "cannot overwrite existing handler") {
			t.Errorf("Error message should mention overwriting existing handler, got: %v", err)
		}

		// Verify error message includes the notification name
		if !strings.Contains(err.Error(), "TEST_NOTIFICATION") {
			t.Errorf("Error message should include notification name, got: %v", err)
		}

		// Verify original handler is still there (not overwritten)
		retrievedHandler = registry.GetHandler("TEST_NOTIFICATION")
		if retrievedHandler != handler1 {
			t.Error("Original handler should still be registered (not overwritten)")
		}

		// Verify second handler was NOT registered
		if retrievedHandler == handler2 {
			t.Error("Second handler should NOT be registered")
		}
	})

	t.Run("CannotOverwriteProtectedHandler", func(t *testing.T) {
		registry := NewRegistry()
		protectedHandler := NewTestHandler("protected")
		newHandler := NewTestHandler("new")

		// Register protected handler
		err := registry.RegisterHandler("PROTECTED_NOTIFICATION", protectedHandler, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error for protected handler: %v", err)
		}

		// Attempt to overwrite protected handler (should fail)
		err = registry.RegisterHandler("PROTECTED_NOTIFICATION", newHandler, false)
		if err == nil {
			t.Error("RegisterHandler should error when trying to overwrite protected handler")
		}

		// Verify error message
		if !strings.Contains(err.Error(), "cannot overwrite existing handler") {
			t.Errorf("Error message should mention overwriting existing handler, got: %v", err)
		}

		// Verify protected handler is still there
		retrievedHandler := registry.GetHandler("PROTECTED_NOTIFICATION")
		if retrievedHandler != protectedHandler {
			t.Error("Protected handler should still be registered")
		}
	})

	t.Run("CanRegisterDifferentHandlers", func(t *testing.T) {
		registry := NewRegistry()
		handler1 := NewTestHandler("handler1")
		handler2 := NewTestHandler("handler2")

		// Register handlers for different notification names (should succeed)
		err := registry.RegisterHandler("NOTIFICATION_1", handler1, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error for first notification: %v", err)
		}

		err = registry.RegisterHandler("NOTIFICATION_2", handler2, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error for second notification: %v", err)
		}

		// Verify both handlers are registered correctly
		retrievedHandler1 := registry.GetHandler("NOTIFICATION_1")
		if retrievedHandler1 != handler1 {
			t.Error("First handler should be registered correctly")
		}

		retrievedHandler2 := registry.GetHandler("NOTIFICATION_2")
		if retrievedHandler2 != handler2 {
			t.Error("Second handler should be registered correctly")
		}
	})
}

// TestProcessor tests the processor implementation
func TestProcessor(t *testing.T) {
	t.Run("NewProcessor", func(t *testing.T) {
		processor := NewProcessor()
		if processor == nil {
			t.Error("NewProcessor should not return nil")
		}

		if processor.registry == nil {
			t.Error("Processor should have a registry")
		}
	})

	t.Run("RegisterAndGetHandler", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")

		err := processor.RegisterHandler("TEST", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		retrievedHandler := processor.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")

		processor.RegisterHandler("TEST", handler, false)

		err := processor.UnregisterHandler("TEST")
		if err != nil {
			t.Errorf("UnregisterHandler should not error: %v", err)
		}

		retrievedHandler := processor.GetHandler("TEST")
		if retrievedHandler != nil {
			t.Error("GetHandler should return nil after unregistering")
		}
	})

	t.Run("ProcessPendingNotifications_NilReader", func(t *testing.T) {
		processor := NewProcessor()
		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error with nil reader: %v", err)
		}
	})
}

// TestVoidProcessor tests the void processor implementation
func TestVoidProcessor(t *testing.T) {
	t.Run("NewVoidProcessor", func(t *testing.T) {
		processor := NewVoidProcessor()
		if processor == nil {
			t.Error("NewVoidProcessor should not return nil")
		}
	})

	t.Run("GetHandler", func(t *testing.T) {
		processor := NewVoidProcessor()
		handler := processor.GetHandler("TEST")
		if handler != nil {
			t.Error("VoidProcessor GetHandler should always return nil")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		processor := NewVoidProcessor()
		handler := NewTestHandler("test")

		err := processor.RegisterHandler("TEST", handler, false)
		if err == nil {
			t.Error("VoidProcessor RegisterHandler should return error")
		}

		if !strings.Contains(err.Error(), "cannot register push notification handler") {
			t.Errorf("Error message should mention registration failure, got: %v", err)
		}

		if !strings.Contains(err.Error(), "push notifications are disabled") {
			t.Errorf("Error message should mention disabled notifications, got: %v", err)
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		processor := NewVoidProcessor()

		err := processor.UnregisterHandler("TEST")
		if err == nil {
			t.Error("VoidProcessor UnregisterHandler should return error")
		}

		if !strings.Contains(err.Error(), "cannot unregister push notification handler") {
			t.Errorf("Error message should mention unregistration failure, got: %v", err)
		}
	})

	t.Run("ProcessPendingNotifications_NilReader", func(t *testing.T) {
		processor := NewVoidProcessor()
		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should never error, got: %v", err)
		}
	})
}

// TestShouldSkipNotification tests the notification filtering logic
func TestShouldSkipNotification(t *testing.T) {
	testCases := []struct {
		name         string
		notification string
		shouldSkip   bool
	}{
		// Pub/Sub notifications that should be skipped
		{"message", "message", true},
		{"pmessage", "pmessage", true},
		{"subscribe", "subscribe", true},
		{"unsubscribe", "unsubscribe", true},
		{"psubscribe", "psubscribe", true},
		{"punsubscribe", "punsubscribe", true},
		{"smessage", "smessage", true},
		{"ssubscribe", "ssubscribe", true},
		{"sunsubscribe", "sunsubscribe", true},

		// Push notifications that should NOT be skipped
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
			result := willHandleNotificationInClient(tc.notification)
			if result != tc.shouldSkip {
				t.Errorf("willHandleNotificationInClient(%q) = %v, want %v", tc.notification, result, tc.shouldSkip)
			}
		})
	}
}

// TestNotificationHandlerInterface tests that our test handler implements the interface correctly
func TestNotificationHandlerInterface(t *testing.T) {
	var _ NotificationHandler = (*TestHandler)(nil)

	handler := NewTestHandler("test")
	ctx := context.Background()
	handlerCtx := NotificationHandlerContext{
		Client:     nil,
		ConnPool:   nil,
		PubSub:     nil,
		Conn:       nil,
		IsBlocking: false,
	}
	notification := []interface{}{"TEST", "data"}

	err := handler.HandlePushNotification(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("HandlePushNotification should not error: %v", err)
	}

	handled := handler.GetHandledNotifications()
	if len(handled) != 1 {
		t.Errorf("Expected 1 handled notification, got %d", len(handled))
	}

	if len(handled[0]) != 2 || handled[0][0] != "TEST" || handled[0][1] != "data" {
		t.Errorf("Handled notification should match input: %v", handled[0])
	}
}

// TestNotificationHandlerError tests error handling in handlers
func TestNotificationHandlerError(t *testing.T) {
	handler := NewTestHandler("test")
	expectedError := errors.New("test error")
	handler.SetReturnError(expectedError)

	ctx := context.Background()
	handlerCtx := NotificationHandlerContext{
		Client:     nil,
		ConnPool:   nil,
		PubSub:     nil,
		Conn:       nil,
		IsBlocking: false,
	}
	notification := []interface{}{"TEST", "data"}

	err := handler.HandlePushNotification(ctx, handlerCtx, notification)
	if err != expectedError {
		t.Errorf("HandlePushNotification should return the set error: got %v, want %v", err, expectedError)
	}

	// Reset and test no error
	handler.Reset()
	err = handler.HandlePushNotification(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("HandlePushNotification should not error after reset: %v", err)
	}
}

// TestRegistryConcurrency tests concurrent access to registry
func TestRegistryConcurrency(t *testing.T) {
	registry := NewRegistry()

	// Test concurrent registration and access
	done := make(chan bool, 10)

	// Start multiple goroutines registering handlers
	for i := 0; i < 5; i++ {
		go func(id int) {
			handler := NewTestHandler("test")
			err := registry.RegisterHandler(fmt.Sprintf("TEST_%d", id), handler, false)
			if err != nil {
				t.Errorf("RegisterHandler should not error: %v", err)
			}
			done <- true
		}(i)
	}

	// Start multiple goroutines reading handlers
	for i := 0; i < 5; i++ {
		go func(id int) {
			registry.GetHandler(fmt.Sprintf("TEST_%d", id))
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestProcessorConcurrency tests concurrent access to processor
func TestProcessorConcurrency(t *testing.T) {
	processor := NewProcessor()

	// Test concurrent registration and access
	done := make(chan bool, 10)

	// Start multiple goroutines registering handlers
	for i := 0; i < 5; i++ {
		go func(id int) {
			handler := NewTestHandler("test")
			err := processor.RegisterHandler(fmt.Sprintf("TEST_%d", id), handler, false)
			if err != nil {
				t.Errorf("RegisterHandler should not error: %v", err)
			}
			done <- true
		}(i)
	}

	// Start multiple goroutines reading handlers
	for i := 0; i < 5; i++ {
		go func(id int) {
			processor.GetHandler(fmt.Sprintf("TEST_%d", id))
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestRegistryEdgeCases tests edge cases for registry
func TestRegistryEdgeCases(t *testing.T) {
	t.Run("RegisterHandlerWithEmptyName", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test")

		err := registry.RegisterHandler("", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error with empty name: %v", err)
		}

		retrievedHandler := registry.GetHandler("")
		if retrievedHandler != handler {
			t.Error("GetHandler should return handler even with empty name")
		}
	})

	t.Run("MultipleProtectedHandlers", func(t *testing.T) {
		registry := NewRegistry()
		handler1 := NewTestHandler("test1")
		handler2 := NewTestHandler("test2")

		// Register multiple protected handlers
		err := registry.RegisterHandler("TEST1", handler1, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		err = registry.RegisterHandler("TEST2", handler2, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		// Try to unregister both
		err = registry.UnregisterHandler("TEST1")
		if err == nil {
			t.Error("UnregisterHandler should error for protected handler")
		}

		err = registry.UnregisterHandler("TEST2")
		if err == nil {
			t.Error("UnregisterHandler should error for protected handler")
		}
	})

	t.Run("CannotOverwriteAnyExistingHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler1 := NewTestHandler("test1")
		handler2 := NewTestHandler("test2")

		// Register protected handler
		err := registry.RegisterHandler("TEST", handler1, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}

		// Try to overwrite with another protected handler (should fail)
		err = registry.RegisterHandler("TEST", handler2, true)
		if err == nil {
			t.Error("RegisterHandler should error when trying to overwrite existing handler")
		}

		if !strings.Contains(err.Error(), "cannot overwrite existing handler") {
			t.Errorf("Error message should mention existing handler, got: %v", err)
		}

		// Original handler should still be there
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler1 {
			t.Error("Existing handler should not be overwritten")
		}
	})
}

// TestProcessorEdgeCases tests edge cases for processor
func TestProcessorEdgeCases(t *testing.T) {
	t.Run("ProcessorWithNilRegistry", func(t *testing.T) {
		// This tests internal consistency - processor should always have a registry
		processor := &Processor{registry: nil}

		// This should panic or handle gracefully
		defer func() {
			if r := recover(); r != nil {
				// Expected behavior - accessing nil registry should panic
				t.Logf("Expected panic when accessing nil registry: %v", r)
			}
		}()

		// This will likely panic, which is expected behavior
		processor.GetHandler("TEST")
	})

	t.Run("ProcessorRegisterNilHandler", func(t *testing.T) {
		processor := NewProcessor()

		err := processor.RegisterHandler("TEST", nil, false)
		if err == nil {
			t.Error("RegisterHandler should error when handler is nil")
		}
	})
}

// TestVoidProcessorEdgeCases tests edge cases for void processor
func TestVoidProcessorEdgeCases(t *testing.T) {
	t.Run("VoidProcessorMultipleOperations", func(t *testing.T) {
		processor := NewVoidProcessor()
		handler := NewTestHandler("test")

		// Multiple register attempts should all fail
		for i := 0; i < 5; i++ {
			err := processor.RegisterHandler(fmt.Sprintf("TEST_%d", i), handler, false)
			if err == nil {
				t.Errorf("VoidProcessor RegisterHandler should always return error")
			}
		}

		// Multiple unregister attempts should all fail
		for i := 0; i < 5; i++ {
			err := processor.UnregisterHandler(fmt.Sprintf("TEST_%d", i))
			if err == nil {
				t.Errorf("VoidProcessor UnregisterHandler should always return error")
			}
		}

		// Multiple get attempts should all return nil
		for i := 0; i < 5; i++ {
			handler := processor.GetHandler(fmt.Sprintf("TEST_%d", i))
			if handler != nil {
				t.Errorf("VoidProcessor GetHandler should always return nil")
			}
		}
	})
}

// Helper functions to create fake RESP3 protocol data for testing

// createFakeRESP3PushNotification creates a fake RESP3 push notification buffer
func createFakeRESP3PushNotification(notificationType string, args ...string) *bytes.Buffer {
	buf := &bytes.Buffer{}

	// RESP3 Push notification format: ><len>\r\n<elements>\r\n
	totalElements := 1 + len(args) // notification type + arguments
	buf.WriteString(fmt.Sprintf(">%d\r\n", totalElements))

	// Write notification type as bulk string
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(notificationType), notificationType))

	// Write arguments as bulk strings
	for _, arg := range args {
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	return buf
}

// createReaderWithPrimedBuffer creates a reader (no longer needs priming)
func createReaderWithPrimedBuffer(buf *bytes.Buffer) *proto.Reader {
	reader := proto.NewReader(buf)
	// No longer need to prime the buffer - PeekPushNotificationName handles it automatically
	return reader
}

// createFakeRESP3Array creates a fake RESP3 array (not push notification)
func createFakeRESP3Array(elements ...string) *bytes.Buffer {
	buf := &bytes.Buffer{}

	// RESP3 Array format: *<len>\r\n<elements>\r\n
	buf.WriteString(fmt.Sprintf("*%d\r\n", len(elements)))

	// Write elements as bulk strings
	for _, element := range elements {
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(element), element))
	}

	return buf
}

// createFakeRESP3Error creates a fake RESP3 error
func createFakeRESP3Error(message string) *bytes.Buffer {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("-%s\r\n", message))
	return buf
}

// createMultipleNotifications creates a buffer with multiple notifications
func createMultipleNotifications(notifications ...[]string) *bytes.Buffer {
	buf := &bytes.Buffer{}

	for _, notification := range notifications {
		if len(notification) == 0 {
			continue
		}

		notificationType := notification[0]
		args := notification[1:]

		// Determine if this should be a push notification or regular array
		if willHandleNotificationInClient(notificationType) {
			// Create as push notification (will be skipped)
			pushBuf := createFakeRESP3PushNotification(notificationType, args...)
			buf.Write(pushBuf.Bytes())
		} else {
			// Create as push notification (will be processed)
			pushBuf := createFakeRESP3PushNotification(notificationType, args...)
			buf.Write(pushBuf.Bytes())
		}
	}

	return buf
}

// TestProcessorWithFakeBuffer tests ProcessPendingNotifications with fake RESP3 data
func TestProcessorWithFakeBuffer(t *testing.T) {
	t.Run("ProcessValidPushNotification", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create fake RESP3 push notification
		buf := createFakeRESP3PushNotification("MOVING", "slot", "123", "from", "node1", "to", "node2")
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 1 {
			t.Errorf("Expected 1 handled notification, got %d", len(handled))
		}

		if len(handled[0]) != 7 || handled[0][0] != "MOVING" {
			t.Errorf("Handled notification should match input: %v", handled[0])
		}

		if handled[0][1] != "slot" || handled[0][2] != "123" {
			t.Errorf("Notification arguments should match: %v", handled[0])
		}
	})

	t.Run("ProcessSkippedPushNotification", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("message", handler, false)

		// Create fake RESP3 push notification for pub/sub message (should be skipped)
		buf := createFakeRESP3PushNotification("message", "channel", "hello world")
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 0 {
			t.Errorf("Expected 0 handled notifications (should be skipped), got %d", len(handled))
		}
	})

	t.Run("ProcessNotificationWithoutHandler", func(t *testing.T) {
		processor := NewProcessor()
		// No handler registered for MOVING

		// Create fake RESP3 push notification
		buf := createFakeRESP3PushNotification("MOVING", "slot", "123")
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error when no handler: %v", err)
		}
	})

	t.Run("ProcessNotificationWithHandlerError", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		handler.SetReturnError(errors.New("handler error"))
		processor.RegisterHandler("MOVING", handler, false)

		// Create fake RESP3 push notification
		buf := createFakeRESP3PushNotification("MOVING", "slot", "123")
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error even when handler errors: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 1 {
			t.Errorf("Expected 1 handled notification even with error, got %d", len(handled))
		}
	})

	t.Run("ProcessNonPushNotification", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create fake RESP3 array (not push notification)
		buf := createFakeRESP3Array("MOVING", "slot", "123")
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 0 {
			t.Errorf("Expected 0 handled notifications (not push type), got %d", len(handled))
		}
	})

	t.Run("ProcessMultipleNotifications", func(t *testing.T) {
		processor := NewProcessor()
		movingHandler := NewTestHandler("moving")
		migratingHandler := NewTestHandler("migrating")
		processor.RegisterHandler("MOVING", movingHandler, false)
		processor.RegisterHandler("MIGRATING", migratingHandler, false)

		// Create buffer with multiple notifications
		buf := createMultipleNotifications(
			[]string{"MOVING", "slot", "123", "from", "node1", "to", "node2"},
			[]string{"MIGRATING", "slot", "456", "from", "node2", "to", "node3"},
		)
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should not error: %v", err)
		}

		// Check MOVING handler
		movingHandled := movingHandler.GetHandledNotifications()
		if len(movingHandled) != 1 {
			t.Errorf("Expected 1 MOVING notification, got %d", len(movingHandled))
		}
		if len(movingHandled) > 0 && movingHandled[0][0] != "MOVING" {
			t.Errorf("Expected MOVING notification, got %v", movingHandled[0][0])
		}

		// Check MIGRATING handler
		migratingHandled := migratingHandler.GetHandledNotifications()
		if len(migratingHandled) != 1 {
			t.Errorf("Expected 1 MIGRATING notification, got %d", len(migratingHandled))
		}
		if len(migratingHandled) > 0 && migratingHandled[0][0] != "MIGRATING" {
			t.Errorf("Expected MIGRATING notification, got %v", migratingHandled[0][0])
		}
	})

	t.Run("ProcessEmptyNotification", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create fake RESP3 push notification with no elements
		buf := &bytes.Buffer{}
		buf.WriteString(">0\r\n") // Empty push notification
		reader := createReaderWithPrimedBuffer(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		// This should panic due to empty notification array
		defer func() {
			if r := recover(); r != nil {
				t.Logf("ProcessPendingNotifications panicked as expected for empty notification: %v", r)
			}
		}()

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Logf("ProcessPendingNotifications errored for empty notification: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 0 {
			t.Errorf("Expected 0 handled notifications for empty notification, got %d", len(handled))
		}
	})

	t.Run("ProcessNotificationWithNonStringType", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create fake RESP3 push notification with integer as first element
		buf := &bytes.Buffer{}
		buf.WriteString(">2\r\n")         // 2 elements
		buf.WriteString(":123\r\n")       // Integer instead of string
		buf.WriteString("$4\r\ndata\r\n") // String data
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle non-string type gracefully: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 0 {
			t.Errorf("Expected 0 handled notifications for non-string type, got %d", len(handled))
		}
	})
}

// TestVoidProcessorWithFakeBuffer tests VoidProcessor with fake RESP3 data
func TestVoidProcessorWithFakeBuffer(t *testing.T) {
	t.Run("ProcessPushNotifications", func(t *testing.T) {
		processor := NewVoidProcessor()

		// Create buffer with multiple push notifications
		buf := createMultipleNotifications(
			[]string{"MOVING", "slot", "123"},
			[]string{"MIGRATING", "slot", "456"},
			[]string{"FAILED_OVER", "node", "node1"},
		)
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should not error: %v", err)
		}

		// VoidProcessor should discard all notifications without processing
		// We can't directly verify this, but the fact that it doesn't error is good
	})

	t.Run("ProcessSkippedNotifications", func(t *testing.T) {
		processor := NewVoidProcessor()

		// Create buffer with pub/sub notifications (should be skipped)
		buf := createMultipleNotifications(
			[]string{"message", "channel", "data"},
			[]string{"pmessage", "pattern", "channel", "data"},
			[]string{"subscribe", "channel", "1"},
		)
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should not error: %v", err)
		}
	})

	t.Run("ProcessMixedNotifications", func(t *testing.T) {
		processor := NewVoidProcessor()

		// Create buffer with mixed push notifications and regular arrays
		buf := &bytes.Buffer{}

		// Add push notification
		pushBuf := createFakeRESP3PushNotification("MOVING", "slot", "123")
		buf.Write(pushBuf.Bytes())

		// Add regular array (should stop processing)
		arrayBuf := createFakeRESP3Array("SOME", "COMMAND")
		buf.Write(arrayBuf.Bytes())

		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should not error: %v", err)
		}
	})

	t.Run("ProcessInvalidNotificationFormat", func(t *testing.T) {
		processor := NewVoidProcessor()

		// Create invalid RESP3 data
		buf := &bytes.Buffer{}
		buf.WriteString(">1\r\n")      // Push notification with 1 element
		buf.WriteString("invalid\r\n") // Invalid format (should be $<len>\r\n<data>\r\n)
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		// VoidProcessor should handle errors gracefully
		if err != nil {
			t.Logf("VoidProcessor handled error gracefully: %v", err)
		}
	})
}

// TestProcessorErrorHandling tests error handling scenarios
func TestProcessorErrorHandling(t *testing.T) {
	t.Run("ProcessWithEmptyBuffer", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create empty buffer
		buf := &bytes.Buffer{}
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle empty buffer gracefully: %v", err)
		}

		handled := handler.GetHandledNotifications()
		if len(handled) != 0 {
			t.Errorf("Expected 0 handled notifications for empty buffer, got %d", len(handled))
		}
	})

	t.Run("ProcessWithCorruptedData", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create buffer with corrupted RESP3 data
		buf := &bytes.Buffer{}
		buf.WriteString(">2\r\n")           // Says 2 elements
		buf.WriteString("$6\r\nMOVING\r\n") // First element OK
		buf.WriteString("corrupted")        // Second element corrupted (no proper format)
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		// Should handle corruption gracefully
		if err != nil {
			t.Logf("Processor handled corrupted data gracefully: %v", err)
		}
	})

	t.Run("ProcessWithPartialData", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test")
		processor.RegisterHandler("MOVING", handler, false)

		// Create buffer with partial RESP3 data
		buf := &bytes.Buffer{}
		buf.WriteString(">2\r\n")           // Says 2 elements
		buf.WriteString("$6\r\nMOVING\r\n") // First element OK
		// Missing second element
		reader := proto.NewReader(buf)

		ctx := context.Background()
		handlerCtx := NotificationHandlerContext{
			Client:     nil,
			ConnPool:   nil,
			PubSub:     nil,
			Conn:       nil,
			IsBlocking: false,
		}

		err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		// Should handle partial data gracefully
		if err != nil {
			t.Logf("Processor handled partial data gracefully: %v", err)
		}
	})
}

// TestProcessorPerformanceWithFakeData tests performance with realistic data
func TestProcessorPerformanceWithFakeData(t *testing.T) {
	processor := NewProcessor()
	handler := NewTestHandler("test")
	processor.RegisterHandler("MOVING", handler, false)
	processor.RegisterHandler("MIGRATING", handler, false)
	processor.RegisterHandler("MIGRATED", handler, false)

	// Create buffer with many notifications
	notifications := make([][]string, 100)
	for i := 0; i < 100; i++ {
		switch i % 3 {
		case 0:
			notifications[i] = []string{"MOVING", "slot", fmt.Sprintf("%d", i), "from", "node1", "to", "node2"}
		case 1:
			notifications[i] = []string{"MIGRATING", "slot", fmt.Sprintf("%d", i), "from", "node2", "to", "node3"}
		case 2:
			notifications[i] = []string{"MIGRATED", "slot", fmt.Sprintf("%d", i), "from", "node3", "to", "node1"}
		}
	}

	buf := createMultipleNotifications(notifications...)
	reader := proto.NewReader(buf)

	ctx := context.Background()
	handlerCtx := NotificationHandlerContext{
		Client:     nil,
		ConnPool:   nil,
		PubSub:     nil,
		Conn:       nil,
		IsBlocking: false,
	}

	err := processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
	if err != nil {
		t.Errorf("ProcessPendingNotifications should not error with many notifications: %v", err)
	}

	handled := handler.GetHandledNotifications()
	if len(handled) != 100 {
		t.Errorf("Expected 100 handled notifications, got %d", len(handled))
	}
}

// TestInterfaceCompliance tests that all types implement their interfaces correctly
func TestInterfaceCompliance(t *testing.T) {
	// Test that Processor implements NotificationProcessor
	var _ NotificationProcessor = (*Processor)(nil)

	// Test that VoidProcessor implements NotificationProcessor
	var _ NotificationProcessor = (*VoidProcessor)(nil)

	// Test that NotificationHandlerContext is a concrete struct (no interface needed)
	var _ NotificationHandlerContext = NotificationHandlerContext{}

	// Test that TestHandler implements NotificationHandler
	var _ NotificationHandler = (*TestHandler)(nil)

	// Test that error types implement error interface
	var _ error = (*HandlerError)(nil)
	var _ error = (*ProcessorError)(nil)
}

// TestErrors tests the error definitions and helper functions
func TestErrors(t *testing.T) {
	t.Run("ErrHandlerNil", func(t *testing.T) {
		err := ErrHandlerNil
		if err == nil {
			t.Error("ErrHandlerNil should not be nil")
		}

		if err.Error() != "handler cannot be nil" {
			t.Errorf("ErrHandlerNil message should be 'handler cannot be nil', got: %s", err.Error())
		}
	})

	t.Run("ErrHandlerExists", func(t *testing.T) {
		notificationName := "TEST_NOTIFICATION"
		err := ErrHandlerExists(notificationName)

		if err == nil {
			t.Error("ErrHandlerExists should not return nil")
		}

		expectedMsg := "cannot overwrite existing handler for push notification: TEST_NOTIFICATION"
		if err.Error() != expectedMsg {
			t.Errorf("ErrHandlerExists message should be '%s', got: %s", expectedMsg, err.Error())
		}
	})

	t.Run("ErrProtectedHandler", func(t *testing.T) {
		notificationName := "PROTECTED_NOTIFICATION"
		err := ErrProtectedHandler(notificationName)

		if err == nil {
			t.Error("ErrProtectedHandler should not return nil")
		}

		expectedMsg := "cannot unregister protected handler for push notification: PROTECTED_NOTIFICATION"
		if err.Error() != expectedMsg {
			t.Errorf("ErrProtectedHandler message should be '%s', got: %s", expectedMsg, err.Error())
		}
	})

	t.Run("ErrVoidProcessorRegister", func(t *testing.T) {
		notificationName := "VOID_TEST"
		err := ErrVoidProcessorRegister(notificationName)

		if err == nil {
			t.Error("ErrVoidProcessorRegister should not return nil")
		}

		expectedMsg := "cannot register push notification handler 'VOID_TEST': push notifications are disabled (using void processor)"
		if err.Error() != expectedMsg {
			t.Errorf("ErrVoidProcessorRegister message should be '%s', got: %s", expectedMsg, err.Error())
		}
	})

	t.Run("ErrVoidProcessorUnregister", func(t *testing.T) {
		notificationName := "VOID_TEST"
		err := ErrVoidProcessorUnregister(notificationName)

		if err == nil {
			t.Error("ErrVoidProcessorUnregister should not return nil")
		}

		expectedMsg := "cannot unregister push notification handler 'VOID_TEST': push notifications are disabled (using void processor)"
		if err.Error() != expectedMsg {
			t.Errorf("ErrVoidProcessorUnregister message should be '%s', got: %s", expectedMsg, err.Error())
		}
	})
}

// TestHandlerError tests the HandlerError structured error type
func TestHandlerError(t *testing.T) {
	t.Run("HandlerErrorWithoutWrappedError", func(t *testing.T) {
		err := NewHandlerError("register", "TEST_NOTIFICATION", "handler already exists", nil)

		if err == nil {
			t.Error("NewHandlerError should not return nil")
		}

		expectedMsg := "handler register failed for 'TEST_NOTIFICATION': handler already exists"
		if err.Error() != expectedMsg {
			t.Errorf("HandlerError message should be '%s', got: %s", expectedMsg, err.Error())
		}

		if err.Operation != "register" {
			t.Errorf("HandlerError Operation should be 'register', got: %s", err.Operation)
		}

		if err.PushNotificationName != "TEST_NOTIFICATION" {
			t.Errorf("HandlerError PushNotificationName should be 'TEST_NOTIFICATION', got: %s", err.PushNotificationName)
		}

		if err.Reason != "handler already exists" {
			t.Errorf("HandlerError Reason should be 'handler already exists', got: %s", err.Reason)
		}

		if err.Unwrap() != nil {
			t.Error("HandlerError Unwrap should return nil when no wrapped error")
		}
	})

	t.Run("HandlerErrorWithWrappedError", func(t *testing.T) {
		wrappedErr := errors.New("underlying error")
		err := NewHandlerError("unregister", "PROTECTED_NOTIFICATION", "protected handler", wrappedErr)

		expectedMsg := "handler unregister failed for 'PROTECTED_NOTIFICATION': protected handler (underlying error)"
		if err.Error() != expectedMsg {
			t.Errorf("HandlerError message should be '%s', got: %s", expectedMsg, err.Error())
		}

		if err.Unwrap() != wrappedErr {
			t.Error("HandlerError Unwrap should return the wrapped error")
		}
	})
}

// TestProcessorError tests the ProcessorError structured error type
func TestProcessorError(t *testing.T) {
	t.Run("ProcessorErrorWithoutWrappedError", func(t *testing.T) {
		err := NewProcessorError("processor", "process", "invalid notification format", nil)

		if err == nil {
			t.Error("NewProcessorError should not return nil")
		}

		expectedMsg := "processor process failed: invalid notification format"
		if err.Error() != expectedMsg {
			t.Errorf("ProcessorError message should be '%s', got: %s", expectedMsg, err.Error())
		}

		if err.ProcessorType != "processor" {
			t.Errorf("ProcessorError ProcessorType should be 'processor', got: %s", err.ProcessorType)
		}

		if err.Operation != "process" {
			t.Errorf("ProcessorError Operation should be 'process', got: %s", err.Operation)
		}

		if err.Reason != "invalid notification format" {
			t.Errorf("ProcessorError Reason should be 'invalid notification format', got: %s", err.Reason)
		}

		if err.Unwrap() != nil {
			t.Error("ProcessorError Unwrap should return nil when no wrapped error")
		}
	})

	t.Run("ProcessorErrorWithWrappedError", func(t *testing.T) {
		wrappedErr := errors.New("network error")
		err := NewProcessorError("void_processor", "register", "disabled", wrappedErr)

		expectedMsg := "void_processor register failed: disabled (network error)"
		if err.Error() != expectedMsg {
			t.Errorf("ProcessorError message should be '%s', got: %s", expectedMsg, err.Error())
		}

		if err.Unwrap() != wrappedErr {
			t.Error("ProcessorError Unwrap should return the wrapped error")
		}
	})
}

// TestErrorHelperFunctions tests the error checking helper functions
func TestErrorHelperFunctions(t *testing.T) {
	t.Run("IsHandlerNilError", func(t *testing.T) {
		// Test with ErrHandlerNil
		if !IsHandlerNilError(ErrHandlerNil) {
			t.Error("IsHandlerNilError should return true for ErrHandlerNil")
		}

		// Test with other error
		otherErr := ErrHandlerExists("TEST")
		if IsHandlerNilError(otherErr) {
			t.Error("IsHandlerNilError should return false for other errors")
		}

		// Test with nil
		if IsHandlerNilError(nil) {
			t.Error("IsHandlerNilError should return false for nil")
		}
	})

	t.Run("IsVoidProcessorError", func(t *testing.T) {
		// Test with void processor register error
		registerErr := ErrVoidProcessorRegister("TEST")
		if !IsVoidProcessorError(registerErr) {
			t.Error("IsVoidProcessorError should return true for void processor register error")
		}

		// Test with void processor unregister error
		unregisterErr := ErrVoidProcessorUnregister("TEST")
		if !IsVoidProcessorError(unregisterErr) {
			t.Error("IsVoidProcessorError should return true for void processor unregister error")
		}

		// Test with other error
		otherErr := ErrHandlerNil
		if IsVoidProcessorError(otherErr) {
			t.Error("IsVoidProcessorError should return false for other errors")
		}

		// Test with nil
		if IsVoidProcessorError(nil) {
			t.Error("IsVoidProcessorError should return false for nil")
		}
	})
}

// TestErrorConstants tests the error message constants
func TestErrorConstants(t *testing.T) {
	t.Run("ErrorMessageConstants", func(t *testing.T) {
		if MsgHandlerNil != "handler cannot be nil" {
			t.Errorf("MsgHandlerNil should be 'handler cannot be nil', got: %s", MsgHandlerNil)
		}

		if MsgHandlerExists != "cannot overwrite existing handler for push notification: %s" {
			t.Errorf("MsgHandlerExists should be 'cannot overwrite existing handler for push notification: %%s', got: %s", MsgHandlerExists)
		}

		if MsgProtectedHandler != "cannot unregister protected handler for push notification: %s" {
			t.Errorf("MsgProtectedHandler should be 'cannot unregister protected handler for push notification: %%s', got: %s", MsgProtectedHandler)
		}

		if MsgVoidProcessorRegister != "cannot register push notification handler '%s': push notifications are disabled (using void processor)" {
			t.Errorf("MsgVoidProcessorRegister constant mismatch, got: %s", MsgVoidProcessorRegister)
		}

		if MsgVoidProcessorUnregister != "cannot unregister push notification handler '%s': push notifications are disabled (using void processor)" {
			t.Errorf("MsgVoidProcessorUnregister constant mismatch, got: %s", MsgVoidProcessorUnregister)
		}
	})
}

// Benchmark tests for performance
func BenchmarkRegistry(b *testing.B) {
	registry := NewRegistry()
	handler := NewTestHandler("test")

	b.Run("RegisterHandler", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			registry.RegisterHandler("TEST", handler, false)
		}
	})

	b.Run("GetHandler", func(b *testing.B) {
		registry.RegisterHandler("TEST", handler, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			registry.GetHandler("TEST")
		}
	})
}

func BenchmarkProcessor(b *testing.B) {
	processor := NewProcessor()
	handler := NewTestHandler("test")
	processor.RegisterHandler("MOVING", handler, false)

	b.Run("RegisterHandler", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			processor.RegisterHandler("TEST", handler, false)
		}
	})

	b.Run("GetHandler", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			processor.GetHandler("MOVING")
		}
	})
}
