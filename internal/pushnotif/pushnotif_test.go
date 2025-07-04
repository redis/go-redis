package pushnotif

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestHandler implements Handler interface for testing
type TestHandler struct {
	name        string
	handled     [][]interface{}
	returnValue bool
}

func NewTestHandler(name string, returnValue bool) *TestHandler {
	return &TestHandler{
		name:        name,
		handled:     make([][]interface{}, 0),
		returnValue: returnValue,
	}
}

func (h *TestHandler) HandlePushNotification(ctx context.Context, handlerCtx *HandlerContext, notification []interface{}) bool {
	h.handled = append(h.handled, notification)
	// Store the handler context for testing if needed
	_ = handlerCtx
	return h.returnValue
}

func (h *TestHandler) GetHandledNotifications() [][]interface{} {
	return h.handled
}

func (h *TestHandler) Reset() {
	h.handled = make([][]interface{}, 0)
}

// TestReaderInterface defines the interface needed for testing
type TestReaderInterface interface {
	PeekReplyType() (byte, error)
	PeekPushNotificationName() (string, error)
	ReadReply() (interface{}, error)
}

// MockReader implements TestReaderInterface for testing
type MockReader struct {
	peekReplies []peekReply
	peekIndex   int
	readReplies []interface{}
	readErrors  []error
	readIndex   int
}

type peekReply struct {
	replyType byte
	err       error
}

func NewMockReader() *MockReader {
	return &MockReader{
		peekReplies: make([]peekReply, 0),
		readReplies: make([]interface{}, 0),
		readErrors:  make([]error, 0),
		readIndex:   0,
		peekIndex:   0,
	}
}

func (m *MockReader) AddPeekReplyType(replyType byte, err error) {
	m.peekReplies = append(m.peekReplies, peekReply{replyType: replyType, err: err})
}

func (m *MockReader) AddReadReply(reply interface{}, err error) {
	m.readReplies = append(m.readReplies, reply)
	m.readErrors = append(m.readErrors, err)
}

func (m *MockReader) PeekReplyType() (byte, error) {
	if m.peekIndex >= len(m.peekReplies) {
		return 0, io.EOF
	}
	peek := m.peekReplies[m.peekIndex]
	m.peekIndex++
	return peek.replyType, peek.err
}

func (m *MockReader) ReadReply() (interface{}, error) {
	if m.readIndex >= len(m.readReplies) {
		return nil, io.EOF
	}
	reply := m.readReplies[m.readIndex]
	err := m.readErrors[m.readIndex]
	m.readIndex++
	return reply, err
}

func (m *MockReader) PeekPushNotificationName() (string, error) {
	// return the notification name from the next read reply
	if m.readIndex >= len(m.readReplies) {
		return "", io.EOF
	}
	reply := m.readReplies[m.readIndex]
	if reply == nil {
		return "", nil
	}
	notification, ok := reply.([]interface{})
	if !ok {
		return "", nil
	}
	if len(notification) == 0 {
		return "", nil
	}
	name, ok := notification[0].(string)
	if !ok {
		return "", nil
	}
	return name, nil
}

func (m *MockReader) Reset() {
	m.readIndex = 0
	m.peekIndex = 0
}

// testProcessPendingNotifications is a test version that accepts our mock reader
func testProcessPendingNotifications(processor *Processor, ctx context.Context, reader TestReaderInterface) error {
	if reader == nil {
		return nil
	}

	// Create a test handler context
	handlerCtx := &HandlerContext{
		Client:   nil,
		ConnPool: nil,
		Conn:     nil,
	}

	for {
		// Check if there are push notifications available
		replyType, err := reader.PeekReplyType()
		if err != nil {
			// No more data or error - this is normal
			break
		}

		// Only process push notifications
		if replyType != proto.RespPush {
			break
		}

		notificationName, err := reader.PeekPushNotificationName()
		if err != nil {
			// Error reading - continue to next iteration
			break
		}

		// Skip notifications that should be handled by other systems
		if shouldSkipNotification(notificationName) {
			break
		}

		// Read the push notification
		reply, err := reader.ReadReply()
		if err != nil {
			// Error reading - continue to next iteration
			internal.Logger.Printf(ctx, "push: error reading push notification: %v", err)
			continue
		}

		// Convert to slice of interfaces
		notification, ok := reply.([]interface{})
		if !ok {
			continue
		}

		// Handle the notification directly
		if len(notification) > 0 {
			// Extract the notification type (first element)
			if notificationType, ok := notification[0].(string); ok {
				// Get the handler for this notification type
				if handler := processor.registry.GetHandler(notificationType); handler != nil {
					// Handle the notification with context
					handler.HandlePushNotification(ctx, handlerCtx, notification)
				}
			}
		}
	}

	return nil
}

// TestRegistry tests the Registry implementation
func TestRegistry(t *testing.T) {
	t.Run("NewRegistry", func(t *testing.T) {
		registry := NewRegistry()
		if registry == nil {
			t.Error("NewRegistry should return a non-nil registry")
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
		handler := NewTestHandler("test", true)

		// Test successful registration
		err := registry.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should succeed, got error: %v", err)
		}

		// Test duplicate registration
		err = registry.RegisterHandler("MOVING", handler, false)
		if err == nil {
			t.Error("RegisterHandler should return error for duplicate registration")
		}
		if !strings.Contains(err.Error(), "handler already registered") {
			t.Errorf("Expected error about duplicate registration, got: %v", err)
		}

		// Test protected registration
		err = registry.RegisterHandler("MIGRATING", handler, true)
		if err != nil {
			t.Errorf("RegisterHandler with protected=true should succeed, got error: %v", err)
		}
	})

	t.Run("GetHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test", true)

		// Test getting non-existent handler
		result := registry.GetHandler("NONEXISTENT")
		if result != nil {
			t.Error("GetHandler should return nil for non-existent handler")
		}

		// Test getting existing handler
		err := registry.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		result = registry.GetHandler("MOVING")
		if result != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test", true)

		// Test unregistering non-existent handler
		err := registry.UnregisterHandler("NONEXISTENT")
		if err == nil {
			t.Error("UnregisterHandler should return error for non-existent handler")
		}
		if !strings.Contains(err.Error(), "no handler registered") {
			t.Errorf("Expected error about no handler registered, got: %v", err)
		}

		// Test unregistering regular handler
		err = registry.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		err = registry.UnregisterHandler("MOVING")
		if err != nil {
			t.Errorf("UnregisterHandler should succeed for regular handler, got error: %v", err)
		}

		// Verify handler is removed
		result := registry.GetHandler("MOVING")
		if result != nil {
			t.Error("Handler should be removed after unregistration")
		}

		// Test unregistering protected handler
		err = registry.RegisterHandler("MIGRATING", handler, true)
		if err != nil {
			t.Fatalf("Failed to register protected handler: %v", err)
		}

		err = registry.UnregisterHandler("MIGRATING")
		if err == nil {
			t.Error("UnregisterHandler should return error for protected handler")
		}
		if !strings.Contains(err.Error(), "cannot unregister protected handler") {
			t.Errorf("Expected error about protected handler, got: %v", err)
		}

		// Verify protected handler is still there
		result = registry.GetHandler("MIGRATING")
		if result != handler {
			t.Error("Protected handler should still be registered after failed unregistration")
		}
	})

	t.Run("GetRegisteredPushNotificationNames", func(t *testing.T) {
		registry := NewRegistry()
		handler1 := NewTestHandler("test1", true)
		handler2 := NewTestHandler("test2", true)

		// Test empty registry
		names := registry.GetRegisteredPushNotificationNames()
		if len(names) != 0 {
			t.Errorf("Empty registry should return empty slice, got: %v", names)
		}

		// Test with registered handlers
		err := registry.RegisterHandler("MOVING", handler1, false)
		if err != nil {
			t.Fatalf("Failed to register handler1: %v", err)
		}

		err = registry.RegisterHandler("MIGRATING", handler2, true)
		if err != nil {
			t.Fatalf("Failed to register handler2: %v", err)
		}

		names = registry.GetRegisteredPushNotificationNames()
		if len(names) != 2 {
			t.Errorf("Expected 2 registered names, got: %d", len(names))
		}

		// Check that both names are present (order doesn't matter)
		nameMap := make(map[string]bool)
		for _, name := range names {
			nameMap[name] = true
		}

		if !nameMap["MOVING"] {
			t.Error("MOVING should be in registered names")
		}
		if !nameMap["MIGRATING"] {
			t.Error("MIGRATING should be in registered names")
		}
	})
}

// TestProcessor tests the Processor implementation
func TestProcessor(t *testing.T) {
	t.Run("NewProcessor", func(t *testing.T) {
		processor := NewProcessor()
		if processor == nil {
			t.Error("NewProcessor should return a non-nil processor")
		}
		if processor.registry == nil {
			t.Error("Processor should have a non-nil registry")
		}
	})

	t.Run("GetHandler", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)

		// Test getting non-existent handler
		result := processor.GetHandler("NONEXISTENT")
		if result != nil {
			t.Error("GetHandler should return nil for non-existent handler")
		}

		// Test getting existing handler
		err := processor.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		result = processor.GetHandler("MOVING")
		if result != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)

		// Test successful registration
		err := processor.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should succeed, got error: %v", err)
		}

		// Test duplicate registration
		err = processor.RegisterHandler("MOVING", handler, false)
		if err == nil {
			t.Error("RegisterHandler should return error for duplicate registration")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)

		// Test unregistering non-existent handler
		err := processor.UnregisterHandler("NONEXISTENT")
		if err == nil {
			t.Error("UnregisterHandler should return error for non-existent handler")
		}

		// Test successful unregistration
		err = processor.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		err = processor.UnregisterHandler("MOVING")
		if err != nil {
			t.Errorf("UnregisterHandler should succeed, got error: %v", err)
		}
	})

	t.Run("ProcessPendingNotifications", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)
		ctx := context.Background()

		// Test with nil reader
		handlerCtx := &HandlerContext{
			Client:   nil,
			ConnPool: nil,
			Conn:     nil,
		}
		err := processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("ProcessPendingNotifications with nil reader should not error, got: %v", err)
		}

		// Test with empty reader (no buffered data)
		reader := proto.NewReader(strings.NewReader(""))
		err = processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications with empty reader should not error, got: %v", err)
		}

		// Register a handler for testing
		err = processor.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		// Test with mock reader - peek error (no push notifications available)
		mockReader := NewMockReader()
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // EOF means no more data
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle peek EOF gracefully, got: %v", err)
		}

		// Test with mock reader - non-push reply type
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespString, nil) // Not RespPush
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle non-push reply types gracefully, got: %v", err)
		}

		// Test with mock reader - push notification with ReadReply error
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		mockReader.AddReadReply(nil, io.ErrUnexpectedEOF)     // ReadReply fails
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle ReadReply errors gracefully, got: %v", err)
		}

		// Test with mock reader - push notification with invalid reply type
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		mockReader.AddReadReply("not-a-slice", nil)           // Invalid reply type
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle invalid reply types gracefully, got: %v", err)
		}

		// Test with mock reader - valid push notification with handler
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		notification := []interface{}{"MOVING", "slot", "12345"}
		mockReader.AddReadReply(notification, nil)
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications

		handler.Reset()
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle valid notifications, got: %v", err)
		}

		// Check that handler was called
		handled := handler.GetHandledNotifications()
		if len(handled) != 1 {
			t.Errorf("Expected 1 handled notification, got: %d", len(handled))
		} else if len(handled[0]) != 3 || handled[0][0] != "MOVING" {
			t.Errorf("Expected MOVING notification, got: %v", handled[0])
		}

		// Test with mock reader - valid push notification without handler
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		notification = []interface{}{"UNKNOWN", "data"}
		mockReader.AddReadReply(notification, nil)
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications

		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle notifications without handlers, got: %v", err)
		}

		// Test with mock reader - empty notification
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		emptyNotification := []interface{}{}
		mockReader.AddReadReply(emptyNotification, nil)
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications

		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle empty notifications, got: %v", err)
		}

		// Test with mock reader - notification with non-string type
		mockReader = NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		invalidTypeNotification := []interface{}{123, "data"} // First element is not string
		mockReader.AddReadReply(invalidTypeNotification, nil)
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications

		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle invalid notification types, got: %v", err)
		}

		// Test the actual ProcessPendingNotifications method with real proto.Reader
		// Test with nil reader
		err = processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("ProcessPendingNotifications with nil reader should not error, got: %v", err)
		}

		// Test with empty reader (no buffered data)
		protoReader := proto.NewReader(strings.NewReader(""))
		err = processor.ProcessPendingNotifications(ctx, handlerCtx, protoReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications with empty reader should not error, got: %v", err)
		}

		// Test with reader that has some data but not push notifications
		protoReader = proto.NewReader(strings.NewReader("+OK\r\n"))
		err = processor.ProcessPendingNotifications(ctx, handlerCtx, protoReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications with non-push data should not error, got: %v", err)
		}
	})
}

// TestVoidProcessor tests the VoidProcessor implementation
func TestVoidProcessor(t *testing.T) {
	t.Run("NewVoidProcessor", func(t *testing.T) {
		processor := NewVoidProcessor()
		if processor == nil {
			t.Error("NewVoidProcessor should return a non-nil processor")
		}
	})

	t.Run("GetHandler", func(t *testing.T) {
		processor := NewVoidProcessor()

		// VoidProcessor should always return nil for any handler name
		result := processor.GetHandler("MOVING")
		if result != nil {
			t.Error("VoidProcessor GetHandler should always return nil")
		}

		result = processor.GetHandler("MIGRATING")
		if result != nil {
			t.Error("VoidProcessor GetHandler should always return nil")
		}

		result = processor.GetHandler("")
		if result != nil {
			t.Error("VoidProcessor GetHandler should always return nil for empty string")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		processor := NewVoidProcessor()
		handler := NewTestHandler("test", true)

		// VoidProcessor should always return error for registration
		err := processor.RegisterHandler("MOVING", handler, false)
		if err == nil {
			t.Error("VoidProcessor RegisterHandler should always return error")
		}
		if !strings.Contains(err.Error(), "cannot register push notification handler") {
			t.Errorf("Expected error about cannot register, got: %v", err)
		}
		if !strings.Contains(err.Error(), "push notifications are disabled") {
			t.Errorf("Expected error about disabled push notifications, got: %v", err)
		}

		// Test with protected flag
		err = processor.RegisterHandler("MIGRATING", handler, true)
		if err == nil {
			t.Error("VoidProcessor RegisterHandler should always return error even with protected=true")
		}

		// Test with empty handler name
		err = processor.RegisterHandler("", handler, false)
		if err == nil {
			t.Error("VoidProcessor RegisterHandler should always return error even with empty name")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		processor := NewVoidProcessor()

		// VoidProcessor should always return error for unregistration
		err := processor.UnregisterHandler("MOVING")
		if err == nil {
			t.Error("VoidProcessor UnregisterHandler should always return error")
		}
		if !strings.Contains(err.Error(), "cannot unregister push notification handler") {
			t.Errorf("Expected error about cannot unregister, got: %v", err)
		}
		if !strings.Contains(err.Error(), "push notifications are disabled") {
			t.Errorf("Expected error about disabled push notifications, got: %v", err)
		}

		// Test with empty handler name
		err = processor.UnregisterHandler("")
		if err == nil {
			t.Error("VoidProcessor UnregisterHandler should always return error even with empty name")
		}
	})

	t.Run("ProcessPendingNotifications", func(t *testing.T) {
		processor := NewVoidProcessor()
		ctx := context.Background()
		handlerCtx := &HandlerContext{
			Client:   nil,
			ConnPool: nil,
			Conn:     nil,
		}

		// VoidProcessor should always succeed and do nothing
		err := processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should never error, got: %v", err)
		}

		// Test with various readers
		reader := proto.NewReader(strings.NewReader(""))
		err = processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should never error, got: %v", err)
		}

		reader = proto.NewReader(strings.NewReader("some data"))
		err = processor.ProcessPendingNotifications(ctx, handlerCtx, reader)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should never error, got: %v", err)
		}
	})
}

// TestShouldSkipNotification tests the shouldSkipNotification function
func TestShouldSkipNotification(t *testing.T) {
	t.Run("PubSubMessages", func(t *testing.T) {
		pubSubMessages := []string{
			"message",      // Regular pub/sub message
			"pmessage",     // Pattern pub/sub message
			"subscribe",    // Subscription confirmation
			"unsubscribe",  // Unsubscription confirmation
			"psubscribe",   // Pattern subscription confirmation
			"punsubscribe", // Pattern unsubscription confirmation
			"smessage",     // Sharded pub/sub message (Redis 7.0+)
		}

		for _, msgType := range pubSubMessages {
			if !shouldSkipNotification(msgType) {
				t.Errorf("shouldSkipNotification(%q) should return true", msgType)
			}
		}
	})

	t.Run("NonPubSubMessages", func(t *testing.T) {
		nonPubSubMessages := []string{
			"MOVING",       // Cluster slot migration
			"MIGRATING",    // Cluster slot migration
			"MIGRATED",     // Cluster slot migration
			"FAILING_OVER", // Cluster failover
			"FAILED_OVER",  // Cluster failover
			"unknown",      // Unknown message type
			"",             // Empty string
			"MESSAGE",      // Case sensitive - should not match
			"PMESSAGE",     // Case sensitive - should not match
		}

		for _, msgType := range nonPubSubMessages {
			if shouldSkipNotification(msgType) {
				t.Errorf("shouldSkipNotification(%q) should return false", msgType)
			}
		}
	})
}

// TestPubSubFiltering tests that pub/sub messages are filtered out during processing
func TestPubSubFiltering(t *testing.T) {
	t.Run("PubSubMessagesIgnored", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)
		ctx := context.Background()

		// Register a handler for a non-pub/sub notification
		err := processor.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		// Test with mock reader - pub/sub message should be ignored
		mockReader := NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		pubSubNotification := []interface{}{"message", "channel", "data"}
		mockReader.AddReadReply(pubSubNotification, nil)
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications

		handler.Reset()
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle pub/sub messages gracefully, got: %v", err)
		}

		// Check that handler was NOT called for pub/sub message
		handled := handler.GetHandledNotifications()
		if len(handled) != 0 {
			t.Errorf("Expected 0 handled notifications for pub/sub message, got: %d", len(handled))
		}
	})

	t.Run("NonPubSubMessagesProcessed", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)
		ctx := context.Background()

		// Register a handler for a non-pub/sub notification
		err := processor.RegisterHandler("MOVING", handler, false)
		if err != nil {
			t.Fatalf("Failed to register handler: %v", err)
		}

		// Test with mock reader - non-pub/sub message should be processed
		mockReader := NewMockReader()
		mockReader.AddPeekReplyType(proto.RespPush, nil)
		clusterNotification := []interface{}{"MOVING", "slot", "12345"}
		mockReader.AddReadReply(clusterNotification, nil)
		mockReader.AddPeekReplyType(proto.RespString, io.EOF) // No more push notifications

		handler.Reset()
		err = testProcessPendingNotifications(processor, ctx, mockReader)
		if err != nil {
			t.Errorf("ProcessPendingNotifications should handle cluster notifications, got: %v", err)
		}

		// Check that handler WAS called for cluster notification
		handled := handler.GetHandledNotifications()
		if len(handled) != 1 {
			t.Errorf("Expected 1 handled notification for cluster message, got: %d", len(handled))
		} else if len(handled[0]) != 3 || handled[0][0] != "MOVING" {
			t.Errorf("Expected MOVING notification, got: %v", handled[0])
		}
	})
}
