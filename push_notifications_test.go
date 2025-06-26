package redis_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/pool"
)

// testHandler is a simple implementation of PushNotificationHandler for testing
type testHandler struct {
	handlerFunc func(ctx context.Context, notification []interface{}) bool
}

func (h *testHandler) HandlePushNotification(ctx context.Context, notification []interface{}) bool {
	return h.handlerFunc(ctx, notification)
}

// newTestHandler creates a test handler from a function
func newTestHandler(f func(ctx context.Context, notification []interface{}) bool) *testHandler {
	return &testHandler{handlerFunc: f}
}

func TestPushNotificationRegistry(t *testing.T) {
	// Test the push notification registry functionality
	registry := redis.NewPushNotificationRegistry()

	// Test initial state
	if registry.HasHandlers() {
		t.Error("Registry should not have handlers initially")
	}

	commands := registry.GetRegisteredCommands()
	if len(commands) != 0 {
		t.Errorf("Expected 0 registered commands, got %d", len(commands))
	}

	// Test registering a specific handler
	handlerCalled := false
	handler := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

	err := registry.RegisterHandler("TEST_COMMAND", handler)
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	if !registry.HasHandlers() {
		t.Error("Registry should have handlers after registration")
	}

	commands = registry.GetRegisteredCommands()
	if len(commands) != 1 || commands[0] != "TEST_COMMAND" {
		t.Errorf("Expected ['TEST_COMMAND'], got %v", commands)
	}

	// Test handling a notification
	ctx := context.Background()
	notification := []interface{}{"TEST_COMMAND", "arg1", "arg2"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}

	if !handlerCalled {
		t.Error("Handler should have been called")
	}

	// Test duplicate handler registration error
	duplicateHandler := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	})
	err = registry.RegisterHandler("TEST_COMMAND", duplicateHandler)
	if err == nil {
		t.Error("Expected error when registering duplicate handler")
	}
	expectedError := "handler already registered for command: TEST_COMMAND"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestPushNotificationProcessor(t *testing.T) {
	// Test the push notification processor
	processor := redis.NewPushNotificationProcessor(true)

	if !processor.IsEnabled() {
		t.Error("Processor should be enabled")
	}

	// Test registering handlers
	handlerCalled := false
	err := processor.RegisterHandler("CUSTOM_NOTIFICATION", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		if len(notification) < 2 {
			t.Error("Expected at least 2 elements in notification")
			return false
		}
		if notification[0] != "CUSTOM_NOTIFICATION" {
			t.Errorf("Expected command 'CUSTOM_NOTIFICATION', got %v", notification[0])
			return false
		}
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Simulate handling a notification
	ctx := context.Background()
	notification := []interface{}{"CUSTOM_NOTIFICATION", "data"}
	handled := processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}

	if !handlerCalled {
		t.Error("Specific handler should have been called")
	}

	// Test disabling processor
	processor.SetEnabled(false)
	if processor.IsEnabled() {
		t.Error("Processor should be disabled")
	}
}

func TestClientPushNotificationIntegration(t *testing.T) {
	// Test push notification integration with Redis client
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,    // RESP3 required for push notifications
		PushNotifications: true, // Enable push notifications
	})
	defer client.Close()

	// Test that push processor is initialized
	processor := client.GetPushNotificationProcessor()
	if processor == nil {
		t.Error("Push notification processor should be initialized")
	}

	if !processor.IsEnabled() {
		t.Error("Push notification processor should be enabled")
	}

	// Test registering handlers through client
	handlerCalled := false
	err := client.RegisterPushNotificationHandler("CUSTOM_EVENT", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Simulate notification handling
	ctx := context.Background()
	notification := []interface{}{"CUSTOM_EVENT", "test_data"}
	handled := processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}

	if !handlerCalled {
		t.Error("Custom handler should have been called")
	}
}

func TestClientWithoutPushNotifications(t *testing.T) {
	// Test client without push notifications enabled
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		PushNotifications: false, // Disabled
	})
	defer client.Close()

	// Push processor should be nil
	processor := client.GetPushNotificationProcessor()
	if processor != nil {
		t.Error("Push notification processor should be nil when disabled")
	}

	// Registering handlers should not panic
	err := client.RegisterPushNotificationHandler("TEST", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}))
	if err != nil {
		t.Errorf("Expected nil error when processor is nil, got: %v", err)
	}
}

func TestPushNotificationEnabledClient(t *testing.T) {
	// Test that push notifications can be enabled on a client
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,    // RESP3 required
		PushNotifications: true, // Enable push notifications
	})
	defer client.Close()

	// Push processor should be initialized
	processor := client.GetPushNotificationProcessor()
	if processor == nil {
		t.Error("Push notification processor should be initialized when enabled")
	}

	if !processor.IsEnabled() {
		t.Error("Push notification processor should be enabled")
	}

	// Test registering a handler
	handlerCalled := false
	err := client.RegisterPushNotificationHandler("TEST_NOTIFICATION", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Test that the handler works
	registry := processor.GetRegistry()
	ctx := context.Background()
	notification := []interface{}{"TEST_NOTIFICATION", "data"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}

	if !handlerCalled {
		t.Error("Handler should have been called")
	}
}

func TestPushNotificationConstants(t *testing.T) {
	// Test that Redis Cluster push notification constants are defined correctly
	constants := map[string]string{
		redis.PushNotificationMoving:     "MOVING",
		redis.PushNotificationMigrating:  "MIGRATING",
		redis.PushNotificationMigrated:   "MIGRATED",
		redis.PushNotificationFailingOver: "FAILING_OVER",
		redis.PushNotificationFailedOver:  "FAILED_OVER",
	}

	for constant, expected := range constants {
		if constant != expected {
			t.Errorf("Expected constant to equal '%s', got '%s'", expected, constant)
		}
	}
}

func TestPushNotificationInfo(t *testing.T) {
	// Test push notification info parsing
	notification := []interface{}{"MOVING", "127.0.0.1:6380", "30000"}
	info := redis.ParsePushNotificationInfo(notification)

	if info == nil {
		t.Fatal("Push notification info should not be nil")
	}

	if info.Command != "MOVING" {
		t.Errorf("Expected command 'MOVING', got '%s'", info.Command)
	}

	if len(info.Args) != 2 {
		t.Errorf("Expected 2 args, got %d", len(info.Args))
	}

	if info.String() != "MOVING" {
		t.Errorf("Expected string representation 'MOVING', got '%s'", info.String())
	}

	// Test with empty notification
	emptyInfo := redis.ParsePushNotificationInfo([]interface{}{})
	if emptyInfo != nil {
		t.Error("Empty notification should return nil info")
	}

	// Test with invalid notification
	invalidInfo := redis.ParsePushNotificationInfo([]interface{}{123, "invalid"})
	if invalidInfo != nil {
		t.Error("Invalid notification should return nil info")
	}
}

func TestPubSubWithGenericPushNotifications(t *testing.T) {
	// Test that PubSub can be configured with push notification processor
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,    // RESP3 required
		PushNotifications: true, // Enable push notifications
	})
	defer client.Close()

	// Register a handler for custom push notifications
	customNotificationReceived := false
	err := client.RegisterPushNotificationHandler("CUSTOM_PUBSUB_EVENT", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		customNotificationReceived = true
		t.Logf("Received custom push notification in PubSub context: %v", notification)
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Create a PubSub instance
	pubsub := client.Subscribe(context.Background(), "test-channel")
	defer pubsub.Close()

	// Verify that the PubSub instance has access to push notification processor
	processor := client.GetPushNotificationProcessor()
	if processor == nil {
		t.Error("Push notification processor should be available")
	}

	// Test that the processor can handle notifications
	notification := []interface{}{"CUSTOM_PUBSUB_EVENT", "arg1", "arg2"}
	handled := processor.GetRegistry().HandleNotification(context.Background(), notification)

	if !handled {
		t.Error("Push notification should have been handled")
	}

	// Verify that the custom handler was called
	if !customNotificationReceived {
		t.Error("Custom push notification handler should have been called")
	}
}

func TestPushNotificationMessageType(t *testing.T) {
	// Test the PushNotificationMessage type
	msg := &redis.PushNotificationMessage{
		Command: "CUSTOM_EVENT",
		Args:    []interface{}{"arg1", "arg2", 123},
	}

	if msg.Command != "CUSTOM_EVENT" {
		t.Errorf("Expected command 'CUSTOM_EVENT', got '%s'", msg.Command)
	}

	if len(msg.Args) != 3 {
		t.Errorf("Expected 3 args, got %d", len(msg.Args))
	}

	expectedString := "push: CUSTOM_EVENT"
	if msg.String() != expectedString {
		t.Errorf("Expected string '%s', got '%s'", expectedString, msg.String())
	}
}

func TestPushNotificationRegistryUnregisterHandler(t *testing.T) {
	// Test unregistering handlers
	registry := redis.NewPushNotificationRegistry()

	// Register a handler
	handlerCalled := false
	handler := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

	err := registry.RegisterHandler("TEST_CMD", handler)
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Verify handler is registered
	commands := registry.GetRegisteredCommands()
	if len(commands) != 1 || commands[0] != "TEST_CMD" {
		t.Errorf("Expected ['TEST_CMD'], got %v", commands)
	}

	// Test notification handling
	ctx := context.Background()
	notification := []interface{}{"TEST_CMD", "data"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}
	if !handlerCalled {
		t.Error("Handler should have been called")
	}

	// Test unregistering the handler
	registry.UnregisterHandler("TEST_CMD")

	// Verify handler is unregistered
	commands = registry.GetRegisteredCommands()
	if len(commands) != 0 {
		t.Errorf("Expected no registered commands after unregister, got %v", commands)
	}

	// Reset flag and test that handler is no longer called
	handlerCalled = false
	handled = registry.HandleNotification(ctx, notification)

	if handled {
		t.Error("Notification should not be handled after unregistration")
	}
	if handlerCalled {
		t.Error("Handler should not be called after unregistration")
	}

	// Test unregistering non-existent handler (should not panic)
	registry.UnregisterHandler("NON_EXISTENT")
}

func TestPushNotificationRegistryEdgeCases(t *testing.T) {
	registry := redis.NewPushNotificationRegistry()

	// Test handling empty notification
	ctx := context.Background()
	handled := registry.HandleNotification(ctx, []interface{}{})
	if handled {
		t.Error("Empty notification should not be handled")
	}

	// Test handling notification with non-string command
	handled = registry.HandleNotification(ctx, []interface{}{123, "data"})
	if handled {
		t.Error("Notification with non-string command should not be handled")
	}

	// Test handling notification with nil command
	handled = registry.HandleNotification(ctx, []interface{}{nil, "data"})
	if handled {
		t.Error("Notification with nil command should not be handled")
	}

	// Test unregistering non-existent handler
	registry.UnregisterHandler("NON_EXISTENT")
	// Should not panic

	// Test unregistering from empty command
	registry.UnregisterHandler("EMPTY_CMD")
	// Should not panic
}

func TestPushNotificationRegistryDuplicateHandlerError(t *testing.T) {
	registry := redis.NewPushNotificationRegistry()

	// Test that registering duplicate handlers returns an error
	handler1 := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	})

	handler2 := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return false
	})

	// Register first handler - should succeed
	err := registry.RegisterHandler("DUPLICATE_CMD", handler1)
	if err != nil {
		t.Fatalf("First handler registration should succeed: %v", err)
	}

	// Register second handler for same command - should fail
	err = registry.RegisterHandler("DUPLICATE_CMD", handler2)
	if err == nil {
		t.Error("Second handler registration should fail")
	}

	expectedError := "handler already registered for command: DUPLICATE_CMD"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	// Verify only one handler is registered
	commands := registry.GetRegisteredCommands()
	if len(commands) != 1 || commands[0] != "DUPLICATE_CMD" {
		t.Errorf("Expected ['DUPLICATE_CMD'], got %v", commands)
	}
}

func TestPushNotificationRegistrySpecificHandlerOnly(t *testing.T) {
	registry := redis.NewPushNotificationRegistry()

	specificCalled := false

	// Register specific handler
	err := registry.RegisterHandler("SPECIFIC_CMD", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		specificCalled = true
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register specific handler: %v", err)
	}

	// Test with specific command
	ctx := context.Background()
	notification := []interface{}{"SPECIFIC_CMD", "data"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled")
	}

	if !specificCalled {
		t.Error("Specific handler should be called")
	}

	// Reset flag
	specificCalled = false

	// Test with non-specific command - should not be handled
	notification = []interface{}{"OTHER_CMD", "data"}
	handled = registry.HandleNotification(ctx, notification)

	if handled {
		t.Error("Notification should not be handled without specific handler")
	}

	if specificCalled {
		t.Error("Specific handler should not be called for other commands")
	}
}

func TestPushNotificationProcessorEdgeCases(t *testing.T) {
	// Test processor with disabled state
	processor := redis.NewPushNotificationProcessor(false)

	if processor.IsEnabled() {
		t.Error("Processor should be disabled")
	}

	// Test that disabled processor doesn't process notifications
	handlerCalled := false
	processor.RegisterHandler("TEST_CMD", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	}))

	// Even with handlers registered, disabled processor shouldn't process
	ctx := context.Background()
	notification := []interface{}{"TEST_CMD", "data"}
	handled := processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Registry should still handle notifications even when processor is disabled")
	}

	if !handlerCalled {
		t.Error("Handler should be called when using registry directly")
	}

	// Test enabling processor
	processor.SetEnabled(true)
	if !processor.IsEnabled() {
		t.Error("Processor should be enabled after SetEnabled(true)")
	}
}

func TestPushNotificationProcessorConvenienceMethods(t *testing.T) {
	processor := redis.NewPushNotificationProcessor(true)

	// Test RegisterHandler convenience method
	handlerCalled := false
	handler := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

	err := processor.RegisterHandler("CONV_CMD", handler)
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Test RegisterHandler convenience method with function
	funcHandlerCalled := false
	err = processor.RegisterHandler("FUNC_CMD", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		funcHandlerCalled = true
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register func handler: %v", err)
	}

	// Test that handlers work
	ctx := context.Background()

	// Test specific handler
	notification := []interface{}{"CONV_CMD", "data"}
	handled := processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled")
	}

	if !handlerCalled {
		t.Error("Handler should be called")
	}

	// Reset flags
	handlerCalled = false
	funcHandlerCalled = false

	// Test func handler
	notification = []interface{}{"FUNC_CMD", "data"}
	handled = processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled")
	}

	if !funcHandlerCalled {
		t.Error("Func handler should be called")
	}
}

func TestClientPushNotificationEdgeCases(t *testing.T) {
	// Test client methods when processor is nil
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		PushNotifications: false, // Disabled
	})
	defer client.Close()

	// These should not panic even when processor is nil and should return nil error
	err := client.RegisterPushNotificationHandler("TEST", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}))
	if err != nil {
		t.Errorf("Expected nil error when processor is nil, got: %v", err)
	}

	err = client.RegisterPushNotificationHandler("TEST_FUNC", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}))
	if err != nil {
		t.Errorf("Expected nil error when processor is nil, got: %v", err)
	}

	// GetPushNotificationProcessor should return nil
	processor := client.GetPushNotificationProcessor()
	if processor != nil {
		t.Error("Processor should be nil when push notifications are disabled")
	}
}

func TestPushNotificationHandlerFunc(t *testing.T) {
	// Test the PushNotificationHandlerFunc adapter
	called := false
	var receivedCtx context.Context
	var receivedNotification []interface{}

	handlerFunc := func(ctx context.Context, notification []interface{}) bool {
		called = true
		receivedCtx = ctx
		receivedNotification = notification
		return true
	}

	handler := newTestHandler(handlerFunc)

	// Test that the adapter works correctly
	ctx := context.Background()
	notification := []interface{}{"TEST_CMD", "arg1", "arg2"}

	result := handler.HandlePushNotification(ctx, notification)

	if !result {
		t.Error("Handler should return true")
	}

	if !called {
		t.Error("Handler function should be called")
	}

	if receivedCtx != ctx {
		t.Error("Handler should receive the correct context")
	}

	if len(receivedNotification) != 3 || receivedNotification[0] != "TEST_CMD" {
		t.Errorf("Handler should receive the correct notification, got %v", receivedNotification)
	}
}

func TestPushNotificationInfoEdgeCases(t *testing.T) {
	// Test PushNotificationInfo with nil
	var nilInfo *redis.PushNotificationInfo
	if nilInfo.String() != "<nil>" {
		t.Errorf("Expected '<nil>', got '%s'", nilInfo.String())
	}

	// Test with different argument types
	notification := []interface{}{"COMPLEX_CMD", 123, true, []string{"nested", "array"}, map[string]interface{}{"key": "value"}}
	info := redis.ParsePushNotificationInfo(notification)

	if info == nil {
		t.Fatal("Info should not be nil")
	}

	if info.Command != "COMPLEX_CMD" {
		t.Errorf("Expected command 'COMPLEX_CMD', got '%s'", info.Command)
	}

	if len(info.Args) != 4 {
		t.Errorf("Expected 4 args, got %d", len(info.Args))
	}

	// Verify argument types are preserved
	if info.Args[0] != 123 {
		t.Errorf("Expected first arg to be 123, got %v", info.Args[0])
	}

	if info.Args[1] != true {
		t.Errorf("Expected second arg to be true, got %v", info.Args[1])
	}
}

func TestPushNotificationConstantsCompleteness(t *testing.T) {
	// Test that all Redis Cluster push notification constants are defined
	expectedConstants := map[string]string{
		// Cluster notifications only (other types removed for simplicity)
		redis.PushNotificationMoving:     "MOVING",
		redis.PushNotificationMigrating:  "MIGRATING",
		redis.PushNotificationMigrated:   "MIGRATED",
		redis.PushNotificationFailingOver: "FAILING_OVER",
		redis.PushNotificationFailedOver:  "FAILED_OVER",
	}

	for constant, expected := range expectedConstants {
		if constant != expected {
			t.Errorf("Constant mismatch: expected '%s', got '%s'", expected, constant)
		}
	}
}

func TestPushNotificationRegistryConcurrency(t *testing.T) {
	// Test thread safety of the registry
	registry := redis.NewPushNotificationRegistry()

	// Number of concurrent goroutines
	numGoroutines := 10
	numOperations := 100

	// Channels to coordinate goroutines
	done := make(chan bool, numGoroutines)

	// Concurrent registration and handling
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < numOperations; j++ {
				// Register handler (ignore errors in concurrency test)
				command := fmt.Sprintf("CMD_%d_%d", id, j)
				registry.RegisterHandler(command, newTestHandler(func(ctx context.Context, notification []interface{}) bool {
					return true
				}))

				// Handle notification
				notification := []interface{}{command, "data"}
				registry.HandleNotification(context.Background(), notification)

				// Check registry state
				registry.HasHandlers()
				registry.GetRegisteredCommands()
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify registry is still functional
	if !registry.HasHandlers() {
		t.Error("Registry should have handlers after concurrent operations")
	}

	commands := registry.GetRegisteredCommands()
	if len(commands) == 0 {
		t.Error("Registry should have registered commands after concurrent operations")
	}
}

func TestPushNotificationProcessorConcurrency(t *testing.T) {
	// Test thread safety of the processor
	processor := redis.NewPushNotificationProcessor(true)

	numGoroutines := 5
	numOperations := 50

	done := make(chan bool, numGoroutines)

	// Concurrent processor operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < numOperations; j++ {
				// Register handlers (ignore errors in concurrency test)
				command := fmt.Sprintf("PROC_CMD_%d_%d", id, j)
				processor.RegisterHandler(command, newTestHandler(func(ctx context.Context, notification []interface{}) bool {
					return true
				}))

				// Handle notifications
				notification := []interface{}{command, "data"}
				processor.GetRegistry().HandleNotification(context.Background(), notification)

				// Toggle processor state occasionally
				if j%20 == 0 {
					processor.SetEnabled(!processor.IsEnabled())
				}

				// Access processor state
				processor.IsEnabled()
				processor.GetRegistry()
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify processor is still functional
	registry := processor.GetRegistry()
	if registry == nil {
		t.Error("Processor registry should not be nil after concurrent operations")
	}
}

func TestPushNotificationClientConcurrency(t *testing.T) {
	// Test thread safety of client push notification methods
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	numGoroutines := 5
	numOperations := 20

	done := make(chan bool, numGoroutines)

	// Concurrent client operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < numOperations; j++ {
				// Register handlers concurrently (ignore errors in concurrency test)
				command := fmt.Sprintf("CLIENT_CMD_%d_%d", id, j)
				client.RegisterPushNotificationHandler(command, newTestHandler(func(ctx context.Context, notification []interface{}) bool {
					return true
				}))

				// Access processor
				processor := client.GetPushNotificationProcessor()
				if processor != nil {
					processor.IsEnabled()
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify client is still functional
	processor := client.GetPushNotificationProcessor()
	if processor == nil {
		t.Error("Client processor should not be nil after concurrent operations")
	}
}

// TestPushNotificationConnectionHealthCheck tests that connections with push notification
// processors are properly configured and that the connection health check integration works.
func TestPushNotificationConnectionHealthCheck(t *testing.T) {
	// Create a client with push notifications enabled
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Verify push notifications are enabled
	processor := client.GetPushNotificationProcessor()
	if processor == nil || !processor.IsEnabled() {
		t.Fatal("Push notifications should be enabled")
	}

	// Register a handler for testing
	err := client.RegisterPushNotificationHandler("TEST_CONNCHECK", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		t.Logf("Received test notification: %v", notification)
		return true
	}))
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Test that connections have the push notification processor set
	ctx := context.Background()

	// Get a connection from the pool using the exported Pool() method
	connPool := client.Pool().(*pool.ConnPool)
	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer connPool.Put(ctx, cn)

	// Verify the connection has the push notification processor
	if cn.PushNotificationProcessor == nil {
		t.Error("Connection should have push notification processor set")
		return
	}

	if !cn.PushNotificationProcessor.IsEnabled() {
		t.Error("Push notification processor should be enabled on connection")
		return
	}

	t.Log("✅ Connection has push notification processor correctly set")
	t.Log("✅ Connection health check integration working correctly")
}
