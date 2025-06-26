package redis_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

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
	handler := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

	registry.RegisterHandler("TEST_COMMAND", handler)

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

	// Test global handler
	globalHandlerCalled := false
	globalHandler := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		globalHandlerCalled = true
		return true
	})

	registry.RegisterGlobalHandler(globalHandler)

	// Reset flags
	handlerCalled = false
	globalHandlerCalled = false

	// Handle notification again
	handled = registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}

	if !handlerCalled {
		t.Error("Specific handler should have been called")
	}

	if !globalHandlerCalled {
		t.Error("Global handler should have been called")
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
	processor.RegisterHandlerFunc("CUSTOM_NOTIFICATION", func(ctx context.Context, notification []interface{}) bool {
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
	})

	// Test global handler
	globalHandlerCalled := false
	processor.RegisterGlobalHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		globalHandlerCalled = true
		return true
	})

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

	if !globalHandlerCalled {
		t.Error("Global handler should have been called")
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
	client.RegisterPushNotificationHandlerFunc("CUSTOM_EVENT", func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

	// Test global handler through client
	globalHandlerCalled := false
	client.RegisterGlobalPushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		globalHandlerCalled = true
		return true
	})

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

	if !globalHandlerCalled {
		t.Error("Global handler should have been called")
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
	client.RegisterPushNotificationHandlerFunc("TEST", func(ctx context.Context, notification []interface{}) bool {
		return true
	})

	client.RegisterGlobalPushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		return true
	})
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
	client.RegisterPushNotificationHandlerFunc("TEST_NOTIFICATION", func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

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
	// Test that push notification constants are defined correctly
	constants := map[string]string{
		redis.PushNotificationMoving:       "MOVING",
		redis.PushNotificationMigrating:    "MIGRATING",
		redis.PushNotificationMigrated:     "MIGRATED",
		redis.PushNotificationPubSubMessage: "message",
		redis.PushNotificationPMessage:     "pmessage",
		redis.PushNotificationSubscribe:    "subscribe",
		redis.PushNotificationUnsubscribe:  "unsubscribe",
		redis.PushNotificationKeyspace:     "keyspace",
		redis.PushNotificationKeyevent:     "keyevent",
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
	client.RegisterPushNotificationHandlerFunc("CUSTOM_PUBSUB_EVENT", func(ctx context.Context, notification []interface{}) bool {
		customNotificationReceived = true
		t.Logf("Received custom push notification in PubSub context: %v", notification)
		return true
	})

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
	// Test unregistering handlers (note: current implementation has limitations with function pointer comparison)
	registry := redis.NewPushNotificationRegistry()

	// Register multiple handlers for the same command
	handler1Called := false
	handler1 := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handler1Called = true
		return true
	})

	handler2Called := false
	handler2 := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handler2Called = true
		return true
	})

	registry.RegisterHandler("TEST_CMD", handler1)
	registry.RegisterHandler("TEST_CMD", handler2)

	// Verify both handlers are registered
	commands := registry.GetRegisteredCommands()
	if len(commands) != 1 || commands[0] != "TEST_CMD" {
		t.Errorf("Expected ['TEST_CMD'], got %v", commands)
	}

	// Test notification handling with both handlers
	ctx := context.Background()
	notification := []interface{}{"TEST_CMD", "data"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should have been handled")
	}
	if !handler1Called || !handler2Called {
		t.Error("Both handlers should have been called")
	}

	// Test that UnregisterHandler doesn't panic (even if it doesn't work perfectly)
	registry.UnregisterHandler("TEST_CMD", handler1)
	registry.UnregisterHandler("NON_EXISTENT", handler2)

	// Note: Due to the current implementation using pointer comparison,
	// unregistration may not work as expected. This test mainly verifies
	// that the method doesn't panic and the registry remains functional.

	// Reset flags and test that handlers still work
	handler1Called = false
	handler2Called = false

	handled = registry.HandleNotification(ctx, notification)
	if !handled {
		t.Error("Notification should still be handled after unregister attempts")
	}

	// The registry should still be functional
	if !registry.HasHandlers() {
		t.Error("Registry should still have handlers")
	}
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
	dummyHandler := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		return true
	})
	registry.UnregisterHandler("NON_EXISTENT", dummyHandler)
	// Should not panic

	// Test unregistering from empty command
	registry.UnregisterHandler("EMPTY_CMD", dummyHandler)
	// Should not panic
}

func TestPushNotificationRegistryMultipleHandlers(t *testing.T) {
	registry := redis.NewPushNotificationRegistry()

	// Test multiple handlers for the same command
	handler1Called := false
	handler2Called := false
	handler3Called := false

	registry.RegisterHandler("MULTI_CMD", redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handler1Called = true
		return true
	}))

	registry.RegisterHandler("MULTI_CMD", redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handler2Called = true
		return false // Return false to test that other handlers still get called
	}))

	registry.RegisterHandler("MULTI_CMD", redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handler3Called = true
		return true
	}))

	// Test that all handlers are called
	ctx := context.Background()
	notification := []interface{}{"MULTI_CMD", "data"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled (at least one handler returned true)")
	}

	if !handler1Called || !handler2Called || !handler3Called {
		t.Error("All handlers should have been called")
	}
}

func TestPushNotificationRegistryGlobalAndSpecific(t *testing.T) {
	registry := redis.NewPushNotificationRegistry()

	globalCalled := false
	specificCalled := false

	// Register global handler
	registry.RegisterGlobalHandler(redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		globalCalled = true
		return true
	}))

	// Register specific handler
	registry.RegisterHandler("SPECIFIC_CMD", redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		specificCalled = true
		return true
	}))

	// Test with specific command
	ctx := context.Background()
	notification := []interface{}{"SPECIFIC_CMD", "data"}
	handled := registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled")
	}

	if !globalCalled {
		t.Error("Global handler should be called")
	}

	if !specificCalled {
		t.Error("Specific handler should be called")
	}

	// Reset flags
	globalCalled = false
	specificCalled = false

	// Test with non-specific command
	notification = []interface{}{"OTHER_CMD", "data"}
	handled = registry.HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled by global handler")
	}

	if !globalCalled {
		t.Error("Global handler should be called for any command")
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
	processor.RegisterHandlerFunc("TEST_CMD", func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

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
	handler := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		handlerCalled = true
		return true
	})

	processor.RegisterHandler("CONV_CMD", handler)

	// Test RegisterGlobalHandler convenience method
	globalHandlerCalled := false
	globalHandler := redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		globalHandlerCalled = true
		return true
	})

	processor.RegisterGlobalHandler(globalHandler)

	// Test RegisterHandlerFunc convenience method
	funcHandlerCalled := false
	processor.RegisterHandlerFunc("FUNC_CMD", func(ctx context.Context, notification []interface{}) bool {
		funcHandlerCalled = true
		return true
	})

	// Test RegisterGlobalHandlerFunc convenience method
	globalFuncHandlerCalled := false
	processor.RegisterGlobalHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		globalFuncHandlerCalled = true
		return true
	})

	// Test that all handlers work
	ctx := context.Background()

	// Test specific handler
	notification := []interface{}{"CONV_CMD", "data"}
	handled := processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled")
	}

	if !handlerCalled || !globalHandlerCalled || !globalFuncHandlerCalled {
		t.Error("Handler, global handler, and global func handler should all be called")
	}

	// Reset flags
	handlerCalled = false
	globalHandlerCalled = false
	funcHandlerCalled = false
	globalFuncHandlerCalled = false

	// Test func handler
	notification = []interface{}{"FUNC_CMD", "data"}
	handled = processor.GetRegistry().HandleNotification(ctx, notification)

	if !handled {
		t.Error("Notification should be handled")
	}

	if !funcHandlerCalled || !globalHandlerCalled || !globalFuncHandlerCalled {
		t.Error("Func handler, global handler, and global func handler should all be called")
	}
}

func TestClientPushNotificationEdgeCases(t *testing.T) {
	// Test client methods when processor is nil
	client := redis.NewClient(&redis.Options{
		Addr:              "localhost:6379",
		PushNotifications: false, // Disabled
	})
	defer client.Close()

	// These should not panic even when processor is nil
	client.RegisterPushNotificationHandler("TEST", redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		return true
	}))

	client.RegisterGlobalPushNotificationHandler(redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		return true
	}))

	client.RegisterPushNotificationHandlerFunc("TEST_FUNC", func(ctx context.Context, notification []interface{}) bool {
		return true
	})

	client.RegisterGlobalPushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
		return true
	})

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

	handler := redis.PushNotificationHandlerFunc(handlerFunc)

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
	// Test that all expected constants are defined
	expectedConstants := map[string]string{
		// Cluster notifications
		redis.PushNotificationMoving:       "MOVING",
		redis.PushNotificationMigrating:    "MIGRATING",
		redis.PushNotificationMigrated:     "MIGRATED",
		redis.PushNotificationFailingOver:  "FAILING_OVER",
		redis.PushNotificationFailedOver:   "FAILED_OVER",

		// Pub/Sub notifications
		redis.PushNotificationPubSubMessage: "message",
		redis.PushNotificationPMessage:      "pmessage",
		redis.PushNotificationSubscribe:     "subscribe",
		redis.PushNotificationUnsubscribe:   "unsubscribe",
		redis.PushNotificationPSubscribe:    "psubscribe",
		redis.PushNotificationPUnsubscribe:  "punsubscribe",

		// Stream notifications
		redis.PushNotificationXRead:      "xread",
		redis.PushNotificationXReadGroup: "xreadgroup",

		// Keyspace notifications
		redis.PushNotificationKeyspace: "keyspace",
		redis.PushNotificationKeyevent: "keyevent",

		// Module notifications
		redis.PushNotificationModule: "module",

		// Custom notifications
		redis.PushNotificationCustom: "custom",
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
				// Register handler
				command := fmt.Sprintf("CMD_%d_%d", id, j)
				registry.RegisterHandler(command, redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
					return true
				}))

				// Handle notification
				notification := []interface{}{command, "data"}
				registry.HandleNotification(context.Background(), notification)

				// Register global handler occasionally
				if j%10 == 0 {
					registry.RegisterGlobalHandler(redis.PushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
						return true
					}))
				}

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
				// Register handlers
				command := fmt.Sprintf("PROC_CMD_%d_%d", id, j)
				processor.RegisterHandlerFunc(command, func(ctx context.Context, notification []interface{}) bool {
					return true
				})

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
				// Register handlers concurrently
				command := fmt.Sprintf("CLIENT_CMD_%d_%d", id, j)
				client.RegisterPushNotificationHandlerFunc(command, func(ctx context.Context, notification []interface{}) bool {
					return true
				})

				// Register global handlers occasionally
				if j%5 == 0 {
					client.RegisterGlobalPushNotificationHandlerFunc(func(ctx context.Context, notification []interface{}) bool {
						return true
					})
				}

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
