package maintnotifications

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// createMockConnection creates a mock connection for testing
// Uses the mockNetConn from pool_hook_test.go
func createMockConnection() *pool.Conn {
	mockNetConn := &mockNetConn{}
	return pool.NewConn(mockNetConn)
}

// TestSMigratingNotificationHandler tests the SMIGRATING notification handler
func TestSMigratingNotificationHandler(t *testing.T) {
	t.Run("ValidSMigratingNotification", func(t *testing.T) {
		// Create a mock manager with config
		config := DefaultConfig()
		manager := &Manager{
			config: config,
		}

		// Create notification handler
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Create a mock connection
		conn := createMockConnection()

		// Create SMIGRATING notification: ["SMIGRATING", slot]
		notification := []interface{}{"SMIGRATING", int64(1234)}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{
			Conn: conn,
		}

		// Handle the notification
		err := handler.handleSMigrating(ctx, handlerCtx, notification)
		if err != nil {
			t.Errorf("handleSMigrating should not error: %v", err)
		}

		// Verify relaxed timeout was applied
		if !conn.HasRelaxedTimeout() {
			t.Error("Relaxed timeout should have been set on the connection")
		}
	})

	t.Run("InvalidSMigratingNotification_TooShort", func(t *testing.T) {
		config := DefaultConfig()
		manager := &Manager{
			config: config,
		}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - too short
		notification := []interface{}{"SMIGRATING"}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		err := handler.handleSMigrating(ctx, handlerCtx, notification)
		if err != ErrInvalidNotification {
			t.Errorf("Expected ErrInvalidNotification, got: %v", err)
		}
	})

	t.Run("InvalidSMigratingNotification_InvalidSlot", func(t *testing.T) {
		config := DefaultConfig()
		manager := &Manager{
			config: config,
		}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - slot is not int64
		notification := []interface{}{"SMIGRATING", "not-a-number"}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		err := handler.handleSMigrating(ctx, handlerCtx, notification)
		if err != ErrInvalidNotification {
			t.Errorf("Expected ErrInvalidNotification, got: %v", err)
		}
	})

	t.Run("SMigratingNotification_NoConnection", func(t *testing.T) {
		config := DefaultConfig()
		manager := &Manager{
			config: config,
		}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		notification := []interface{}{"SMIGRATING", int64(1234)}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{} // No connection

		err := handler.handleSMigrating(ctx, handlerCtx, notification)
		if err != ErrInvalidNotification {
			t.Errorf("Expected ErrInvalidNotification when no connection, got: %v", err)
		}
	})
}

// TestSMigratingNotificationRegistration tests that SMIGRATING is registered in the notification types
func TestSMigratingNotificationRegistration(t *testing.T) {
	found := false
	for _, notifType := range maintenanceNotificationTypes {
		if notifType == NotificationSMigrating {
			found = true
			break
		}
	}

	if !found {
		t.Error("SMIGRATING should be registered in maintenanceNotificationTypes")
	}
}

// TestSMigratingConstant tests that the SMIGRATING constant is defined correctly
func TestSMigratingConstant(t *testing.T) {
	if NotificationSMigrating != "SMIGRATING" {
		t.Errorf("NotificationSMigrating constant should be 'SMIGRATING', got: %s", NotificationSMigrating)
	}
}

// TestSMigratedNotificationHandler tests the SMIGRATED notification handler
func TestSMigratedNotificationHandler(t *testing.T) {
	t.Run("ValidSMigratedNotification", func(t *testing.T) {
		// Track if callback was called
		var callbackCalled atomic.Bool
		var receivedSlot int

		// Create a mock manager with callback
		manager := &Manager{
			clusterStateReloadCallback: func(ctx context.Context, slot int) {
				callbackCalled.Store(true)
				receivedSlot = slot
			},
		}

		// Create notification handler
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Create SMIGRATED notification: ["SMIGRATED", slot]
		notification := []interface{}{"SMIGRATED", int64(1234)}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		// Handle the notification
		err := handler.handleSMigrated(ctx, handlerCtx, notification)
		if err != nil {
			t.Errorf("handleSMigrated should not error: %v", err)
		}

		// Verify callback was called
		if !callbackCalled.Load() {
			t.Error("Cluster state reload callback should have been called")
		}

		// Verify slot was passed correctly
		if receivedSlot != 1234 {
			t.Errorf("Expected slot 1234, got %d", receivedSlot)
		}
	})

	t.Run("InvalidSMigratedNotification_TooShort", func(t *testing.T) {
		manager := &Manager{}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - too short
		notification := []interface{}{"SMIGRATED"}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		err := handler.handleSMigrated(ctx, handlerCtx, notification)
		if err != ErrInvalidNotification {
			t.Errorf("Expected ErrInvalidNotification, got: %v", err)
		}
	})

	t.Run("InvalidSMigratedNotification_InvalidSlot", func(t *testing.T) {
		manager := &Manager{}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - slot is not int64
		notification := []interface{}{"SMIGRATED", "not-a-number"}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		err := handler.handleSMigrated(ctx, handlerCtx, notification)
		if err != ErrInvalidNotification {
			t.Errorf("Expected ErrInvalidNotification, got: %v", err)
		}
	})

	t.Run("SMigratedNotification_NoCallback", func(t *testing.T) {
		// Manager without callback should not panic
		manager := &Manager{}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		notification := []interface{}{"SMIGRATED", int64(1234)}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		err := handler.handleSMigrated(ctx, handlerCtx, notification)
		if err != nil {
			t.Errorf("handleSMigrated should not error even without callback: %v", err)
		}
	})
}

// TestSMigratedNotificationRegistration tests that SMIGRATED is registered in the notification types
func TestSMigratedNotificationRegistration(t *testing.T) {
	found := false
	for _, notifType := range maintenanceNotificationTypes {
		if notifType == NotificationSMigrated {
			found = true
			break
		}
	}

	if !found {
		t.Error("SMIGRATED should be registered in maintenanceNotificationTypes")
	}
}

// TestSMigratedConstant tests that the SMIGRATED constant is defined correctly
func TestSMigratedConstant(t *testing.T) {
	if NotificationSMigrated != "SMIGRATED" {
		t.Errorf("NotificationSMigrated constant should be 'SMIGRATED', got: %s", NotificationSMigrated)
	}
}

// TestClusterStateReloadCallback tests the callback setter and trigger
func TestClusterStateReloadCallback(t *testing.T) {
	t.Run("SetAndTriggerCallback", func(t *testing.T) {
		var callbackCalled atomic.Bool
		var receivedCtx context.Context
		var receivedSlot int

		manager := &Manager{}
		callback := func(ctx context.Context, slot int) {
			callbackCalled.Store(true)
			receivedCtx = ctx
			receivedSlot = slot
		}

		manager.SetClusterStateReloadCallback(callback)

		ctx := context.Background()
		testSlot := 1234
		manager.TriggerClusterStateReload(ctx, testSlot)

		if !callbackCalled.Load() {
			t.Error("Callback should have been called")
		}

		if receivedCtx != ctx {
			t.Error("Callback should receive the correct context")
		}

		if receivedSlot != testSlot {
			t.Errorf("Callback should receive the correct slot, got %d, want %d", receivedSlot, testSlot)
		}
	})

	t.Run("TriggerWithoutCallback", func(t *testing.T) {
		manager := &Manager{}
		// Should not panic
		ctx := context.Background()
		manager.TriggerClusterStateReload(ctx, 1234)
	})
}

