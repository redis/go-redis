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

		// Create SMIGRATING notification: ["SMIGRATING", SeqID, slot/range, ...]
		notification := []interface{}{"SMIGRATING", int64(123), "1234", "5000-6000"}

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

	t.Run("InvalidSMigratingNotification_InvalidSeqID", func(t *testing.T) {
		config := DefaultConfig()
		manager := &Manager{
			config: config,
		}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - SeqID is not int64
		notification := []interface{}{"SMIGRATING", "not-a-number", "1234"}

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

		notification := []interface{}{"SMIGRATING", int64(123), "1234"}

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
		var receivedHostPort string
		var receivedSlotRanges []string

		// Create a mock manager with callback
		manager := &Manager{
			clusterStateReloadCallback: func(ctx context.Context, hostPort string, slotRanges []string) {
				callbackCalled.Store(true)
				receivedHostPort = hostPort
				receivedSlotRanges = slotRanges
			},
		}

		// Create notification handler
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Create SMIGRATED notification: ["SMIGRATED", SeqID, host:port, slot/range, ...]
		notification := []interface{}{"SMIGRATED", int64(123), "127.0.0.1:6379", "1234", "5000-6000"}

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

		// Verify host:port was passed correctly
		if receivedHostPort != "127.0.0.1:6379" {
			t.Errorf("Expected host:port '127.0.0.1:6379', got '%s'", receivedHostPort)
		}

		// Verify slot ranges were passed correctly
		if len(receivedSlotRanges) != 2 {
			t.Errorf("Expected 2 slot ranges, got %d", len(receivedSlotRanges))
		}
		if len(receivedSlotRanges) >= 2 {
			if receivedSlotRanges[0] != "1234" {
				t.Errorf("Expected first slot range '1234', got '%s'", receivedSlotRanges[0])
			}
			if receivedSlotRanges[1] != "5000-6000" {
				t.Errorf("Expected second slot range '5000-6000', got '%s'", receivedSlotRanges[1])
			}
		}
	})

t.Run("SMigratedNotification_Deduplication", func(t *testing.T) {
	// Track callback invocations
	var callbackCount atomic.Int32

	// Create a mock manager with callback
	manager := &Manager{
		clusterStateReloadCallback: func(ctx context.Context, hostPort string, slotRanges []string) {
			callbackCount.Add(1)
		},
	}

	// Create notification handler
	handler := &NotificationHandler{
		manager:           manager,
		operationsManager: manager,
	}

	// Create SMIGRATED notification with SeqID 456
	notification := []interface{}{"SMIGRATED", int64(456), "127.0.0.1:6379", "1234"}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{}

	// Handle the notification first time
	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated should not error on first call: %v", err)
	}

	// Verify callback was called once
	if callbackCount.Load() != 1 {
		t.Errorf("Expected callback to be called once, got %d", callbackCount.Load())
	}

	// Handle the same notification again (simulating multiple connections)
	err = handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated should not error on second call: %v", err)
	}

	// Verify callback was NOT called again (still 1)
	if callbackCount.Load() != 1 {
		t.Errorf("Expected callback to be called only once (deduplication), got %d", callbackCount.Load())
	}

	// Handle a different notification with different SeqID
	notification2 := []interface{}{"SMIGRATED", int64(789), "127.0.0.1:6380", "5678"}
	err = handler.handleSMigrated(ctx, handlerCtx, notification2)
	if err != nil {
		t.Errorf("handleSMigrated should not error on third call: %v", err)
	}

	// Verify callback was called again (now 2)
	if callbackCount.Load() != 2 {
		t.Errorf("Expected callback to be called twice (different SeqID), got %d", callbackCount.Load())
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

	t.Run("InvalidSMigratedNotification_InvalidSeqID", func(t *testing.T) {
		manager := &Manager{}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - SeqID is not int64
		notification := []interface{}{"SMIGRATED", "not-a-number", "127.0.0.1:6379", "1234"}

		ctx := context.Background()
		handlerCtx := push.NotificationHandlerContext{}

		err := handler.handleSMigrated(ctx, handlerCtx, notification)
		if err != ErrInvalidNotification {
			t.Errorf("Expected ErrInvalidNotification, got: %v", err)
		}
	})

	t.Run("InvalidSMigratedNotification_InvalidHostPort", func(t *testing.T) {
		manager := &Manager{}
		handler := &NotificationHandler{
			manager:           manager,
			operationsManager: manager,
		}

		// Invalid notification - host:port is not string
		notification := []interface{}{"SMIGRATED", int64(123), int64(999), "1234"}

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

		notification := []interface{}{"SMIGRATED", int64(123), "127.0.0.1:6379", "1234"}

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
		var receivedHostPort string
		var receivedSlotRanges []string

		manager := &Manager{}
		callback := func(ctx context.Context, hostPort string, slotRanges []string) {
			callbackCalled.Store(true)
			receivedCtx = ctx
			receivedHostPort = hostPort
			receivedSlotRanges = slotRanges
		}

		manager.SetClusterStateReloadCallback(callback)

		ctx := context.Background()
		testHostPort := "127.0.0.1:6379"
		testSlotRanges := []string{"1234", "5000-6000"}
		manager.TriggerClusterStateReload(ctx, testHostPort, testSlotRanges)

		if !callbackCalled.Load() {
			t.Error("Callback should have been called")
		}

		if receivedCtx != ctx {
			t.Error("Callback should receive the correct context")
		}

		if receivedHostPort != testHostPort {
			t.Errorf("Callback should receive the correct host:port, got %s, want %s", receivedHostPort, testHostPort)
		}

		if len(receivedSlotRanges) != len(testSlotRanges) {
			t.Errorf("Callback should receive the correct slot ranges, got %v, want %v", receivedSlotRanges, testSlotRanges)
		}
	})

	t.Run("TriggerWithoutCallback", func(t *testing.T) {
		manager := &Manager{}
		// Should not panic
		ctx := context.Background()
		manager.TriggerClusterStateReload(ctx, "127.0.0.1:6379", []string{"1234"})
	})
}

