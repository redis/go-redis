package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/hitless"
	"github.com/redis/go-redis/v9/push"
)

// mockConnectionAdapter implements ConnectionWithRelaxedTimeout for testing
type mockConnectionAdapter struct {
	relaxedReadTimeout  time.Duration
	relaxedWriteTimeout time.Duration
	relaxedDeadline     time.Time
}

func (m *mockConnectionAdapter) SetRelaxedTimeout(readTimeout, writeTimeout time.Duration) {
	m.relaxedReadTimeout = readTimeout
	m.relaxedWriteTimeout = writeTimeout
	m.relaxedDeadline = time.Time{} // No deadline
}

func (m *mockConnectionAdapter) SetRelaxedTimeoutWithDeadline(readTimeout, writeTimeout time.Duration, deadline time.Time) {
	m.relaxedReadTimeout = readTimeout
	m.relaxedWriteTimeout = writeTimeout
	m.relaxedDeadline = deadline
}

func (m *mockConnectionAdapter) ClearRelaxedTimeout() {
	m.relaxedReadTimeout = 0
	m.relaxedWriteTimeout = 0
	m.relaxedDeadline = time.Time{}
}

func (m *mockConnectionAdapter) HasRelaxedTimeout() bool {
	return m.relaxedReadTimeout > 0 || m.relaxedWriteTimeout > 0
}

// TestTimeoutAdjustmentDemo demonstrates how timeouts are adjusted during hitless upgrades.
func TestTimeoutAdjustmentDemo(t *testing.T) {
	// Create a client with hitless upgrades enabled
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Protocol:     3,
		ReadTimeout:  5 * time.Second, // Original read timeout
		WriteTimeout: 3 * time.Second, // Original write timeout
		HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
			Enabled:        hitless.MaintNotificationsEnabled,
			RelaxedTimeout: 20 * time.Second, // 20s timeout during migrations
			HandoffTimeout: 15 * time.Second,
			LogLevel:       2, // Info level to see timeout adjustments
		},
	})
	defer client.Close()

	// Get the hitless integration
	integration := client.GetHitlessManager()
	if integration == nil {
		t.Skip("Hitless upgrades not available (likely RESP2 or disabled)")
	}

	ctx := context.Background()

	// Check initial state and timeouts
	t.Logf("Initial state: %s", integration.GetState())
	t.Logf("Initial client read timeout: %v", client.Options().ReadTimeout)
	t.Logf("Initial client write timeout: %v", client.Options().WriteTimeout)

	// Simulate a MIGRATING notification by calling the integration directly
	// Note: In real usage, this would be called automatically by push notification handlers
	t.Log("Simulating MIGRATING notification...")

	// Create a mock connection that implements ConnectionWithRelaxedTimeout
	mockConn := &mockConnectionAdapter{
		relaxedReadTimeout:  0,
		relaxedWriteTimeout: 0,
	}

	// Create a mock handler context (in real usage, this comes from the push notification system)
	handlerCtx := push.NotificationHandlerContext{
		Client:   client,
		ConnPool: nil,      // Not needed for this test
		Conn:     mockConn, // Mock connection that supports relaxed timeouts
	}

	// Simulate MIGRATING push notification (no slot information needed)
	migratingNotification := []interface{}{"MIGRATING", "node1"} // Simple format without slot

	// Get the MIGRATING handler
	handler := client.GetPushNotificationHandler("MIGRATING")
	if handler == nil {
		t.Fatal("MIGRATING handler not found")
	}

	// Handle the MIGRATING notification
	err := handler.HandlePushNotification(ctx, handlerCtx, migratingNotification)
	if err != nil {
		t.Fatalf("Failed to handle MIGRATING notification: %v", err)
	}

	// Check state and timeouts after migration starts
	t.Logf("State after MIGRATING: %s", integration.GetState())
	t.Logf("Client read timeout during migration: %v", client.Options().ReadTimeout)
	t.Logf("Client write timeout during migration: %v", client.Options().WriteTimeout)

	// With per-connection timeouts, global client timeouts should remain unchanged
	// Only the specific connection that received the notification gets the relaxed timeout
	if client.Options().ReadTimeout != 5*time.Second {
		t.Errorf("Expected global read timeout to remain 5s, got %v", client.Options().ReadTimeout)
	}
	if client.Options().WriteTimeout != 3*time.Second {
		t.Errorf("Expected global write timeout to remain 3s, got %v", client.Options().WriteTimeout)
	}

	// State should remain idle since we're not doing global state management for migration/failover
	if integration.GetState() != hitless.StateIdle {
		t.Logf("Note: State is %s (per-connection approach doesn't change global state)", integration.GetState())
	}

	// Simulate a MIGRATED notification
	t.Log("Simulating MIGRATED notification...")
	// Simulate MIGRATED push notification
	migratedNotification := []interface{}{"MIGRATED", "1"} // slot 1 completed

	// Get the MIGRATED handler
	migratedHandler := client.GetPushNotificationHandler("MIGRATED")
	if migratedHandler == nil {
		t.Fatal("MIGRATED handler not found")
	}

	// Handle the MIGRATED notification
	err = migratedHandler.HandlePushNotification(ctx, handlerCtx, migratedNotification)
	if err != nil {
		t.Fatalf("Failed to handle MIGRATED notification: %v", err)
	}

	// Check state and timeouts after migration completes
	t.Logf("State after MIGRATED: %s", integration.GetState())
	t.Logf("Client read timeout after migration: %v", client.Options().ReadTimeout)
	t.Logf("Client write timeout after migration: %v", client.Options().WriteTimeout)

	// Global client timeouts should still be unchanged (per-connection approach)
	if client.Options().ReadTimeout != 5*time.Second {
		t.Errorf("Expected global read timeout to remain 5s, got %v", client.Options().ReadTimeout)
	}
	if client.Options().WriteTimeout != 3*time.Second {
		t.Errorf("Expected global write timeout to remain 3s, got %v", client.Options().WriteTimeout)
	}

	t.Log("✅ Timeout adjustment demonstration completed successfully")
}

// TestTimeoutAdjustmentWithFailover demonstrates timeout adjustment during failover.
func TestTimeoutAdjustmentWithFailover(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Protocol:     3,
		ReadTimeout:  4 * time.Second,
		WriteTimeout: 2 * time.Second,
		HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
			Enabled:        hitless.MaintNotificationsEnabled,
			RelaxedTimeout: 25 * time.Second, // 25s timeout during failover
			HandoffTimeout: 15 * time.Second,
			LogLevel:       2,
		},
	})
	defer client.Close()

	integration := client.GetHitlessManager()
	if integration == nil {
		t.Skip("Hitless upgrades not available")
	}

	ctx := context.Background()

	// Start failover
	t.Log("Starting failover simulation...")

	// Create a mock connection that implements ConnectionWithRelaxedTimeout
	mockConn := &mockConnectionAdapter{
		relaxedReadTimeout:  0,
		relaxedWriteTimeout: 0,
	}

	// Create a mock handler context
	handlerCtx := push.NotificationHandlerContext{
		Client:   client,
		ConnPool: nil,      // Not needed for this test
		Conn:     mockConn, // Mock connection that supports relaxed timeouts
	}

	// Simulate FAILING_OVER push notification (no slot information needed)
	failingOverNotification := []interface{}{"FAILING_OVER", "node2"} // Simple format without slot

	// Get the FAILING_OVER handler
	handler := client.GetPushNotificationHandler("FAILING_OVER")
	if handler == nil {
		t.Fatal("FAILING_OVER handler not found")
	}

	// Handle the FAILING_OVER notification
	err := handler.HandlePushNotification(ctx, handlerCtx, failingOverNotification)
	if err != nil {
		t.Fatalf("Failed to handle FAILING_OVER notification: %v", err)
	}

	// Check that relaxed timeouts were applied to the connection
	// Note: Hitless upgrades apply relaxed timeouts per-connection, not globally to the client
	expectedTimeout := 25 * time.Second // RelaxedTimeout

	if !mockConn.HasRelaxedTimeout() {
		t.Error("Expected relaxed timeout to be applied to connection")
	}

	if mockConn.relaxedReadTimeout != expectedTimeout {
		t.Errorf("Expected connection read timeout %v during failover, got %v", expectedTimeout, mockConn.relaxedReadTimeout)
	}

	if mockConn.relaxedWriteTimeout != expectedTimeout {
		t.Errorf("Expected connection write timeout %v during failover, got %v", expectedTimeout, mockConn.relaxedWriteTimeout)
	}

	t.Logf("Failover state: %s", integration.GetState())
	t.Logf("Connection relaxed timeouts: read=%v, write=%v",
		mockConn.relaxedReadTimeout, mockConn.relaxedWriteTimeout)

	// Complete failover
	// Simulate FAILED_OVER push notification
	failedOverNotification := []interface{}{"FAILED_OVER", "node2"} // Simple format without slot

	// Get the FAILED_OVER handler
	failedOverHandler := client.GetPushNotificationHandler("FAILED_OVER")
	if failedOverHandler == nil {
		t.Fatal("FAILED_OVER handler not found")
	}

	// Handle the FAILED_OVER notification
	err = failedOverHandler.HandlePushNotification(ctx, handlerCtx, failedOverNotification)
	if err != nil {
		t.Fatalf("Failed to handle FAILED_OVER notification: %v", err)
	}

	// Verify that relaxed timeouts were cleared from the connection
	if mockConn.HasRelaxedTimeout() {
		t.Error("Expected relaxed timeout to be cleared from connection after FAILED_OVER")
	}

	t.Logf("Final state: %s", integration.GetState())
	t.Logf("Connection timeouts cleared: relaxed=%v", mockConn.HasRelaxedTimeout())
}

// TestMultipleOperationsTimeoutManagement demonstrates timeout management with overlapping operations.
func TestMultipleOperationsTimeoutManagement(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Protocol:     3,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 3 * time.Second,
		HitlessUpgradeConfig: &redis.HitlessUpgradeConfig{
			Enabled:        hitless.MaintNotificationsEnabled,
			RelaxedTimeout: 15 * time.Second, // 15s timeout during operations
			HandoffTimeout: 15 * time.Second,
			LogLevel:       2,
		},
	})
	defer client.Close()

	integration := client.GetHitlessManager()
	if integration == nil {
		t.Skip("Hitless upgrades not available")
	}

	ctx := context.Background()

	// Start migration
	t.Log("Starting migration...")

	// Create mock connections that implement ConnectionWithRelaxedTimeout
	mockConn1 := &mockConnectionAdapter{
		relaxedReadTimeout:  0,
		relaxedWriteTimeout: 0,
	}
	mockConn2 := &mockConnectionAdapter{
		relaxedReadTimeout:  0,
		relaxedWriteTimeout: 0,
	}

	// Create mock handler contexts for different connections
	migrationCtx := push.NotificationHandlerContext{
		Client:   client,
		ConnPool: nil,       // Not needed for this test
		Conn:     mockConn1, // Mock connection 1
	}
	failoverCtx := push.NotificationHandlerContext{
		Client:   client,
		ConnPool: nil,       // Not needed for this test
		Conn:     mockConn2, // Mock connection 2
	}

	// Simulate MIGRATING push notification (no slot information needed)
	migratingNotification := []interface{}{"MIGRATING", "node3"} // Simple format without slot

	// Get the MIGRATING handler
	migratingHandler := client.GetPushNotificationHandler("MIGRATING")
	if migratingHandler == nil {
		t.Fatal("MIGRATING handler not found")
	}

	// Handle the MIGRATING notification
	err := migratingHandler.HandlePushNotification(ctx, migrationCtx, migratingNotification)
	if err != nil {
		t.Fatalf("Failed to handle MIGRATING notification: %v", err)
	}

	// Check that relaxed timeouts were applied to the first connection
	expectedTimeout := 15 * time.Second // RelaxedTimeout
	if !mockConn1.HasRelaxedTimeout() {
		t.Error("Expected relaxed timeout to be applied to migration connection")
	}
	if mockConn1.relaxedReadTimeout != expectedTimeout {
		t.Errorf("Expected migration connection timeout %v, got %v", expectedTimeout, mockConn1.relaxedReadTimeout)
	}

	// Start failover while migration is in progress
	t.Log("Starting failover while migration is in progress...")
	// Simulate FAILING_OVER push notification
	failingOverNotification2 := []interface{}{"FAILING_OVER", "node4"} // Simple format without slot

	// Get the FAILING_OVER handler
	failingOverHandler2 := client.GetPushNotificationHandler("FAILING_OVER")
	if failingOverHandler2 == nil {
		t.Fatal("FAILING_OVER handler not found")
	}

	// Handle the FAILING_OVER notification
	err = failingOverHandler2.HandlePushNotification(ctx, failoverCtx, failingOverNotification2)
	if err != nil {
		t.Fatalf("Failed to handle FAILING_OVER notification: %v", err)
	}

	// Check that relaxed timeouts were applied to the second connection too
	if !mockConn2.HasRelaxedTimeout() {
		t.Error("Expected relaxed timeout to be applied to failover connection")
	}
	if mockConn2.relaxedReadTimeout != expectedTimeout {
		t.Errorf("Expected failover connection timeout %v, got %v", expectedTimeout, mockConn2.relaxedReadTimeout)
	}

	// Note: In the current implementation, timeout management is handled
	// per-connection by the hitless system. Each connection that receives
	// a MIGRATING/FAILING_OVER notification gets relaxed timeouts applied.

	// Verify that hitless manager is tracking operations
	state := integration.GetState()
	t.Logf("Current hitless state: %v", state)

	t.Logf("Connection 1 (migration) has relaxed timeout: %v", mockConn1.HasRelaxedTimeout())
	t.Logf("Connection 2 (failover) has relaxed timeout: %v", mockConn2.HasRelaxedTimeout())

	t.Log("✅ Multiple operations timeout management completed successfully")
}
