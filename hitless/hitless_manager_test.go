package hitless

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/interfaces"
)

// MockClient implements interfaces.ClientInterface for testing
type MockClient struct {
	options interfaces.OptionsInterface
}

func (mc *MockClient) GetOptions() interfaces.OptionsInterface {
	return mc.options
}

func (mc *MockClient) GetPushProcessor() interfaces.NotificationProcessor {
	return &MockPushProcessor{}
}

// MockPushProcessor implements interfaces.NotificationProcessor for testing
type MockPushProcessor struct{}

func (mpp *MockPushProcessor) RegisterHandler(notificationType string, handler interface{}, protected bool) error {
	return nil
}

func (mpp *MockPushProcessor) UnregisterHandler(pushNotificationName string) error {
	return nil
}

func (mpp *MockPushProcessor) GetHandler(pushNotificationName string) interface{} {
	return nil
}

// MockOptions implements interfaces.OptionsInterface for testing
type MockOptions struct{}

func (mo *MockOptions) GetReadTimeout() time.Duration {
	return 5 * time.Second
}

func (mo *MockOptions) GetWriteTimeout() time.Duration {
	return 5 * time.Second
}

func (mo *MockOptions) GetAddr() string {
	return "localhost:6379"
}

func (mo *MockOptions) IsTLSEnabled() bool {
	return false
}

func (mo *MockOptions) GetProtocol() int {
	return 3 // RESP3
}

func (mo *MockOptions) GetPoolSize() int {
	return 10
}

func (mo *MockOptions) GetNetwork() string {
	return "tcp"
}

func (mo *MockOptions) NewDialer() func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		return nil, nil
	}
}

func TestHitlessManagerRefactoring(t *testing.T) {
	t.Run("AtomicStateTracking", func(t *testing.T) {
		config := DefaultConfig()
		client := &MockClient{options: &MockOptions{}}

		manager, err := NewHitlessManager(client, nil, config)
		if err != nil {
			t.Fatalf("Failed to create hitless manager: %v", err)
		}
		defer manager.Close()

		// Test initial state
		if manager.IsHandoffInProgress() {
			t.Error("Expected no handoff in progress initially")
		}

		if manager.GetActiveOperationCount() != 0 {
			t.Errorf("Expected 0 active operations, got %d", manager.GetActiveOperationCount())
		}

		if manager.GetState() != StateIdle {
			t.Errorf("Expected StateIdle, got %v", manager.GetState())
		}

		// Add an operation
		ctx := context.Background()
		deadline := time.Now().Add(30 * time.Second)
		err = manager.TrackMovingOperationWithConnID(ctx, "new-endpoint:6379", deadline, 12345, 1)
		if err != nil {
			t.Fatalf("Failed to track operation: %v", err)
		}

		// Test state after adding operation
		if !manager.IsHandoffInProgress() {
			t.Error("Expected handoff in progress after adding operation")
		}

		if manager.GetActiveOperationCount() != 1 {
			t.Errorf("Expected 1 active operation, got %d", manager.GetActiveOperationCount())
		}

		if manager.GetState() != StateMoving {
			t.Errorf("Expected StateMoving, got %v", manager.GetState())
		}

		// Remove the operation
		manager.UntrackOperationWithConnID(12345, 1)

		// Test state after removing operation
		if manager.IsHandoffInProgress() {
			t.Error("Expected no handoff in progress after removing operation")
		}

		if manager.GetActiveOperationCount() != 0 {
			t.Errorf("Expected 0 active operations, got %d", manager.GetActiveOperationCount())
		}

		if manager.GetState() != StateIdle {
			t.Errorf("Expected StateIdle, got %v", manager.GetState())
		}
	})

	t.Run("SyncMapPerformance", func(t *testing.T) {
		config := DefaultConfig()
		client := &MockClient{options: &MockOptions{}}

		manager, err := NewHitlessManager(client, nil, config)
		if err != nil {
			t.Fatalf("Failed to create hitless manager: %v", err)
		}
		defer manager.Close()

		ctx := context.Background()
		deadline := time.Now().Add(30 * time.Second)

		// Test concurrent operations
		const numOps = 100
		for i := 0; i < numOps; i++ {
			err := manager.TrackMovingOperationWithConnID(ctx, "endpoint:6379", deadline, int64(i), uint64(i))
			if err != nil {
				t.Fatalf("Failed to track operation %d: %v", i, err)
			}
		}

		if manager.GetActiveOperationCount() != numOps {
			t.Errorf("Expected %d active operations, got %d", numOps, manager.GetActiveOperationCount())
		}

		// Test GetActiveMovingOperations
		operations := manager.GetActiveMovingOperations()
		if len(operations) != numOps {
			t.Errorf("Expected %d operations in map, got %d", numOps, len(operations))
		}

		// Remove all operations
		for i := 0; i < numOps; i++ {
			manager.UntrackOperationWithConnID(int64(i), uint64(i))
		}

		if manager.GetActiveOperationCount() != 0 {
			t.Errorf("Expected 0 active operations after cleanup, got %d", manager.GetActiveOperationCount())
		}
	})

	t.Run("DuplicateOperationHandling", func(t *testing.T) {
		config := DefaultConfig()
		client := &MockClient{options: &MockOptions{}}

		manager, err := NewHitlessManager(client, nil, config)
		if err != nil {
			t.Fatalf("Failed to create hitless manager: %v", err)
		}
		defer manager.Close()

		ctx := context.Background()
		deadline := time.Now().Add(30 * time.Second)

		// Add operation
		err = manager.TrackMovingOperationWithConnID(ctx, "endpoint:6379", deadline, 12345, 1)
		if err != nil {
			t.Fatalf("Failed to track operation: %v", err)
		}

		// Try to add duplicate operation
		err = manager.TrackMovingOperationWithConnID(ctx, "endpoint:6379", deadline, 12345, 1)
		if err != nil {
			t.Fatalf("Duplicate operation should not return error: %v", err)
		}

		// Should still have only 1 operation
		if manager.GetActiveOperationCount() != 1 {
			t.Errorf("Expected 1 active operation after duplicate, got %d", manager.GetActiveOperationCount())
		}
	})

	t.Run("NotificationTypeConstants", func(t *testing.T) {
		// Test that constants are properly defined
		expectedTypes := []string{
			NotificationMoving,
			NotificationMigrating,
			NotificationMigrated,
			NotificationFailingOver,
			NotificationFailedOver,
		}

		if len(hitlessNotificationTypes) != len(expectedTypes) {
			t.Errorf("Expected %d notification types, got %d", len(expectedTypes), len(hitlessNotificationTypes))
		}

		// Test that all expected types are present
		typeMap := make(map[string]bool)
		for _, t := range hitlessNotificationTypes {
			typeMap[t] = true
		}

		for _, expected := range expectedTypes {
			if !typeMap[expected] {
				t.Errorf("Expected notification type %s not found in hitlessNotificationTypes", expected)
			}
		}

		// Test that hitlessNotificationTypes contains all expected constants
		expectedConstants := []string{
			NotificationMoving,
			NotificationMigrating,
			NotificationMigrated,
			NotificationFailingOver,
			NotificationFailedOver,
		}

		for _, expected := range expectedConstants {
			found := false
			for _, actual := range hitlessNotificationTypes {
				if actual == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected constant %s not found in hitlessNotificationTypes", expected)
			}
		}
	})
}
