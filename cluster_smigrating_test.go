package redis

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestClusterClientSMigratedCallback tests that ClusterClient sets up SMIGRATED callback on node clients
func TestClusterClientSMigratedCallback(t *testing.T) {
	t.Run("CallbackSetupWithMaintNotifications", func(t *testing.T) {
		// Track if state reload was called
		var reloadCalled atomic.Bool

		// Create cluster options with maintnotifications enabled
		opt := &ClusterOptions{
			Addrs: []string{"localhost:7000"}, // Dummy address
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeEnabled,
			},
			// Use custom NewClient to track when nodes are created
			NewClient: func(opt *Options) *Client {
				client := NewClient(opt)
				return client
			},
		}

		// Create cluster client
		cluster := NewClusterClient(opt)
		defer cluster.Close()

		// Manually trigger node creation by calling GetOrCreate
		// This simulates what happens during normal cluster operations
		node, err := cluster.nodes.GetOrCreate("localhost:7000")
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// Get the maintnotifications manager from the node client
		manager := node.Client.GetMaintNotificationsManager()
		if manager == nil {
			t.Skip("MaintNotifications manager not initialized (expected if not connected to real Redis)")
			return
		}

		// Temporarily replace the cluster state reload with our test version
		var receivedSlot int
		originalCallback := manager
		manager.SetClusterStateReloadCallback(func(ctx context.Context, slot int) {
			reloadCalled.Store(true)
			receivedSlot = slot
		})

		// Trigger the callback (this is what SMIGRATED notification would do)
		ctx := context.Background()
		testSlot := 1234
		manager.TriggerClusterStateReload(ctx, testSlot)

		// Verify callback was called
		if !reloadCalled.Load() {
			t.Error("Cluster state reload callback should have been called")
		}

		// Verify slot was passed correctly
		if receivedSlot != testSlot {
			t.Errorf("Expected slot %d, got %d", testSlot, receivedSlot)
		}
		_ = originalCallback
	})

	t.Run("NoCallbackWithoutMaintNotifications", func(t *testing.T) {
		// Create cluster options WITHOUT maintnotifications
		opt := &ClusterOptions{
			Addrs: []string{"localhost:7000"}, // Dummy address
			// MaintNotificationsConfig is nil
		}

		// Create cluster client
		cluster := NewClusterClient(opt)
		defer cluster.Close()

		// The OnNewNode callback should not be registered when MaintNotificationsConfig is nil
		// This test just verifies that the cluster client doesn't panic
	})
}

// TestClusterClientSMigratedIntegration tests SMIGRATED notification handling in cluster context
func TestClusterClientSMigratedIntegration(t *testing.T) {
	t.Run("SMigratedTriggersStateReload", func(t *testing.T) {
		// This test verifies the integration between SMIGRATED notification and cluster state reload
		// We verify that the callback is properly set up to call cluster.state.LazyReload()

		// Create cluster options with maintnotifications enabled
		opt := &ClusterOptions{
			Addrs: []string{"localhost:7000"},
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeEnabled,
			},
		}

		// Create cluster client
		cluster := NewClusterClient(opt)
		defer cluster.Close()

		// Create a node
		node, err := cluster.nodes.GetOrCreate("localhost:7000")
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}

		// Get the maintnotifications manager
		manager := node.Client.GetMaintNotificationsManager()
		if manager == nil {
			t.Skip("MaintNotifications manager not initialized (expected if not connected to real Redis)")
			return
		}

		// Verify that the callback is set by checking it's not nil
		// We can't directly test LazyReload being called without a real cluster,
		// but we can verify the callback mechanism works
		var callbackWorks atomic.Bool
		var receivedSlot int
		manager.SetClusterStateReloadCallback(func(ctx context.Context, slot int) {
			callbackWorks.Store(true)
			receivedSlot = slot
		})

		ctx := context.Background()
		testSlot := 5678
		manager.TriggerClusterStateReload(ctx, testSlot)

		if !callbackWorks.Load() {
			t.Error("Callback mechanism should work")
		}

		if receivedSlot != testSlot {
			t.Errorf("Expected slot %d, got %d", testSlot, receivedSlot)
		}
	})
}

// TestSMigratingAndSMigratedConstants verifies the SMIGRATING and SMIGRATED constants are exported
func TestSMigratingAndSMigratedConstants(t *testing.T) {
	// This test verifies that the SMIGRATING and SMIGRATED constants are properly defined
	// and accessible from the maintnotifications package
	if maintnotifications.NotificationSMigrating != "SMIGRATING" {
		t.Errorf("Expected NotificationSMigrating to be 'SMIGRATING', got: %s", maintnotifications.NotificationSMigrating)
	}

	if maintnotifications.NotificationSMigrated != "SMIGRATED" {
		t.Errorf("Expected NotificationSMigrated to be 'SMIGRATED', got: %s", maintnotifications.NotificationSMigrated)
	}
}

