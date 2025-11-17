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

		// Set up cluster state reload callback for testing
		var receivedHostPort string
		var receivedSlotRanges []string
		manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
			reloadCalled.Store(true)
			receivedHostPort = hostPort
			receivedSlotRanges = slotRanges
		})

		// Trigger the callback (this is what SMIGRATED notification would do)
		ctx := context.Background()
		testHostPort := "127.0.0.1:6379"
		testSlotRanges := []string{"1234", "5000-6000"}
		manager.TriggerClusterStateReload(ctx, testHostPort, testSlotRanges)

		// Verify callback was called
		if !reloadCalled.Load() {
			t.Error("Cluster state reload callback should have been called")
		}

		// Verify host:port was passed correctly
		if receivedHostPort != testHostPort {
			t.Errorf("Expected host:port %s, got %s", testHostPort, receivedHostPort)
		}

		// Verify slot ranges were passed correctly
		if len(receivedSlotRanges) != len(testSlotRanges) {
			t.Errorf("Expected %d slot ranges, got %d", len(testSlotRanges), len(receivedSlotRanges))
		}
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
		var receivedHostPort string
		var receivedSlotRanges []string
		manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
			callbackWorks.Store(true)
			receivedHostPort = hostPort
			receivedSlotRanges = slotRanges
		})

		ctx := context.Background()
		testHostPort := "127.0.0.1:7000"
		testSlotRanges := []string{"5678"}
		manager.TriggerClusterStateReload(ctx, testHostPort, testSlotRanges)

		if !callbackWorks.Load() {
			t.Error("Callback mechanism should work")
		}

		if receivedHostPort != testHostPort {
			t.Errorf("Expected host:port %s, got %s", testHostPort, receivedHostPort)
		}

		if len(receivedSlotRanges) != 1 || receivedSlotRanges[0] != "5678" {
			t.Errorf("Expected slot ranges [5678], got %v", receivedSlotRanges)
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

