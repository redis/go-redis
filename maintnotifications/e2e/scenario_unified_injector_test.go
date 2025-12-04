package e2e

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestUnifiedInjector_SMIGRATING demonstrates using the unified notification injector
// This test works with EITHER the real fault injector OR the proxy-based mock
func TestUnifiedInjector_SMIGRATING(t *testing.T) {
	ctx := context.Background()
	
	// Create notification injector (automatically chooses based on environment)
	injector, err := NewNotificationInjector()
	if err != nil {
		t.Fatalf("Failed to create notification injector: %v", err)
	}
	
	// Start the injector
	if err := injector.Start(); err != nil {
		t.Fatalf("Failed to start injector: %v", err)
	}
	defer injector.Stop()
	
	t.Logf("Using %s injector", map[bool]string{true: "REAL", false: "MOCK"}[injector.IsReal()])
	t.Logf("Cluster addresses: %v", injector.GetClusterAddrs())
	
	// Create cluster client with maintnotifications enabled
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    injector.GetClusterAddrs(),
		Protocol: 3, // RESP3 required for push notifications

		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:           maintnotifications.ModeEnabled,
			RelaxedTimeout: 30 * time.Second,
		},
	})
	defer client.Close()
	
	// Set up notification tracking
	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(client, tracker)
	
	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to cluster: %v", err)
	}
	
	// Perform some operations to establish connections
	for i := 0; i < 10; i++ {
		if err := client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err(); err != nil {
			t.Logf("Warning: Failed to set key: %v", err)
		}
	}

	// Start a blocking operation in background to keep connection active
	// This ensures the proxy has an active connection to send notifications to
	blockingDone := make(chan error, 1)
	go func() {
		// BLPOP with 10 second timeout - keeps connection active
		_, err := client.BLPop(ctx, 10*time.Second, "notification-test-list").Result()
		blockingDone <- err
	}()

	// Wait for blocking command to start
	time.Sleep(500 * time.Millisecond)

	// Inject SMIGRATING notification while connection is active
	t.Log("Injecting SMIGRATING notification...")
	if err := injector.InjectSMIGRATING(ctx, 12345, "1000-2000", "3000"); err != nil {
		t.Fatalf("Failed to inject SMIGRATING: %v", err)
	}
	
	// Wait for notification processing
	time.Sleep(1 * time.Second)
	
	// Verify notification was received
	analysis := tracker.GetAnalysis()
	if analysis.MigratingCount == 0 {
		t.Errorf("Expected to receive SMIGRATING notification, got 0")
	} else {
		t.Logf("✓ Received %d SMIGRATING notification(s)", analysis.MigratingCount)
	}
	
	// Verify operations still work (timeouts should be relaxed)
	if err := client.Set(ctx, "test-key-during-migration", "value", 0).Err(); err != nil {
		t.Errorf("Expected operations to work during migration, got error: %v", err)
	}
	
	// Print analysis
	analysis.Print(t)
	
	t.Log("✓ SMIGRATING test passed")
}

// TestUnifiedInjector_SMIGRATED demonstrates SMIGRATED notification handling
func TestUnifiedInjector_SMIGRATED(t *testing.T) {
	ctx := context.Background()
	
	injector, err := NewNotificationInjector()
	if err != nil {
		t.Fatalf("Failed to create notification injector: %v", err)
	}
	
	if err := injector.Start(); err != nil {
		t.Fatalf("Failed to start injector: %v", err)
	}
	defer injector.Stop()
	
	t.Logf("Using %s injector", map[bool]string{true: "REAL", false: "MOCK"}[injector.IsReal()])
	
	// Track cluster state reloads
	var reloadCount atomic.Int32
	
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    injector.GetClusterAddrs(),
		Protocol: 3,

		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:           maintnotifications.ModeEnabled,
			RelaxedTimeout: 30 * time.Second,
		},
	})
	defer client.Close()

	// Set up notification tracking
	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(client, tracker)

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Set up reload callback on existing nodes
	client.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
				t.Logf("Cluster state reload triggered for %s, slots: %v", hostPort, slotRanges)
			})
		}
		return nil
	})

	// Set up reload callback for new nodes
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
				t.Logf("Cluster state reload triggered for %s, slots: %v", hostPort, slotRanges)
			})
		}
	})

	// Perform some operations to establish connections
	for i := 0; i < 10; i++ {
		if err := client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err(); err != nil {
			t.Logf("Warning: Failed to set key: %v", err)
		}
	}

	initialReloads := reloadCount.Load()

	// Start a blocking operation in background to keep connection active
	blockingDone := make(chan error, 1)
	go func() {
		// BLPOP with 10 second timeout - keeps connection active
		_, err := client.BLPop(ctx, 10*time.Second, "notification-test-list").Result()
		blockingDone <- err
	}()

	// Wait for blocking command to start
	time.Sleep(500 * time.Millisecond)

	// Inject SMIGRATED notification
	t.Log("Injecting SMIGRATED notification...")

	// Get first node address for endpoint
	addrs := injector.GetClusterAddrs()
	hostPort := addrs[0]

	if err := injector.InjectSMIGRATED(ctx, 12346, hostPort, "1000-2000", "3000"); err != nil {
		if injector.IsReal() {
			t.Logf("Note: Real fault injector cannot directly inject SMIGRATED (expected): %v", err)
			t.Skip("Skipping SMIGRATED test with real fault injector")
		} else {
			t.Fatalf("Failed to inject SMIGRATED: %v", err)
		}
	}

	// Wait for notification processing
	time.Sleep(1 * time.Second)

	// Wait for blocking operation to complete
	<-blockingDone
	
	// Verify notification was received
	analysis := tracker.GetAnalysis()
	if analysis.MigratedCount == 0 {
		t.Errorf("Expected to receive SMIGRATED notification, got 0")
	} else {
		t.Logf("✓ Received %d SMIGRATED notification(s)", analysis.MigratedCount)
	}
	
	// Verify cluster state was reloaded
	finalReloads := reloadCount.Load()
	if finalReloads <= initialReloads {
		t.Errorf("Expected cluster state reload after SMIGRATED, reloads: initial=%d, final=%d",
			initialReloads, finalReloads)
	} else {
		t.Logf("✓ Cluster state reloaded %d time(s)", finalReloads-initialReloads)
	}
	
	// Print analysis
	analysis.Print(t)
	
	t.Log("✓ SMIGRATED test passed")
}

// TestUnifiedInjector_ComplexScenario demonstrates a complex migration scenario
func TestUnifiedInjector_ComplexScenario(t *testing.T) {
	ctx := context.Background()
	
	injector, err := NewNotificationInjector()
	if err != nil {
		t.Fatalf("Failed to create notification injector: %v", err)
	}
	
	if err := injector.Start(); err != nil {
		t.Fatalf("Failed to start injector: %v", err)
	}
	defer injector.Stop()
	
	t.Logf("Using %s injector", map[bool]string{true: "REAL", false: "MOCK"}[injector.IsReal()])
	
	var reloadCount atomic.Int32
	
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    injector.GetClusterAddrs(),
		Protocol: 3,
	})
	defer client.Close()
	
	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(client, tracker)
	
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
			})
		}
	})
	
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	
	// Perform operations
	for i := 0; i < 20; i++ {
		client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}
	
	time.Sleep(500 * time.Millisecond)
	
	// Simulate a multi-step migration scenario
	t.Log("Step 1: Injecting SMIGRATING for slots 0-5000...")
	if err := injector.InjectSMIGRATING(ctx, 10001, "0-5000"); err != nil {
		t.Fatalf("Failed to inject SMIGRATING: %v", err)
	}
	
	time.Sleep(500 * time.Millisecond)
	
	// Verify operations work during migration
	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("migration-key%d", i), "value", 0).Err(); err != nil {
			t.Logf("Warning: Operation failed during migration: %v", err)
		}
	}
	
	if !injector.IsReal() {
		// Only inject SMIGRATED with mock injector
		t.Log("Step 2: Injecting SMIGRATED for completed migration...")
		addrs := injector.GetClusterAddrs()
		hostPort := addrs[0]

		if err := injector.InjectSMIGRATED(ctx, 10002, hostPort, "0-5000"); err != nil {
			t.Fatalf("Failed to inject SMIGRATED: %v", err)
		}
		
		time.Sleep(500 * time.Millisecond)
	}
	
	// Verify operations still work
	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("post-migration-key%d", i), "value", 0).Err(); err != nil {
			t.Errorf("Operations failed after migration: %v", err)
		}
	}
	
	// Print final analysis
	analysis := tracker.GetAnalysis()
	analysis.Print(t)
	
	t.Logf("✓ Complex scenario test passed")
	t.Logf("  - SMIGRATING notifications: %d", analysis.MigratingCount)
	t.Logf("  - SMIGRATED notifications: %d", analysis.MigratedCount)
	t.Logf("  - Cluster state reloads: %d", reloadCount.Load())
}

