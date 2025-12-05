package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestProxyFaultInjectorServer_ExistingE2ETest demonstrates how existing e2e tests
// can work unchanged with the proxy fault injector server
func TestProxyFaultInjectorServer_ExistingE2ETest(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	// Use the Docker fault injector (or real fault injector if environment is configured)
	client, cleanup, err := CreateTestFaultInjectorWithCleanup()
	if err != nil {
		t.Fatalf("Failed to create fault injector: %v", err)
	}
	defer cleanup()

	// Always use Docker proxy default for cluster addresses
	clusterAddrs := []string{"127.0.0.1:17000"} // Use 127.0.0.1 to force IPv4

	t.Logf("✓ Using fault injector client")
	t.Logf("✓ Cluster addresses: %v", clusterAddrs)

	// Test 1: List actions
	ctx := context.Background()
	actions, err := client.ListActions(ctx)
	if err != nil {
		t.Fatalf("Failed to list actions: %v", err)
	}
	t.Logf("✓ Available actions: %v", actions)

	// Test 2: Create Redis cluster client
	// The proxy is configured with multiple listen ports (17000, 17001, 17002)
	// and will return cluster topology pointing to these ports
	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    clusterAddrs,
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	defer redisClient.Close()

	// Set up notification tracking
	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(redisClient, tracker)

	// Verify connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to cluster: %v", err)
	}

	// Debug: Check if maintnotifications manager exists on nodes
	if checkErr := redisClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager == nil {
			t.Logf("⚠️  WARNING: Maintnotifications manager is nil for node: %s", nodeClient.Options().Addr)
		} else {
			t.Logf("✓ Maintnotifications manager exists for node: %s", nodeClient.Options().Addr)
		}
		return nil
	}); checkErr != nil {
		t.Logf("Warning: Failed to check maintnotifications managers: %v", checkErr)
	}

	// Enable CLIENT TRACKING to ensure the proxy sends push notifications
	// This is required for the proxy to send notifications to this client
	if err := redisClient.Do(ctx, "CLIENT", "TRACKING", "ON").Err(); err != nil {
		t.Logf("Warning: Failed to enable CLIENT TRACKING: %v", err)
	}

	// Keep connections active by continuously executing commands
	// This ensures there are always active connections in the pool to receive notifications
	stopChan := make(chan struct{})
	defer close(stopChan)

	go func() {
		i := 0
		for {
			select {
			case <-stopChan:
				return
			default:
				// Execute simple commands continuously to keep connections active
				redisClient.Set(ctx, fmt.Sprintf("key-%d", i%100), fmt.Sprintf("value-%d", i), 0)
				redisClient.Get(ctx, fmt.Sprintf("key-%d", i%100))
				redisClient.Ping(ctx)
				i++
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Give the command loop time to establish connections
	time.Sleep(100 * time.Millisecond)

	// Test 3: Trigger slot migration using EXISTING fault injector client API
	t.Log("Triggering slot migration via fault injector API...")
	resp, err := client.TriggerSlotMigration(ctx, 1000, 2000, "node-1", "node-2")
	if err != nil {
		t.Fatalf("Failed to trigger slot migration: %v", err)
	}

	t.Logf("✓ Action triggered: %s (status: %s)", resp.ActionID, resp.Status)

	// Wait for action to complete (commands continue running in background)
	status, err := client.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}

	t.Logf("✓ Action completed: %s (status: %s)", status.ActionID, status.Status)
	t.Logf("  Output: %+v", status.Output)

	// Continue operations for a bit longer to ensure notifications are received
	time.Sleep(2 * time.Second)

	// Verify notifications were received
	analysis := tracker.GetAnalysis()
	if analysis.MigratingCount == 0 {
		t.Error("Expected to receive SMIGRATING notification")
	} else {
		t.Logf("✓ Received %d SMIGRATING notification(s)", analysis.MigratingCount)
	}

	if analysis.MigratedCount == 0 {
		t.Error("Expected to receive SMIGRATED notification")
	} else {
		t.Logf("✓ Received %d SMIGRATED notification(s)", analysis.MigratedCount)
	}

	// Print full analysis
	analysis.Print(t)

	t.Log("✓ Test passed - existing e2e test works with proxy FI server!")
}

// TestProxyFaultInjectorServer_ClusterReshard tests cluster resharding
func TestProxyFaultInjectorServer_ClusterReshard(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	// Use the Docker fault injector (or real fault injector if environment is configured)
	client, cleanup, err := CreateTestFaultInjectorWithCleanup()
	if err != nil {
		t.Fatalf("Failed to create fault injector: %v", err)
	}
	defer cleanup()

	// Always use Docker proxy default for cluster addresses
	clusterAddrs := []string{"127.0.0.1:17000"} // Use 127.0.0.1 to force IPv4

	ctx := context.Background()

	// Create Redis client
	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    clusterAddrs,
		Protocol: 3,
	})
	defer redisClient.Close()

	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(redisClient, tracker)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Keep connection active with a blocking operation
	// The proxy only tracks "active" connections (those currently executing commands)
	go func() {
		for i := 0; i < 100; i++ {
			redisClient.BLPop(ctx, 100*time.Millisecond, "blocking-key")
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Perform some operations
	for i := 0; i < 10; i++ {
		redisClient.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	time.Sleep(500 * time.Millisecond)

	// Trigger cluster reshard
	slots := []int{100, 200, 300, 400, 500}
	resp, err := client.TriggerClusterReshard(ctx, slots, "node-1", "node-2")
	if err != nil {
		t.Fatalf("Failed to trigger reshard: %v", err)
	}
	
	t.Logf("Reshard action: %s", resp.ActionID)
	
	// Wait for completion
	status, err := client.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}
	
	t.Logf("Reshard completed: %+v", status.Output)
	
	time.Sleep(1 * time.Second)
	
	// Verify notifications
	analysis := tracker.GetAnalysis()
	if analysis.MigratingCount == 0 || analysis.MigratedCount == 0 {
		t.Error("Expected both SMIGRATING and SMIGRATED notifications")
	}
	
	analysis.Print(t)
	t.Log("✓ Cluster reshard test passed")
}

// TestProxyFaultInjectorServer_WithEnvironment shows how to use environment variables
// to make existing tests work with either real FI or proxy FI
func TestProxyFaultInjectorServer_WithEnvironment(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	// Use the Docker fault injector (or real fault injector if environment is configured)
	client, cleanup, err := CreateTestFaultInjectorWithCleanup()
	if err != nil {
		t.Fatalf("Failed to create fault injector: %v", err)
	}
	defer cleanup()

	// Always use Docker proxy default for cluster addresses
	clusterAddrs := []string{"127.0.0.1:17000"} // Use 127.0.0.1 to force IPv4

	// From here on, the test code is IDENTICAL regardless of which backend is used
	ctx := context.Background()

	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    clusterAddrs,
		Protocol: 3,
	})
	defer redisClient.Close()
	
	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(redisClient, tracker)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Keep connection active with a blocking operation
	// The proxy only tracks "active" connections (those currently executing commands)
	go func() {
		for i := 0; i < 100; i++ {
			redisClient.BLPop(ctx, 100*time.Millisecond, "blocking-key")
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Perform some operations
	for i := 0; i < 10; i++ {
		redisClient.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	time.Sleep(500 * time.Millisecond)

	// Trigger action
	resp, err := client.TriggerSlotMigration(ctx, 5000, 6000, "node-1", "node-2")
	if err != nil {
		t.Fatalf("Failed to trigger migration: %v", err)
	}
	
	status, err := client.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}
	
	t.Logf("Action completed: %s", status.Status)
	
	time.Sleep(1 * time.Second)
	
	analysis := tracker.GetAnalysis()
	analysis.Print(t)
	
	t.Log("✓ Test passed with either real or proxy fault injector!")
}

// TestProxyFaultInjectorServer_MultipleActions tests multiple sequential actions
func TestProxyFaultInjectorServer_MultipleActions(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	// Use the Docker fault injector (or real fault injector if environment is configured)
	client, cleanup, err := CreateTestFaultInjectorWithCleanup()
	if err != nil {
		t.Fatalf("Failed to create fault injector: %v", err)
	}
	defer cleanup()

	// Always use Docker proxy default for cluster addresses
	clusterAddrs := []string{"127.0.0.1:17000"} // Use 127.0.0.1 to force IPv4

	ctx := context.Background()

	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    clusterAddrs,
		Protocol: 3,
	})
	defer redisClient.Close()
	
	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(redisClient, tracker)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Keep connection active with a blocking operation
	// The proxy only tracks "active" connections (those currently executing commands)
	go func() {
		for i := 0; i < 200; i++ {
			redisClient.BLPop(ctx, 100*time.Millisecond, "blocking-key")
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Perform some operations
	for i := 0; i < 10; i++ {
		redisClient.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	time.Sleep(500 * time.Millisecond)

	// Trigger multiple migrations
	migrations := []struct {
		start int
		end   int
	}{
		{0, 1000},
		{1001, 2000},
		{2001, 3000},
	}
	
	for i, mig := range migrations {
		t.Logf("Migration %d: slots %d-%d", i+1, mig.start, mig.end)
		
		resp, err := client.TriggerSlotMigration(ctx, mig.start, mig.end, "node-1", "node-2")
		if err != nil {
			t.Fatalf("Failed to trigger migration %d: %v", i+1, err)
		}
		
		status, err := client.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
		if err != nil {
			t.Fatalf("Failed to wait for migration %d: %v", i+1, err)
		}
		
		t.Logf("  ✓ Migration %d completed: %s", i+1, status.Status)
	}
	
	time.Sleep(1 * time.Second)
	
	analysis := tracker.GetAnalysis()
	t.Logf("Total SMIGRATING: %d", analysis.MigratingCount)
	t.Logf("Total SMIGRATED: %d", analysis.MigratedCount)
	
	if analysis.MigratingCount < int64(len(migrations)) {
		t.Errorf("Expected at least %d SMIGRATING notifications, got %d",
			len(migrations), analysis.MigratingCount)
	}
	
	analysis.Print(t)
	t.Log("✓ Multiple actions test passed")
}

// TestProxyFaultInjectorServer_NewConnectionsReceiveNotifications tests that new connections
// joining during an active migration receive the SMIGRATING notification
func TestProxyFaultInjectorServer_NewConnectionsReceiveNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test 1: Get fault injector client
	fiClient, cleanup, err := CreateTestFaultInjectorWithCleanup()
	if err != nil {
		t.Fatalf("Failed to create fault injector: %v", err)
	}
	defer cleanup()

	clusterAddrs := []string{"127.0.0.1:17000"} // Use 127.0.0.1 to force IPv4

	// Test 2: Create first Redis client
	client1 := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    clusterAddrs,
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	defer client1.Close()

	tracker1 := NewTrackingNotificationsHook()
	setupNotificationHook(client1, tracker1)

	if err := client1.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect client1: %v", err)
	}

	// Enable CLIENT TRACKING to ensure the proxy sends push notifications
	if err := client1.Do(ctx, "CLIENT", "TRACKING", "ON").Err(); err != nil {
		t.Logf("Warning: Failed to enable CLIENT TRACKING: %v", err)
	}

	// Keep connections active by continuously executing commands
	stopChan1 := make(chan struct{})
	defer close(stopChan1)

	go func() {
		i := 0
		for {
			select {
			case <-stopChan1:
				return
			default:
				client1.Set(ctx, fmt.Sprintf("key1-%d", i%100), fmt.Sprintf("value-%d", i), 0)
				client1.Ping(ctx)
				i++
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Give the command loop time to establish connections
	time.Sleep(100 * time.Millisecond)

	// Test 3: Trigger a slot migration (this will send SMIGRATING, wait 500ms, then send SMIGRATED)
	// We'll create a new client during the 500ms window
	t.Log("Triggering slot migration...")
	resp, err := fiClient.TriggerSlotMigration(ctx, 1000, 2000, "node-1", "node-2")
	if err != nil {
		t.Fatalf("Failed to trigger slot migration: %v", err)
	}
	t.Logf("Action triggered: %s", resp.ActionID)

	// Wait a bit for SMIGRATING to be sent
	time.Sleep(200 * time.Millisecond)

	// Test 4: Create second client DURING the migration (should receive SMIGRATING)
	t.Log("Creating second client during migration...")
	client2 := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    clusterAddrs,
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	defer client2.Close()

	tracker2 := NewTrackingNotificationsHook()
	setupNotificationHook(client2, tracker2)

	if err := client2.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect client2: %v", err)
	}

	// Enable CLIENT TRACKING to ensure the proxy sends push notifications
	if err := client2.Do(ctx, "CLIENT", "TRACKING", "ON").Err(); err != nil {
		t.Logf("Warning: Failed to enable CLIENT TRACKING: %v", err)
	}

	// Keep connections active for client2 as well
	stopChan2 := make(chan struct{})
	defer close(stopChan2)

	go func() {
		i := 0
		for {
			select {
			case <-stopChan2:
				return
			default:
				client2.Set(ctx, fmt.Sprintf("key2-%d", i%100), fmt.Sprintf("value-%d", i), 0)
				client2.Ping(ctx)
				i++
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Give the command loop time to establish connections
	time.Sleep(100 * time.Millisecond)

	// Wait for migration to complete
	status, err := fiClient.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}
	if status.Status != StatusSuccess {
		t.Fatalf("Action failed: %s", status.Error)
	}

	// Wait a bit for notifications to be processed
	time.Sleep(500 * time.Millisecond)

	// Test 5: Verify both clients received notifications
	analysis1 := tracker1.GetAnalysis()
	analysis2 := tracker2.GetAnalysis()

	t.Logf("Client1 - SMIGRATING: %d, SMIGRATED: %d",
		analysis1.MigratingCount,
		analysis1.MigratedCount)
	t.Logf("Client2 - SMIGRATING: %d, SMIGRATED: %d",
		analysis2.MigratingCount,
		analysis2.MigratedCount)

	// Client1 should have received both SMIGRATING and SMIGRATED
	if analysis1.MigratingCount == 0 {
		t.Error("Client1 should have received SMIGRATING notification")
	}
	if analysis1.MigratedCount == 0 {
		t.Error("Client1 should have received SMIGRATED notification")
	}

	// Client2 (joined late) should have received at least SMIGRATING
	// It might also receive SMIGRATED if it joined early enough
	if analysis2.MigratingCount == 0 {
		t.Error("Client2 (joined during migration) should have received SMIGRATING notification from active notification tracking")
	}

	t.Log("✓ New connections receive active notifications test passed")
}
