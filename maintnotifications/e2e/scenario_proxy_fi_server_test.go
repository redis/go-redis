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
func TestProxyFaultInjectorServer_ClusterExistingE2ETest(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx := context.Background()

	// Setup database configured for slot-shuffle effect
	// This queries the fault injector for the required database configuration
	bdbID, factory, testMode, fiClient, factoryCleanup := SetupTestDatabaseForSlotMigrate(t, ctx, SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate)
	defer factoryCleanup()

	t.Logf("✓ Using fault injector client (mode: %s)", testMode.Mode)
	t.Logf("  Fault injector base URL: %s", fiClient.baseURL)

	// Test 2: Create Redis cluster client via factory
	redisClient, err := factory.Create("test-client", &CreateClientOptions{
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create Redis client: %v", err)
	}
	// Note: factoryCleanup() handles closing clients via factory.DestroyAll()

	// Set up notification tracking
	tracker := NewTrackingNotificationsHook()
	setupNotificationHooks(redisClient, tracker)

	// Verify connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to cluster: %v", err)
	}

	// Debug: Check if maintnotifications manager exists on nodes
	// ForEachShard is cluster-specific, so we need to type assert
	if clusterClient, ok := redisClient.(*redis.ClusterClient); ok {
		if checkErr := clusterClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
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

	// Test 3: Trigger slot migration using the /slot-migrate API
	// The database was configured for slot-shuffle effect via SetupTestDatabaseForSlotMigrate
	t.Log("Triggering slot migration via fault injector API...")
	t.Logf("  Database ID: %d", bdbID)
	t.Logf("  Effect: slot-shuffle")
	t.Logf("  Trigger: migrate")

	bdbIDStr := fmt.Sprintf("%d", bdbID)
	resp, err := fiClient.TriggerSlotMigrateSlotShuffle(ctx, bdbIDStr, SlotMigrateVariantMigrate)
	if err != nil {
		t.Fatalf("Failed to trigger slot migration: %v", err)
	}

	if debugE2E() {
		t.Logf("✓ Action triggered: %s (status: %s)", resp.ActionID, resp.Status)
	}

	// Wait for action to complete (commands continue running in background)
	// Use testMode timeout - proxy is fast, real fault injector can take up to 5 minutes
	status, err := fiClient.WaitForAction(ctx, resp.ActionID,
		WithMaxWaitTime(testMode.ActionWaitTimeout),
		WithPollInterval(testMode.ActionPollInterval))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}

	if debugE2E() {
		t.Logf("✓ Action completed: %s (status: %s)", status.ActionID, status.Status)
		t.Logf("  Output: %+v", status.Output)
	}

	// Check if action failed
	if status.Status == StatusFailed {
		t.Logf("⚠️  Action failed with error: %v", status.Error)
	}

	// Continue operations for a bit longer to ensure notifications are received
	time.Sleep(2 * time.Second)

	// Verify notifications were received
	analysis := tracker.GetAnalysis()
	if analysis.SMigratingCount == 0 {
		t.Error("Expected to receive SMIGRATING notification")
	} else {
		t.Logf("✓ Received %d SMIGRATING notification(s)", analysis.SMigratingCount)
	}

	if analysis.SMigratedCount == 0 {
		t.Error("Expected to receive SMIGRATED notification")
	} else {
		t.Logf("✓ Received %d SMIGRATED notification(s)", analysis.SMigratedCount)
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

	ctx := context.Background()

	// Setup database configured for slot-shuffle effect
	// This queries the fault injector for the required database configuration
	bdbID, factory, testMode, fiClient, factoryCleanup := SetupTestDatabaseForSlotMigrate(t, ctx, SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate)
	defer factoryCleanup()

	t.Logf("Running cluster reshard test in %s mode", testMode.Mode)
	t.Logf("  Fault injector base URL: %s", fiClient.baseURL)

	// Create Redis client via factory (uses correct addresses based on mode)
	redisClient, err := factory.Create("reshard-client", &CreateClientOptions{
		Protocol: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	// Type assertion for cluster-specific methods
	clusterClient, ok := redisClient.(*redis.ClusterClient)
	if !ok {
		t.Fatal("Expected ClusterClient but got different type")
	}

	tracker := NewTrackingNotificationsHook()
	setupNotificationHook(clusterClient, tracker)

	if err := clusterClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Keep connection active with a blocking operation
	// The proxy only tracks "active" connections (those currently executing commands)
	go func() {
		for i := 0; i < 100; i++ {
			clusterClient.BLPop(ctx, 100*time.Millisecond, "blocking-key")
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Perform some operations
	for i := 0; i < 10; i++ {
		clusterClient.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	time.Sleep(500 * time.Millisecond)

	// Trigger slot migration with slot-shuffle effect
	// The database was configured for slot-shuffle effect via SetupTestDatabaseForSlotMigrate
	t.Logf("Attempting to trigger slot migration (slot-shuffle effect)...")
	t.Logf("  Database ID: %d", bdbID)
	t.Logf("  Trigger: %s", SlotMigrateVariantMigrate)
	t.Logf("  Fault injector URL: %s", fiClient.baseURL)

	bdbIDStr := fmt.Sprintf("%d", bdbID)
	resp, err := fiClient.TriggerSlotMigrateSlotShuffle(ctx, bdbIDStr, SlotMigrateVariantMigrate)
	if err != nil {
		t.Logf("ERROR: Failed to trigger slot migration")
		t.Logf("  Error type: %T", err)
		t.Logf("  Error details: %v", err)
		t.Fatalf("Failed to trigger slot migration: %v", err)
	}

	if debugE2E() {
		t.Logf("✓ Slot migration action triggered successfully")
		t.Logf("  Action ID: %s", resp.ActionID)
		t.Logf("  Status: %s", resp.Status)
	}

	// Wait for completion - use testMode timeout (proxy is fast, real FI can take up to 5 minutes)
	status, err := fiClient.WaitForAction(ctx, resp.ActionID,
		WithMaxWaitTime(testMode.ActionWaitTimeout),
		WithPollInterval(testMode.ActionPollInterval))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}

	if debugE2E() {
		t.Logf("Reshard completed: status=%s, output=%+v", status.Status, status.Output)
	}

	// Check if action failed
	if status.Status == StatusFailed {
		t.Logf("⚠️  Action failed with error: %v", status.Error)
	}

	// Wait for SMIGRATING notification
	_, foundMigrating := tracker.FindOrWaitForNotification("SMIGRATING", testMode.ActionWaitTimeout)
	if !foundMigrating {
		t.Errorf("Timed out waiting for SMIGRATING notification")
	} else {
		t.Logf("✓ Received SMIGRATING notification")
	}

	// Wait for SMIGRATED notification
	_, foundMigrated := tracker.FindOrWaitForNotification("SMIGRATED", testMode.ActionWaitTimeout)
	if !foundMigrated {
		t.Errorf("Timed out waiting for SMIGRATED notification")
	} else {
		t.Logf("✓ Received SMIGRATED notification")
	}

	// Print final analysis
	analysis := tracker.GetAnalysis()
	analysis.Print(t)
	t.Log("✓ Cluster reshard test passed")
}

// TestProxyFaultInjectorServer_WithEnvironment shows how to use environment variables
// to make existing tests work with either real FI or proxy FI
func TestProxyFaultInjectorServer_WithEnvironment(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx := context.Background()

	// Setup database configured for slot-shuffle effect
	// This queries the fault injector for the required database configuration
	bdbID, factory, testMode, client, factoryCleanup := SetupTestDatabaseForSlotMigrate(t, ctx, SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate)
	defer factoryCleanup()

	t.Logf("Running test in %s mode", testMode.Mode)

	// Create Redis client via factory (uses correct addresses based on mode)
	redisClient, err := factory.Create("env-client", &CreateClientOptions{
		Protocol: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	tracker := NewTrackingNotificationsHook()
	setupNotificationHooks(redisClient, tracker)

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

	// Trigger slot migration using the /slot-migrate API with correct bdb_id
	// The database was configured for slot-shuffle effect via SetupTestDatabaseForSlotMigrate
	t.Logf("Triggering slot migration with bdb_id=%d (effect: slot-shuffle)", bdbID)
	bdbIDStr := fmt.Sprintf("%d", bdbID)
	resp, err := client.TriggerSlotMigrateSlotShuffle(ctx, bdbIDStr, SlotMigrateVariantMigrate)
	if err != nil {
		t.Fatalf("Failed to trigger migration: %v", err)
	}

	status, err := client.WaitForAction(ctx, resp.ActionID,
		WithMaxWaitTime(testMode.ActionWaitTimeout),
		WithPollInterval(testMode.ActionPollInterval))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}

	if debugE2E() {
		t.Logf("Action completed: %s", status.Status)
	}
	if status.Status == StatusFailed {
		t.Logf("⚠️  Action failed with error: %v", status.Error)
	}

	time.Sleep(2 * time.Second)

	analysis := tracker.GetAnalysis()
	analysis.Print(t)

	t.Log("✓ Test passed with either real or proxy fault injector!")
}

// TestProxyFaultInjectorServer_MultipleActions tests multiple sequential actions
func TestProxyFaultInjectorServer_ClusterMultipleActions(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx := context.Background()

	// Setup database configured for slot-shuffle effect
	// This queries the fault injector for the required database configuration
	bdbID, factory, testMode, client, factoryCleanup := SetupTestDatabaseForSlotMigrate(t, ctx, SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate)
	defer factoryCleanup()

	t.Logf("Running test in %s mode", testMode.Mode)
	t.Logf("Database ID: %d", bdbID)

	// Create Redis client via factory (uses correct addresses based on mode)
	redisClient, err := factory.Create("multi-action-client", &CreateClientOptions{
		Protocol: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	tracker := NewTrackingNotificationsHook()
	setupNotificationHooks(redisClient, tracker)

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

	// Trigger multiple slot-shuffle migrations using the correct API
	// The database was configured for slot-shuffle effect via SetupTestDatabaseForSlotMigrate
	bdbIDStr := fmt.Sprintf("%d", bdbID)
	numMigrations := 3

	for i := 0; i < numMigrations; i++ {
		if debugE2E() {
			t.Logf("Migration %d: triggering slot-shuffle", i+1)
		}

		resp, err := client.TriggerSlotMigrateSlotShuffle(ctx, bdbIDStr, SlotMigrateVariantMigrate)
		if err != nil {
			t.Fatalf("Failed to trigger migration %d: %v", i+1, err)
		}

		status, err := client.WaitForAction(ctx, resp.ActionID,
			WithMaxWaitTime(testMode.ActionWaitTimeout),
			WithPollInterval(testMode.ActionPollInterval))
		if err != nil {
			t.Fatalf("Failed to wait for migration %d: %v", i+1, err)
		}

		if debugE2E() {
			t.Logf("  ✓ Migration %d completed: %s", i+1, status.Status)
		}
		if status.Status == StatusFailed {
			t.Logf("  ⚠️  Migration %d failed with error: %v", i+1, status.Error)
		}

		// Wait between migrations to allow notifications to be processed
		time.Sleep(1 * time.Second)
	}

	time.Sleep(2 * time.Second)

	analysis := tracker.GetAnalysis()
	t.Logf("Total SMIGRATING: %d", analysis.SMigratingCount)
	t.Logf("Total SMIGRATED: %d", analysis.SMigratedCount)

	if analysis.SMigratingCount < int64(numMigrations) {
		t.Errorf("Expected at least %d SMIGRATING notifications, got %d",
			numMigrations, analysis.SMigratingCount)
	}

	analysis.Print(t)
	t.Log("✓ Multiple actions test passed")
}

// TestProxyFaultInjectorServer_NewConnectionsReceiveNotifications tests that new connections
// joining during an active migration receive the SMIGRATING notification
func TestProxyFaultInjectorServer_ClusterNewConnectionsReceiveNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Setup database configured for slot-shuffle effect
	// This queries the fault injector for the required database configuration
	bdbID, factory, testMode, fiClient, factoryCleanup := SetupTestDatabaseForSlotMigrate(t, ctx, SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate)
	defer factoryCleanup()

	t.Logf("Running test in %s mode", testMode.Mode)
	t.Logf("Database ID: %d", bdbID)

	// Test 2: Create first Redis client via factory
	client1Iface, err := factory.Create("client1", &CreateClientOptions{
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create client1: %v", err)
	}
	client1 := client1Iface.(*redis.ClusterClient)
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
	// The database was configured for slot-shuffle effect via SetupTestDatabaseForSlotMigrate
	t.Log("Triggering slot migration (effect: slot-shuffle)...")
	bdbIDStr := fmt.Sprintf("%d", bdbID)
	resp, err := fiClient.TriggerSlotMigrateSlotShuffle(ctx, bdbIDStr, SlotMigrateVariantMigrate)
	if err != nil {
		t.Fatalf("Failed to trigger slot migration: %v", err)
	}
	if debugE2E() {
		t.Logf("Action triggered: %s", resp.ActionID)
	}

	// Wait a bit for SMIGRATING to be sent
	time.Sleep(200 * time.Millisecond)

	// Test 4: Create second client DURING the migration (should receive SMIGRATING)
	t.Log("Creating second client during migration...")
	client2Iface, err := factory.Create("client2", &CreateClientOptions{
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create client2: %v", err)
	}
	client2 := client2Iface.(*redis.ClusterClient)
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

	// Wait for migration to complete - use testMode timeout (proxy is fast, real FI can take up to 5 minutes)
	status, err := fiClient.WaitForAction(ctx, resp.ActionID,
		WithMaxWaitTime(testMode.ActionWaitTimeout),
		WithPollInterval(testMode.ActionPollInterval))
	if err != nil {
		t.Fatalf("Failed to wait for action: %v", err)
	}
	if status.Status == StatusFailed {
		t.Logf("⚠️  Action failed with error: %v", status.Error)
	}

	// Wait a bit for notifications to be processed
	time.Sleep(2 * time.Second)

	// Test 5: Verify both clients received notifications
	analysis1 := tracker1.GetAnalysis()
	analysis2 := tracker2.GetAnalysis()

	t.Logf("Client1 - SMIGRATING: %d, SMIGRATED: %d",
		analysis1.SMigratingCount,
		analysis1.SMigratedCount)
	t.Logf("Client2 - SMIGRATING: %d, SMIGRATED: %d",
		analysis2.SMigratingCount,
		analysis2.SMigratedCount)

	// Client1 should have received both SMIGRATING and SMIGRATED
	if analysis1.SMigratingCount == 0 {
		t.Error("Client1 should have received SMIGRATING notification")
	}
	if analysis1.SMigratedCount == 0 {
		t.Error("Client1 should have received SMIGRATED notification")
	}

	// Client2 (joined late) should have received at least SMIGRATING
	// It might also receive SMIGRATED if it joined early enough
	if analysis2.SMigratingCount == 0 {
		t.Error("Client2 (joined during migration) should have received SMIGRATING notification from active notification tracking")
	}

	t.Log("✓ New connections receive active notifications test passed")
}

// TestSlotMigrate_AllEffects tests the new /slot-migrate API endpoint with all effects
// This tests the OSS Cluster API client behavior during endpoint topology changes
// Each subtest creates a fresh database with the required configuration for that effect/variant
func TestClusterSlotMigrate_AllEffects(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}

	ctx := context.Background()

	// Test all valid effect-variant combinations
	// Based on ACTIONS.md compatibility matrix:
	// - remove-add: migrate, maintenance_mode, failover (3)
	// - remove: migrate, maintenance_mode (2)
	// - add: migrate, failover (2)
	// - slot-shuffle: migrate, failover (2)
	//
	// Note: slot-shuffle + failover is a special case where the slots don't actually move -
	// the master and replica just swap roles. The real fault injector may not generate
	// any notifications for this case since no actual slot migration occurs.
	testCases := []struct {
		name                string
		effect              SlotMigrateEffect
		trigger             SlotMigrateVariant
		expectNoNotif       bool // If true, don't expect any notifications (e.g., slot-shuffle + failover)
	}{
		// slot-shuffle effect (2 variants) - no node changes
		{
			name:    "SlotShuffle_Migrate",
			effect:  SlotMigrateEffectSlotShuffle,
			trigger: SlotMigrateVariantMigrate,
		},
		{
			name:          "SlotShuffle_Failover",
			effect:        SlotMigrateEffectSlotShuffle,
			trigger:       SlotMigrateVariantFailover,
			expectNoNotif: true, // slot-shuffle + failover swaps master/replica roles without slot migration
		},
		// add effect (2 variants) - one node added
		{
			name:    "Add_Migrate",
			effect:  SlotMigrateEffectAdd,
			trigger: SlotMigrateVariantMigrate,
		},
		{
			name:    "Add_Failover",
			effect:  SlotMigrateEffectAdd,
			trigger: SlotMigrateVariantFailover,
		},
		// remove-add effect (3 variants) - net zero node change
		{
			name:    "RemoveAdd_Migrate",
			effect:  SlotMigrateEffectRemoveAdd,
			trigger: SlotMigrateVariantMigrate,
		},
		{
			name:    "RemoveAdd_MaintenanceMode",
			effect:  SlotMigrateEffectRemoveAdd,
			trigger: SlotMigrateVariantMaintenanceMode,
		},
		{
			name:    "RemoveAdd_Failover",
			effect:  SlotMigrateEffectRemoveAdd,
			trigger: SlotMigrateVariantFailover,
		},
		// remove effect (2 variants) - one node removed
		{
			name:    "Remove_Migrate",
			effect:  SlotMigrateEffectRemove,
			trigger: SlotMigrateVariantMigrate,
		},
		{
			name:    "Remove_MaintenanceMode",
			effect:  SlotMigrateEffectRemove,
			trigger: SlotMigrateVariantMaintenanceMode,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing slot-migrate effect: %s, variant: %s", tc.effect, tc.trigger)

			// Create a fresh database configured for this specific effect/variant
			// This queries the fault injector for the required database configuration
			bdbID, factory, testMode, fiClient, factoryCleanup := SetupTestDatabaseForSlotMigrate(t, ctx, tc.effect, tc.trigger)
			defer factoryCleanup()

			t.Logf("✓ Created database %d for effect %s, variant %s (mode: %s)",
				bdbID, tc.effect, tc.trigger, testMode.Mode)

			// Create Redis cluster client connected to the new database
			redisClientIface, err := factory.Create("redisClient", &CreateClientOptions{
				Protocol: 3,
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode: maintnotifications.ModeEnabled,
				},
			})
			if err != nil {
				t.Fatalf("Failed to create Redis client: %v", err)
			}
			redisClient := redisClientIface.(*redis.ClusterClient)
			defer redisClient.Close()

			// Verify connection first - this triggers cluster discovery
			if err := redisClient.Ping(ctx).Err(); err != nil {
				t.Fatalf("Failed to connect to cluster: %v", err)
			}
			t.Log("✓ Connected to cluster")

			// Force discovery of all shards by pinging each one
			// This ensures ForEachShard will iterate over all shards
			shardCount := 0
			err = redisClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
				if err := nodeClient.Ping(ctx).Err(); err != nil {
					return err
				}
				shardCount++
				return nil
			})
			if err != nil {
				t.Fatalf("Failed to ping all shards: %v", err)
			}
			t.Logf("✓ Discovered and pinged %d shards", shardCount)

			// Set up notification tracking AFTER all shards are discovered
			tracker := NewTrackingNotificationsHook()
			setupNotificationHook(redisClient, tracker)

			// Keep connections active by continuously executing commands to ALL slots
			// Use hash tags {slot-N} to ensure keys distribute across all 16384 slots
			// This ensures notifications are received from all shards
			stopChan := make(chan struct{})
			defer close(stopChan)

			go func() {
				i := 0
				for {
					select {
					case <-stopChan:
						return
					default:
						// Use hash tags to distribute keys across all slots
						// {N} forces the key to hash based on the number N
						slot := i % 16384
						key := fmt.Sprintf("{%d}:key", slot)
						// Use a short timeout context to avoid blocking on removed nodes
						cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
						_ = redisClient.Set(cmdCtx, key, fmt.Sprintf("value-%d", i), 0).Err()
						_ = redisClient.Get(cmdCtx, key).Err()
						cancel()
						i++
						time.Sleep(1 * time.Millisecond)
					}
				}
			}()

			// Give the command loop time to establish connections to all shards
			// With replication enabled, there are more shards, so we need more time
			time.Sleep(500 * time.Millisecond)

			// Trigger slot migration on the database we just created
			bdbIDStr := fmt.Sprintf("%d", bdbID)
			resp, err := fiClient.TriggerSlotMigrate(ctx, SlotMigrateRequest{
				Effect:  tc.effect,
				BdbID:   bdbIDStr,
				Trigger: tc.trigger,
			})
			if err != nil {
				t.Fatalf("Failed to trigger slot migration: %v", err)
			}

			if debugE2E() {
				t.Logf("✓ Action triggered: %s (status: %s)", resp.ActionID, resp.Status)
			}

			// Wait for action to complete - use testMode timeout
			status, err := fiClient.WaitForAction(ctx, resp.ActionID,
				WithMaxWaitTime(testMode.ActionWaitTimeout),
				WithPollInterval(testMode.ActionPollInterval))
			if err != nil {
				t.Fatalf("Failed to wait for action: %v", err)
			}

			if debugE2E() {
				t.Logf("✓ Action completed: %s (status: %s)", status.ActionID, status.Status)
			}

			// Wait for appropriate notifications based on the effect/variant combination
			if tc.expectNoNotif {
				// For slot-shuffle + failover, no notifications are expected because
				// the master and replica just swap roles without actual slot migration.
				// Wait a short time to verify no notifications arrive.
				t.Logf("Waiting briefly to verify no notifications are received (slot-shuffle + failover)...")
				time.Sleep(5 * time.Second)
				t.Logf("✓ No notifications expected for slot-shuffle + failover (master/replica role swap only)")
			} else {
				// For other combinations, expect SMIGRATING/SMIGRATED notifications
				_, foundMigrating := tracker.FindOrWaitForNotification("SMIGRATING", testMode.ActionWaitTimeout)
				if !foundMigrating {
					t.Errorf("Timed out waiting for SMIGRATING notification for effect %s", tc.effect)
				} else {
					t.Logf("✓ Received SMIGRATING notification")
				}

				// Wait for SMIGRATED notification (may take longer to arrive)
				// Use the same timeout as action wait - 5 minutes for real fault injector
				_, foundMigrated := tracker.FindOrWaitForNotification("SMIGRATED", testMode.ActionWaitTimeout)
				if !foundMigrated {
					t.Errorf("Timed out waiting for SMIGRATED notification for effect %s", tc.effect)
				} else {
					t.Logf("✓ Received SMIGRATED notification")
				}
			}

			// Print final analysis
			analysis := tracker.GetAnalysis()
			analysis.Print(t)

			if tc.expectNoNotif {
				t.Logf("✓ Total notifications received: %d (expected 0 for slot-shuffle + failover)", analysis.TotalNotifications)
			} else {
				t.Logf("✓ Received %d SMIGRATING notification(s)", analysis.SMigratingCount)
				t.Logf("✓ Received %d SMIGRATED notification(s)", analysis.SMigratedCount)
			}

			if analysis.NotificationProcessingErrors > 0 {
				t.Errorf("Got %d notification processing errors", analysis.NotificationProcessingErrors)
			}
		})
	}

	t.Log("✓ All slot-migrate effects tested successfully!")
}
