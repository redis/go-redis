package e2e

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// startBackgroundTraffic starts a background goroutine that continuously sends commands
// to slots spread across all shards (0, 1001, 2002, ...). This ensures connections remain
// active on all shards to receive notifications. Returns a stop function to call when done.
// The slotKeys array is defined in slot_keys.go.
// The stop function waits for the goroutine to finish before returning.
func startBackgroundTraffic(ctx context.Context, client redis.UniversalClient) (stop func()) {
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		for {
			// Send commands to slots spread across all shards: 0, 1001, 2002, 3003, ...
			for slot := 0; slot < 16384; slot += 1001 {
				select {
				case <-stopChan:
					return
				default:
				}
				key := slotKeys[slot]
				client.Set(ctx, key, "v", 0)
				client.Get(ctx, key)
			}
		}
	}()
	return func() {
		close(stopChan)
		<-doneChan // Wait for goroutine to finish
	}
}

// startPubSubConnections creates pubsub subscriptions on channels spread across different slots
// to ensure pubsub connections are active during slot migrations. Returns a stop function.
// The pubsub connections use sharded pubsub (SSUBSCRIBE) to ensure they're on specific shards.
// Returns nil stop function if client is not a ClusterClient.
// The stop function waits for all goroutines to finish before returning.
func startPubSubConnections(ctx context.Context, t *testing.T, client redis.UniversalClient) (stop func()) {
	clusterClient, ok := client.(*redis.ClusterClient)
	if !ok {
		t.Log("Skipping pubsub connections - client is not a ClusterClient")
		return func() {}
	}
	stopChan := make(chan struct{})
	var pubsubs []*redis.PubSub
	var wg sync.WaitGroup

	// Create pubsub subscriptions on channels in different slots
	// Use channels that hash to slots spread across shards: 0, 1001, 2002, ...
	for slot := 0; slot < 16384; slot += 1001 {
		channelName := fmt.Sprintf("test-channel-%s", slotKeys[slot])

		// Use sharded pubsub (SSUBSCRIBE) for cluster-aware subscriptions
		pubsub := clusterClient.SSubscribe(ctx, channelName)
		pubsubs = append(pubsubs, pubsub)

		// Start a goroutine to receive messages (keeps connection active)
		wg.Add(1)
		go func(ps *redis.PubSub, ch string) {
			defer wg.Done()
			msgCh := ps.Channel()
			for {
				select {
				case <-stopChan:
					return
				case msg, ok := <-msgCh:
					if !ok {
						return
					}
					if debugE2E() {
						t.Logf("PubSub received message on %s: %v", ch, msg)
					}
				}
			}
		}(pubsub, channelName)
	}

	// Start a goroutine to publish messages periodically to keep connections active
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		msgCount := 0
		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
				// Publish to channels in different slots
				for slot := 0; slot < 16384; slot += 1001 {
					channelName := fmt.Sprintf("test-channel-%s", slotKeys[slot])
					// Use SPublish for sharded pubsub
					_ = clusterClient.SPublish(ctx, channelName, fmt.Sprintf("msg-%d", msgCount)).Err()
				}
				msgCount++
			}
		}
	}()

	t.Logf("✓ Started %d pubsub connections across different slots", len(pubsubs))

	return func() {
		close(stopChan)
		for _, ps := range pubsubs {
			_ = ps.Close()
		}
		wg.Wait() // Wait for all goroutines to finish
	}
}

// waitForLogsToSettle waits for a fixed duration to allow the log collector
// to collect all meaningful logs after notifications are received.
func waitForLogsToSettle(t *testing.T, waitDuration time.Duration) {
	t.Logf("Waiting %v for logs to settle...", waitDuration)
	time.Sleep(waitDuration)
	t.Logf("✓ Wait complete")
}

// waitForSMigratedOnShardsWithSMigrating waits for SMIGRATED notifications only on shards
// that received SMIGRATING. This handles topology changes where some shards may be removed.
// Returns the number of shards that received both SMIGRATING and SMIGRATED.
func waitForSMigratedOnShardsWithSMigrating(t *testing.T, shards []*shardInfo, timeout time.Duration) int {
	// First, find shards that received SMIGRATING
	var shardsWithSMigrating []*shardInfo
	for _, shard := range shards {
		if _, found := shard.hook.FindNotification("SMIGRATING"); found {
			shardsWithSMigrating = append(shardsWithSMigrating, shard)
		}
	}

	if len(shardsWithSMigrating) == 0 {
		t.Log("No shards received SMIGRATING, skipping SMIGRATED wait")
		return 0
	}

	t.Logf("Waiting for SMIGRATED on %d shards that received SMIGRATING...", len(shardsWithSMigrating))
	deadline := time.Now().Add(timeout)

	var shardsWithBoth int
	for time.Now().Before(deadline) {
		shardsWithBoth = 0
		for _, shard := range shardsWithSMigrating {
			if _, found := shard.hook.FindNotification("SMIGRATED"); found {
				shardsWithBoth++
			}
		}
		if shardsWithBoth == len(shardsWithSMigrating) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Log results for shards that received SMIGRATING
	for _, shard := range shardsWithSMigrating {
		if _, found := shard.hook.FindNotification("SMIGRATED"); found {
			t.Logf("  ✓ Shard %s (nodeAddress=%s): received SMIGRATING + SMIGRATED",
				shard.addr, shard.nodeAddress)
		} else {
			t.Logf("  ✗ Shard %s (nodeAddress=%s): received SMIGRATING but NO SMIGRATED",
				shard.addr, shard.nodeAddress)
		}
	}

	// Log shards that didn't receive SMIGRATING (for debugging)
	for _, shard := range shards {
		if _, found := shard.hook.FindNotification("SMIGRATING"); !found {
			t.Logf("  - Shard %s (nodeAddress=%s): no SMIGRATING received",
				shard.addr, shard.nodeAddress)
		}
	}

	t.Logf("✓ SMIGRATED received on %d/%d shards (that had SMIGRATING)", shardsWithBoth, len(shardsWithSMigrating))
	return shardsWithBoth
}

// shardInfo holds information about a cluster shard for notification tracking
type shardInfo struct {
	addr        string
	nodeAddress string
	hook        *TrackingNotificationsHook
}

// printPerShardNotificationSummary prints a summary of notifications received on each shard.
// This helps debug whether notifications are being received on all nodes or just some.
func printPerShardNotificationSummary(t *testing.T, shards []*shardInfo) {
	t.Logf("--- Per-Shard Notification Summary ---")
	for _, shard := range shards {
		if shard.hook == nil {
			t.Logf("  Shard %s (nodeAddress=%s): hook is nil", shard.addr, shard.nodeAddress)
			continue
		}
		analysis := shard.hook.GetAnalysis()
		t.Logf("  Shard %s (nodeAddress=%s): SMIGRATING=%d, SMIGRATED=%d, Total=%d",
			shard.addr, shard.nodeAddress,
			analysis.SMigratingCount, analysis.SMigratedCount, analysis.TotalNotifications)
	}
	t.Logf("--------------------------------------")

	// Print detailed events from each shard
	t.Logf("--- Per-Shard Detailed Notification Events ---")
	for _, shard := range shards {
		if shard.hook == nil {
			continue
		}
		events := shard.hook.GetDiagnosticsLog()
		if len(events) == 0 {
			t.Logf("  Shard %s: no events", shard.addr)
			continue
		}
		t.Logf("  Shard %s (%d events):", shard.addr, len(events))
		for _, event := range events {
			if event.Pre {
				slots := extractSlotsFromEvent(event)
				if slots != "" {
					t.Logf("    [%s] type=%s connID=%d seqID=%d slots=%s",
						event.Timestamp.Format("15:04:05.000"), event.Type, event.ConnID, event.SeqID, slots)
				} else {
					t.Logf("    [%s] type=%s connID=%d seqID=%d",
						event.Timestamp.Format("15:04:05.000"), event.Type, event.ConnID, event.SeqID)
				}
			}
		}
	}
	t.Logf("----------------------------------------------")
}

// clearAllTrackers clears the log collector, global tracker, and all per-shard hooks.
// Call this after setup is complete but before triggering the fault injector
// to ensure we only count notifications from the actual test action.
func clearAllTrackers(logCollector *TestLogCollector, tracker *TrackingNotificationsHook, shards []*shardInfo) {
	if logCollector != nil {
		logCollector.Clear()
	}
	if tracker != nil {
		tracker.Clear()
	}
	for _, shard := range shards {
		if shard.hook != nil {
			shard.hook.Clear()
		}
	}
}

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

	// Set up log collector to track cluster state reloads
	localLogCollector := NewTestLogCollector()
	localLogCollector.Clear()   // Clear any previous logs
	localLogCollector.DoPrint() // Print logs for debugging
	redis.SetLogger(localLogCollector)
	defer redis.SetLogger(logCollector) // Restore global logger after test

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
	logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(redisClient, tracker, logger)

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

	// Start background traffic to keep connections active on all shards
	stopTraffic := startBackgroundTraffic(ctx, redisClient)
	defer stopTraffic()

	// Start pubsub connections to test notifications on pubsub connections
	stopPubSub := startPubSubConnections(ctx, t, redisClient)
	defer stopPubSub()

	// Give the background traffic and pubsub time to establish connections on all shards
	time.Sleep(500 * time.Millisecond)

	// Clear all trackers before triggering the fault injector
	// This ensures we only count notifications from the actual test action
	clearAllTrackers(localLogCollector, tracker, nil)

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

	// Get log analysis for cluster state reloads
	logAnalysis := localLogCollector.GetAnalysis()
	t.Logf("✓ Cluster state reloads: %d", logAnalysis.ClusterStateReloadCount)
	logAnalysis.Print(t)

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

	// Set up log collector to track cluster state reloads
	localLogCollector := NewTestLogCollector()
	localLogCollector.Clear()   // Clear any previous logs
	localLogCollector.DoPrint() // Print logs for debugging
	redis.SetLogger(localLogCollector)
	defer redis.SetLogger(logCollector) // Restore global logger after test

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
	logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(clusterClient, tracker, logger)

	if err := clusterClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Set up per-shard notification tracking
	var shards []*shardInfo
	var shardsMu sync.Mutex

	err = clusterClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
		if err := nodeClient.Ping(ctx).Err(); err != nil {
			return err
		}

		addr := nodeClient.Options().Addr
		nodeAddress := nodeClient.NodeAddress()

		// Create per-shard tracking hook
		hook := NewTrackingNotificationsHookWithShard(addr)
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.AddNotificationHook(hook)
			t.Logf("  ✓ Added per-shard hook for %s (nodeAddress=%s)", addr, nodeAddress)
		} else {
			t.Logf("  ⚠️  WARNING: MaintNotificationsManager is nil for %s (nodeAddress=%s)", addr, nodeAddress)
		}

		shardsMu.Lock()
		shards = append(shards, &shardInfo{
			addr:        addr,
			nodeAddress: nodeAddress,
			hook:        hook,
		})
		shardsMu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to ping all shards: %v", err)
	}
	t.Logf("✓ Discovered and pinged %d shards", len(shards))

	// Start background traffic to keep connections active on all shards
	stopTraffic := startBackgroundTraffic(ctx, clusterClient)
	defer stopTraffic()

	// Start pubsub connections to test notifications on pubsub connections
	stopPubSub := startPubSubConnections(ctx, t, clusterClient)
	defer stopPubSub()

	// Give the background traffic and pubsub time to establish connections on all shards
	time.Sleep(500 * time.Millisecond)

	// Clear all trackers before triggering the fault injector
	// This ensures we only count notifications from the actual test action
	clearAllTrackers(localLogCollector, tracker, shards)

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

	// Wait for SMIGRATED notification (at least one)
	_, foundMigrated := tracker.FindOrWaitForNotification("SMIGRATED", testMode.ActionWaitTimeout)
	if !foundMigrated {
		t.Errorf("Timed out waiting for SMIGRATED notification")
	} else {
		t.Logf("✓ Received SMIGRATED notification")
	}

	// Wait for SMIGRATED only on shards that received SMIGRATING
	waitForSMigratedOnShardsWithSMigrating(t, shards, testMode.ActionWaitTimeout)

	// Wait for logs to settle
	waitForLogsToSettle(t, 2*time.Second)

	// Print per-shard notification summary
	printPerShardNotificationSummary(t, shards)

	// Print final analysis
	analysis := tracker.GetAnalysis()
	analysis.Print(t)

	// Get log analysis for cluster state reloads
	logAnalysis := localLogCollector.GetAnalysis()
	t.Logf("✓ Cluster state reloads: %d", logAnalysis.ClusterStateReloadCount)
	logAnalysis.Print(t)

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

	// Set up log collector to track cluster state reloads
	localLogCollector := NewTestLogCollector()
	localLogCollector.Clear()   // Clear any previous logs
	localLogCollector.DoPrint() // Print logs for debugging
	redis.SetLogger(localLogCollector)
	defer redis.SetLogger(logCollector) // Restore global logger after test

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
	logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(redisClient, tracker, logger)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Set up per-shard notification tracking for cluster clients
	var shards []*shardInfo
	var shardsMu sync.Mutex

	// Type assertion for cluster-specific methods
	clusterClient, isCluster := redisClient.(*redis.ClusterClient)
	if isCluster {
		err = clusterClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
			if err := nodeClient.Ping(ctx).Err(); err != nil {
				return err
			}

			addr := nodeClient.Options().Addr
			nodeAddress := nodeClient.NodeAddress()

			// Create per-shard tracking hook
			hook := NewTrackingNotificationsHookWithShard(addr)
			manager := nodeClient.GetMaintNotificationsManager()
			if manager != nil {
				manager.AddNotificationHook(hook)
				t.Logf("  ✓ Added per-shard hook for %s (nodeAddress=%s)", addr, nodeAddress)
			} else {
				t.Logf("  ⚠️  WARNING: MaintNotificationsManager is nil for %s (nodeAddress=%s)", addr, nodeAddress)
			}

			shardsMu.Lock()
			shards = append(shards, &shardInfo{
				addr:        addr,
				nodeAddress: nodeAddress,
				hook:        hook,
			})
			shardsMu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to ping all shards: %v", err)
		}
		t.Logf("✓ Discovered and pinged %d shards", len(shards))

	}

	// Start background traffic to keep connections active on all shards
	stopTraffic := startBackgroundTraffic(ctx, redisClient)
	defer stopTraffic()

	// Start pubsub connections to test notifications on pubsub connections
	stopPubSub := startPubSubConnections(ctx, t, redisClient)
	defer stopPubSub()

	// Give the background traffic and pubsub time to establish connections on all shards
	time.Sleep(500 * time.Millisecond)

	// Clear all trackers before triggering the fault injector
	// This ensures we only count notifications from the actual test action
	clearAllTrackers(localLogCollector, tracker, shards)

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

	// Wait for SMIGRATED only on shards that received SMIGRATING (for cluster clients)
	if isCluster && len(shards) > 0 {
		waitForSMigratedOnShardsWithSMigrating(t, shards, testMode.ActionWaitTimeout)

		// Wait for logs to settle
		waitForLogsToSettle(t, 2*time.Second)
	}

	// Print per-shard notification summary
	printPerShardNotificationSummary(t, shards)

	analysis := tracker.GetAnalysis()
	analysis.Print(t)

	// Get log analysis for cluster state reloads
	logAnalysis := localLogCollector.GetAnalysis()
	t.Logf("✓ Cluster state reloads: %d", logAnalysis.ClusterStateReloadCount)
	logAnalysis.Print(t)

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

	// Set up log collector to track cluster state reloads
	localLogCollector := NewTestLogCollector()
	localLogCollector.Clear()   // Clear any previous logs
	localLogCollector.DoPrint() // Print logs for debugging
	redis.SetLogger(localLogCollector)
	defer redis.SetLogger(logCollector) // Restore global logger after test

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
	logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(redisClient, tracker, logger)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Set up per-shard notification tracking for cluster clients
	var shards []*shardInfo
	var shardsMu sync.Mutex

	// Type assertion for cluster-specific methods
	clusterClient, isCluster := redisClient.(*redis.ClusterClient)
	if isCluster {
		err = clusterClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
			if err := nodeClient.Ping(ctx).Err(); err != nil {
				return err
			}

			addr := nodeClient.Options().Addr
			nodeAddress := nodeClient.NodeAddress()

			// Create per-shard tracking hook
			hook := NewTrackingNotificationsHookWithShard(addr)
			manager := nodeClient.GetMaintNotificationsManager()
			if manager != nil {
				manager.AddNotificationHook(hook)
				t.Logf("  ✓ Added per-shard hook for %s (nodeAddress=%s)", addr, nodeAddress)
			} else {
				t.Logf("  ⚠️  WARNING: MaintNotificationsManager is nil for %s (nodeAddress=%s)", addr, nodeAddress)
			}

			shardsMu.Lock()
			shards = append(shards, &shardInfo{
				addr:        addr,
				nodeAddress: nodeAddress,
				hook:        hook,
			})
			shardsMu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to ping all shards: %v", err)
		}
		t.Logf("✓ Discovered and pinged %d shards", len(shards))

	}

	// Start background traffic to keep connections active on all shards
	stopTraffic := startBackgroundTraffic(ctx, redisClient)
	defer stopTraffic()

	// Start pubsub connections to test notifications on pubsub connections
	stopPubSub := startPubSubConnections(ctx, t, redisClient)
	defer stopPubSub()

	// Give the background traffic and pubsub time to establish connections on all shards
	time.Sleep(500 * time.Millisecond)

	// Clear all trackers before triggering the fault injector
	// This ensures we only count notifications from the actual test action
	clearAllTrackers(localLogCollector, tracker, shards)

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

	// Wait for SMIGRATED only on shards that received SMIGRATING (for cluster clients)
	if isCluster && len(shards) > 0 {
		waitForSMigratedOnShardsWithSMigrating(t, shards, testMode.ActionWaitTimeout)

		// Wait for logs to settle
		waitForLogsToSettle(t, 2*time.Second)
	}

	// Print per-shard notification summary
	printPerShardNotificationSummary(t, shards)

	analysis.Print(t)

	// Get log analysis for cluster state reloads
	logAnalysis := localLogCollector.GetAnalysis()
	t.Logf("✓ Cluster state reloads: %d", logAnalysis.ClusterStateReloadCount)
	logAnalysis.Print(t)

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

	// Set up log collector to track cluster state reloads
	localLogCollector := NewTestLogCollector()
	localLogCollector.Clear()   // Clear any previous logs
	localLogCollector.DoPrint() // Print logs for debugging
	redis.SetLogger(localLogCollector)
	defer redis.SetLogger(logCollector) // Restore global logger after test

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
	logger1 := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(client1, tracker1, logger1)

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
	logger2 := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
	setupNotificationHooks(client2, tracker2, logger2)

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

	// Get log analysis for cluster state reloads
	logAnalysis := localLogCollector.GetAnalysis()
	t.Logf("✓ Cluster state reloads: %d", logAnalysis.ClusterStateReloadCount)
	logAnalysis.Print(t)

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
	// All slot-migrate effects should generate SMIGRATING/SMIGRATED notifications
	// that are received on ALL shards in the cluster.
	testCases := []struct {
		name    string
		effect  SlotMigrateEffect
		trigger SlotMigrateVariant
	}{
		// slot-shuffle effect (2 variants) - no node changes
		{
			name:    "SlotShuffle_Migrate",
			effect:  SlotMigrateEffectSlotShuffle,
			trigger: SlotMigrateVariantMigrate,
		},
		{
			name:    "SlotShuffle_Failover",
			effect:  SlotMigrateEffectSlotShuffle,
			trigger: SlotMigrateVariantFailover,
			// For cluster scenarios, slot-shuffle + failover should still send SMIGRATING/SMIGRATED
			// because the slots effectively move from one endpoint to another when master/replica swap
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
			name:    "RemoveAdd_Failover",
			effect:  SlotMigrateEffectRemoveAdd,
			trigger: SlotMigrateVariantFailover,
		},
		// remove effect - one node removed
		{
			name:    "Remove_Migrate",
			effect:  SlotMigrateEffectRemove,
			trigger: SlotMigrateVariantMigrate,
		},
	}

	for _, tc := range testCases {
		// Get the number of requirements for this effect/variant
		// Each requirement represents a different database configuration that should be tested
		reqCount := GetSlotMigrateRequirementsCount(t, ctx, tc.effect, tc.trigger)

		for reqIdx := 0; reqIdx < reqCount; reqIdx++ {
			reqIdx := reqIdx // capture loop variable
			testName := tc.name
			if reqCount > 1 {
				testName = fmt.Sprintf("%s_Req%d", tc.name, reqIdx)
			}

			t.Run(testName, func(t *testing.T) {
				t.Logf("Testing slot-migrate effect: %s, variant: %s (requirement %d/%d)",
					tc.effect, tc.trigger, reqIdx+1, reqCount)

				// Create a fresh database configured for this specific effect/variant/requirement
				// This queries the fault injector for the required database configuration
				bdbID, factory, testMode, fiClient, factoryCleanup := SetupTestDatabaseForSlotMigrateWithRequirementIndex(t, ctx, tc.effect, tc.trigger, reqIdx)
				defer factoryCleanup()

				t.Logf("✓ Created database %d for effect %s, variant %s, requirement %d (mode: %s)",
					bdbID, tc.effect, tc.trigger, reqIdx, testMode.Mode)

				// Set up log collector to track cluster state reloads
				localLogCollector := NewTestLogCollector()
				localLogCollector.Clear()   // Clear any previous logs
				localLogCollector.DoPrint() // Print logs for debugging
				redis.SetLogger(localLogCollector)
				defer redis.SetLogger(logCollector) // Restore global logger after test

				// Create Redis cluster client connected to the new database
				redisClientIface, err := factory.Create("redisClient", &CreateClientOptions{
					Protocol: 3,
					MaintNotificationsConfig: &maintnotifications.Config{
						Mode: maintnotifications.ModeEnabled,
					},
					// Disable automatic cluster state reloads so we only get notification-triggered reloads
					ClusterStateReloadInterval: 10 * time.Minute,
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
				// Also set up per-shard notification tracking
				var shards []*shardInfo
				var shardsMu sync.Mutex

				err = redisClient.ForEachShard(ctx, func(ctx context.Context, nodeClient *redis.Client) error {
					if err := nodeClient.Ping(ctx).Err(); err != nil {
						return err
					}

					addr := nodeClient.Options().Addr
					nodeAddress := nodeClient.NodeAddress()

					// Create per-shard tracking hook
					hook := NewTrackingNotificationsHookWithShard(addr)
					manager := nodeClient.GetMaintNotificationsManager()
					if manager != nil {
						manager.AddNotificationHook(hook)
						t.Logf("  ✓ Added per-shard hook for %s (nodeAddress=%s)", addr, nodeAddress)
					} else {
						t.Logf("  ⚠️  WARNING: MaintNotificationsManager is nil for %s (nodeAddress=%s)", addr, nodeAddress)
					}

					shardsMu.Lock()
					shards = append(shards, &shardInfo{
						addr:        addr,
						nodeAddress: nodeAddress,
						hook:        hook,
					})
					shardsMu.Unlock()
					return nil
				})
				if err != nil {
					t.Fatalf("Failed to ping all shards: %v", err)
				}
				t.Logf("✓ Discovered and pinged %d shards", len(shards))

				// Also set up a global tracker for backward compatibility
				tracker := NewTrackingNotificationsHook()
				logger := maintnotifications.NewLoggingHook(int(logging.LogLevelDebug))
				setupNotificationHooks(redisClient, tracker, logger)

				// Start background traffic to keep connections active on all shards
				stopTraffic := startBackgroundTraffic(ctx, redisClient)
				defer stopTraffic()

				// Start pubsub connections to test notifications on pubsub connections
				stopPubSub := startPubSubConnections(ctx, t, redisClient)
				defer stopPubSub()

				// Give the background traffic and pubsub time to establish connections on all shards
				time.Sleep(500 * time.Millisecond)

				// Clear all trackers before triggering the fault injector
				// This ensures we only count notifications from the actual test action
				clearAllTrackers(localLogCollector, tracker, shards)

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
				// For all slot-migrate effects, expect SMIGRATING/SMIGRATED notifications
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

				// Wait for SMIGRATED only on shards that received SMIGRATING
				waitForSMigratedOnShardsWithSMigrating(t, shards, testMode.ActionWaitTimeout)

				// Wait for logs to settle
				waitForLogsToSettle(t, 2*time.Second)

				// Print per-shard notification summary
				printPerShardNotificationSummary(t, shards)

				// Print final notification tracker analysis
				analysis := tracker.GetAnalysis()
				analysis.Print(t)

				t.Logf("✓ Received %d SMIGRATING notification(s)", analysis.SMigratingCount)
				t.Logf("✓ Received %d SMIGRATED notification(s)", analysis.SMigratedCount)

				if analysis.NotificationProcessingErrors > 0 {
					t.Errorf("Got %d notification processing errors", analysis.NotificationProcessingErrors)
				}

				// Get log analysis for cluster state reloads
				logAnalysis := localLogCollector.GetAnalysis()
				t.Logf("✓ Cluster state reloads: %d", logAnalysis.ClusterStateReloadCount)
				logAnalysis.Print(t)
			})
		}
	}

	t.Log("✓ All slot-migrate effects tested successfully!")
}
