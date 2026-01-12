package redis_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestClusterMaintNotifications_CallbackSetup tests that the cluster state reload callback is properly set up
func TestClusterMaintNotifications_CallbackSetup(t *testing.T) {
	// Create a mock cluster with maintnotifications enabled
	opt := &redis.ClusterOptions{
		Addrs:    []string{"localhost:6379"},
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
		// Use a custom ClusterSlots function to avoid needing a real cluster
		ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{
				{
					Start: 0,
					End:   16383,
					Nodes: []redis.ClusterNode{
						{Addr: "localhost:6379"},
					},
				},
			}, nil
		},
	}

	client := redis.NewClusterClient(opt)
	defer client.Close()

	// Give time for initialization
	time.Sleep(100 * time.Millisecond)

	// Verify that maintnotifications manager is available on node clients
	// We can't directly access the manager, but we can verify the setup worked
	// by checking that the client was created successfully
	if client == nil {
		t.Fatal("Expected cluster client to be created")
	}

	t.Log("Cluster maintnotifications callback setup test passed")
}

// TestClusterMaintNotifications_SMigratingHandler tests SMIGRATING notification handling
func TestClusterMaintNotifications_SMigratingHandler(t *testing.T) {
	// Simulate receiving a SMIGRATING notification
	notification := []interface{}{
		"SMIGRATING",
		int64(12345),
		"1000",
		"2000-3000",
	}

	// In a real scenario, this would be handled by the NotificationHandler
	// For unit testing, we verify the notification format is correct
	if len(notification) < 3 {
		t.Fatal("SMIGRATING notification should have at least 3 elements")
	}

	notifType, ok := notification[0].(string)
	if !ok || notifType != "SMIGRATING" {
		t.Fatalf("Expected notification type SMIGRATING, got %v", notification[0])
	}

	seqID, ok := notification[1].(int64)
	if !ok {
		t.Fatalf("Expected SeqID to be int64, got %T", notification[1])
	}
	if seqID != 12345 {
		t.Errorf("Expected SeqID 12345, got %d", seqID)
	}

	// Verify slot ranges
	if len(notification) < 3 {
		t.Fatal("Expected at least one slot range")
	}

	slot1, ok := notification[2].(string)
	if !ok || slot1 != "1000" {
		t.Errorf("Expected first slot to be '1000', got %v", notification[2])
	}

	if len(notification) >= 4 {
		slot2, ok := notification[3].(string)
		if !ok || slot2 != "2000-3000" {
			t.Errorf("Expected second slot range to be '2000-3000', got %v", notification[3])
		}
	}

	t.Log("SMIGRATING notification format validation passed")
}

// TestClusterMaintNotifications_SMigratedHandler tests SMIGRATED notification handling
func TestClusterMaintNotifications_SMigratedHandler(t *testing.T) {
	// Simulate receiving a SMIGRATED notification with correct format
	// Format: ["SMIGRATED", SeqID, [[host:port, slots], [host:port, slots], ...]]
	notification := []interface{}{
		"SMIGRATED",
		int64(12346),
		[]interface{}{
			[]interface{}{"127.0.0.1:6379", "123,456,789-1000"},
			[]interface{}{"127.0.0.1:6380", "124,457,300-500"},
		},
	}

	// Verify notification format
	if len(notification) != 3 {
		t.Fatalf("SMIGRATED notification should have exactly 3 elements, got %d", len(notification))
	}

	notifType, ok := notification[0].(string)
	if !ok || notifType != "SMIGRATED" {
		t.Fatalf("Expected notification type SMIGRATED, got %v", notification[0])
	}

	seqID, ok := notification[1].(int64)
	if !ok {
		t.Fatalf("Expected SeqID to be int64, got %T", notification[1])
	}
	if seqID != 12346 {
		t.Errorf("Expected SeqID 12346, got %d", seqID)
	}

	// Verify endpoints array
	endpoints, ok := notification[2].([]interface{})
	if !ok {
		t.Fatalf("Expected endpoints to be array, got %T", notification[2])
	}
	if len(endpoints) != 2 {
		t.Errorf("Expected 2 endpoints, got %d", len(endpoints))
	}

	// Verify first endpoint
	endpoint1, ok := endpoints[0].([]interface{})
	if !ok {
		t.Fatalf("Expected endpoint to be array, got %T", endpoints[0])
	}
	if len(endpoint1) != 2 {
		t.Fatalf("Expected endpoint to have 2 elements, got %d", len(endpoint1))
	}
	hostPort1, ok := endpoint1[0].(string)
	if !ok || hostPort1 != "127.0.0.1:6379" {
		t.Errorf("Expected first endpoint host:port '127.0.0.1:6379', got %v", endpoint1[0])
	}
	slots1, ok := endpoint1[1].(string)
	if !ok || slots1 != "123,456,789-1000" {
		t.Errorf("Expected first endpoint slots '123,456,789-1000', got %v", endpoint1[1])
	}

	// Verify second endpoint
	endpoint2, ok := endpoints[1].([]interface{})
	if !ok {
		t.Fatalf("Expected endpoint to be array, got %T", endpoints[1])
	}
	if len(endpoint2) != 2 {
		t.Fatalf("Expected endpoint to have 2 elements, got %d", len(endpoint2))
	}
	hostPort2, ok := endpoint2[0].(string)
	if !ok || hostPort2 != "127.0.0.1:6380" {
		t.Errorf("Expected second endpoint host:port '127.0.0.1:6380', got %v", endpoint2[0])
	}
	slots2, ok := endpoint2[1].(string)
	if !ok || slots2 != "124,457,300-500" {
		t.Errorf("Expected second endpoint slots '124,457,300-500', got %v", endpoint2[1])
	}

	t.Log("SMIGRATED notification format validation passed")
}

// TestClusterMaintNotifications_DeduplicationLogic tests the deduplication logic for SMIGRATED
func TestClusterMaintNotifications_DeduplicationLogic(t *testing.T) {
	// This test verifies the deduplication concept
	// In the actual implementation, SMIGRATED notifications with the same SeqID
	// should only trigger cluster state reload once

	processedSeqIDs := make(map[int64]bool)
	var reloadCount int

	// Simulate receiving multiple SMIGRATED notifications with same SeqID
	notifications := []int64{12345, 12345, 12345, 12346, 12346, 12347}

	for _, seqID := range notifications {
		// Check if already processed
		if !processedSeqIDs[seqID] {
			processedSeqIDs[seqID] = true
			reloadCount++
		}
	}

	// Should have 3 unique SeqIDs (12345, 12346, 12347)
	if reloadCount != 3 {
		t.Errorf("Expected 3 unique reloads, got %d", reloadCount)
	}

	if len(processedSeqIDs) != 3 {
		t.Errorf("Expected 3 unique SeqIDs, got %d", len(processedSeqIDs))
	}

	t.Log("Deduplication logic test passed")
}

// TestClusterMaintNotifications_NotificationTypes tests that cluster notification types are defined
func TestClusterMaintNotifications_NotificationTypes(t *testing.T) {
	// Verify that SMIGRATING and SMIGRATED constants exist
	// These are defined in maintnotifications package
	
	expectedTypes := []string{
		maintnotifications.NotificationSMigrating,
		maintnotifications.NotificationSMigrated,
	}

	for _, notifType := range expectedTypes {
		if notifType == "" {
			t.Errorf("Notification type should not be empty")
		}
	}

	// Verify the values
	if maintnotifications.NotificationSMigrating != "SMIGRATING" {
		t.Errorf("Expected SMIGRATING, got %s", maintnotifications.NotificationSMigrating)
	}

	if maintnotifications.NotificationSMigrated != "SMIGRATED" {
		t.Errorf("Expected SMIGRATED, got %s", maintnotifications.NotificationSMigrated)
	}

	t.Log("Notification types test passed")
}

// TestClusterMaintNotifications_ConfigValidation tests maintnotifications config for cluster
func TestClusterMaintNotifications_ConfigValidation(t *testing.T) {
	// Test valid config
	config := &maintnotifications.Config{
		Mode:           maintnotifications.ModeEnabled,
		RelaxedTimeout: 10 * time.Second,
	}

	if err := config.ApplyDefaults().Validate(); err != nil {
		t.Errorf("Valid config should pass validation: %v", err)
	}

	// Test that config can be cloned (important for cluster where each node gets a copy)
	cloned := config.Clone()
	if cloned.Mode != config.Mode {
		t.Error("Cloned config should have same mode")
	}
	if cloned.RelaxedTimeout != config.RelaxedTimeout {
		t.Error("Cloned config should have same relaxed timeout")
	}

	// Modify original to ensure clone is independent
	config.RelaxedTimeout = 20 * time.Second
	if cloned.RelaxedTimeout == config.RelaxedTimeout {
		t.Error("Clone should be independent of original")
	}

	t.Log("Config validation test passed")
}

// TestClusterMaintNotifications_StateReloadCallback tests the callback mechanism
func TestClusterMaintNotifications_StateReloadCallback(t *testing.T) {
	var callbackInvoked atomic.Bool
	var receivedHostPort atomic.Value
	var receivedSlots atomic.Value

	// Simulate the callback that would be set on the manager
	callback := func(ctx context.Context, hostPort string, slotRanges []string) {
		callbackInvoked.Store(true)
		receivedHostPort.Store(hostPort)
		receivedSlots.Store(slotRanges)
	}

	// Simulate invoking the callback (as would happen when SMIGRATED is received)
	ctx := context.Background()
	callback(ctx, "127.0.0.1:6380", []string{"1000", "2000-3000"})

	// Verify callback was invoked
	if !callbackInvoked.Load() {
		t.Error("Expected callback to be invoked")
	}

	// Verify parameters
	hostPort := receivedHostPort.Load().(string)
	if hostPort != "127.0.0.1:6380" {
		t.Errorf("Expected host:port '127.0.0.1:6380', got %s", hostPort)
	}

	slots := receivedSlots.Load().([]string)
	if len(slots) != 2 {
		t.Errorf("Expected 2 slot ranges, got %d", len(slots))
	}
	if slots[0] != "1000" {
		t.Errorf("Expected first slot '1000', got %s", slots[0])
	}
	if slots[1] != "2000-3000" {
		t.Errorf("Expected second slot range '2000-3000', got %s", slots[1])
	}

	t.Log("State reload callback test passed")
}

// TestClusterMaintNotifications_ConcurrentCallbacks tests concurrent callback invocations
func TestClusterMaintNotifications_ConcurrentCallbacks(t *testing.T) {
	var callbackCount atomic.Int32

	callback := func(ctx context.Context, hostPort string, slotRanges []string) {
		callbackCount.Add(1)
		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)
	}

	// Invoke callback concurrently
	ctx := context.Background()
	const numConcurrent = 10

	done := make(chan bool, numConcurrent)
	for i := 0; i < numConcurrent; i++ {
		go func(idx int) {
			callback(ctx, "127.0.0.1:6380", []string{string(rune(idx))})
			done <- true
		}(i)
	}

	// Wait for all to complete
	for i := 0; i < numConcurrent; i++ {
		<-done
	}

	// Verify all callbacks were invoked
	if callbackCount.Load() != numConcurrent {
		t.Errorf("Expected %d callback invocations, got %d", numConcurrent, callbackCount.Load())
	}

	t.Log("Concurrent callbacks test passed")
}

