package examples

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/maintnotifications/e2e"
)

// FaultInjectorExample demonstrates how to use the fault injector for E2E testing
func FaultInjectorExample() {
	ctx := context.Background()

	// Initialize fault injector client
	faultInjector := e2e.NewFaultInjectorClient("http://localhost:8080")

	// Initialize Redis cluster client with hitless upgrades
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		Protocol: 3, // RESP3 for push notifications
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:           maintnotifications.ModeEnabled,
			HandoffTimeout: 30 * time.Second,
			RelaxedTimeout: 10 * time.Second,
			MaxWorkers:     20,
		},
	})
	defer client.Close()

	// Verify initial connectivity
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis cluster: %v", err)
	}

	log.Println("=== Fault Injector E2E Testing Example ===")

	// Example 1: Simple cluster failover
	log.Println("\n1. Testing cluster failover...")
	if err := testClusterFailover(ctx, faultInjector, client); err != nil {
		log.Printf("Cluster failover test failed: %v", err)
	} else {
		log.Println("✓ Cluster failover test passed")
	}

	// Example 2: Network latency simulation
	log.Println("\n2. Testing network latency...")
	if err := testNetworkLatency(ctx, faultInjector, client); err != nil {
		log.Printf("Network latency test failed: %v", err)
	} else {
		log.Println("✓ Network latency test passed")
	}

	// Example 3: Complex sequence of faults
	log.Println("\n3. Testing complex fault sequence...")
	if err := testComplexFaultSequence(ctx, faultInjector, client); err != nil {
		log.Printf("Complex fault sequence test failed: %v", err)
	} else {
		log.Println("✓ Complex fault sequence test passed")
	}

	// Example 4: Rolling restart simulation
	log.Println("\n4. Testing rolling restart...")
	if err := testRollingRestart(ctx, faultInjector, client); err != nil {
		log.Printf("Rolling restart test failed: %v", err)
	} else {
		log.Println("✓ Rolling restart test passed")
	}

	log.Println("\n=== All tests completed ===")
}

func testClusterFailover(ctx context.Context, faultInjector *e2e.FaultInjectorClient, client *redis.ClusterClient) error {
	// Start background operations
	stopCh := make(chan struct{})
	defer close(stopCh)

	go performBackgroundOperations(ctx, client, stopCh)

	// Wait for baseline operations
	time.Sleep(2 * time.Second)

	// Trigger cluster failover
	masterNodes := e2e.GetMasterNodes()
	resp, err := faultInjector.TriggerClusterFailover(ctx, masterNodes[0], false)
	if err != nil {
		return fmt.Errorf("failed to trigger failover: %w", err)
	}

	log.Printf("Failover triggered: %s", resp.ActionID)

	// Wait for failover to complete
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
		e2e.WithMaxWaitTime(60*time.Second),
		e2e.WithPollInterval(2*time.Second))

	if err != nil {
		return fmt.Errorf("failover failed: %w", err)
	}

	log.Printf("Failover completed: %s", status.Status)

	// Verify cluster is still operational
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cluster not responsive after failover: %w", err)
	}

	return nil
}

func testNetworkLatency(ctx context.Context, faultInjector *e2e.FaultInjectorClient, client *redis.ClusterClient) error {
	// Start background operations
	stopCh := make(chan struct{})
	defer close(stopCh)

	go performBackgroundOperations(ctx, client, stopCh)

	// Add network latency
	nodes := e2e.GetClusterNodes()
	resp, err := faultInjector.SimulateNetworkLatency(ctx, nodes, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		return fmt.Errorf("failed to simulate latency: %w", err)
	}

	log.Printf("Network latency applied: %s", resp.ActionID)

	// Wait for latency to be applied
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
		e2e.WithMaxWaitTime(30*time.Second))

	if err != nil {
		return fmt.Errorf("latency simulation failed: %w", err)
	}

	log.Printf("Latency simulation completed: %s", status.Status)

	// Test operations under latency
	start := time.Now()
	if err := client.Set(ctx, "latency_test", "value", time.Minute).Err(); err != nil {
		return fmt.Errorf("operation failed under latency: %w", err)
	}
	duration := time.Since(start)

	log.Printf("Operation completed in %v (with added latency)", duration)

	// Restore normal network conditions
	restoreResp, err := faultInjector.RestoreNetwork(ctx, nodes)
	if err != nil {
		return fmt.Errorf("failed to restore network: %w", err)
	}

	_, err = faultInjector.WaitForAction(ctx, restoreResp.ActionID,
		e2e.WithMaxWaitTime(30*time.Second))

	if err != nil {
		return fmt.Errorf("network restore failed: %w", err)
	}

	log.Println("Network conditions restored")

	return nil
}

func testComplexFaultSequence(ctx context.Context, faultInjector *e2e.FaultInjectorClient, client *redis.ClusterClient) error {
	// Start background operations
	stopCh := make(chan struct{})
	defer close(stopCh)

	go performBackgroundOperations(ctx, client, stopCh)

	// Create a complex sequence of faults
	sequence := []e2e.SequenceAction{
		{
			Type: e2e.ActionNetworkLatency,
			Parameters: map[string]interface{}{
				"nodes":   e2e.GetClusterNodes(),
				"latency": "50ms",
				"jitter":  "10ms",
			},
		},
		{
			Type: e2e.ActionNetworkPacketLoss,
			Parameters: map[string]interface{}{
				"nodes":        e2e.GetClusterNodes(),
				"loss_percent": 1.0,
			},
			Delay: 10 * time.Second,
		},
		{
			Type: e2e.ActionClusterFailover,
			Parameters: map[string]interface{}{
				"node_id": e2e.GetMasterNodes()[0],
				"force":   false,
			},
			Delay: 15 * time.Second,
		},
		{
			Type: e2e.ActionNetworkRestore,
			Parameters: map[string]interface{}{
				"nodes": e2e.GetClusterNodes(),
			},
			Delay: 20 * time.Second,
		},
	}

	resp, err := faultInjector.ExecuteSequence(ctx, sequence)
	if err != nil {
		return fmt.Errorf("failed to execute sequence: %w", err)
	}

	log.Printf("Complex sequence started: %s", resp.ActionID)

	// Wait for sequence to complete
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
		e2e.WithMaxWaitTime(120*time.Second),
		e2e.WithPollInterval(5*time.Second))

	if err != nil {
		return fmt.Errorf("sequence execution failed: %w", err)
	}

	log.Printf("Complex sequence completed: %s", status.Status)
	log.Printf("Sequence output: %s", status.Output)

	// Verify cluster health after sequence
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cluster not responsive after sequence: %w", err)
	}

	return nil
}

func testRollingRestart(ctx context.Context, faultInjector *e2e.FaultInjectorClient, client *redis.ClusterClient) error {
	// Start background operations
	stopCh := make(chan struct{})
	defer close(stopCh)

	go performBackgroundOperations(ctx, client, stopCh)

	// Trigger rolling restart
	resp, err := faultInjector.SimulateClusterUpgrade(ctx, e2e.GetClusterNodes())
	if err != nil {
		return fmt.Errorf("failed to trigger rolling restart: %w", err)
	}

	log.Printf("Rolling restart started: %s", resp.ActionID)

	// Wait for rolling restart to complete
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
		e2e.WithMaxWaitTime(180*time.Second), // 3 minutes for rolling restart
		e2e.WithPollInterval(10*time.Second))

	if err != nil {
		return fmt.Errorf("rolling restart failed: %w", err)
	}

	log.Printf("Rolling restart completed: %s", status.Status)

	// Verify cluster health after restart
	time.Sleep(10 * time.Second) // Allow time for cluster to stabilize

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cluster not responsive after rolling restart: %w", err)
	}

	return nil
}

func performBackgroundOperations(ctx context.Context, client *redis.ClusterClient, stopCh <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			key := fmt.Sprintf("bg_test_key_%d", counter)
			value := fmt.Sprintf("value_%d", counter)

			// Perform operation with timeout
			opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := client.Set(opCtx, key, value, time.Minute).Err()
			cancel()

			if err != nil {
				log.Printf("Background operation failed: %v", err)
			}

			counter++
		}
	}
}

// Additional utility functions for demonstration

func demonstrateFaultInjectorCapabilities(ctx context.Context) {
	faultInjector := e2e.NewFaultInjectorClient("http://localhost:8080")

	log.Println("=== Fault Injector Capabilities Demo ===")

	// List available actions
	actions, err := faultInjector.ListActions(ctx)
	if err != nil {
		log.Printf("Failed to list actions: %v", err)
		return
	}

	log.Printf("Available actions: %v", actions)

	// Demonstrate individual action types
	demonstrateClusterActions(ctx, faultInjector)
	demonstrateNetworkActions(ctx, faultInjector)
	demonstrateNodeActions(ctx, faultInjector)
}

func demonstrateClusterActions(ctx context.Context, faultInjector *e2e.FaultInjectorClient) {
	log.Println("\n--- Cluster Actions ---")

	// Cluster failover
	log.Println("Triggering cluster failover...")
	resp, err := faultInjector.TriggerClusterFailover(ctx, "node-1", false)
	if err != nil {
		log.Printf("Failover failed: %v", err)
	} else {
		log.Printf("Failover triggered: %s", resp.ActionID)
	}

	// Slot migration
	log.Println("Triggering slot migration...")
	resp, err = faultInjector.TriggerSlotMigration(ctx, 0, 1000, "node-1", "node-2")
	if err != nil {
		log.Printf("Slot migration failed: %v", err)
	} else {
		log.Printf("Slot migration triggered: %s", resp.ActionID)
	}
}

func demonstrateNetworkActions(ctx context.Context, faultInjector *e2e.FaultInjectorClient) {
	log.Println("\n--- Network Actions ---")

	nodes := []string{"node-1", "node-2"}

	// Network latency
	log.Println("Adding network latency...")
	resp, err := faultInjector.SimulateNetworkLatency(ctx, nodes, 100*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		log.Printf("Latency simulation failed: %v", err)
	} else {
		log.Printf("Latency simulation triggered: %s", resp.ActionID)
	}

	// Packet loss
	log.Println("Simulating packet loss...")
	resp, err = faultInjector.SimulatePacketLoss(ctx, nodes, 2.0)
	if err != nil {
		log.Printf("Packet loss simulation failed: %v", err)
	} else {
		log.Printf("Packet loss simulation triggered: %s", resp.ActionID)
	}

	// Network partition
	log.Println("Creating network partition...")
	resp, err = faultInjector.SimulateNetworkPartition(ctx, nodes, 30*time.Second)
	if err != nil {
		log.Printf("Network partition failed: %v", err)
	} else {
		log.Printf("Network partition triggered: %s", resp.ActionID)
	}
}

func demonstrateNodeActions(ctx context.Context, faultInjector *e2e.FaultInjectorClient) {
	log.Println("\n--- Node Actions ---")

	// Node restart
	log.Println("Restarting node...")
	resp, err := faultInjector.RestartNode(ctx, "node-1", true)
	if err != nil {
		log.Printf("Node restart failed: %v", err)
	} else {
		log.Printf("Node restart triggered: %s", resp.ActionID)
	}

	// Node stop
	log.Println("Stopping node...")
	resp, err = faultInjector.StopNode(ctx, "node-2", true)
	if err != nil {
		log.Printf("Node stop failed: %v", err)
	} else {
		log.Printf("Node stop triggered: %s", resp.ActionID)
	}

	// Node start
	log.Println("Starting node...")
	resp, err = faultInjector.StartNode(ctx, "node-2")
	if err != nil {
		log.Printf("Node start failed: %v", err)
	} else {
		log.Printf("Node start triggered: %s", resp.ActionID)
	}
}
