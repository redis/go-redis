package redis_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// multiNodeProxy represents a cae-resp-proxy instance that can mimic multiple cluster nodes
type multiNodeProxy struct {
	apiPort    int
	apiBaseURL string
	cmd        *exec.Cmd
	httpClient *http.Client
	nodes      []proxyNode
}

// proxyNode represents a single node in the multi-node proxy
type proxyNode struct {
	listenPort int
	targetHost string
	targetPort int
	proxyAddr  string
	nodeID     string
}

// newMultiNodeProxy creates a new multi-node proxy instance
func newMultiNodeProxy(apiPort int) *multiNodeProxy {
	return &multiNodeProxy{
		apiPort:    apiPort,
		apiBaseURL: fmt.Sprintf("http://localhost:%d", apiPort),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		nodes: make([]proxyNode, 0),
	}
}

// start starts the proxy server with initial node
func (mp *multiNodeProxy) start(initialListenPort, targetPort int, targetHost string) error {
	// Start cae-resp-proxy with just the API port
	// We'll add nodes dynamically via the API
	mp.cmd = exec.Command("cae-resp-proxy",
		"--api-port", fmt.Sprintf("%d", mp.apiPort),
		"--listen-port", fmt.Sprintf("%d", initialListenPort),
		"--target-host", targetHost,
		"--target-port", fmt.Sprintf("%d", targetPort),
	)

	if err := mp.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start proxy: %w", err)
	}

	// Wait for proxy to be ready
	time.Sleep(500 * time.Millisecond)

	// Verify proxy is responding
	for i := 0; i < 10; i++ {
		resp, err := mp.httpClient.Get(mp.apiBaseURL + "/stats")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				// Add the initial node to our tracking
				mp.nodes = append(mp.nodes, proxyNode{
					listenPort: initialListenPort,
					targetHost: targetHost,
					targetPort: targetPort,
					proxyAddr:  fmt.Sprintf("localhost:%d", initialListenPort),
					nodeID:     fmt.Sprintf("localhost:%d:%d", initialListenPort, targetPort),
				})
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("proxy did not become ready")
}

// addNode adds a new proxy node dynamically
func (mp *multiNodeProxy) addNode(listenPort, targetPort int, targetHost string) (*proxyNode, error) {
	// Use the /nodes API to add a new proxy node
	nodeConfig := map[string]interface{}{
		"listenPort": listenPort,
		"targetHost": targetHost,
		"targetPort": targetPort,
	}

	jsonData, err := json.Marshal(nodeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal node config: %w", err)
	}

	resp, err := mp.httpClient.Post(
		mp.apiBaseURL+"/nodes",
		"application/json",
		bytes.NewReader(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to add node: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to add node, status %d: %s", resp.StatusCode, string(body))
	}

	// Create node tracking
	node := proxyNode{
		listenPort: listenPort,
		targetHost: targetHost,
		targetPort: targetPort,
		proxyAddr:  fmt.Sprintf("localhost:%d", listenPort),
		nodeID:     fmt.Sprintf("localhost:%d:%d", listenPort, targetPort),
	}

	mp.nodes = append(mp.nodes, node)

	// Wait a bit for the node to be ready
	time.Sleep(200 * time.Millisecond)

	return &node, nil
}

// getNodes returns all proxy nodes
func (mp *multiNodeProxy) getNodes() []proxyNode {
	return mp.nodes
}

// getNodeAddrs returns all proxy node addresses for cluster client
func (mp *multiNodeProxy) getNodeAddrs() []string {
	addrs := make([]string, len(mp.nodes))
	for i, node := range mp.nodes {
		addrs[i] = node.proxyAddr
	}
	return addrs
}

// stop stops the proxy server
func (mp *multiNodeProxy) stop() error {
	if mp.cmd != nil && mp.cmd.Process != nil {
		return mp.cmd.Process.Kill()
	}
	return nil
}

// injectNotification injects a RESP3 push notification to all connected clients
func (mp *multiNodeProxy) injectNotification(notification string) error {
	url := mp.apiBaseURL + "/send-to-all-clients?encoding=raw"
	resp, err := mp.httpClient.Post(url, "application/octet-stream", strings.NewReader(notification))
	if err != nil {
		return fmt.Errorf("failed to inject notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("injection failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// injectNotificationToNode injects a notification to clients connected to a specific node
func (mp *multiNodeProxy) injectNotificationToNode(nodeAddr string, notification string) error {
	// Get all connections
	resp, err := mp.httpClient.Get(mp.apiBaseURL + "/connections")
	if err != nil {
		return fmt.Errorf("failed to get connections: %w", err)
	}
	defer resp.Body.Close()

	var connResp struct {
		ConnectionIDs []string `json:"connectionIds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&connResp); err != nil {
		return fmt.Errorf("failed to decode connections: %w", err)
	}

	// For simplicity, inject to all clients
	// In a real scenario, you'd filter by node
	return mp.injectNotification(notification)
}

// formatSMigratingNotification creates a SMIGRATING push notification in RESP3 format
// Format: ["SMIGRATING", SeqID, slot/range1-range2, ...]
func formatSMigratingNotification(seqID int64, slots ...string) string {
	// >N\r\n where N is the number of elements
	parts := []string{fmt.Sprintf(">%d\r\n", 2+len(slots))}

	// $10\r\nSMIGRATING\r\n
	parts = append(parts, "$10\r\nSMIGRATING\r\n")

	// :seqID\r\n
	parts = append(parts, fmt.Sprintf(":%d\r\n", seqID))

	// Add each slot/range as bulk string
	for _, slot := range slots {
		parts = append(parts, fmt.Sprintf("$%d\r\n%s\r\n", len(slot), slot))
	}

	return strings.Join(parts, "")
}

// formatSMigratedNotification creates a SMIGRATED push notification in RESP3 format
// RESP3 wire format:
//
//	>3                      <- push frame with 3 top-level elements
//	+SMIGRATED              <- message name
//	:SeqID                  <- sequence id integer
//	*<num_entries>          <- array of triplet arrays
//	  *3                    <- each triplet is a 3-element array
//	    +<source>           <- node from which slots are migrating FROM
//	    +<destination>      <- node to which slots are migrating TO
//	    +<slots>            <- comma-separated slots and/or ranges
//
// Each triplet is formatted as: "source target slots"
// Example: "abc.com:6789 abc.com:6790 123,789-1000"
func formatSMigratedNotification(seqID int64, triplets ...string) string {
	parts := []string{">3\r\n"}

	// +SMIGRATED\r\n
	parts = append(parts, "+SMIGRATED\r\n")

	// :seqID\r\n
	parts = append(parts, fmt.Sprintf(":%d\r\n", seqID))

	// Outer array containing all triplets
	parts = append(parts, fmt.Sprintf("*%d\r\n", len(triplets)))

	for _, triplet := range triplets {
		// Split triplet into source, target, and slots
		tripletParts := strings.SplitN(triplet, " ", 3)
		if len(tripletParts) != 3 {
			continue
		}
		source := tripletParts[0]
		target := tripletParts[1]
		slots := tripletParts[2]

		// Each triplet is a 3-element array
		parts = append(parts, "*3\r\n")
		parts = append(parts, fmt.Sprintf("+%s\r\n", source))
		parts = append(parts, fmt.Sprintf("+%s\r\n", target))
		parts = append(parts, fmt.Sprintf("+%s\r\n", slots))
	}

	return strings.Join(parts, "")
}

// TestClusterMaintNotifications_SMIGRATING tests SMIGRATING notification handling
func TestClusterMaintNotifications_SMIGRATING(t *testing.T) {
	if os.Getenv("CLUSTER_MAINT_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping cluster maintnotifications integration test. Set CLUSTER_MAINT_INTEGRATION_TEST=true to run")
	}

	ctx := context.Background()

	// Create multi-node proxy that mimics a 3-node cluster
	proxy := newMultiNodeProxy(8000)
	if err := proxy.start(7000, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.stop()

	// Add two more nodes to mimic a 3-node cluster
	if _, err := proxy.addNode(7001, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 2: %v", err)
	}
	if _, err := proxy.addNode(7002, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 3: %v", err)
	}

	t.Logf("Started proxy with %d nodes: %v", len(proxy.getNodes()), proxy.getNodeAddrs())

	// Create cluster client pointing to all proxy nodes
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    proxy.getNodeAddrs(),
		Protocol: 3, // RESP3 required for push notifications
	})
	defer client.Close()

	// Verify connection works
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to cluster via proxy: %v", err)
	}

	// Perform some operations to establish connections
	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0).Err(); err != nil {
			t.Logf("Warning: Failed to set key: %v", err)
		}
	}

	// Inject SMIGRATING notification to all nodes
	notification := formatSMigratingNotification(12345, "1000", "2000-3000")
	if err := proxy.injectNotification(notification); err != nil {
		t.Fatalf("Failed to inject SMIGRATING notification: %v", err)
	}

	// Wait for notification processing
	time.Sleep(200 * time.Millisecond)

	// Verify operations still work (timeouts should be relaxed)
	if err := client.Set(ctx, "test-key-during-migration", "value", 0).Err(); err != nil {
		t.Errorf("Expected operations to work during migration, got error: %v", err)
	}

	t.Log("SMIGRATING notification test passed")
}

// TestClusterMaintNotifications_SMIGRATED tests SMIGRATED notification handling and cluster state reload
func TestClusterMaintNotifications_SMIGRATED(t *testing.T) {
	if os.Getenv("CLUSTER_MAINT_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping cluster maintnotifications integration test. Set CLUSTER_MAINT_INTEGRATION_TEST=true to run")
	}

	ctx := context.Background()

	// Create multi-node proxy
	proxy := newMultiNodeProxy(8001)
	if err := proxy.start(7010, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.stop()

	// Add more nodes
	if _, err := proxy.addNode(7011, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 2: %v", err)
	}
	if _, err := proxy.addNode(7012, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 3: %v", err)
	}

	t.Logf("Started proxy with %d nodes: %v", len(proxy.getNodes()), proxy.getNodeAddrs())

	// Track cluster state reloads
	var reloadCount atomic.Int32

	// Create cluster client
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    proxy.getNodeAddrs(),
		Protocol: 3,
	})
	defer client.Close()

	// Hook to track state reloads via callback
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
			})
		}
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Perform operations to establish connections
	for i := 0; i < 5; i++ {
		client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	initialReloads := reloadCount.Load()

	// Inject SMIGRATED notification with new format
	// Simulate migration from node 1 to node 2
	notification := formatSMigratedNotification(12346, "127.0.0.1:7011 1000,2000-3000")
	if err := proxy.injectNotification(notification); err != nil {
		t.Fatalf("Failed to inject SMIGRATED notification: %v", err)
	}

	// Wait for notification processing and state reload
	time.Sleep(500 * time.Millisecond)

	// Verify cluster state was reloaded
	finalReloads := reloadCount.Load()
	if finalReloads <= initialReloads {
		t.Errorf("Expected cluster state reload after SMIGRATED, reloads: initial=%d, final=%d",
			initialReloads, finalReloads)
	}

	// Verify operations still work
	if err := client.Set(ctx, "test-key-after-smigrated", "value", 0).Err(); err != nil {
		t.Errorf("Expected operations to work after SMIGRATED, got error: %v", err)
	}

	t.Log("SMIGRATED notification test passed")
}

// TestClusterMaintNotifications_Deduplication tests that SMIGRATED notifications are deduplicated by SeqID
func TestClusterMaintNotifications_Deduplication(t *testing.T) {
	if os.Getenv("CLUSTER_MAINT_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping cluster maintnotifications integration test. Set CLUSTER_MAINT_INTEGRATION_TEST=true to run")
	}

	ctx := context.Background()

	// Create multi-node proxy
	proxy := newMultiNodeProxy(8002)
	if err := proxy.start(7020, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.stop()

	// Add more nodes
	if _, err := proxy.addNode(7021, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 2: %v", err)
	}
	if _, err := proxy.addNode(7022, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 3: %v", err)
	}

	t.Logf("Started proxy with %d nodes: %v", len(proxy.getNodes()), proxy.getNodeAddrs())

	var reloadCount atomic.Int32

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    proxy.getNodeAddrs(),
		Protocol: 3,
	})
	defer client.Close()

	// Track reloads via callback
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
			})
		}
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Perform operations
	for i := 0; i < 5; i++ {
		client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	initialReloads := reloadCount.Load()

	// Inject the same SMIGRATED notification multiple times (same SeqID)
	// This simulates receiving the same notification from multiple nodes
	seqID := int64(99999)
	notification := formatSMigratedNotification(seqID, "127.0.0.1:7021 5000-6000")

	// Inject 5 times to all nodes
	for i := 0; i < 5; i++ {
		if err := proxy.injectNotification(notification); err != nil {
			t.Fatalf("Failed to inject notification: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify only ONE reload happened (deduplication by SeqID)
	finalReloads := reloadCount.Load()
	reloadDiff := finalReloads - initialReloads
	if reloadDiff != 1 {
		t.Errorf("Expected exactly 1 reload due to deduplication, got %d reloads", reloadDiff)
	}

	t.Log("Deduplication test passed")
}

// TestClusterMaintNotifications_MultiNode tests notifications across multiple cluster nodes
func TestClusterMaintNotifications_MultiNode(t *testing.T) {
	if os.Getenv("CLUSTER_MAINT_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping cluster maintnotifications integration test. Set CLUSTER_MAINT_INTEGRATION_TEST=true to run")
	}

	ctx := context.Background()

	// Create multi-node proxy with 5 nodes to mimic a real cluster
	proxy := newMultiNodeProxy(8003)
	if err := proxy.start(7030, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.stop()

	// Add 4 more nodes for a 5-node cluster
	for i := 1; i < 5; i++ {
		if _, err := proxy.addNode(7030+i, 6379, "localhost"); err != nil {
			t.Fatalf("Failed to add node %d: %v", i+1, err)
		}
	}

	nodes := proxy.getNodes()
	t.Logf("Started proxy with %d nodes: %v", len(nodes), proxy.getNodeAddrs())

	// Track notifications received
	var migratingCount atomic.Int32
	var migratedCount atomic.Int32
	var reloadCount atomic.Int32

	// Create cluster client
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    proxy.getNodeAddrs(),
		Protocol: 3,
	})
	defer client.Close()

	// Set up tracking
	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
				t.Logf("Cluster state reload triggered for %s, slots: %v", hostPort, slotRanges)
			})
		}
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Perform operations to establish connections to all nodes
	for i := 0; i < 20; i++ {
		client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	time.Sleep(500 * time.Millisecond)

	// Simulate slot migration scenario:
	// 1. SMIGRATING: Slots 0-5000 are migrating from node 1 to node 2
	t.Log("Injecting SMIGRATING notification...")
	migratingNotif := formatSMigratingNotification(10001, "0-5000")
	if err := proxy.injectNotification(migratingNotif); err != nil {
		t.Fatalf("Failed to inject SMIGRATING: %v", err)
	}
	migratingCount.Add(1)

	time.Sleep(300 * time.Millisecond)

	// 2. SMIGRATED: Migration completed, slots now on node 2
	t.Log("Injecting SMIGRATED notification...")
	migratedNotif := formatSMigratedNotification(10002,
		fmt.Sprintf("127.0.0.1:%d 0-5000", nodes[1].listenPort))
	if err := proxy.injectNotification(migratedNotif); err != nil {
		t.Fatalf("Failed to inject SMIGRATED: %v", err)
	}
	migratedCount.Add(1)

	time.Sleep(500 * time.Millisecond)

	// 3. Another migration: Slots 5001-10000 from node 2 to node 3
	t.Log("Injecting second SMIGRATING notification...")
	migratingNotif2 := formatSMigratingNotification(10003, "5001-10000")
	if err := proxy.injectNotification(migratingNotif2); err != nil {
		t.Fatalf("Failed to inject second SMIGRATING: %v", err)
	}
	migratingCount.Add(1)

	time.Sleep(300 * time.Millisecond)

	// 4. Second migration completed
	t.Log("Injecting second SMIGRATED notification...")
	migratedNotif2 := formatSMigratedNotification(10004,
		fmt.Sprintf("127.0.0.1:%d 5001-10000", nodes[2].listenPort))
	if err := proxy.injectNotification(migratedNotif2); err != nil {
		t.Fatalf("Failed to inject second SMIGRATED: %v", err)
	}
	migratedCount.Add(1)

	time.Sleep(500 * time.Millisecond)

	// Verify cluster state was reloaded for each SMIGRATED
	finalReloads := reloadCount.Load()
	if finalReloads < 2 {
		t.Errorf("Expected at least 2 cluster state reloads, got %d", finalReloads)
	}

	// Verify operations still work after migrations
	for i := 0; i < 10; i++ {
		if err := client.Set(ctx, fmt.Sprintf("post-migration-key%d", i), "value", 0).Err(); err != nil {
			t.Errorf("Expected operations to work after migrations, got error: %v", err)
		}
	}

	t.Logf("Multi-node test passed: SMIGRATING=%d, SMIGRATED=%d, Reloads=%d",
		migratingCount.Load(), migratedCount.Load(), finalReloads)
}

// TestClusterMaintNotifications_ComplexMigration tests complex multi-endpoint migration
func TestClusterMaintNotifications_ComplexMigration(t *testing.T) {
	if os.Getenv("CLUSTER_MAINT_INTEGRATION_TEST") != "true" {
		t.Skip("Skipping cluster maintnotifications integration test. Set CLUSTER_MAINT_INTEGRATION_TEST=true to run")
	}

	ctx := context.Background()

	// Create multi-node proxy
	proxy := newMultiNodeProxy(8004)
	if err := proxy.start(7040, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer proxy.stop()

	// Add 2 more nodes
	if _, err := proxy.addNode(7041, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 2: %v", err)
	}
	if _, err := proxy.addNode(7042, 6379, "localhost"); err != nil {
		t.Fatalf("Failed to add node 3: %v", err)
	}

	nodes := proxy.getNodes()
	t.Logf("Started proxy with %d nodes: %v", len(nodes), proxy.getNodeAddrs())

	var reloadCount atomic.Int32

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    proxy.getNodeAddrs(),
		Protocol: 3,
	})
	defer client.Close()

	client.OnNewNode(func(nodeClient *redis.Client) {
		manager := nodeClient.GetMaintNotificationsManager()
		if manager != nil {
			manager.SetClusterStateReloadCallback(func(ctx context.Context, hostPort string, slotRanges []string) {
				reloadCount.Add(1)
				t.Logf("Reload for %s, slots: %v", hostPort, slotRanges)
			})
		}
	})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Perform operations
	for i := 0; i < 10; i++ {
		client.Set(ctx, fmt.Sprintf("key%d", i), "value", 0)
	}

	initialReloads := reloadCount.Load()

	// Test new SMIGRATED format with multiple endpoints
	// Simulate a complex resharding where slots are distributed to multiple nodes
	t.Log("Injecting complex SMIGRATED notification with multiple endpoints...")
	notification := formatSMigratedNotification(20001,
		fmt.Sprintf("127.0.0.1:%d 0-5000,10000-12000", nodes[0].listenPort),
		fmt.Sprintf("127.0.0.1:%d 5001-9999", nodes[1].listenPort),
		fmt.Sprintf("127.0.0.1:%d 12001-16383", nodes[2].listenPort),
	)

	if err := proxy.injectNotification(notification); err != nil {
		t.Fatalf("Failed to inject complex SMIGRATED: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify reload happened
	finalReloads := reloadCount.Load()
	if finalReloads <= initialReloads {
		t.Errorf("Expected cluster state reload, reloads: initial=%d, final=%d",
			initialReloads, finalReloads)
	}

	// Verify operations work
	for i := 0; i < 10; i++ {
		if err := client.Set(ctx, fmt.Sprintf("complex-key%d", i), "value", 0).Err(); err != nil {
			t.Errorf("Operations failed after complex migration: %v", err)
		}
	}

	t.Log("Complex migration test passed")
}
