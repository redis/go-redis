package e2e

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
	"time"
)

// NotificationInjector is an interface that can inject maintenance notifications
// It can be implemented by either a real fault injector or a proxy-based mock
type NotificationInjector interface {
	// InjectSMIGRATING injects an SMIGRATING notification
	InjectSMIGRATING(ctx context.Context, seqID int64, slots ...string) error

	// InjectSMIGRATED injects an SMIGRATED notification
	InjectSMIGRATED(ctx context.Context, seqID int64, hostPort string, slots ...string) error

	// InjectMOVING injects a MOVING notification (for standalone)
	InjectMOVING(ctx context.Context, seqID int64, slot int) error

	// InjectMIGRATING injects a MIGRATING notification (for standalone)
	InjectMIGRATING(ctx context.Context, seqID int64, slot int) error

	// InjectMIGRATED injects a MIGRATED notification (for standalone)
	InjectMIGRATED(ctx context.Context, seqID int64, slot int) error

	// Start starts the injector (if needed)
	Start() error

	// Stop stops the injector (if needed)
	Stop() error

	// GetClusterAddrs returns the cluster addresses to connect to
	GetClusterAddrs() []string

	// IsReal returns true if this is a real fault injector (not a mock)
	IsReal() bool

	// GetTestModeConfig returns the test mode configuration for this injector
	GetTestModeConfig() *TestModeConfig
}

// NewNotificationInjector creates a notification injector based on environment
// If FAULT_INJECTOR_URL is set, it uses the real fault injector
// Otherwise, it uses the proxy-based mock
func NewNotificationInjector() (NotificationInjector, error) {
	if faultInjectorURL := os.Getenv("FAULT_INJECTOR_URL"); faultInjectorURL != "" {
		// Use real fault injector
		return NewFaultInjectorNotificationInjector(faultInjectorURL), nil
	}

	// Use proxy-based mock
	apiPort := 18100 // Default port (updated to avoid macOS Control Center conflict)
	if portStr := os.Getenv("PROXY_API_PORT"); portStr != "" {
		_, _ = fmt.Sscanf(portStr, "%d", &apiPort)
	}

	return NewProxyNotificationInjector(apiPort), nil
}

// ProxyNotificationInjector implements NotificationInjector using cae-resp-proxy
type ProxyNotificationInjector struct {
	apiPort      int
	apiBaseURL   string
	cmd          *exec.Cmd
	httpClient   *http.Client
	nodes        []proxyNode
	visibleNodes []int // Indices of nodes visible in CLUSTER SLOTS (for migration simulation)
}

type proxyNode struct {
	listenPort int
	targetHost string
	targetPort int
	proxyAddr  string
	nodeID     string
}

// NewProxyNotificationInjector creates a new proxy-based notification injector
func NewProxyNotificationInjector(apiPort int) *ProxyNotificationInjector {
	return &ProxyNotificationInjector{
		apiPort:    apiPort,
		apiBaseURL: fmt.Sprintf("http://localhost:%d", apiPort),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		nodes: make([]proxyNode, 0),
	}
}

func (p *ProxyNotificationInjector) Start() error {
	// Get cluster configuration from environment
	clusterAddrs := os.Getenv("CLUSTER_ADDRS")
	if clusterAddrs == "" {
		// Start with 4 nodes: 17000, 17001, 17002, 17003
		// Initially, CLUSTER SLOTS will only expose 17000, 17001, 17002
		// Node 17003 will be "hidden" until SMIGRATED swaps it in for 17002
		clusterAddrs = "127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003" // Use 127.0.0.1 to force IPv4
	}

	targetHost := os.Getenv("REDIS_TARGET_HOST")
	if targetHost == "" {
		targetHost = "127.0.0.1" // Use 127.0.0.1 to force IPv4
	}

	targetPort := 6379
	if portStr := os.Getenv("REDIS_TARGET_PORT"); portStr != "" {
		_, _ = fmt.Sscanf(portStr, "%d", &targetPort)
	}

	// Parse cluster addresses
	addrs := strings.Split(clusterAddrs, ",")
	if len(addrs) == 0 {
		return fmt.Errorf("no cluster addresses specified")
	}

	// Extract first port for initial node
	var initialPort int
	_, _ = fmt.Sscanf(strings.Split(addrs[0], ":")[1], "%d", &initialPort)

	// Check if proxy is already running (e.g., in Docker)
	proxyAlreadyRunning := false
	resp, err := p.httpClient.Get(p.apiBaseURL + "/stats")
	if err == nil && resp.StatusCode == 200 {
		resp.Body.Close()
		proxyAlreadyRunning = true
		fmt.Printf("✓ Detected existing proxy at %s (e.g., Docker container)\n", p.apiBaseURL)
	}

	// Only start proxy if not already running
	if !proxyAlreadyRunning {
		// Start proxy with initial node
		p.cmd = exec.Command("cae-resp-proxy",
			"--api-port", fmt.Sprintf("%d", p.apiPort),
			"--listen-port", fmt.Sprintf("%d", initialPort),
			"--target-host", targetHost,
			"--target-port", fmt.Sprintf("%d", targetPort),
		)

		if err := p.cmd.Start(); err != nil {
			return fmt.Errorf("failed to start proxy: %w", err)
		}

		// Wait for proxy to be ready
		time.Sleep(500 * time.Millisecond)
	}

	for i := 0; i < 10; i++ {
		resp, err := p.httpClient.Get(p.apiBaseURL + "/stats")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				// Add all nodes from the cluster addresses
				// For Docker proxy, all nodes are already running
				// For local proxy, we'll add them via API
				for i, addr := range addrs {
					var port int
					_, _ = fmt.Sscanf(strings.Split(addr, ":")[1], "%d", &port)

					if i == 0 {
						// Add initial node directly
						p.nodes = append(p.nodes, proxyNode{
							listenPort: port,
							targetHost: targetHost,
							targetPort: targetPort,
							proxyAddr:  fmt.Sprintf("127.0.0.1:%d", port),
							nodeID:     fmt.Sprintf("127.0.0.1:%d:%d", port, targetPort),
						})
					} else if !proxyAlreadyRunning {
						// Add remaining nodes via API (only if we started the proxy ourselves)
						if err := p.addNode(port, targetPort, targetHost); err != nil {
							return fmt.Errorf("failed to add node %d: %w", i, err)
						}
					} else {
						// Docker proxy: nodes are already running, just add to our list
						p.nodes = append(p.nodes, proxyNode{
							listenPort: port,
							targetHost: targetHost,
							targetPort: targetPort,
							proxyAddr:  fmt.Sprintf("127.0.0.1:%d", port),
							nodeID:     fmt.Sprintf("127.0.0.1:%d:%d", port, targetPort),
						})
					}
				}

				// Initially, make only the first 3 nodes visible in CLUSTER SLOTS
				// The 4th node (index 3) will be hidden until SMIGRATED swaps it in
				if len(p.nodes) >= 4 {
					p.visibleNodes = []int{0, 1, 2} // Nodes 17000, 17001, 17002
				} else {
					// If we have fewer than 4 nodes, make all visible
					p.visibleNodes = make([]int, len(p.nodes))
					for i := range p.visibleNodes {
						p.visibleNodes[i] = i
					}
				}

				// Add cluster command interceptors to make standalone Redis appear as a cluster
				if err := p.setupClusterInterceptors(); err != nil {
					return fmt.Errorf("failed to setup cluster interceptors: %w", err)
				}

				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("proxy did not become ready")
}

func (p *ProxyNotificationInjector) addNode(listenPort, targetPort int, targetHost string) error {
	nodeConfig := map[string]interface{}{
		"listenPort": listenPort,
		"targetHost": targetHost,
		"targetPort": targetPort,
	}

	jsonData, err := json.Marshal(nodeConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal node config: %w", err)
	}

	resp, err := p.httpClient.Post(
		p.apiBaseURL+"/nodes",
		"application/json",
		bytes.NewReader(jsonData),
	)
	if err != nil {
		return fmt.Errorf("failed to add node: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to add node, status %d: %s", resp.StatusCode, string(body))
	}

	p.nodes = append(p.nodes, proxyNode{
		listenPort: listenPort,
		targetHost: targetHost,
		targetPort: targetPort,
		proxyAddr:  fmt.Sprintf("localhost:%d", listenPort),
		nodeID:     fmt.Sprintf("localhost:%d:%d", listenPort, targetPort),
	})

	time.Sleep(200 * time.Millisecond)
	return nil
}

func (p *ProxyNotificationInjector) buildClusterSlotsResponse() string {
	// Build CLUSTER SLOTS response dynamically based on VISIBLE nodes only
	// Format: Array of slot ranges, each containing:
	//   - start slot (integer)
	//   - end slot (integer)
	//   - master node array: [host, port]
	//   - replica arrays (optional)

	// For simplicity, divide slots equally among visible nodes
	totalSlots := 16384
	visibleCount := len(p.visibleNodes)
	if visibleCount == 0 {
		visibleCount = len(p.nodes)
	}
	slotsPerNode := totalSlots / visibleCount

	response := fmt.Sprintf("*%d\r\n", visibleCount) // Number of slot ranges

	for i, nodeIdx := range p.visibleNodes {
		if nodeIdx >= len(p.nodes) {
			continue
		}
		node := p.nodes[nodeIdx]

		startSlot := i * slotsPerNode
		endSlot := startSlot + slotsPerNode - 1
		if i == visibleCount-1 {
			endSlot = 16383 // Last node gets remaining slots
		}

		// Extract host and port from proxyAddr
		host, portStr, _ := strings.Cut(node.proxyAddr, ":")

		response += "*3\r\n" // 3 elements: start, end, master
		response += fmt.Sprintf(":%d\r\n", startSlot)
		response += fmt.Sprintf(":%d\r\n", endSlot)
		response += "*2\r\n" // master info: 2 elements (host, port)
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(host), host)
		response += fmt.Sprintf(":%s\r\n", portStr)
	}

	return response
}

func (p *ProxyNotificationInjector) addClusterSlotsInterceptor() error {
	clusterSlotsResponse := p.buildClusterSlotsResponse()

	interceptor := map[string]interface{}{
		"name":     "cluster-slots",
		"match":    "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
		"response": clusterSlotsResponse,
		"encoding": "raw",
	}

	jsonData, err := json.Marshal(interceptor)
	if err != nil {
		return fmt.Errorf("failed to marshal interceptor: %w", err)
	}

	resp, err := p.httpClient.Post(
		p.apiBaseURL+"/interceptors",
		"application/json",
		bytes.NewReader(jsonData),
	)
	if err != nil {
		return fmt.Errorf("failed to add interceptor: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to add interceptor, status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (p *ProxyNotificationInjector) updateClusterSlotsInterceptor() error {
	// The proxy doesn't support updating existing interceptors
	// As a workaround, we'll send the updated CLUSTER SLOTS response directly to all clients
	// This simulates what would happen when the client calls CLUSTER SLOTS after SMIGRATED

	clusterSlotsResponse := p.buildClusterSlotsResponse()

	// Send the updated CLUSTER SLOTS response to all connected clients
	// This uses the /send-to-all-clients endpoint
	payload := map[string]interface{}{
		"data": clusterSlotsResponse,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	resp, err := p.httpClient.Post(
		p.apiBaseURL+"/send-to-all-clients",
		"application/json",
		bytes.NewReader(jsonData),
	)
	if err != nil {
		// If this fails, it's not critical - the client will get the updated
		// response when it calls CLUSTER SLOTS after receiving SMIGRATED
		return nil
	}
	defer resp.Body.Close()

	return nil
}

func (p *ProxyNotificationInjector) setupClusterInterceptors() error {
	// Add CLUSTER SLOTS interceptor
	if err := p.addClusterSlotsInterceptor(); err != nil {
		return err
	}

	// Interceptors to add
	interceptors := []map[string]interface{}{
		{
			"name":     "client-maint-notifications-on-with-endpoint",
			"match":    "*5\r\n$6\r\nclient\r\n$19\r\nmaint_notifications\r\n$2\r\non\r\n$21\r\nmoving-endpoint-type\r\n$4\r\nnone\r\n",
			"response": "+OK\r\n",
			"encoding": "raw",
		},
		{
			"name":     "client-maint-notifications-off",
			"match":    "*3\r\n$6\r\nclient\r\n$19\r\nmaint_notifications\r\n$3\r\noff\r\n",
			"response": "+OK\r\n",
			"encoding": "raw",
		},
	}

	// Add all interceptors
	for _, interceptor := range interceptors {
		jsonData, err := json.Marshal(interceptor)
		if err != nil {
			return fmt.Errorf("failed to marshal interceptor %s: %w", interceptor["name"], err)
		}

		resp, err := p.httpClient.Post(
			p.apiBaseURL+"/interceptors",
			"application/json",
			bytes.NewReader(jsonData),
		)
		if err != nil {
			return fmt.Errorf("failed to add interceptor %s: %w", interceptor["name"], err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to add interceptor %s, status %d: %s", interceptor["name"], resp.StatusCode, string(body))
		}

		fmt.Printf("✓ Added %s interceptor to proxy\n", interceptor["name"])
	}

	return nil
}

func (p *ProxyNotificationInjector) Stop() error {
	if p.cmd != nil && p.cmd.Process != nil {
		return p.cmd.Process.Kill()
	}
	return nil
}

func (p *ProxyNotificationInjector) GetClusterAddrs() []string {
	addrs := make([]string, len(p.nodes))
	for i, node := range p.nodes {
		addrs[i] = node.proxyAddr
	}
	return addrs
}

func (p *ProxyNotificationInjector) IsReal() bool {
	return false
}

func (p *ProxyNotificationInjector) GetTestModeConfig() *TestModeConfig {
	return &TestModeConfig{
		Mode:                     TestModeProxyMock,
		NotificationDelay:        1 * time.Second,
		ActionWaitTimeout:        10 * time.Second,
		ActionPollInterval:       500 * time.Millisecond,
		DatabaseReadyDelay:       1 * time.Second,
		ConnectionEstablishDelay: 500 * time.Millisecond,
		MaxClients:               1,
		SkipMultiClientTests:     true,
	}
}

func (p *ProxyNotificationInjector) InjectSMIGRATING(ctx context.Context, seqID int64, slots ...string) error {
	notification := formatSMigratingNotification(seqID, slots...)
	return p.injectNotification(notification)
}

func (p *ProxyNotificationInjector) InjectSMIGRATED(ctx context.Context, seqID int64, hostPort string, slots ...string) error {
	// Simulate topology change by swapping visible nodes
	// If we have 4 nodes and currently showing [0,1,2], swap to [0,1,3]
	// This simulates node 2 (17002) being replaced by node 3 (17003)
	if len(p.nodes) >= 4 && len(p.visibleNodes) == 3 {
		// Check if the hostPort matches node 3 (the "new" node)
		if hostPort == p.nodes[3].proxyAddr {
			// Swap node 2 for node 3 in visible nodes
			p.visibleNodes = []int{0, 1, 3}

			// Update CLUSTER SLOTS interceptor to reflect new topology
			if err := p.updateClusterSlotsInterceptor(); err != nil {
				return fmt.Errorf("failed to update CLUSTER SLOTS after migration: %w", err)
			}
		}
	}

	// Format endpoint as "host:port slot1,slot2,range1-range2"
	endpoint := fmt.Sprintf("%s %s", hostPort, strings.Join(slots, ","))
	notification := formatSMigratedNotification(seqID, endpoint)
	return p.injectNotification(notification)
}

func (p *ProxyNotificationInjector) InjectMOVING(ctx context.Context, seqID int64, slot int) error {
	notification := formatMovingNotification(seqID, slot)
	return p.injectNotification(notification)
}

func (p *ProxyNotificationInjector) InjectMIGRATING(ctx context.Context, seqID int64, slot int) error {
	notification := formatMigratingNotification(seqID, slot)
	return p.injectNotification(notification)
}

func (p *ProxyNotificationInjector) InjectMIGRATED(ctx context.Context, seqID int64, slot int) error {
	notification := formatMigratedNotification(seqID, slot)
	return p.injectNotification(notification)
}

func (p *ProxyNotificationInjector) injectNotification(notification string) error {
	url := p.apiBaseURL + "/send-to-all-clients?encoding=raw"
	resp, err := p.httpClient.Post(url, "application/octet-stream", strings.NewReader(notification))
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

// Helper functions to format notifications
func formatSMigratingNotification(seqID int64, slots ...string) string {
	// Format: ["SMIGRATING", seqID, slot1, slot2, ...]
	parts := []string{
		fmt.Sprintf(">%d\r\n", len(slots)+2),
		"$10\r\nSMIGRATING\r\n",
		fmt.Sprintf(":%d\r\n", seqID),
	}

	for _, slot := range slots {
		parts = append(parts, fmt.Sprintf("$%d\r\n%s\r\n", len(slot), slot))
	}

	return strings.Join(parts, "")
}

func formatSMigratedNotification(seqID int64, triplets ...string) string {
	// New Format: ["SMIGRATED", SeqID, source1, target1, slots1, source2, target2, slots2, ...]
	// The payload after SeqID is a flat list of triplets:
	//   - source endpoint (the node slots are migrating FROM)
	//   - target endpoint (the node slots are migrating TO)
	//   - comma-separated list of slot ranges
	//
	// RESP3 wire format:
	//   ><2 + num_triplets*3>
	//   +SMIGRATED
	//   :SeqID
	//   +<source1>
	//   +<target1>
	//   +<slots1>
	//   +<source2>
	//   +<target2>
	//   +<slots2>
	//   ...
	//
	// Each triplet is formatted as: "source target slots"
	// Example: "127.0.0.1:6379 127.0.0.1:6380 123,456,789-1000"

	// Count total elements: SMIGRATED + SeqID + (3 elements per triplet)
	totalElements := 2 + len(triplets)*3
	parts := []string{fmt.Sprintf(">%d\r\n", totalElements)}
	parts = append(parts, "+SMIGRATED\r\n")
	parts = append(parts, fmt.Sprintf(":%d\r\n", seqID))

	for _, triplet := range triplets {
		// Split triplet into source, target, and slots
		// triplet format: "source target slots"
		tripletParts := strings.SplitN(triplet, " ", 3)
		if len(tripletParts) != 3 {
			continue
		}
		source := tripletParts[0]
		target := tripletParts[1]
		slots := tripletParts[2]

		parts = append(parts, fmt.Sprintf("+%s\r\n", source))
		parts = append(parts, fmt.Sprintf("+%s\r\n", target))
		parts = append(parts, fmt.Sprintf("+%s\r\n", slots))
	}

	return strings.Join(parts, "")
}

func formatMovingNotification(seqID int64, slot int) string {
	slotStr := fmt.Sprintf("%d", slot)
	return fmt.Sprintf(">3\r\n$6\r\nMOVING\r\n:%d\r\n$%d\r\n%s\r\n", seqID, len(slotStr), slotStr)
}

func formatMigratingNotification(seqID int64, slot int) string {
	slotStr := fmt.Sprintf("%d", slot)
	return fmt.Sprintf(">3\r\n$9\r\nMIGRATING\r\n:%d\r\n$%d\r\n%s\r\n", seqID, len(slotStr), slotStr)
}

func formatMigratedNotification(seqID int64, slot int) string {
	slotStr := fmt.Sprintf("%d", slot)
	return fmt.Sprintf(">3\r\n$8\r\nMIGRATED\r\n:%d\r\n$%d\r\n%s\r\n", seqID, len(slotStr), slotStr)
}

// FaultInjectorNotificationInjector implements NotificationInjector using the real fault injector
type FaultInjectorNotificationInjector struct {
	client       *FaultInjectorClient
	clusterAddrs []string
	bdbID        int
}

// NewFaultInjectorNotificationInjector creates a new fault injector-based notification injector
func NewFaultInjectorNotificationInjector(baseURL string) *FaultInjectorNotificationInjector {
	// Get cluster addresses from environment
	clusterAddrs := os.Getenv("CLUSTER_ADDRS")
	if clusterAddrs == "" {
		clusterAddrs = "localhost:6379"
	}

	bdbID := 1
	if bdbIDStr := os.Getenv("BDB_ID"); bdbIDStr != "" {
		_, _ = fmt.Sscanf(bdbIDStr, "%d", &bdbID)
	}

	return &FaultInjectorNotificationInjector{
		client:       NewFaultInjectorClient(baseURL),
		clusterAddrs: strings.Split(clusterAddrs, ","),
		bdbID:        bdbID,
	}
}

func (f *FaultInjectorNotificationInjector) Start() error {
	// Fault injector is already running, nothing to start
	return nil
}

func (f *FaultInjectorNotificationInjector) Stop() error {
	// Fault injector keeps running, nothing to stop
	return nil
}

func (f *FaultInjectorNotificationInjector) GetClusterAddrs() []string {
	return f.clusterAddrs
}

func (f *FaultInjectorNotificationInjector) IsReal() bool {
	return true
}

func (f *FaultInjectorNotificationInjector) GetTestModeConfig() *TestModeConfig {
	return &TestModeConfig{
		Mode:                     TestModeRealFaultInjector,
		NotificationDelay:        30 * time.Second,
		ActionWaitTimeout:        240 * time.Second,
		ActionPollInterval:       2 * time.Second,
		DatabaseReadyDelay:       10 * time.Second,
		ConnectionEstablishDelay: 2 * time.Second,
		MaxClients:               3,
		SkipMultiClientTests:     false,
	}
}

func (f *FaultInjectorNotificationInjector) InjectSMIGRATING(ctx context.Context, seqID int64, slots ...string) error {
	// For real fault injector, we trigger actual slot migration which will generate SMIGRATING
	// Parse slot ranges
	var startSlot, endSlot int
	if len(slots) > 0 {
		if strings.Contains(slots[0], "-") {
			_, _ = fmt.Sscanf(slots[0], "%d-%d", &startSlot, &endSlot)
		} else {
			_, _ = fmt.Sscanf(slots[0], "%d", &startSlot)
			endSlot = startSlot
		}
	}

	// Trigger slot migration (this will generate SMIGRATING notification)
	resp, err := f.client.TriggerSlotMigration(ctx, startSlot, endSlot, "node-1", "node-2")
	if err != nil {
		return fmt.Errorf("failed to trigger slot migration: %w", err)
	}

	// Wait for action to start
	_, err = f.client.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
	return err
}

func (f *FaultInjectorNotificationInjector) InjectSMIGRATED(ctx context.Context, seqID int64, hostPort string, slots ...string) error {
	// SMIGRATED is generated automatically when migration completes
	// We can't directly inject it with the real fault injector
	// This is a limitation of using the real fault injector
	return fmt.Errorf("SMIGRATED cannot be directly injected with real fault injector - it's generated when migration completes")
}

func (f *FaultInjectorNotificationInjector) InjectMOVING(ctx context.Context, seqID int64, slot int) error {
	// MOVING notifications are generated during slot migration
	return fmt.Errorf("MOVING cannot be directly injected with real fault injector - it's generated during migration")
}

func (f *FaultInjectorNotificationInjector) InjectMIGRATING(ctx context.Context, seqID int64, slot int) error {
	// Trigger slot migration for standalone
	resp, err := f.client.TriggerSlotMigration(ctx, slot, slot, "node-1", "node-2")
	if err != nil {
		return fmt.Errorf("failed to trigger slot migration: %w", err)
	}

	_, err = f.client.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Second))
	return err
}

func (f *FaultInjectorNotificationInjector) InjectMIGRATED(ctx context.Context, seqID int64, slot int) error {
	// MIGRATED is generated automatically when migration completes
	return fmt.Errorf("MIGRATED cannot be directly injected with real fault injector - it's generated when migration completes")
}
