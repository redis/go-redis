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
	apiPort := 8100
	if portStr := os.Getenv("PROXY_API_PORT"); portStr != "" {
		_, _ = fmt.Sscanf(portStr, "%d", &apiPort)
	}

	return NewProxyNotificationInjector(apiPort), nil
}

// ProxyNotificationInjector implements NotificationInjector using cae-resp-proxy
type ProxyNotificationInjector struct {
	apiPort    int
	apiBaseURL string
	cmd        *exec.Cmd
	httpClient *http.Client
	nodes      []proxyNode
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
		clusterAddrs = "localhost:7000,localhost:7001,localhost:7002"
	}

	targetHost := os.Getenv("REDIS_TARGET_HOST")
	if targetHost == "" {
		targetHost = "localhost"
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
				// Add initial node
				p.nodes = append(p.nodes, proxyNode{
					listenPort: initialPort,
					targetHost: targetHost,
					targetPort: targetPort,
					proxyAddr:  fmt.Sprintf("localhost:%d", initialPort),
					nodeID:     fmt.Sprintf("localhost:%d:%d", initialPort, targetPort),
				})

				// Add remaining nodes (only if we started the proxy ourselves)
				// If using Docker proxy, it's already configured
				if !proxyAlreadyRunning {
					for i := 1; i < len(addrs); i++ {
						var port int
						_, _ = fmt.Sscanf(strings.Split(addrs[i], ":")[1], "%d", &port)
						if err := p.addNode(port, targetPort, targetHost); err != nil {
							return fmt.Errorf("failed to add node %d: %w", i, err)
						}
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

func (p *ProxyNotificationInjector) setupClusterInterceptors() error {
	// Create CLUSTER SLOTS response for a single-node cluster
	// Format: Array of slot ranges, each containing:
	//   - start slot (integer)
	//   - end slot (integer)
	//   - master node array: [host, port]
	//   - replica arrays (optional)

	// Build the CLUSTER SLOTS response
	clusterSlotsResponse := "*1\r\n" + // 1 slot range
		"*3\r\n" + // 3 elements: start, end, master
		":0\r\n" + // start slot
		":16383\r\n" + // end slot
		"*2\r\n" + // master info: 2 elements (host, port)
		"$9\r\nlocalhost\r\n" + // host
		":7000\r\n" // port

	// Interceptors to add
	interceptors := []map[string]interface{}{
		{
			"name":     "cluster-slots",
			"match":    "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n",
			"response": clusterSlotsResponse,
			"encoding": "raw",
		},
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

func formatSMigratedNotification(seqID int64, endpoints ...string) string {
	// New Format: ["SMIGRATED", SeqID, count, [endpoint1, endpoint2, ...]]
	// Each endpoint is formatted as: "host:port slot1,slot2,range1-range2"
	// Example: >4\r\n$9\r\nSMIGRATED\r\n:15\r\n:2\r\n*2\r\n$31\r\n127.0.0.1:6379 123,456,789-1000\r\n$30\r\n127.0.0.1:6380 124,457,300-500\r\n
	parts := []string{">4\r\n"}
	parts = append(parts, "$9\r\nSMIGRATED\r\n")
	parts = append(parts, fmt.Sprintf(":%d\r\n", seqID))

	count := len(endpoints)
	parts = append(parts, fmt.Sprintf(":%d\r\n", count))
	parts = append(parts, fmt.Sprintf("*%d\r\n", count))

	for _, endpoint := range endpoints {
		parts = append(parts, fmt.Sprintf("$%d\r\n%s\r\n", len(endpoint), endpoint))
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


