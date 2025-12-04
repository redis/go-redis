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
	"sync"
	"sync/atomic"
	"time"
)

// ProxyFaultInjectorServer mimics the fault injector server using cae-resp-proxy
// This allows existing e2e tests to work unchanged - they just point to this server
// instead of the real fault injector
type ProxyFaultInjectorServer struct {
	// HTTP server for fault injector API
	httpServer *http.Server
	listenAddr string

	// Proxy management
	proxyCmd      *exec.Cmd
	proxyAPIPort  int
	proxyAPIURL   string
	proxyHTTP     *http.Client

	// Cluster node tracking
	nodes         []proxyNodeInfo
	nodesMutex    sync.RWMutex

	// Action tracking
	actions       map[string]*actionState
	actionsMutex  sync.RWMutex
	actionCounter atomic.Int64
	seqIDCounter  atomic.Int64 // Counter for generating sequence IDs (starts at 1001)

	// Track if this instance started the server
	startedServer bool

	// Track active notifications for new connections
	activeNotifications     map[string]string // map[notificationType]notification (RESP format)
	activeNotificationsMutex sync.RWMutex
	knownConnections        map[string]bool // map[connectionID]bool
	knownConnectionsMutex   sync.RWMutex
	monitoringActive        atomic.Bool
}

type proxyNodeInfo struct {
	listenPort int
	targetHost string
	targetPort int
	proxyAddr  string
	nodeID     string
}

type actionState struct {
	ID         string
	Type       ActionType
	Status     ActionStatus
	Parameters map[string]interface{}
	StartTime  time.Time
	EndTime    time.Time
	Error      error
	Output     map[string]interface{}
}

// NewProxyFaultInjectorServer creates a new proxy-based fault injector server
func NewProxyFaultInjectorServer(listenAddr string, proxyAPIPort int) *ProxyFaultInjectorServer {
	s := &ProxyFaultInjectorServer{
		listenAddr:   listenAddr,
		proxyAPIPort: proxyAPIPort,
		proxyAPIURL:  fmt.Sprintf("http://localhost:%d", proxyAPIPort),
		proxyHTTP: &http.Client{
			Timeout: 5 * time.Second,
		},
		nodes:               make([]proxyNodeInfo, 0),
		actions:             make(map[string]*actionState),
		activeNotifications: make(map[string]string),
		knownConnections:    make(map[string]bool),
	}
	s.seqIDCounter.Store(1000) // Start at 1001 (will be incremented before use)
	return s
}

// NewProxyFaultInjectorServerWithURL creates a new proxy-based fault injector server with a custom proxy API URL
func NewProxyFaultInjectorServerWithURL(listenAddr string, proxyAPIURL string) *ProxyFaultInjectorServer {
	s := &ProxyFaultInjectorServer{
		listenAddr:  listenAddr,
		proxyAPIURL: proxyAPIURL,
		proxyHTTP: &http.Client{
			Timeout: 5 * time.Second,
		},
		nodes:               make([]proxyNodeInfo, 0),
		actions:             make(map[string]*actionState),
		activeNotifications: make(map[string]string),
		knownConnections:    make(map[string]bool),
	}
	s.seqIDCounter.Store(1000) // Start at 1001 (will be incremented before use)
	return s
}

// Start starts both the proxy and the fault injector API server
func (s *ProxyFaultInjectorServer) Start() error {
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
		fmt.Sscanf(portStr, "%d", &targetPort)
	}

	// Parse cluster addresses
	addrs := strings.Split(clusterAddrs, ",")
	if len(addrs) == 0 {
		return fmt.Errorf("no cluster addresses specified")
	}

	// Extract first port for initial node
	var initialPort int
	fmt.Sscanf(strings.Split(addrs[0], ":")[1], "%d", &initialPort)

	// Check if proxy is already running (e.g., in Docker)
	proxyAlreadyRunning := false
	resp, err := s.proxyHTTP.Get(s.proxyAPIURL + "/stats")
	if err == nil && resp.StatusCode == 200 {
		resp.Body.Close()
		proxyAlreadyRunning = true
		fmt.Printf("✓ Detected existing proxy at %s (e.g., Docker container)\n", s.proxyAPIURL)
	}

	// Only start proxy if not already running
	if !proxyAlreadyRunning {
		// Start cae-resp-proxy with initial node
		s.proxyCmd = exec.Command("cae-resp-proxy",
			"--api-port", fmt.Sprintf("%d", s.proxyAPIPort),
			"--listen-port", fmt.Sprintf("%d", initialPort),
			"--target-host", targetHost,
			"--target-port", fmt.Sprintf("%d", targetPort),
		)

		if err := s.proxyCmd.Start(); err != nil {
			return fmt.Errorf("failed to start proxy: %w", err)
		}

		// Wait for proxy to be ready
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for proxy to be ready and configure nodes
	for i := 0; i < 10; i++ {
		resp, err := s.proxyHTTP.Get(s.proxyAPIURL + "/stats")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				// Add initial node
				s.nodesMutex.Lock()
				s.nodes = append(s.nodes, proxyNodeInfo{
					listenPort: initialPort,
					targetHost: targetHost,
					targetPort: targetPort,
					proxyAddr:  fmt.Sprintf("localhost:%d", initialPort),
					nodeID:     fmt.Sprintf("node-%d", initialPort),
				})
				s.nodesMutex.Unlock()

				// Add remaining nodes (only if we started the proxy ourselves)
				// If using Docker proxy, it's already configured
				if !proxyAlreadyRunning {
					for i := 1; i < len(addrs); i++ {
						var port int
						fmt.Sscanf(strings.Split(addrs[i], ":")[1], "%d", &port)
						if err := s.addProxyNode(port, targetPort, targetHost); err != nil {
							return fmt.Errorf("failed to add node %d: %w", i, err)
						}
					}
				}

				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Check if HTTP server is already running on this address
	testClient := &http.Client{Timeout: 1 * time.Second}
	testResp, err := testClient.Get("http://" + s.listenAddr + "/actions")
	if err == nil {
		testResp.Body.Close()
		if testResp.StatusCode == http.StatusOK {
			fmt.Printf("✓ Fault injector server already running at %s\n", s.listenAddr)
			fmt.Printf("[ProxyFI] Proxy API at %s\n", s.proxyAPIURL)
			fmt.Printf("[ProxyFI] Cluster nodes: %d\n", len(s.nodes))
			s.startedServer = false // This instance didn't start the server
			return nil
		}
	}

	// Start HTTP server for fault injector API
	mux := http.NewServeMux()

	// Add logging middleware
	loggingMux := http.NewServeMux()
	loggingMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[ProxyFI HTTP] %s %s from %s\n", r.Method, r.URL.Path, r.RemoteAddr)
		mux.ServeHTTP(w, r)
	})

	mux.HandleFunc("/actions", s.handleListActions)
	mux.HandleFunc("/action", s.handleTriggerAction)
	mux.HandleFunc("/action/", s.handleActionStatus)

	s.httpServer = &http.Server{
		Addr:    s.listenAddr,
		Handler: loggingMux,
	}

	go func() {
		fmt.Printf("[ProxyFI] HTTP server starting on %s\n", s.listenAddr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[ProxyFI] HTTP server error: %v\n", err)
		}
	}()

	// Wait for HTTP server to be ready
	time.Sleep(200 * time.Millisecond)

	// Start monitoring for new connections
	s.startConnectionMonitoring()

	fmt.Printf("[ProxyFI] Started fault injector server at %s\n", s.listenAddr)
	fmt.Printf("[ProxyFI] Proxy API at %s\n", s.proxyAPIURL)
	fmt.Printf("[ProxyFI] Cluster nodes: %d\n", len(s.nodes))

	s.startedServer = true // This instance started the server
	return nil
}

// Stop stops both the proxy and the HTTP server
// Only stops the server if this instance started it
func (s *ProxyFaultInjectorServer) Stop() error {
	// Only stop if this instance started the server
	if !s.startedServer {
		return nil
	}

	// Stop connection monitoring
	s.stopConnectionMonitoring()

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(ctx)
	}

	if s.proxyCmd != nil && s.proxyCmd.Process != nil {
		return s.proxyCmd.Process.Kill()
	}

	return nil
}

func (s *ProxyFaultInjectorServer) addProxyNode(listenPort, targetPort int, targetHost string) error {
	nodeConfig := map[string]interface{}{
		"listenPort": listenPort,
		"targetHost": targetHost,
		"targetPort": targetPort,
	}

	jsonData, err := json.Marshal(nodeConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal node config: %w", err)
	}

	resp, err := s.proxyHTTP.Post(
		s.proxyAPIURL+"/nodes",
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

	s.nodesMutex.Lock()
	s.nodes = append(s.nodes, proxyNodeInfo{
		listenPort: listenPort,
		targetHost: targetHost,
		targetPort: targetPort,
		proxyAddr:  fmt.Sprintf("localhost:%d", listenPort),
		nodeID:     fmt.Sprintf("node-%d", listenPort),
	})
	s.nodesMutex.Unlock()

	time.Sleep(200 * time.Millisecond)
	return nil
}

// HTTP Handlers - mimic fault injector API

func (s *ProxyFaultInjectorServer) handleListActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Return list of supported actions
	actions := []ActionType{
		ActionSlotMigration,
		ActionClusterReshard,
		ActionClusterMigrate,
		ActionFailover,
		ActionMigrate,
		ActionBind,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(actions)
}

func (s *ProxyFaultInjectorServer) handleTriggerAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	fmt.Printf("[ProxyFI] handleTriggerAction received: Type='%s', Parameters=%+v\n", req.Type, req.Parameters)

	// Create action
	actionID := fmt.Sprintf("action-%d", s.actionCounter.Add(1))
	action := &actionState{
		ID:         actionID,
		Type:       req.Type,
		Status:     StatusRunning,
		Parameters: req.Parameters,
		StartTime:  time.Now(),
		Output:     make(map[string]interface{}),
	}

	s.actionsMutex.Lock()
	s.actions[actionID] = action
	s.actionsMutex.Unlock()

	fmt.Printf("[ProxyFI] Starting executeAction goroutine for action %s\n", actionID)

	// Execute action asynchronously
	go s.executeAction(action)

	// Return response
	resp := ActionResponse{
		ActionID: actionID,
		Status:   string(StatusRunning),
		Message:  fmt.Sprintf("Action %s started", req.Type),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *ProxyFaultInjectorServer) handleActionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract action ID from path
	actionID := strings.TrimPrefix(r.URL.Path, "/action/")

	s.actionsMutex.RLock()
	action, exists := s.actions[actionID]
	s.actionsMutex.RUnlock()

	if !exists {
		http.Error(w, "Action not found", http.StatusNotFound)
		return
	}

	resp := ActionStatusResponse{
		ActionID:  action.ID,
		Status:    action.Status,
		StartTime: action.StartTime,
		EndTime:   action.EndTime,
		Output:    action.Output,
	}

	if action.Error != nil {
		resp.Error = action.Error.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// executeAction executes an action and injects appropriate notifications
func (s *ProxyFaultInjectorServer) executeAction(action *actionState) {
	defer func() {
		action.EndTime = time.Now()
		if action.Status == StatusRunning {
			action.Status = StatusSuccess
		}
	}()

	fmt.Printf("[ProxyFI] executeAction called with type: '%s' (ActionBind='%s')\n", action.Type, ActionBind)

	switch action.Type {
	case ActionSlotMigration:
		s.executeSlotMigration(action)
	case ActionClusterReshard:
		s.executeClusterReshard(action)
	case ActionClusterMigrate:
		s.executeClusterMigrate(action)
	case ActionFailover:
		s.executeFailover(action)
	case ActionMigrate:
		s.executeMigrate(action)
	case ActionBind:
		fmt.Printf("[ProxyFI] Matched ActionBind case\n")
		s.executeBind(action)
	default:
		fmt.Printf("[ProxyFI] No match, using default case\n")
		action.Status = StatusFailed
		action.Error = fmt.Errorf("unsupported action type: %s", action.Type)
	}
}

func (s *ProxyFaultInjectorServer) executeSlotMigration(action *actionState) {
	// Extract parameters
	startSlot, _ := action.Parameters["start_slot"].(float64)
	endSlot, _ := action.Parameters["end_slot"].(float64)
	sourceNode, _ := action.Parameters["source_node"].(string)
	targetNode, _ := action.Parameters["target_node"].(string)

	fmt.Printf("[ProxyFI] Executing slot migration: slots %d-%d from %s to %s\n",
		int(startSlot), int(endSlot), sourceNode, targetNode)

	// Generate sequence ID using counter (starts at 1001)
	seqID := s.generateSeqID()

	// Step 1: Inject SMIGRATING notification
	slotRange := fmt.Sprintf("%d-%d", int(startSlot), int(endSlot))
	notification := formatSMigratingNotification(seqID, slotRange)

	// Track this as an active notification for new connections
	s.setActiveNotification("SMIGRATING", notification)

	if err := s.injectNotification(notification); err != nil {
		s.clearActiveNotification("SMIGRATING")
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject SMIGRATING: %w", err)
		return
	}

	action.Output["smigrating_injected"] = true
	action.Output["seqID"] = seqID

	// Wait a bit to simulate migration in progress
	time.Sleep(500 * time.Millisecond)

	// Step 2: Inject SMIGRATED notification
	s.nodesMutex.RLock()
	targetAddr := "localhost:7001" // Default
	if len(s.nodes) > 1 {
		targetAddr = s.nodes[1].proxyAddr
	}
	s.nodesMutex.RUnlock()

	endpoint := fmt.Sprintf("%s %s", targetAddr, slotRange)
	migratedNotif := formatSMigratedNotification(seqID+1, endpoint)

	// Clear SMIGRATING from active notifications before sending SMIGRATED
	s.clearActiveNotification("SMIGRATING")

	if err := s.injectNotification(migratedNotif); err != nil {
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject SMIGRATED: %w", err)
		return
	}

	action.Output["smigrated_injected"] = true
	action.Output["target_endpoint"] = endpoint

	fmt.Printf("[ProxyFI] Slot migration completed: %s\n", slotRange)
}

func (s *ProxyFaultInjectorServer) executeClusterReshard(action *actionState) {
	// Similar to slot migration but for multiple slots
	slots, _ := action.Parameters["slots"].([]interface{})
	sourceNode, _ := action.Parameters["source_node"].(string)
	targetNode, _ := action.Parameters["target_node"].(string)

	fmt.Printf("[ProxyFI] Executing cluster reshard: %d slots from %s to %s\n",
		len(slots), sourceNode, targetNode)

	// Generate sequence ID using counter (starts at 1001)
	seqID := s.generateSeqID()

	// Convert slots to string ranges
	slotStrs := make([]string, len(slots))
	for i, slot := range slots {
		slotStrs[i] = fmt.Sprintf("%d", int(slot.(float64)))
	}

	// Inject SMIGRATING
	notification := formatSMigratingNotification(seqID, slotStrs...)
	s.setActiveNotification("SMIGRATING", notification)

	if err := s.injectNotification(notification); err != nil {
		s.clearActiveNotification("SMIGRATING")
		action.Status = StatusFailed
		action.Error = err
		return
	}

	time.Sleep(500 * time.Millisecond)

	// Inject SMIGRATED
	s.nodesMutex.RLock()
	targetAddr := "localhost:7001"
	if len(s.nodes) > 1 {
		targetAddr = s.nodes[1].proxyAddr
	}
	s.nodesMutex.RUnlock()

	endpoint := fmt.Sprintf("%s %s", targetAddr, strings.Join(slotStrs, ","))
	migratedNotif := formatSMigratedNotification(seqID+1, endpoint)

	// Clear SMIGRATING from active notifications before sending SMIGRATED
	s.clearActiveNotification("SMIGRATING")

	if err := s.injectNotification(migratedNotif); err != nil {
		action.Status = StatusFailed
		action.Error = err
		return
	}

	action.Output["slots_migrated"] = len(slots)
}

func (s *ProxyFaultInjectorServer) executeClusterMigrate(action *actionState) {
	// Similar to slot migration
	s.executeSlotMigration(action)
}

func (s *ProxyFaultInjectorServer) injectNotification(notification string) error {
	url := s.proxyAPIURL + "/send-to-all-clients?encoding=raw"

	fmt.Printf("[ProxyFI] Injecting notification to %s\n", url)
	fmt.Printf("[ProxyFI] Notification (first 100 chars): %s\n", notification[:min(100, len(notification))])

	resp, err := s.proxyHTTP.Post(url, "application/octet-stream", strings.NewReader(notification))
	if err != nil {
		fmt.Printf("[ProxyFI] Failed to inject notification: %v\n", err)
		return fmt.Errorf("failed to inject notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("[ProxyFI] Injection failed with status %d: %s\n", resp.StatusCode, string(body))
		return fmt.Errorf("injection failed with status %d: %s", resp.StatusCode, string(body))
	}

	fmt.Printf("[ProxyFI] Notification injected successfully\n")
	return nil
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *ProxyFaultInjectorServer) executeFailover(action *actionState) {
	fmt.Printf("[ProxyFI] Executing failover\n")

	// Generate sequence ID using counter (starts at 1001)
	seqID := s.generateSeqID()

	// Step 1: Inject FAILING_OVER notification
	// Format: ["FAILING_OVER", SeqID]
	failingOverNotif := fmt.Sprintf(">2\r\n$12\r\nFAILING_OVER\r\n:%d\r\n", seqID)

	s.setActiveNotification("FAILING_OVER", failingOverNotif)

	if err := s.injectNotification(failingOverNotif); err != nil {
		s.clearActiveNotification("FAILING_OVER")
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject FAILING_OVER: %w", err)
		return
	}

	action.Output["failing_over_injected"] = true
	action.Output["seqID"] = seqID

	// Wait to simulate failover in progress
	time.Sleep(1 * time.Second)

	// Step 2: Inject FAILED_OVER notification
	// Format: ["FAILED_OVER", SeqID]
	failedOverNotif := fmt.Sprintf(">2\r\n$11\r\nFAILED_OVER\r\n:%d\r\n", seqID+1)

	s.clearActiveNotification("FAILING_OVER")

	if err := s.injectNotification(failedOverNotif); err != nil {
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject FAILED_OVER: %w", err)
		return
	}

	action.Output["failed_over_injected"] = true

	fmt.Printf("[ProxyFI] Failover completed\n")
}

func (s *ProxyFaultInjectorServer) executeMigrate(action *actionState) {
	fmt.Printf("[ProxyFI] Executing migrate\n")

	// Generate sequence ID using counter (starts at 1001)
	seqID := s.generateSeqID()
	slot := 1000 // Default slot for migration

	// Step 1: Inject MIGRATING notification (no MOVING for migrate action)
	// Format: ["MIGRATING", seqID, slot]
	slotStr := fmt.Sprintf("%d", slot)
	migratingNotif := fmt.Sprintf(">3\r\n$9\r\nMIGRATING\r\n:%d\r\n$%d\r\n%s\r\n",
		seqID, len(slotStr), slotStr)

	s.setActiveNotification("MIGRATING", migratingNotif)

	if err := s.injectNotification(migratingNotif); err != nil {
		s.clearActiveNotification("MIGRATING")
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject MIGRATING: %w", err)
		return
	}

	action.Output["migrating_injected"] = true
	action.Output["seqID"] = seqID

	// Wait to simulate migration in progress
	time.Sleep(500 * time.Millisecond)

	// Step 2: Inject MIGRATED notification
	// Format: ["MIGRATED", seqID, slot]
	migratedNotif := fmt.Sprintf(">3\r\n$8\r\nMIGRATED\r\n:%d\r\n$%d\r\n%s\r\n",
		seqID+1, len(slotStr), slotStr)

	s.clearActiveNotification("MIGRATING")

	if err := s.injectNotification(migratedNotif); err != nil {
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject MIGRATED: %w", err)
		return
	}

	action.Output["migrated_injected"] = true

	fmt.Printf("[ProxyFI] Migrate completed\n")
}

func (s *ProxyFaultInjectorServer) executeBind(action *actionState) {
	fmt.Printf("[ProxyFI] Executing bind\n")

	// Generate sequence ID using counter (starts at 1001)
	seqID := s.generateSeqID()
	timeS := int64(5) // Time in seconds for handoff

	// Get endpoint type from parameters (if provided)
	endpointType, _ := action.Parameters["endpoint_type"].(string)
	fmt.Printf("[ProxyFI] Bind action - endpoint_type parameter: '%s'\n", endpointType)

	// Determine target endpoint based on endpoint type
	var targetEndpoint string
	s.nodesMutex.RLock()
	defaultAddr := "localhost:7000"
	if len(s.nodes) > 0 {
		defaultAddr = s.nodes[0].proxyAddr
	}
	s.nodesMutex.RUnlock()

	switch endpointType {
	case "external-ip":
		// Return IP address (use 127.0.0.1 for localhost)
		host := strings.Split(defaultAddr, ":")[0]
		port := strings.Split(defaultAddr, ":")[1]
		if host == "localhost" {
			host = "127.0.0.1"
		}
		targetEndpoint = fmt.Sprintf("%s:%s", host, port)
		fmt.Printf("[ProxyFI] Using external-ip format: %s\n", targetEndpoint)

	case "external-fqdn":
		// Return FQDN format (e.g., node-1.localhost:7000)
		// Extract host and port from defaultAddr
		parts := strings.Split(defaultAddr, ":")
		host := parts[0]
		port := parts[1]

		// Create FQDN by prepending "node-1." to the host
		// This ensures the domain suffix matches the endpointConfig.Host
		targetEndpoint = fmt.Sprintf("node-1.%s:%s", host, port)
		fmt.Printf("[ProxyFI] Using external-fqdn format: %s\n", targetEndpoint)

	case "none":
		// Return null for "none" endpoint type
		// For RESP3, null is represented as "_\r\n"
		// But in array context, we use bulk string "$-1\r\n"
		// Actually, for "none" we should send the special null value
		// Let's use the internal.RedisNull constant which is "-"
		targetEndpoint = "-"
		fmt.Printf("[ProxyFI] Using none format: null\n")

	case "":
		// Empty endpoint type - use default
		targetEndpoint = defaultAddr
		fmt.Printf("[ProxyFI] Empty endpoint_type, using default: %s\n", targetEndpoint)

	default:
		// Default to localhost address
		targetEndpoint = defaultAddr
		fmt.Printf("[ProxyFI] Unknown endpoint_type '%s', using default: %s\n", endpointType, targetEndpoint)
	}

	// Inject MOVING notification
	// Format: ["MOVING", seqID, timeS, endpoint]
	var movingNotif string
	if targetEndpoint == "-" {
		// Special case for null endpoint (EndpointTypeNone)
		// Use RESP3 null: "_\r\n" in array context
		movingNotif = fmt.Sprintf(">4\r\n$6\r\nMOVING\r\n:%d\r\n:%d\r\n_\r\n",
			seqID, timeS)
	} else {
		movingNotif = fmt.Sprintf(">4\r\n$6\r\nMOVING\r\n:%d\r\n:%d\r\n$%d\r\n%s\r\n",
			seqID, timeS, len(targetEndpoint), targetEndpoint)
	}

	s.setActiveNotification("MOVING", movingNotif)

	if err := s.injectNotification(movingNotif); err != nil {
		s.clearActiveNotification("MOVING")
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject MOVING: %w", err)
		return
	}

	action.Output["moving_injected"] = true
	action.Output["seqID"] = seqID
	action.Output["target_endpoint"] = targetEndpoint
	action.Output["endpoint_type"] = endpointType

	fmt.Printf("[ProxyFI] Bind completed - MOVING notification sent (endpoint_type=%s, endpoint=%s)\n", endpointType, targetEndpoint)
}

// GetClusterAddrs returns the cluster addresses for connecting
func (s *ProxyFaultInjectorServer) GetClusterAddrs() []string {
	s.nodesMutex.RLock()
	defer s.nodesMutex.RUnlock()

	addrs := make([]string, len(s.nodes))
	for i, node := range s.nodes {
		addrs[i] = node.proxyAddr
	}
	return addrs
}

// generateSeqID generates a new sequence ID starting from 1001
func (s *ProxyFaultInjectorServer) generateSeqID() int64 {
	return s.seqIDCounter.Add(1)
}

// setActiveNotification stores a notification that should be sent to new connections
func (s *ProxyFaultInjectorServer) setActiveNotification(notifType string, notification string) {
	s.activeNotificationsMutex.Lock()
	defer s.activeNotificationsMutex.Unlock()
	s.activeNotifications[notifType] = notification
	fmt.Printf("[ProxyFI] Set active notification: %s\n", notifType)
}

// clearActiveNotification removes an active notification
func (s *ProxyFaultInjectorServer) clearActiveNotification(notifType string) {
	s.activeNotificationsMutex.Lock()
	defer s.activeNotificationsMutex.Unlock()
	delete(s.activeNotifications, notifType)
	fmt.Printf("[ProxyFI] Cleared active notification: %s\n", notifType)
}

// getActiveNotifications returns a copy of all active notifications
func (s *ProxyFaultInjectorServer) getActiveNotifications() map[string]string {
	s.activeNotificationsMutex.RLock()
	defer s.activeNotificationsMutex.RUnlock()

	notifications := make(map[string]string, len(s.activeNotifications))
	for k, v := range s.activeNotifications {
		notifications[k] = v
	}
	return notifications
}

// startConnectionMonitoring starts monitoring for new connections
func (s *ProxyFaultInjectorServer) startConnectionMonitoring() {
	if s.monitoringActive.Swap(true) {
		return // Already monitoring
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond) // Poll every 100ms for faster detection
		defer ticker.Stop()

		for s.monitoringActive.Load() {
			<-ticker.C
			s.checkForNewConnections()
		}
	}()
	fmt.Printf("[ProxyFI] Started connection monitoring (polling every 100ms)\n")
}

// stopConnectionMonitoring stops monitoring for new connections
func (s *ProxyFaultInjectorServer) stopConnectionMonitoring() {
	s.monitoringActive.Store(false)
	fmt.Printf("[ProxyFI] Stopped connection monitoring\n")
}

// checkForNewConnections checks for new connections and sends active notifications
func (s *ProxyFaultInjectorServer) checkForNewConnections() {
	// Debug: Log that we're checking
	activeNotifs := s.getActiveNotifications()
	fmt.Printf("[ProxyFI] Connection monitoring: checking (active notifications: %d)...\n", len(activeNotifs))

	// Get current connections from proxy stats endpoint
	resp, err := s.proxyHTTP.Get(s.proxyAPIURL + "/stats")
	if err != nil {
		fmt.Printf("[ProxyFI] Connection monitoring: failed to get stats: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("[ProxyFI] Connection monitoring: stats returned status %d\n", resp.StatusCode)
		return
	}

	// Parse stats response - format is map[backend]stats
	// connections is an array of connection objects with "id" field
	var stats map[string]struct {
		Connections []struct {
			ID string `json:"id"`
		} `json:"connections"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		fmt.Printf("[ProxyFI] Connection monitoring: failed to decode stats: %v\n", err)
		return
	}

	// Collect all connection IDs from all backends
	allConnIDs := make([]string, 0)
	for backend, backendStats := range stats {
		for _, conn := range backendStats.Connections {
			allConnIDs = append(allConnIDs, conn.ID)
		}
		fmt.Printf("[ProxyFI] Connection monitoring: backend %s has %d connections\n", backend, len(backendStats.Connections))
	}

	// Check for new connections
	s.knownConnectionsMutex.Lock()
	newConnections := make([]string, 0)
	for _, connID := range allConnIDs {
		if !s.knownConnections[connID] {
			s.knownConnections[connID] = true
			newConnections = append(newConnections, connID)
		}
	}
	totalKnown := len(s.knownConnections)
	s.knownConnectionsMutex.Unlock()

	fmt.Printf("[ProxyFI] Connection monitoring: total=%d, known=%d, new=%d\n", len(allConnIDs), totalKnown, len(newConnections))

	// Send active notifications to new connections
	if len(newConnections) > 0 {
		activeNotifs := s.getActiveNotifications()
		fmt.Printf("[ProxyFI] Found %d new connection(s), have %d active notification(s)\n",
			len(newConnections), len(activeNotifs))

		if len(activeNotifs) > 0 {
			for _, connID := range newConnections {
				for notifType, notification := range activeNotifs {
					if err := s.sendToConnection(connID, notification); err != nil {
						fmt.Printf("[ProxyFI] Failed to send %s to new connection %s: %v\n",
							notifType, connID, err)
					} else {
						fmt.Printf("[ProxyFI] Sent %s to new connection %s\n", notifType, connID)
					}
				}
			}
		}
	}
}

// sendToConnection sends a notification to a specific connection
func (s *ProxyFaultInjectorServer) sendToConnection(connID string, notification string) error {
	url := fmt.Sprintf("%s/send-to-client/%s?encoding=raw", s.proxyAPIURL, connID)

	resp, err := s.proxyHTTP.Post(url, "application/octet-stream", strings.NewReader(notification))
	if err != nil {
		return fmt.Errorf("failed to send to connection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send to connection, status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
