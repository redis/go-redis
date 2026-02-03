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
	"strconv"
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
	proxyCmd     *exec.Cmd
	proxyAPIPort int
	proxyAPIURL  string
	proxyHTTP    *http.Client

	// Cluster node tracking
	nodes      []proxyNodeInfo
	nodesMutex sync.RWMutex

	// Action tracking
	actions       map[string]*actionState
	actionsMutex  sync.RWMutex
	actionCounter atomic.Int64
	seqIDCounter  atomic.Int64 // Counter for generating sequence IDs (starts at 1001)

	// Track if this instance started the server
	startedServer bool

	// Track active notifications for new connections
	activeNotifications      map[string]string // map[notificationType]notification (RESP format)
	activeNotificationsMutex sync.RWMutex
	knownConnections         map[string]bool // map[connectionID]bool
	knownConnectionsMutex    sync.RWMutex
	monitoringActive         atomic.Bool
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
		clusterAddrs = "127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002" // Use 127.0.0.1 to force IPv4
	}

	targetHost := os.Getenv("REDIS_TARGET_HOST")
	if targetHost == "" {
		targetHost = "localhost"
	}

	targetPort := 6379
	if portStr := os.Getenv("REDIS_TARGET_PORT"); portStr != "" {
		if _, err := fmt.Sscanf(portStr, "%d", &targetPort); err != nil {
			return fmt.Errorf("invalid REDIS_TARGET_PORT: %w", err)
		}
	}

	// Parse cluster addresses
	addrs := strings.Split(clusterAddrs, ",")
	if len(addrs) == 0 {
		return fmt.Errorf("no cluster addresses specified")
	}

	// Extract first port for initial node
	var initialPort int
	if _, err := fmt.Sscanf(strings.Split(addrs[0], ":")[1], "%d", &initialPort); err != nil {
		return fmt.Errorf("invalid port in cluster address %s: %w", addrs[0], err)
	}

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
						if _, err := fmt.Sscanf(strings.Split(addrs[i], ":")[1], "%d", &port); err != nil {
							return fmt.Errorf("invalid port in cluster address %s: %w", addrs[i], err)
						}
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

	// IMPORTANT: The real fault injector uses /action (singular) for both listing and triggering
	// - GET /action -> list all actions
	// - POST /action -> trigger a new action
	// - GET /action/{action_id} -> get status of a specific action
	mux.HandleFunc("/action", s.handleAction)
	mux.HandleFunc("/action/", s.handleActionStatus)
	mux.HandleFunc("/slot-migrate", s.handleSlotMigrate)

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
		_ = s.httpServer.Shutdown(ctx)
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

// handleAction handles both GET (list actions) and POST (trigger action) requests to /action
// This matches the real fault injector API which uses /action (singular) for both operations
func (s *ProxyFaultInjectorServer) handleAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleListActions(w, r)
	case http.MethodPost:
		s.handleTriggerAction(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *ProxyFaultInjectorServer) handleListActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Return list of actions in the same format as the real fault injector API
	// The real API returns an array of objects with job_id, action_type, status, and submitted_at
	actions := []ActionListItem{
		{
			JobID:       "mock-job-1",
			ActionType:  string(ActionSlotMigration),
			Status:      "completed",
			SubmittedAt: "2026-01-26T00:00:00+00:00",
		},
		{
			JobID:       "mock-job-2",
			ActionType:  string(ActionClusterMigrate),
			Status:      "completed",
			SubmittedAt: "2026-01-26T00:00:00+00:00",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(actions)
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
	_ = json.NewEncoder(w).Encode(resp)
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
	_ = json.NewEncoder(w).Encode(resp)
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
	case ActionCreateDatabase:
		fmt.Printf("[ProxyFI] Executing CreateDatabase\n")
		s.executeCreateDatabase(action)
	case ActionDeleteDatabase:
		fmt.Printf("[ProxyFI] Executing DeleteDatabase\n")
		s.executeDeleteDatabase(action)
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
	// Get source and target addresses from nodes
	s.nodesMutex.RLock()
	sourceAddr := "localhost:7000" // Default source
	targetAddr := "localhost:7001" // Default target
	if len(s.nodes) > 0 {
		sourceAddr = s.nodes[0].proxyAddr
	}
	if len(s.nodes) > 1 {
		targetAddr = s.nodes[1].proxyAddr
	}
	s.nodesMutex.RUnlock()

	// Format as triplet: "source target slots"
	triplet := fmt.Sprintf("%s %s %s", sourceAddr, targetAddr, slotRange)
	migratedNotif := formatSMigratedNotification(seqID+1, triplet)

	// Clear SMIGRATING from active notifications before sending SMIGRATED
	s.clearActiveNotification("SMIGRATING")

	if err := s.injectNotification(migratedNotif); err != nil {
		action.Status = StatusFailed
		action.Error = fmt.Errorf("failed to inject SMIGRATED: %w", err)
		return
	}

	action.Output["smigrated_injected"] = true
	action.Output["source_endpoint"] = sourceAddr
	action.Output["target_endpoint"] = targetAddr

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

	// Inject SMIGRATED with triplet format: "source target slots"
	s.nodesMutex.RLock()
	sourceAddr := "localhost:7000" // Default source
	targetAddr := "localhost:7001" // Default target
	if len(s.nodes) > 0 {
		sourceAddr = s.nodes[0].proxyAddr
	}
	if len(s.nodes) > 1 {
		targetAddr = s.nodes[1].proxyAddr
	}
	s.nodesMutex.RUnlock()

	// Format as triplet: "source target slots"
	triplet := fmt.Sprintf("%s %s %s", sourceAddr, targetAddr, strings.Join(slotStrs, ","))
	migratedNotif := formatSMigratedNotification(seqID+1, triplet)

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

// handleSlotMigrate handles the /slot-migrate endpoint
// GET: Returns available triggers for a slot migration effect
// POST: Triggers a slot migration action
func (s *ProxyFaultInjectorServer) handleSlotMigrate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleSlotMigrateGetTriggers(w, r)
	case http.MethodPost:
		s.handleSlotMigrateTrigger(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleSlotMigrateGetTriggers returns available triggers for a slot migration effect
func (s *ProxyFaultInjectorServer) handleSlotMigrateGetTriggers(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	effect := SlotMigrateEffect(query.Get("effect"))
	if effect == "" {
		http.Error(w, "Missing required parameter: effect", http.StatusBadRequest)
		return
	}

	// Parse cluster_index (optional, default: 0)
	clusterIndex := 0
	if clusterIndexStr := query.Get("cluster_index"); clusterIndexStr != "" {
		if _, err := fmt.Sscanf(clusterIndexStr, "%d", &clusterIndex); err != nil {
			http.Error(w, "Invalid cluster_index parameter", http.StatusBadRequest)
			return
		}
	}

	// Build cluster field with proper structure: {index, nodes: [{host, port}, ...]}
	s.nodesMutex.RLock()
	nodesArray := make([]map[string]interface{}, len(s.nodes))
	for i, node := range s.nodes {
		// Parse host and port from proxyAddr (format: "localhost:7001")
		parts := strings.Split(node.proxyAddr, ":")
		host := parts[0]
		port := node.listenPort
		nodesArray[i] = map[string]interface{}{
			"host": host,
			"port": port,
		}
	}
	s.nodesMutex.RUnlock()

	clusterInfo := map[string]interface{}{
		"index": clusterIndex,
		"nodes": nodesArray,
	}

	// Return mock triggers based on effect
	var triggers []SlotMigrateTrigger

	// Helper function to create requirements with dbconfig and cluster
	makeRequirements := func(dbconfig map[string]interface{}, description string) []SlotMigrateTriggerRequirement {
		return []SlotMigrateTriggerRequirement{
			{
				DBConfig:    dbconfig,
				Cluster:     clusterInfo,
				Description: description,
			},
		}
	}

	// Default dbconfig for migrate/maintenance_mode (sharded cluster)
	defaultDBConfig := map[string]interface{}{
		"shards_count":     3,
		"shards_placement": "sparse",
	}

	// Failover requirements (requires replication)
	failoverDBConfig := map[string]interface{}{
		"shards_count":     3,
		"shards_placement": "sparse",
		"replication":      true,
	}

	switch effect {
	case SlotMigrateEffectRemoveAdd:
		triggers = []SlotMigrateTrigger{
			{
				Name:         "migrate",
				Description:  "Migrate all shards from source node to empty node",
				Requirements: makeRequirements(defaultDBConfig, "Requires sharded cluster"),
			},
			{
				Name:         "maintenance_mode",
				Description:  "Put source node in maintenance mode",
				Requirements: makeRequirements(defaultDBConfig, "Requires sharded cluster"),
			},
			{
				Name:         "failover",
				Description:  "Trigger failover (requires replication)",
				Requirements: makeRequirements(failoverDBConfig, "Requires replication enabled for failover"),
			},
		}
	case SlotMigrateEffectRemove:
		triggers = []SlotMigrateTrigger{
			{
				Name:         "migrate",
				Description:  "Migrate all shards from source node to existing node",
				Requirements: makeRequirements(defaultDBConfig, "Requires sharded cluster"),
			},
			{
				Name:         "maintenance_mode",
				Description:  "Put source node in maintenance mode",
				Requirements: makeRequirements(defaultDBConfig, "Requires sharded cluster"),
			},
		}
	case SlotMigrateEffectAdd:
		triggers = []SlotMigrateTrigger{
			{
				Name:         "migrate",
				Description:  "Migrate one shard to empty node",
				Requirements: makeRequirements(defaultDBConfig, "Requires sharded cluster"),
			},
			{
				Name:         "failover",
				Description:  "Trigger failover (requires replication)",
				Requirements: makeRequirements(failoverDBConfig, "Requires replication enabled for failover"),
			},
		}
	case SlotMigrateEffectSlotShuffle:
		triggers = []SlotMigrateTrigger{
			{
				Name:         "migrate",
				Description:  "Migrate one shard between existing nodes",
				Requirements: makeRequirements(defaultDBConfig, "Requires sharded cluster"),
			},
			{
				Name:         "failover",
				Description:  "Trigger failover (requires replication)",
				Requirements: makeRequirements(failoverDBConfig, "Requires replication enabled for failover"),
			},
		}
	default:
		http.Error(w, fmt.Sprintf("Unknown effect: %s", effect), http.StatusBadRequest)
		return
	}

	response := SlotMigrateTriggersResponse{
		Effect:   effect,
		Cluster:  clusterInfo,
		Triggers: triggers,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// handleSlotMigrateTrigger triggers a slot migration action
func (s *ProxyFaultInjectorServer) handleSlotMigrateTrigger(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	effect := SlotMigrateEffect(query.Get("effect"))
	bdbID := query.Get("bdb_id")
	// IMPORTANT: The real fault injector API uses "variant" as the query parameter name (rest_api.py line 301)
	// We use "trigger" as the variable name internally, but must read from "variant" parameter
	trigger := SlotMigrateVariant(query.Get("variant"))
	sourceNode := query.Get("source_node")
	targetNode := query.Get("target_node")
	clusterIndexStr := query.Get("cluster_index")

	// Parse cluster_index with default of 0
	clusterIndex := 0
	if clusterIndexStr != "" {
		if idx, err := strconv.Atoi(clusterIndexStr); err == nil {
			clusterIndex = idx
		}
	}

	if effect == "" {
		http.Error(w, "Missing required parameter: effect", http.StatusBadRequest)
		return
	}
	if bdbID == "" {
		http.Error(w, "Missing required parameter: bdb_id", http.StatusBadRequest)
		return
	}

	// Default trigger is "migrate"
	if trigger == "" || trigger == SlotMigrateVariantDefault {
		trigger = SlotMigrateVariantMigrate
	}

	fmt.Printf("[ProxyFI] Slot-migrate: effect=%s, bdb_id=%s, trigger=%s, cluster_index=%d, source_node=%s, target_node=%s\n",
		effect, bdbID, trigger, clusterIndex, sourceNode, targetNode)

	// Create action
	actionID := fmt.Sprintf("slot-migrate-%d", s.actionCounter.Add(1))
	action := &actionState{
		ID:     actionID,
		Type:   ActionSlotMigrate,
		Status: StatusRunning,
		Parameters: map[string]interface{}{
			"effect":        string(effect),
			"bdb_id":        bdbID,
			"variant":       string(trigger),
			"cluster_index": clusterIndex,
			"source_node":   sourceNode,
			"target_node":   targetNode,
		},
		StartTime: time.Now(),
		Output:    make(map[string]interface{}),
	}

	s.actionsMutex.Lock()
	s.actions[actionID] = action
	s.actionsMutex.Unlock()

	// Execute action asynchronously
	go s.executeSlotMigrateAction(action, effect, trigger)

	// Return response per spec: only {"action_id": "..."}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"action_id": actionID})
}

// executeSlotMigrateAction executes a slot-migrate action with the specified effect and trigger
func (s *ProxyFaultInjectorServer) executeSlotMigrateAction(action *actionState, effect SlotMigrateEffect, trigger SlotMigrateVariant) {
	defer func() {
		action.EndTime = time.Now()
		if action.Status == StatusRunning {
			action.Status = StatusSuccess
		}
	}()

	fmt.Printf("[ProxyFI] Executing slot-migrate: effect=%s, trigger=%s\n", effect, trigger)

	switch effect {
	case SlotMigrateEffectRemoveAdd:
		s.executeSlotMigrateRemoveAdd(action, trigger)
	case SlotMigrateEffectRemove:
		s.executeSlotMigrateRemove(action, trigger)
	case SlotMigrateEffectAdd:
		s.executeSlotMigrateAdd(action, trigger)
	case SlotMigrateEffectSlotShuffle:
		s.executeSlotMigrateSlotShuffle(action, trigger)
	default:
		action.Status = StatusFailed
		action.Error = fmt.Errorf("unsupported slot-migrate effect: %s", effect)
	}
}

// executeSlotMigrateRemoveAdd simulates migrating all shards from one node to a new node
// Effect: One endpoint removed, one endpoint added
func (s *ProxyFaultInjectorServer) executeSlotMigrateRemoveAdd(action *actionState, trigger SlotMigrateVariant) {
	s.nodesMutex.RLock()
	if len(s.nodes) < 2 {
		s.nodesMutex.RUnlock()
		action.Status = StatusFailed
		action.Error = fmt.Errorf("need at least 2 nodes for remove-add effect")
		return
	}

	// Pick a source node (the one that will be "removed")
	sourceNode := s.nodes[0]
	sourceAddr := sourceNode.proxyAddr
	numNodes := len(s.nodes)
	s.nodesMutex.RUnlock()

	// Calculate simulated slot range for this node (16384 slots total, distributed evenly)
	slotsPerNode := 16384 / numNodes
	slotStart := 0 // First node starts at slot 0
	slotEnd := slotsPerNode - 1

	// Generate a new endpoint address (simulating a new node)
	newAddr := fmt.Sprintf("127.0.0.1:%d", 7000+numNodes+1)

	fmt.Printf("[ProxyFI] remove-add: source=%s (slots %d-%d) -> new=%s\n", sourceAddr, slotStart, slotEnd, newAddr)

	// Generate sequence ID for notifications
	seqID := s.generateSeqID()

	// Send SMIGRATING notification for all slots on source node
	smigratingMsg := formatSMigratingNotification(seqID, fmt.Sprintf("%d-%d", slotStart, slotEnd))
	if err := s.injectNotification(smigratingMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATING: %v\n", err)
	}

	// Brief delay to simulate migration in progress
	time.Sleep(100 * time.Millisecond)

	// Send SMIGRATED notification with the new endpoint
	smigratedMsg := formatSMigratedNotification(seqID+1, fmt.Sprintf("%s %d-%d", newAddr, slotStart, slotEnd))
	if err := s.injectNotification(smigratedMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATED: %v\n", err)
	}

	action.Output["source_addr"] = sourceAddr
	action.Output["new_addr"] = newAddr
	action.Output["slots"] = fmt.Sprintf("%d-%d", slotStart, slotEnd)
	action.Output["variant"] = string(trigger)
}

// executeSlotMigrateRemove simulates migrating all shards from one node to an existing node
// Effect: One endpoint removed (node count decreases by 1)
func (s *ProxyFaultInjectorServer) executeSlotMigrateRemove(action *actionState, trigger SlotMigrateVariant) {
	s.nodesMutex.Lock()
	if len(s.nodes) < 2 {
		s.nodesMutex.Unlock()
		action.Status = StatusFailed
		action.Error = fmt.Errorf("need at least 2 nodes for remove effect")
		return
	}

	// Pick source and destination nodes
	sourceNode := s.nodes[0]
	destNode := s.nodes[1]
	sourceAddr := sourceNode.proxyAddr
	destAddr := destNode.proxyAddr
	numNodes := len(s.nodes)

	// Remove the source node from the node list (topology change)
	s.nodes = s.nodes[1:]
	s.nodesMutex.Unlock()

	// Calculate simulated slot range for source node
	slotsPerNode := 16384 / numNodes
	slotStart := 0
	slotEnd := slotsPerNode - 1

	fmt.Printf("[ProxyFI] remove: source=%s (slots %d-%d) -> dest=%s (nodes: %d -> %d)\n", sourceAddr, slotStart, slotEnd, destAddr, numNodes, numNodes-1)

	// Generate sequence ID for notifications
	seqID := s.generateSeqID()

	// Send SMIGRATING notification
	smigratingMsg := formatSMigratingNotification(seqID, fmt.Sprintf("%d-%d", slotStart, slotEnd))
	if err := s.injectNotification(smigratingMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATING: %v\n", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Send SMIGRATED notification with existing endpoint
	smigratedMsg := formatSMigratedNotification(seqID+1, fmt.Sprintf("%s %d-%d", destAddr, slotStart, slotEnd))
	if err := s.injectNotification(smigratedMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATED: %v\n", err)
	}

	action.Output["source_addr"] = sourceAddr
	action.Output["dest_addr"] = destAddr
	action.Output["slots"] = fmt.Sprintf("%d-%d", slotStart, slotEnd)
	action.Output["variant"] = string(trigger)
	action.Output["nodes_before"] = numNodes
	action.Output["nodes_after"] = numNodes - 1
}

// executeSlotMigrateAdd simulates migrating one shard from an existing node to a new node
// Effect: One endpoint added (net +1 node)
func (s *ProxyFaultInjectorServer) executeSlotMigrateAdd(action *actionState, trigger SlotMigrateVariant) {
	s.nodesMutex.Lock()
	if len(s.nodes) < 1 {
		s.nodesMutex.Unlock()
		action.Status = StatusFailed
		action.Error = fmt.Errorf("need at least 1 node for add effect")
		return
	}

	// Pick a source node (an existing node that will give up some slots)
	sourceNode := s.nodes[0]
	sourceAddr := sourceNode.proxyAddr
	nodesBefore := len(s.nodes)

	// Calculate partial slot range (one shard worth - roughly 1/3 of the node's slots)
	slotsPerNode := 16384 / nodesBefore
	slotStart := 0
	slotRange := slotsPerNode / 3
	slotEnd := slotStart + slotRange

	// Generate a new endpoint address (simulating a new node)
	newAddr := fmt.Sprintf("127.0.0.1:%d", 7000+nodesBefore+1)

	// Add the new node to the cluster
	newNode := proxyNodeInfo{
		listenPort: 7000 + nodesBefore + 1,
		proxyAddr:  newAddr,
		nodeID:     fmt.Sprintf("node-%d", nodesBefore+1),
	}
	s.nodes = append(s.nodes, newNode)
	nodesAfter := len(s.nodes)
	s.nodesMutex.Unlock()

	fmt.Printf("[ProxyFI] add: source=%s (slots %d-%d) -> new=%s, nodes: %d -> %d\n",
		sourceAddr, slotStart, slotEnd, newAddr, nodesBefore, nodesAfter)

	// Generate sequence ID for notifications
	seqID := s.generateSeqID()

	// Send SMIGRATING notification for partial slot range
	smigratingMsg := formatSMigratingNotification(seqID, fmt.Sprintf("%d-%d", slotStart, slotEnd))
	if err := s.injectNotification(smigratingMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATING: %v\n", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Send SMIGRATED notification with the new endpoint
	smigratedMsg := formatSMigratedNotification(seqID+1, fmt.Sprintf("%s %d-%d", newAddr, slotStart, slotEnd))
	if err := s.injectNotification(smigratedMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATED: %v\n", err)
	}

	action.Output["source_addr"] = sourceAddr
	action.Output["new_addr"] = newAddr
	action.Output["slots"] = fmt.Sprintf("%d-%d", slotStart, slotEnd)
	action.Output["variant"] = string(trigger)
	action.Output["nodes_before"] = nodesBefore
	action.Output["nodes_after"] = nodesAfter
}

// executeSlotMigrateSlotShuffle simulates moving slots between existing nodes
// Effect: Slots move, no endpoint changes (same nodes, different slot distribution)
func (s *ProxyFaultInjectorServer) executeSlotMigrateSlotShuffle(action *actionState, trigger SlotMigrateVariant) {
	s.nodesMutex.RLock()
	if len(s.nodes) < 2 {
		s.nodesMutex.RUnlock()
		action.Status = StatusFailed
		action.Error = fmt.Errorf("need at least 2 nodes for slot-shuffle effect")
		return
	}

	// Pick source and destination nodes (both existing)
	sourceNode := s.nodes[0]
	sourceAddr := sourceNode.proxyAddr
	destNode := s.nodes[1]
	destAddr := destNode.proxyAddr
	numNodes := len(s.nodes)

	// Calculate partial slot range (one shard worth)
	slotsPerNode := 16384 / numNodes
	slotStart := 0
	slotRange := slotsPerNode / 3
	slotEnd := slotStart + slotRange
	s.nodesMutex.RUnlock()

	fmt.Printf("[ProxyFI] slot-shuffle: source=%s (slots %d-%d) -> dest=%s\n", sourceAddr, slotStart, slotEnd, destAddr)

	// Generate sequence ID for notifications
	seqID := s.generateSeqID()

	// Send SMIGRATING notification
	smigratingMsg := formatSMigratingNotification(seqID, fmt.Sprintf("%d-%d", slotStart, slotEnd))
	if err := s.injectNotification(smigratingMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATING: %v\n", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Send SMIGRATED notification with existing destination endpoint
	smigratedMsg := formatSMigratedNotification(seqID+1, fmt.Sprintf("%s %d-%d", destAddr, slotStart, slotEnd))
	if err := s.injectNotification(smigratedMsg); err != nil {
		fmt.Printf("[ProxyFI] Error sending SMIGRATED: %v\n", err)
	}

	action.Output["source_addr"] = sourceAddr
	action.Output["dest_addr"] = destAddr
	action.Output["slots"] = fmt.Sprintf("%d-%d", slotStart, slotEnd)
	action.Output["variant"] = string(trigger)
}

// executeDeleteDatabase clears all nodes to simulate database deletion
func (s *ProxyFaultInjectorServer) executeDeleteDatabase(action *actionState) {
	s.nodesMutex.Lock()
	s.nodes = make([]proxyNodeInfo, 0)
	s.nodesMutex.Unlock()

	fmt.Printf("[ProxyFI] DeleteDatabase: cleared all nodes\n")
	action.Status = StatusSuccess
}

// executeCreateDatabase resets nodes to initial state with 2 nodes
// so that all slot-migrate effects can work (remove, remove-add, slot-shuffle need >= 2 nodes)
func (s *ProxyFaultInjectorServer) executeCreateDatabase(action *actionState) {
	s.nodesMutex.Lock()
	// Get target host/port from the existing proxy or use defaults
	targetHost := "cae-resp-proxy"
	targetPort := 6379

	// Create 2 nodes so all effects can work
	s.nodes = []proxyNodeInfo{
		{listenPort: 7001, targetHost: targetHost, targetPort: targetPort, proxyAddr: "localhost:7001", nodeID: "node-7001"},
		{listenPort: 7002, targetHost: targetHost, targetPort: targetPort, proxyAddr: "localhost:7002", nodeID: "node-7002"},
	}
	s.nodesMutex.Unlock()

	fmt.Printf("[ProxyFI] CreateDatabase: initialized with 2 nodes\n")
	action.Status = StatusSuccess
}
