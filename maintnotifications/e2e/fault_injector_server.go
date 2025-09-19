package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// MockFaultInjectorServer provides a mock implementation for testing
type MockFaultInjectorServer struct {
	server   *http.Server
	actions  map[string]*ActionStatusResponse
	mutex    sync.RWMutex
	handlers map[ActionType]ActionHandler
}

// ActionHandler defines the interface for handling specific actions
type ActionHandler interface {
	Execute(ctx context.Context, parameters map[string]interface{}) error
	GetProgress() float64
	GetOutput() string
}

// NewMockFaultInjectorServer creates a new mock fault injector server
func NewMockFaultInjectorServer(addr string) *MockFaultInjectorServer {
	s := &MockFaultInjectorServer{
		actions:  make(map[string]*ActionStatusResponse),
		handlers: make(map[ActionType]ActionHandler),
	}

	// Register default handlers
	s.registerDefaultHandlers()

	router := http.NewServeMux()
	s.setupRoutes(router)

	s.server = &http.Server{
		Addr:    addr,
		Handler: router,
	}

	return s
}

// Start starts the mock server
func (s *MockFaultInjectorServer) Start() error {
	log.Printf("Starting mock fault injector server on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

// Stop stops the mock server
func (s *MockFaultInjectorServer) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// RegisterHandler registers a custom action handler
func (s *MockFaultInjectorServer) RegisterHandler(actionType ActionType, handler ActionHandler) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.handlers[actionType] = handler
}

func (s *MockFaultInjectorServer) setupRoutes(router *http.ServeMux) {
	router.HandleFunc("/actions", s.handleListActions)
	router.HandleFunc("/action", s.handleActionRoutes)
	router.HandleFunc("/health", s.handleHealth)
}

func (s *MockFaultInjectorServer) handleListActions(w http.ResponseWriter, r *http.Request) {
	actions := []ActionType{
		ActionClusterFailover,
		ActionClusterReshard,
		ActionNodeRestart,
		ActionNodeStop,
		ActionNodeStart,
		ActionNetworkPartition,
		ActionNetworkLatency,
		ActionNetworkPacketLoss,
		ActionNetworkRestore,
		ActionSequence,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(actions)
}

func (s *MockFaultInjectorServer) handleTriggerAction(w http.ResponseWriter, r *http.Request) {
	var request ActionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	actionID := s.generateActionID()

	// Create action status
	status := &ActionStatusResponse{
		ActionID:  actionID,
		Status:    StatusPending,
		StartTime: time.Now(),
		Progress:  0.0,
	}

	s.mutex.Lock()
	s.actions[actionID] = status
	s.mutex.Unlock()

	// Execute action asynchronously
	go s.executeAction(actionID, request)

	response := ActionResponse{
		ActionID: actionID,
		Status:   string(StatusPending),
		Message:  fmt.Sprintf("Action %s queued for execution", request.Type),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *MockFaultInjectorServer) handleActionRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		s.handleTriggerAction(w, r)
	case "GET":
		// Extract action ID from path
		path := r.URL.Path
		if strings.HasPrefix(path, "/action/") {
			actionID := strings.TrimPrefix(path, "/action/")
			s.handleGetActionStatus(w, r, actionID)
		} else {
			http.Error(w, "Invalid action path", http.StatusBadRequest)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *MockFaultInjectorServer) handleGetActionStatus(w http.ResponseWriter, r *http.Request, actionID string) {
	s.mutex.RLock()
	status, exists := s.actions[actionID]
	s.mutex.RUnlock()

	if !exists {
		http.Error(w, "Action not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *MockFaultInjectorServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (s *MockFaultInjectorServer) executeAction(actionID string, request ActionRequest) {
	s.updateActionStatus(actionID, StatusRunning, 0.1, "Starting action execution")

	s.mutex.RLock()
	handler, exists := s.handlers[request.Type]
	s.mutex.RUnlock()

	if !exists {
		s.updateActionStatus(actionID, StatusFailed, 0.0, fmt.Sprintf("No handler for action type: %s", request.Type))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Execute the action
	err := handler.Execute(ctx, request.Parameters)

	if err != nil {
		s.updateActionStatus(actionID, StatusFailed, handler.GetProgress(), fmt.Sprintf("Action failed: %v", err))
		return
	}

	s.updateActionStatus(actionID, StatusSuccess, 1.0, handler.GetOutput())
}

func (s *MockFaultInjectorServer) updateActionStatus(actionID string, status ActionStatus, progress float64, output string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if action, exists := s.actions[actionID]; exists {
		action.Status = status
		action.Progress = progress
		action.Output = map[string]interface{}{"result": output}

		if status == StatusSuccess || status == StatusFailed || status == StatusCancelled {
			action.EndTime = time.Now()
		}
	}
}

func (s *MockFaultInjectorServer) generateActionID() string {
	return fmt.Sprintf("action_%d_%d", time.Now().Unix(), rand.Intn(10000))
}

func (s *MockFaultInjectorServer) registerDefaultHandlers() {
	s.handlers[ActionClusterFailover] = &ClusterFailoverHandler{}
	s.handlers[ActionClusterReshard] = &ClusterReshardHandler{}
	s.handlers[ActionNodeRestart] = &NodeRestartHandler{}
	s.handlers[ActionNodeStop] = &NodeStopHandler{}
	s.handlers[ActionNodeStart] = &NodeStartHandler{}
	s.handlers[ActionNetworkPartition] = &NetworkPartitionHandler{}
	s.handlers[ActionNetworkLatency] = &NetworkLatencyHandler{}
	s.handlers[ActionNetworkPacketLoss] = &NetworkPacketLossHandler{}
	s.handlers[ActionNetworkRestore] = &NetworkRestoreHandler{}
	s.handlers[ActionSequence] = &SequenceHandler{server: s}
}

// Default Action Handlers

type ClusterFailoverHandler struct {
	progress float64
	output   string
}

func (h *ClusterFailoverHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodeID, _ := params["node_id"].(string)
	force, _ := params["force"].(bool)

	h.progress = 0.2
	h.output = fmt.Sprintf("Initiating failover for node %s (force: %v)", nodeID, force)

	// Simulate failover process
	time.Sleep(2 * time.Second)
	h.progress = 0.5
	h.output += "\nFailover in progress..."

	time.Sleep(3 * time.Second)
	h.progress = 1.0
	h.output += "\nFailover completed successfully"

	return nil
}

func (h *ClusterFailoverHandler) GetProgress() float64 { return h.progress }
func (h *ClusterFailoverHandler) GetOutput() string    { return h.output }

type ClusterReshardHandler struct {
	progress float64
	output   string
}

func (h *ClusterReshardHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	sourceNode, _ := params["source_node"].(string)
	targetNode, _ := params["target_node"].(string)

	h.progress = 0.1
	h.output = fmt.Sprintf("Starting reshard from %s to %s", sourceNode, targetNode)

	// Simulate reshard process
	for i := 1; i <= 10; i++ {
		time.Sleep(500 * time.Millisecond)
		h.progress = float64(i) / 10.0
		h.output += fmt.Sprintf("\nReshard progress: %d0%%", i)
	}

	h.output += "\nReshard completed successfully"
	return nil
}

func (h *ClusterReshardHandler) GetProgress() float64 { return h.progress }
func (h *ClusterReshardHandler) GetOutput() string    { return h.output }

type NodeRestartHandler struct {
	progress float64
	output   string
}

func (h *NodeRestartHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodeID, _ := params["node_id"].(string)
	graceful, _ := params["graceful"].(bool)

	h.progress = 0.1
	h.output = fmt.Sprintf("Restarting node %s (graceful: %v)", nodeID, graceful)

	// Simulate restart process
	time.Sleep(1 * time.Second)
	h.progress = 0.3
	h.output += "\nStopping node..."

	time.Sleep(2 * time.Second)
	h.progress = 0.7
	h.output += "\nStarting node..."

	time.Sleep(2 * time.Second)
	h.progress = 1.0
	h.output += "\nNode restart completed"

	return nil
}

func (h *NodeRestartHandler) GetProgress() float64 { return h.progress }
func (h *NodeRestartHandler) GetOutput() string    { return h.output }

type NodeStopHandler struct {
	progress float64
	output   string
}

func (h *NodeStopHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodeID, _ := params["node_id"].(string)
	graceful, _ := params["graceful"].(bool)

	h.progress = 0.2
	h.output = fmt.Sprintf("Stopping node %s (graceful: %v)", nodeID, graceful)

	time.Sleep(2 * time.Second)
	h.progress = 1.0
	h.output += "\nNode stopped successfully"

	return nil
}

func (h *NodeStopHandler) GetProgress() float64 { return h.progress }
func (h *NodeStopHandler) GetOutput() string    { return h.output }

type NodeStartHandler struct {
	progress float64
	output   string
}

func (h *NodeStartHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodeID, _ := params["node_id"].(string)

	h.progress = 0.2
	h.output = fmt.Sprintf("Starting node %s", nodeID)

	time.Sleep(3 * time.Second)
	h.progress = 1.0
	h.output += "\nNode started successfully"

	return nil
}

func (h *NodeStartHandler) GetProgress() float64 { return h.progress }
func (h *NodeStartHandler) GetOutput() string    { return h.output }

type NetworkPartitionHandler struct {
	progress float64
	output   string
}

func (h *NetworkPartitionHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodes, _ := params["nodes"].([]interface{})
	duration, _ := params["duration"].(string)

	nodeList := make([]string, len(nodes))
	for i, node := range nodes {
		nodeList[i] = node.(string)
	}

	h.progress = 0.1
	h.output = fmt.Sprintf("Creating network partition for nodes: %v (duration: %s)", nodeList, duration)

	// Simulate network partition
	time.Sleep(1 * time.Second)
	h.progress = 0.5
	h.output += "\nNetwork partition active"

	// Parse duration and wait
	if dur, err := time.ParseDuration(duration); err == nil && dur > 0 {
		time.Sleep(dur)
	} else {
		time.Sleep(10 * time.Second) // Default duration
	}

	h.progress = 1.0
	h.output += "\nNetwork partition completed"

	return nil
}

func (h *NetworkPartitionHandler) GetProgress() float64 { return h.progress }
func (h *NetworkPartitionHandler) GetOutput() string    { return h.output }

type NetworkLatencyHandler struct {
	progress float64
	output   string
}

func (h *NetworkLatencyHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodes, _ := params["nodes"].([]interface{})
	latency, _ := params["latency"].(string)
	jitter, _ := params["jitter"].(string)

	h.progress = 0.2
	h.output = fmt.Sprintf("Adding network latency: %s (jitter: %s) to nodes: %v", latency, jitter, nodes)

	time.Sleep(1 * time.Second)
	h.progress = 1.0
	h.output += "\nNetwork latency applied successfully"

	return nil
}

func (h *NetworkLatencyHandler) GetProgress() float64 { return h.progress }
func (h *NetworkLatencyHandler) GetOutput() string    { return h.output }

type NetworkPacketLossHandler struct {
	progress float64
	output   string
}

func (h *NetworkPacketLossHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodes, _ := params["nodes"].([]interface{})
	lossPercent, _ := params["loss_percent"].(float64)

	h.progress = 0.2
	h.output = fmt.Sprintf("Applying %.1f%% packet loss to nodes: %v", lossPercent, nodes)

	time.Sleep(1 * time.Second)
	h.progress = 1.0
	h.output += "\nPacket loss applied successfully"

	return nil
}

func (h *NetworkPacketLossHandler) GetProgress() float64 { return h.progress }
func (h *NetworkPacketLossHandler) GetOutput() string    { return h.output }

type NetworkRestoreHandler struct {
	progress float64
	output   string
}

func (h *NetworkRestoreHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodes, _ := params["nodes"].([]interface{})

	h.progress = 0.2
	h.output = fmt.Sprintf("Restoring normal network conditions for nodes: %v", nodes)

	time.Sleep(1 * time.Second)
	h.progress = 1.0
	h.output += "\nNetwork conditions restored successfully"

	return nil
}

func (h *NetworkRestoreHandler) GetProgress() float64 { return h.progress }
func (h *NetworkRestoreHandler) GetOutput() string    { return h.output }

type SequenceHandler struct {
	server   *MockFaultInjectorServer
	progress float64
	output   string
}

func (h *SequenceHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	actionsParam, exists := params["actions"]
	if !exists {
		return fmt.Errorf("no actions specified in sequence")
	}

	// Convert to sequence actions
	actionsData, err := json.Marshal(actionsParam)
	if err != nil {
		return fmt.Errorf("failed to marshal actions: %w", err)
	}

	var actions []SequenceAction
	if err := json.Unmarshal(actionsData, &actions); err != nil {
		return fmt.Errorf("failed to unmarshal actions: %w", err)
	}

	h.output = fmt.Sprintf("Executing sequence of %d actions", len(actions))

	for i, action := range actions {
		h.progress = float64(i) / float64(len(actions))
		h.output += fmt.Sprintf("\nExecuting action %d: %s", i+1, action.Type)

		// Apply delay if specified
		if action.Delay > 0 {
			time.Sleep(action.Delay)
		}

		// Execute the action (simplified for mock)
		if handler, exists := h.server.handlers[action.Type]; exists {
			if err := handler.Execute(ctx, action.Parameters); err != nil {
				return fmt.Errorf("action %d failed: %w", i+1, err)
			}
		}
	}

	h.progress = 1.0
	h.output += "\nSequence completed successfully"

	return nil
}

func (h *SequenceHandler) GetProgress() float64 { return h.progress }
func (h *SequenceHandler) GetOutput() string    { return h.output }

// Real Infrastructure Handlers (for actual Docker/Kubernetes integration)

type DockerNodeHandler struct {
	progress float64
	output   string
}

func (h *DockerNodeHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	nodeID, _ := params["node_id"].(string)
	action, _ := params["action"].(string) // "stop", "start", "restart", "kill"

	containerName := fmt.Sprintf("redis-%s", nodeID)

	h.progress = 0.1
	h.output = fmt.Sprintf("Executing Docker %s on container %s", action, containerName)

	var cmd *exec.Cmd
	switch action {
	case "stop":
		cmd = exec.CommandContext(ctx, "docker", "stop", containerName)
	case "start":
		cmd = exec.CommandContext(ctx, "docker", "start", containerName)
	case "restart":
		cmd = exec.CommandContext(ctx, "docker", "restart", containerName)
	case "kill":
		cmd = exec.CommandContext(ctx, "docker", "kill", containerName)
	default:
		return fmt.Errorf("unknown action: %s", action)
	}

	h.progress = 0.5
	output, err := cmd.CombinedOutput()

	if err != nil {
		h.output += fmt.Sprintf("\nCommand failed: %v\nOutput: %s", err, string(output))
		return err
	}

	h.progress = 1.0
	h.output += fmt.Sprintf("\nDocker %s completed successfully\nOutput: %s", action, string(output))

	return nil
}

func (h *DockerNodeHandler) GetProgress() float64 { return h.progress }
func (h *DockerNodeHandler) GetOutput() string    { return h.output }

type NetworkControlHandler struct {
	progress float64
	output   string
}

func (h *NetworkControlHandler) Execute(ctx context.Context, params map[string]interface{}) error {
	action, _ := params["action"].(string)
	nodes, _ := params["nodes"].([]interface{})

	h.progress = 0.1
	h.output = fmt.Sprintf("Executing network control action: %s on nodes: %v", action, nodes)

	// This would integrate with actual network control tools
	// For now, just simulate the action
	time.Sleep(2 * time.Second)

	h.progress = 1.0
	h.output += fmt.Sprintf("\nNetwork control action %s completed", action)

	return nil
}

func (h *NetworkControlHandler) GetProgress() float64 { return h.progress }
func (h *NetworkControlHandler) GetOutput() string    { return h.output }
