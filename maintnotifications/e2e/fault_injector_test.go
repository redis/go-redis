package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ActionType represents the type of fault injection action
type ActionType string

const (
	// Redis cluster actions
	ActionClusterFailover   ActionType = "cluster_failover"
	ActionClusterReshard    ActionType = "cluster_reshard"
	ActionClusterAddNode    ActionType = "cluster_add_node"
	ActionClusterRemoveNode ActionType = "cluster_remove_node"
	ActionClusterMigrate    ActionType = "cluster_migrate"

	// Node-level actions
	ActionNodeRestart ActionType = "node_restart"
	ActionNodeStop    ActionType = "node_stop"
	ActionNodeStart   ActionType = "node_start"
	ActionNodeKill    ActionType = "node_kill"

	// Network simulation actions
	ActionNetworkPartition  ActionType = "network_partition"
	ActionNetworkLatency    ActionType = "network_latency"
	ActionNetworkPacketLoss ActionType = "network_packet_loss"
	ActionNetworkBandwidth  ActionType = "network_bandwidth"
	ActionNetworkRestore    ActionType = "network_restore"

	// Redis configuration actions
	ActionConfigChange    ActionType = "config_change"
	ActionMaintenanceMode ActionType = "maintenance_mode"
	ActionSlotMigration   ActionType = "slot_migration"

	// Sequence and complex actions
	ActionSequence       ActionType = "sequence_of_actions"
	ActionExecuteCommand ActionType = "execute_command"
)

// ActionStatus represents the status of an action
type ActionStatus string

const (
	StatusPending   ActionStatus = "pending"
	StatusRunning   ActionStatus = "running"
	StatusFinished  ActionStatus = "finished"
	StatusFailed    ActionStatus = "failed"
	StatusSuccess   ActionStatus = "success"
	StatusCancelled ActionStatus = "cancelled"
)

// ActionRequest represents a request to trigger an action
type ActionRequest struct {
	Type       ActionType             `json:"type"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// ActionResponse represents the response from triggering an action
type ActionResponse struct {
	ActionID string `json:"action_id"`
	Status   string `json:"status"`
	Message  string `json:"message,omitempty"`
}

// ActionStatusResponse represents the status of an action
type ActionStatusResponse struct {
	ActionID  string                 `json:"action_id"`
	Status    ActionStatus           `json:"status"`
	Error     interface{}            `json:"error,omitempty"`
	Output    map[string]interface{} `json:"output,omitempty"`
	Progress  float64                `json:"progress,omitempty"`
	StartTime time.Time              `json:"start_time,omitempty"`
	EndTime   time.Time              `json:"end_time,omitempty"`
}

// SequenceAction represents an action in a sequence
type SequenceAction struct {
	Type       ActionType             `json:"type"`
	Parameters map[string]interface{} `json:"params,omitempty"`
	Delay      time.Duration          `json:"delay,omitempty"`
}

// FaultInjectorClient provides programmatic control over test infrastructure
type FaultInjectorClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewFaultInjectorClient creates a new fault injector client
func NewFaultInjectorClient(baseURL string) *FaultInjectorClient {
	return &FaultInjectorClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetBaseURL returns the base URL of the fault injector server
func (c *FaultInjectorClient) GetBaseURL() string {
	return c.baseURL
}

// ListActions lists all available actions
func (c *FaultInjectorClient) ListActions(ctx context.Context) ([]ActionType, error) {
	var actions []ActionType
	err := c.request(ctx, "GET", "/actions", nil, &actions)
	return actions, err
}

// TriggerAction triggers a specific action
func (c *FaultInjectorClient) TriggerAction(ctx context.Context, action ActionRequest) (*ActionResponse, error) {
	var response ActionResponse
	err := c.request(ctx, "POST", "/action", action, &response)
	return &response, err
}

func (c *FaultInjectorClient) TriggerSequence(ctx context.Context, bdbID int, actions []SequenceAction) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionSequence,
		Parameters: map[string]interface{}{
			"bdb_id":  bdbID,
			"actions": actions,
		},
	})
}

// GetActionStatus gets the status of a specific action
func (c *FaultInjectorClient) GetActionStatus(ctx context.Context, actionID string) (*ActionStatusResponse, error) {
	var status ActionStatusResponse
	err := c.request(ctx, "GET", fmt.Sprintf("/action/%s", actionID), nil, &status)
	return &status, err
}

// WaitForAction waits for an action to complete
func (c *FaultInjectorClient) WaitForAction(ctx context.Context, actionID string, options ...WaitOption) (*ActionStatusResponse, error) {
	config := &waitConfig{
		pollInterval: 1 * time.Second,
		maxWaitTime:  60 * time.Second,
	}

	for _, opt := range options {
		opt(config)
	}

	deadline := time.Now().Add(config.maxWaitTime)
	ticker := time.NewTicker(config.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Until(deadline)):
			return nil, fmt.Errorf("timeout waiting for action %s after %v", actionID, config.maxWaitTime)
		case <-ticker.C:
			status, err := c.GetActionStatus(ctx, actionID)
			if err != nil {
				return nil, fmt.Errorf("failed to get action status: %w", err)
			}

			switch status.Status {
			case StatusFinished, StatusSuccess, StatusFailed, StatusCancelled:
				return status, nil
			}
		}
	}
}

// Cluster Management Actions

// TriggerClusterFailover triggers a cluster failover
func (c *FaultInjectorClient) TriggerClusterFailover(ctx context.Context, nodeID string, force bool) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionClusterFailover,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
			"force":   force,
		},
	})
}

// TriggerClusterReshard triggers cluster resharding
func (c *FaultInjectorClient) TriggerClusterReshard(ctx context.Context, slots []int, sourceNode, targetNode string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionClusterReshard,
		Parameters: map[string]interface{}{
			"slots":       slots,
			"source_node": sourceNode,
			"target_node": targetNode,
		},
	})
}

// TriggerSlotMigration triggers migration of specific slots
func (c *FaultInjectorClient) TriggerSlotMigration(ctx context.Context, startSlot, endSlot int, sourceNode, targetNode string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionSlotMigration,
		Parameters: map[string]interface{}{
			"start_slot":  startSlot,
			"end_slot":    endSlot,
			"source_node": sourceNode,
			"target_node": targetNode,
		},
	})
}

// Node Management Actions

// RestartNode restarts a specific Redis node
func (c *FaultInjectorClient) RestartNode(ctx context.Context, nodeID string, graceful bool) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNodeRestart,
		Parameters: map[string]interface{}{
			"node_id":  nodeID,
			"graceful": graceful,
		},
	})
}

// StopNode stops a specific Redis node
func (c *FaultInjectorClient) StopNode(ctx context.Context, nodeID string, graceful bool) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNodeStop,
		Parameters: map[string]interface{}{
			"node_id":  nodeID,
			"graceful": graceful,
		},
	})
}

// StartNode starts a specific Redis node
func (c *FaultInjectorClient) StartNode(ctx context.Context, nodeID string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNodeStart,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
		},
	})
}

// KillNode forcefully kills a Redis node
func (c *FaultInjectorClient) KillNode(ctx context.Context, nodeID string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNodeKill,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
		},
	})
}

// Network Simulation Actions

// SimulateNetworkPartition simulates a network partition
func (c *FaultInjectorClient) SimulateNetworkPartition(ctx context.Context, nodes []string, duration time.Duration) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNetworkPartition,
		Parameters: map[string]interface{}{
			"nodes":    nodes,
			"duration": duration.String(),
		},
	})
}

// SimulateNetworkLatency adds network latency
func (c *FaultInjectorClient) SimulateNetworkLatency(ctx context.Context, nodes []string, latency time.Duration, jitter time.Duration) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNetworkLatency,
		Parameters: map[string]interface{}{
			"nodes":   nodes,
			"latency": latency.String(),
			"jitter":  jitter.String(),
		},
	})
}

// SimulatePacketLoss simulates packet loss
func (c *FaultInjectorClient) SimulatePacketLoss(ctx context.Context, nodes []string, lossPercent float64) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNetworkPacketLoss,
		Parameters: map[string]interface{}{
			"nodes":        nodes,
			"loss_percent": lossPercent,
		},
	})
}

// LimitBandwidth limits network bandwidth
func (c *FaultInjectorClient) LimitBandwidth(ctx context.Context, nodes []string, bandwidth string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNetworkBandwidth,
		Parameters: map[string]interface{}{
			"nodes":     nodes,
			"bandwidth": bandwidth,
		},
	})
}

// RestoreNetwork restores normal network conditions
func (c *FaultInjectorClient) RestoreNetwork(ctx context.Context, nodes []string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionNetworkRestore,
		Parameters: map[string]interface{}{
			"nodes": nodes,
		},
	})
}

// Configuration Actions

// ChangeConfig changes Redis configuration
func (c *FaultInjectorClient) ChangeConfig(ctx context.Context, nodeID string, config map[string]string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionConfigChange,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
			"config":  config,
		},
	})
}

// EnableMaintenanceMode enables maintenance mode
func (c *FaultInjectorClient) EnableMaintenanceMode(ctx context.Context, nodeID string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionMaintenanceMode,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
			"enabled": true,
		},
	})
}

// DisableMaintenanceMode disables maintenance mode
func (c *FaultInjectorClient) DisableMaintenanceMode(ctx context.Context, nodeID string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionMaintenanceMode,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
			"enabled": false,
		},
	})
}

// Complex Actions

// ExecuteSequence executes a sequence of actions
func (c *FaultInjectorClient) ExecuteSequence(ctx context.Context, actions []SequenceAction) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionSequence,
		Parameters: map[string]interface{}{
			"actions": actions,
		},
	})
}

// ExecuteCommand executes a custom command
func (c *FaultInjectorClient) ExecuteCommand(ctx context.Context, nodeID, command string) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionExecuteCommand,
		Parameters: map[string]interface{}{
			"node_id": nodeID,
			"command": command,
		},
	})
}

// Convenience Methods

// SimulateClusterUpgrade simulates a complete cluster upgrade scenario
func (c *FaultInjectorClient) SimulateClusterUpgrade(ctx context.Context, nodes []string) (*ActionResponse, error) {
	actions := make([]SequenceAction, 0, len(nodes)*2)

	// Rolling restart of all nodes
	for i, nodeID := range nodes {
		actions = append(actions, SequenceAction{
			Type: ActionNodeRestart,
			Parameters: map[string]interface{}{
				"node_id":  nodeID,
				"graceful": true,
			},
			Delay: time.Duration(i*10) * time.Second, // Stagger restarts
		})
	}

	return c.ExecuteSequence(ctx, actions)
}

// SimulateNetworkIssues simulates various network issues
func (c *FaultInjectorClient) SimulateNetworkIssues(ctx context.Context, nodes []string) (*ActionResponse, error) {
	actions := []SequenceAction{
		{
			Type: ActionNetworkLatency,
			Parameters: map[string]interface{}{
				"nodes":   nodes,
				"latency": "100ms",
				"jitter":  "20ms",
			},
		},
		{
			Type: ActionNetworkPacketLoss,
			Parameters: map[string]interface{}{
				"nodes":        nodes,
				"loss_percent": 2.0,
			},
			Delay: 30 * time.Second,
		},
		{
			Type: ActionNetworkRestore,
			Parameters: map[string]interface{}{
				"nodes": nodes,
			},
			Delay: 60 * time.Second,
		},
	}

	return c.ExecuteSequence(ctx, actions)
}

// Helper types and functions

type waitConfig struct {
	pollInterval time.Duration
	maxWaitTime  time.Duration
}

type WaitOption func(*waitConfig)

// WithPollInterval sets the polling interval for waiting
func WithPollInterval(interval time.Duration) WaitOption {
	return func(c *waitConfig) {
		c.pollInterval = interval
	}
}

// WithMaxWaitTime sets the maximum wait time
func WithMaxWaitTime(maxWait time.Duration) WaitOption {
	return func(c *waitConfig) {
		c.maxWaitTime = maxWait
	}
}

// Internal HTTP request method
func (c *FaultInjectorClient) request(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	url := c.baseURL + path

	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			// happens when the API changes and the response structure changes
			// sometimes the output of the action status is map, sometimes it is json.
			// since we don't have a proper response structure we are going to handle it here
			if result, ok := result.(*ActionStatusResponse); ok {
				mapResult := map[string]interface{}{}
				err = json.Unmarshal(respBody, &mapResult)
				if err != nil {
					fmt.Println("Failed to unmarshal response:", string(respBody))
					panic(err)
				}
				result.Error = mapResult["error"]
				result.Output = map[string]interface{}{"result": mapResult["output"]}
				if status, ok := mapResult["status"].(string); ok {
					result.Status = ActionStatus(status)
				}
				if result.Status == StatusSuccess || result.Status == StatusFailed || result.Status == StatusCancelled {
					result.EndTime = time.Now()
				}
				if progress, ok := mapResult["progress"].(float64); ok {
					result.Progress = progress
				}
				if actionID, ok := mapResult["action_id"].(string); ok {
					result.ActionID = actionID
				}
				return nil
			}
			fmt.Println("Failed to unmarshal response:", string(respBody))
			panic(err)
		}
	}

	return nil
}

// Utility functions for common scenarios

// GetClusterNodes returns a list of cluster node IDs
func GetClusterNodes() []string {
	// TODO Implement
	// This would typically be configured via environment or discovery
	return []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"}
}

// GetMasterNodes returns a list of master node IDs
func GetMasterNodes() []string {
	// TODO Implement
	return []string{"node-1", "node-2", "node-3"}
}

// GetSlaveNodes returns a list of slave node IDs
func GetSlaveNodes() []string {
	// TODO Implement
	return []string{"node-4", "node-5", "node-6"}
}

// ParseNodeID extracts node ID from various formats
func ParseNodeID(nodeAddr string) string {
	// Extract node ID from address like "redis-node-1:7001" -> "node-1"
	parts := strings.Split(nodeAddr, ":")
	if len(parts) > 0 {
		addr := parts[0]
		if strings.Contains(addr, "redis-") {
			return strings.TrimPrefix(addr, "redis-")
		}
		return addr
	}
	return nodeAddr
}

// FormatSlotRange formats a slot range for Redis commands
func FormatSlotRange(start, end int) string {
	if start == end {
		return strconv.Itoa(start)
	}
	return fmt.Sprintf("%d-%d", start, end)
}
