package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// ActionType represents the type of fault injection action
type ActionType string

// ActionListItem represents a single action in the list actions response
type ActionListItem struct {
	JobID       string `json:"job_id"`
	ActionType  string `json:"action_type"`
	Status      string `json:"status"`
	SubmittedAt string `json:"submitted_at"`
}

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
	ActionSlotMigration   ActionType = "slot_migrate"

	// Sequence and complex actions
	ActionSequence       ActionType = "sequence_of_actions"
	ActionExecuteCommand ActionType = "execute_command"

	// Database management actions
	ActionDeleteDatabase ActionType = "delete_database"
	ActionCreateDatabase ActionType = "create_database"
	ActionFailover       ActionType = "failover"
	ActionMigrate        ActionType = "migrate"
	ActionBind           ActionType = "bind"

	// Slot migrate action (OSS Cluster API testing)
	ActionSlotMigrate ActionType = "slot_migrate"
)

// SlotMigrateEffect represents the effect type for slot migration
type SlotMigrateEffect string

const (
	// SlotMigrateEffectRemoveAdd migrates all shards from source node to empty node
	// Result: One endpoint removed, one endpoint added
	SlotMigrateEffectRemoveAdd SlotMigrateEffect = "remove-add"

	// SlotMigrateEffectRemove migrates all shards from source node to existing node
	// Result: One endpoint removed
	SlotMigrateEffectRemove SlotMigrateEffect = "remove"

	// SlotMigrateEffectAdd migrates one shard to empty node
	// Result: One endpoint added
	SlotMigrateEffectAdd SlotMigrateEffect = "add"

	// SlotMigrateEffectSlotShuffle migrates one shard between existing nodes
	// Result: Slots move, endpoints unchanged
	SlotMigrateEffectSlotShuffle SlotMigrateEffect = "slot-shuffle"
)

// SlotMigrateVariant represents the mechanism to achieve the slot migration effect
type SlotMigrateVariant string

const (
	// SlotMigrateVariantDefault is an alias for migrate
	SlotMigrateVariantDefault SlotMigrateVariant = "default"

	// SlotMigrateVariantMigrate uses rladmin migrate to move shards
	SlotMigrateVariantMigrate SlotMigrateVariant = "migrate"

	// SlotMigrateVariantFailover triggers failover to swap master/replica roles
	// Requires replication to be enabled
	SlotMigrateVariantFailover SlotMigrateVariant = "failover"
)

// SlotMigrateRequest represents a request to trigger a slot migration
type SlotMigrateRequest struct {
	Effect       SlotMigrateEffect  `json:"effect"`
	BdbID        string             `json:"bdb_id"`
	ClusterIndex int                `json:"cluster_index,omitempty"`
	Trigger      SlotMigrateVariant `json:"variant,omitempty"`
	SourceNode   *int               `json:"source_node,omitempty"`
	TargetNode   *int               `json:"target_node,omitempty"`
}

// SlotMigrateTrigger represents a trigger configuration for slot migration
type SlotMigrateTrigger struct {
	Name         string                          `json:"name"`
	Description  string                          `json:"description"`
	Requirements []SlotMigrateTriggerRequirement `json:"requirements"`
}

// SlotMigrateTriggerRequirement represents database configuration requirements
type SlotMigrateTriggerRequirement struct {
	DBConfig    map[string]interface{} `json:"dbconfig"`
	Cluster     map[string]interface{} `json:"cluster"`
	Description string                 `json:"description"`
}

// SlotMigrateTriggersResponse represents the response from GET /slot-migrate
type SlotMigrateTriggersResponse struct {
	Effect   SlotMigrateEffect      `json:"effect"`
	Cluster  map[string]interface{} `json:"cluster"`
	Triggers []SlotMigrateTrigger   `json:"triggers"`
}

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

// ActionsListResponse is the wrapper response from the /action GET endpoint
type ActionsListResponse struct {
	Actions []ActionListItem `json:"actions"`
}

// ListActions lists all available actions
func (c *FaultInjectorClient) ListActions(ctx context.Context) ([]ActionListItem, error) {
	var response ActionsListResponse
	err := c.request(ctx, "GET", "/action", nil, &response)
	return response.Actions, err
}

// TriggerAction triggers a specific action
func (c *FaultInjectorClient) TriggerAction(ctx context.Context, action ActionRequest) (*ActionResponse, error) {
	var response ActionResponse
	fmt.Printf("[FI] Triggering action: %+v\n", action)
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

// TriggerClusterReshard is DEPRECATED - use TriggerSlotMigrateSlotShuffle instead
// The fault injector does not support a 'cluster_reshard' action type.
// Use the /slot-migrate endpoint with effect=slot-shuffle instead.
func (c *FaultInjectorClient) TriggerClusterReshard(ctx context.Context, slots []int, sourceNode, targetNode string) (*ActionResponse, error) {
	return nil, fmt.Errorf("TriggerClusterReshard is deprecated: action type 'cluster_reshard' does not exist in fault injector. Use TriggerSlotMigrateSlotShuffle instead")
}

// TriggerSlotMigration triggers migration of specific slots (legacy API)
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

// Slot Migrate Actions (OSS Cluster API Testing)
// These methods use the /slot-migrate endpoint for testing cluster topology changes

// GetSlotMigrateTriggers returns available triggers for a slot migration effect
// This is useful for discovering what database configurations are needed for each effect/variant
func (c *FaultInjectorClient) GetSlotMigrateTriggers(ctx context.Context, effect SlotMigrateEffect, clusterIndex int) (*SlotMigrateTriggersResponse, error) {
	var response SlotMigrateTriggersResponse
	path := fmt.Sprintf("/slot-migrate?effect=%s&cluster_index=%d", effect, clusterIndex)
	fmt.Printf("[FI] GET slot-migrate: %+v\n", path)
	err := c.request(ctx, "GET", path, nil, &response)
	return &response, err
}

// GetSlotMigrateTriggersRaw returns the raw JSON response from GET /slot-migrate
// This is useful for debugging when the response structure doesn't match expectations
func (c *FaultInjectorClient) GetSlotMigrateTriggersRaw(ctx context.Context, effect SlotMigrateEffect, clusterIndex int) ([]byte, error) {
	path := fmt.Sprintf("/slot-migrate?effect=%s&cluster_index=%d", effect, clusterIndex)
	url := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// TriggerSlotMigrate triggers a slot migration with the specified effect and variant
// This is the new API for testing OSS Cluster API client behavior during endpoint changes
//
// Effects:
//   - remove-add: One endpoint removed, one added (migrate all shards to empty node)
//   - remove: One endpoint removed (migrate all shards to existing node)
//   - add: One endpoint added (migrate one shard to empty node)
//   - slot-shuffle: Slots moved without endpoint change (migrate one shard between existing nodes)
//
// Variants:
//   - default/migrate: Use rladmin migrate to move shards
//   - maintenance_mode: Put node in maintenance mode (only for remove-add, remove)
//   - failover: Trigger failover to swap master/replica roles (requires replication)
func (c *FaultInjectorClient) TriggerSlotMigrate(ctx context.Context, req SlotMigrateRequest) (*ActionResponse, error) {
	var response ActionResponse

	// Build query parameters
	path := fmt.Sprintf("/slot-migrate?effect=%s&bdb_id=%s&cluster_index=%d",
		req.Effect, req.BdbID, req.ClusterIndex)

	if req.Trigger != "" {
		path += fmt.Sprintf("&trigger=%s", req.Trigger)
	}
	if req.SourceNode != nil {
		path += fmt.Sprintf("&source_node=%d", *req.SourceNode)
	}
	if req.TargetNode != nil {
		path += fmt.Sprintf("&target_node=%d", *req.TargetNode)
	}

	fmt.Printf("[FI] POST slot-migrate: %+v\n %+v\n", path, req)
	err := c.request(ctx, "POST", path, nil, &response)
	return &response, err
}

// TriggerSlotMigrateRemoveAdd triggers a remove-add slot migration
// This migrates all shards from source node to an empty node
// Result: One endpoint removed, one endpoint added
func (c *FaultInjectorClient) TriggerSlotMigrateRemoveAdd(ctx context.Context, bdbID string, trigger SlotMigrateVariant) (*ActionResponse, error) {
	return c.TriggerSlotMigrate(ctx, SlotMigrateRequest{
		Effect:  SlotMigrateEffectRemoveAdd,
		BdbID:   bdbID,
		Trigger: trigger,
	})
}

// TriggerSlotMigrateRemove triggers a remove slot migration
// This migrates all shards from source node to an existing node
// Result: One endpoint removed
func (c *FaultInjectorClient) TriggerSlotMigrateRemove(ctx context.Context, bdbID string, trigger SlotMigrateVariant) (*ActionResponse, error) {
	return c.TriggerSlotMigrate(ctx, SlotMigrateRequest{
		Effect:  SlotMigrateEffectRemove,
		BdbID:   bdbID,
		Trigger: trigger,
	})
}

// TriggerSlotMigrateAdd triggers an add slot migration
// This migrates one shard to an empty node
// Result: One endpoint added
func (c *FaultInjectorClient) TriggerSlotMigrateAdd(ctx context.Context, bdbID string, trigger SlotMigrateVariant) (*ActionResponse, error) {
	return c.TriggerSlotMigrate(ctx, SlotMigrateRequest{
		Effect:  SlotMigrateEffectAdd,
		BdbID:   bdbID,
		Trigger: trigger,
	})
}

// TriggerSlotMigrateSlotShuffle triggers a slot-shuffle migration
// This migrates one shard between existing nodes
// Result: Slots move, endpoints unchanged
func (c *FaultInjectorClient) TriggerSlotMigrateSlotShuffle(ctx context.Context, bdbID string, trigger SlotMigrateVariant) (*ActionResponse, error) {
	return c.TriggerSlotMigrate(ctx, SlotMigrateRequest{
		Effect:  SlotMigrateEffectSlotShuffle,
		BdbID:   bdbID,
		Trigger: trigger,
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

// Database Management Actions

// EnvDatabaseConfig represents the configuration for creating a database
type DatabaseConfig struct {
	Name                         string                 `json:"name"`
	Port                         int                    `json:"port"`
	MemorySize                   int64                  `json:"memory_size"`
	Replication                  bool                   `json:"replication"`
	EvictionPolicy               string                 `json:"eviction_policy"`
	Sharding                     bool                   `json:"sharding"`
	AutoUpgrade                  bool                   `json:"auto_upgrade"`
	ShardsCount                  int                    `json:"shards_count"`
	ModuleList                   []DatabaseModule       `json:"module_list,omitempty"`
	OSSCluster                   bool                   `json:"oss_cluster"`
	OSSClusterAPIPreferredIPType string                 `json:"oss_cluster_api_preferred_ip_type,omitempty"`
	ProxyPolicy                  string                 `json:"proxy_policy,omitempty"`
	ShardsPlacement              string                 `json:"shards_placement,omitempty"`
	ShardKeyRegex                []ShardKeyRegexPattern `json:"shard_key_regex,omitempty"`
}

// DatabaseModule represents a Redis module configuration
type DatabaseModule struct {
	ModuleArgs string `json:"module_args"`
	ModuleName string `json:"module_name"`
}

// ShardKeyRegexPattern represents a shard key regex pattern
type ShardKeyRegexPattern struct {
	Regex string `json:"regex"`
}

// DeleteDatabase deletes a database
// Parameters:
//   - clusterIndex: The index of the cluster
//   - bdbID: The database ID to delete
func (c *FaultInjectorClient) DeleteDatabase(ctx context.Context, clusterIndex int, bdbID int) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionDeleteDatabase,
		Parameters: map[string]interface{}{
			"cluster_index": clusterIndex,
			"bdb_id":        bdbID,
		},
	})
}

// CreateDatabase creates a new database
// Parameters:
//   - clusterIndex: The index of the cluster
//   - databaseConfig: The database configuration
func (c *FaultInjectorClient) CreateDatabase(ctx context.Context, clusterIndex int, databaseConfig DatabaseConfig) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionCreateDatabase,
		Parameters: map[string]interface{}{
			"cluster_index":   clusterIndex,
			"database_config": databaseConfig,
		},
	})
}

// CreateDatabaseFromMap creates a new database using a map for configuration
// This is useful when you want to pass a raw configuration map
// Parameters:
//   - clusterIndex: The index of the cluster
//   - databaseConfig: The database configuration as a map
func (c *FaultInjectorClient) CreateDatabaseFromMap(ctx context.Context, clusterIndex int, databaseConfig map[string]interface{}) (*ActionResponse, error) {
	return c.TriggerAction(ctx, ActionRequest{
		Type: ActionCreateDatabase,
		Parameters: map[string]interface{}{
			"cluster_index":   clusterIndex,
			"database_config": databaseConfig,
		},
	})
}

// isPortUnavailableError checks if the error is a port unavailable error
func isPortUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "port_unavailable") || strings.Contains(errStr, "Unavailable or invalid port")
}

// CreateDatabaseWithPortRetry creates a new database, retrying with incremented port numbers if the port is unavailable
// Parameters:
//   - clusterIndex: The index of the cluster
//   - databaseConfig: The database configuration as a map (must contain "port" key)
//   - maxRetries: Maximum number of port increments to try (default 100 if <= 0)
//
// Returns the ActionResponse and the final port used
func (c *FaultInjectorClient) CreateDatabaseWithPortRetry(ctx context.Context, clusterIndex int, databaseConfig map[string]interface{}, maxRetries int) (*ActionResponse, int, error) {
	if maxRetries <= 0 {
		maxRetries = 100
	}

	// Get the initial port from the config
	port, ok := databaseConfig["port"]
	if !ok {
		// No port specified, just try once
		resp, err := c.CreateDatabaseFromMap(ctx, clusterIndex, databaseConfig)
		return resp, 0, err
	}

	// Convert port to int
	var currentPort int
	switch p := port.(type) {
	case int:
		currentPort = p
	case int64:
		currentPort = int(p)
	case float64:
		currentPort = int(p)
	default:
		// Can't handle this port type, just try once
		resp, err := c.CreateDatabaseFromMap(ctx, clusterIndex, databaseConfig)
		return resp, 0, err
	}

	// Try creating the database, incrementing port on failure
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Update the port in the config
		databaseConfig["port"] = currentPort

		resp, err := c.CreateDatabaseFromMap(ctx, clusterIndex, databaseConfig)
		if err == nil {
			return resp, currentPort, nil
		}

		// Check if it's a port unavailable error
		if !isPortUnavailableError(err) {
			// Different error, don't retry
			return nil, currentPort, err
		}

		// Port unavailable, try next port
		if DebugE2E() {
			fmt.Printf("[FI] Port %d unavailable, trying port %d\n", currentPort, currentPort+1)
		}
		currentPort++
	}

	return nil, currentPort, fmt.Errorf("failed to create database after %d port retries (last port tried: %d)", maxRetries, currentPort-1)
}

// CreateDatabaseConfigWithPortRetry creates a new database using DatabaseConfig, retrying with incremented port numbers if the port is unavailable
// Parameters:
//   - clusterIndex: The index of the cluster
//   - databaseConfig: The database configuration
//   - maxRetries: Maximum number of port increments to try (default 100 if <= 0)
//
// Returns the ActionResponse and the final port used
func (c *FaultInjectorClient) CreateDatabaseConfigWithPortRetry(ctx context.Context, clusterIndex int, databaseConfig DatabaseConfig, maxRetries int) (*ActionResponse, int, error) {
	if maxRetries <= 0 {
		maxRetries = 100
	}

	currentPort := databaseConfig.Port

	// Try creating the database, incrementing port on failure
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Update the port in the config
		databaseConfig.Port = currentPort

		resp, err := c.CreateDatabase(ctx, clusterIndex, databaseConfig)
		if err == nil {
			return resp, currentPort, nil
		}

		// Check if it's a port unavailable error
		if !isPortUnavailableError(err) {
			// Different error, don't retry
			return nil, currentPort, err
		}

		// Port unavailable, try next port
		if DebugE2E() {
			fmt.Printf("[FI] Port %d unavailable, trying port %d\n", currentPort, currentPort+1)
		}
		currentPort++
	}

	return nil, currentPort, fmt.Errorf("failed to create database after %d port retries (last port tried: %d)", maxRetries, currentPort-1)
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

// DebugE2E returns true if E2E_DEBUG environment variable is set to "true"
// Use this to control verbose debug logging in e2e tests
func DebugE2E() bool {
	return os.Getenv("E2E_DEBUG") == "true"
}

// formatSMigratingNotification formats an SMIGRATING notification in RESP3 wire format
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

// formatSMigratedNotification formats an SMIGRATED notification in RESP3 wire format
func formatSMigratedNotification(seqID int64, endpoints ...string) string {
	// Correct Format: ["SMIGRATED", SeqID, [[host:port, slots], [host:port, slots], ...]]
	// RESP3 wire format:
	//   >3
	//   +SMIGRATED
	//   :SeqID
	//   *<num_entries>
	//     *2
	//       +<host:port>
	//       +<slots-or-ranges>
	// Each endpoint is formatted as: "host:port slot1,slot2,range1-range2"
	parts := []string{">3\r\n"}
	parts = append(parts, "+SMIGRATED\r\n")
	parts = append(parts, fmt.Sprintf(":%d\r\n", seqID))

	count := len(endpoints)
	parts = append(parts, fmt.Sprintf("*%d\r\n", count))

	for _, endpoint := range endpoints {
		// Split endpoint into host:port and slots
		// endpoint format: "host:port slot1,slot2,range1-range2"
		endpointParts := strings.SplitN(endpoint, " ", 2)
		if len(endpointParts) != 2 {
			continue
		}
		hostPort := endpointParts[0]
		slots := endpointParts[1]

		// Each entry is an array with 2 elements
		parts = append(parts, "*2\r\n")
		parts = append(parts, fmt.Sprintf("+%s\r\n", hostPort))
		parts = append(parts, fmt.Sprintf("+%s\r\n", slots))
	}

	return strings.Join(parts, "")
}
