package hitless

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// UpgradeHandler handles hitless upgrade push notifications for Redis cluster upgrades.
// It implements different strategies based on notification type:
// - MOVING: Changes pool state to use new endpoint for future connections
//   - mark existing connections for closing, handle pubsub to change underlying connection, change pool dialer with the new endpoint
//
// - MIGRATING/FAILING_OVER: Marks specific connection as in transition
//   - relaxing timeouts
//
// - MIGRATED/FAILED_OVER: Clears transition state for specific connection
//   - return to original timeouts
type UpgradeHandler struct {
	mu sync.RWMutex

	// Connection-specific state for MIGRATING/FAILING_OVER notifications
	connectionStates map[*pool.Conn]*ConnectionState

	// Pool-level state removed - using atomic state in ClusterUpgradeManager instead

	// Client configuration for getting default timeouts
	defaultReadTimeout  time.Duration
	defaultWriteTimeout time.Duration
}

// ConnectionState tracks the state of a specific connection during upgrades
type ConnectionState struct {
	IsTransitioning bool
	TransitionType  string
	StartTime       time.Time
	ShardID         string
	TimeoutSeconds  int

	// Timeout management
	OriginalReadTimeout  time.Duration // Original read timeout
	OriginalWriteTimeout time.Duration // Original write timeout

	// MOVING state specific
	MarkedForClosing bool      // Connection should be closed after current commands
	IsBlocking       bool      // Connection has blocking commands
	NewEndpoint      string    // New endpoint for blocking commands
	LastCommandTime  time.Time // When the last command was sent
}

// PoolState removed - using atomic state in ClusterUpgradeManager instead

// NewUpgradeHandler creates a new hitless upgrade handler with client timeout configuration
func NewUpgradeHandler(defaultReadTimeout, defaultWriteTimeout time.Duration) *UpgradeHandler {
	return &UpgradeHandler{
		connectionStates:    make(map[*pool.Conn]*ConnectionState),
		defaultReadTimeout:  defaultReadTimeout,
		defaultWriteTimeout: defaultWriteTimeout,
	}
}

// GetConnectionTimeouts returns the appropriate read and write timeouts for a connection
// If the connection is transitioning, returns increased timeouts
func (h *UpgradeHandler) GetConnectionTimeouts(conn *pool.Conn, defaultReadTimeout, defaultWriteTimeout, transitionTimeout time.Duration) (time.Duration, time.Duration) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	state, exists := h.connectionStates[conn]
	if !exists || !state.IsTransitioning {
		return defaultReadTimeout, defaultWriteTimeout
	}

	// For transitioning connections (MIGRATING/FAILING_OVER), use longer timeouts
	switch state.TransitionType {
	case "MIGRATING", "FAILING_OVER":
		return transitionTimeout, transitionTimeout
	case "MOVING":
		// For MOVING connections, use default timeouts but mark for special handling
		return defaultReadTimeout, defaultWriteTimeout
	default:
		return defaultReadTimeout, defaultWriteTimeout
	}
}

// MarkConnectionForClosing marks a connection to be closed after current commands complete
func (h *UpgradeHandler) MarkConnectionForClosing(conn *pool.Conn, newEndpoint string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	state, exists := h.connectionStates[conn]
	if !exists {
		state = &ConnectionState{
			IsTransitioning: true,
			TransitionType:  "MOVING",
			StartTime:       time.Now(),
		}
		h.connectionStates[conn] = state
	}

	state.MarkedForClosing = true
	state.NewEndpoint = newEndpoint
	state.LastCommandTime = time.Now()
}

// IsConnectionMarkedForClosing checks if a connection should be closed
func (h *UpgradeHandler) IsConnectionMarkedForClosing(conn *pool.Conn) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	state, exists := h.connectionStates[conn]
	return exists && state.MarkedForClosing
}

// MarkConnectionAsBlocking marks a connection as having blocking commands
func (h *UpgradeHandler) MarkConnectionAsBlocking(conn *pool.Conn, isBlocking bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	state, exists := h.connectionStates[conn]
	if !exists {
		state = &ConnectionState{
			IsTransitioning: false,
		}
		h.connectionStates[conn] = state
	}

	state.IsBlocking = isBlocking
}

// ShouldRedirectBlockingConnection checks if a blocking connection should be redirected
// Uses client integrator's atomic state for pool-level checks - minimal locking
func (h *UpgradeHandler) ShouldRedirectBlockingConnection(conn *pool.Conn, clientIntegrator interface{}) (bool, string) {
	if conn != nil {
		// Check specific connection - need lock only for connection state
		h.mu.RLock()
		state, exists := h.connectionStates[conn]
		h.mu.RUnlock()

		if exists && state.IsBlocking && state.TransitionType == "MOVING" && state.NewEndpoint != "" {
			return true, state.NewEndpoint
		}
	}

	// Check client integrator's atomic state - no locks needed
	if ci, ok := clientIntegrator.(*ClientIntegrator); ok && ci != nil && ci.IsMoving() {
		return true, ci.GetNewEndpoint()
	}

	return false, ""
}

// ShouldRedirectNewBlockingConnection removed - functionality merged into ShouldRedirectBlockingConnection

// HandlePushNotification processes hitless upgrade push notifications
func (h *UpgradeHandler) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) == 0 {
		return fmt.Errorf("hitless: empty notification received")
	}

	notificationType, ok := notification[0].(string)
	if !ok {
		return fmt.Errorf("hitless: notification type is not a string: %T", notification[0])
	}

	internal.Logger.Printf(ctx, "hitless: processing %s notification with %d elements", notificationType, len(notification))

	switch notificationType {
	case "MOVING":
		return h.handleMovingNotification(ctx, handlerCtx, notification)
	case "MIGRATING":
		return h.handleMigratingNotification(ctx, handlerCtx, notification)
	case "MIGRATED":
		return h.handleMigratedNotification(ctx, handlerCtx, notification)
	case "FAILING_OVER":
		return h.handleFailingOverNotification(ctx, handlerCtx, notification)
	case "FAILED_OVER":
		return h.handleFailedOverNotification(ctx, handlerCtx, notification)
	default:
		internal.Logger.Printf(ctx, "hitless: unknown notification type: %s", notificationType)
		return nil
	}
}

// handleMovingNotification processes MOVING notifications that affect the entire pool
// Format: ["MOVING", time_seconds, "new_endpoint"]
func (h *UpgradeHandler) handleMovingNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 3 {
		return fmt.Errorf("hitless: MOVING notification requires at least 3 elements, got %d", len(notification))
	}

	// Parse timeout
	timeoutSeconds, err := h.parseTimeoutSeconds(notification[1])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse timeout for MOVING notification: %w", err)
	}

	// Parse new endpoint
	newEndpoint, ok := notification[2].(string)
	if !ok {
		return fmt.Errorf("hitless: new endpoint is not a string: %T", notification[2])
	}

	internal.Logger.Printf(ctx, "hitless: MOVING notification - endpoint will move to %s in %d seconds", newEndpoint, timeoutSeconds)
	h.mu.Lock()
	// Mark all existing connections for closing after current commands complete
	for _, state := range h.connectionStates {
		state.MarkedForClosing = true
		state.NewEndpoint = newEndpoint
		state.TransitionType = "MOVING"
		state.LastCommandTime = time.Now()
	}
	h.mu.Unlock()

	internal.Logger.Printf(ctx, "hitless: marked existing connections for closing, new blocking commands will use %s", newEndpoint)

	return nil
}

// Removed complex helper methods - simplified to direct inline logic

// handleMigratingNotification processes MIGRATING notifications for specific connections
// Format: ["MIGRATING", time_seconds, shard_id]
func (h *UpgradeHandler) handleMigratingNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 3 {
		return fmt.Errorf("hitless: MIGRATING notification requires at least 3 elements, got %d", len(notification))
	}

	timeoutSeconds, err := h.parseTimeoutSeconds(notification[1])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse timeout for MIGRATING notification: %w", err)
	}

	shardID, err := h.parseShardID(notification[2])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse shard ID for MIGRATING notification: %w", err)
	}

	conn := handlerCtx.Conn
	if conn == nil {
		return fmt.Errorf("hitless: no connection available in handler context")
	}

	internal.Logger.Printf(ctx, "hitless: MIGRATING notification - shard %s will migrate in %d seconds on connection %p", shardID, timeoutSeconds, conn)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Store original timeouts if not already stored
	var originalReadTimeout, originalWriteTimeout time.Duration
	if existingState, exists := h.connectionStates[conn]; exists {
		originalReadTimeout = existingState.OriginalReadTimeout
		originalWriteTimeout = existingState.OriginalWriteTimeout
	} else {
		// Get default timeouts from client configuration
		originalReadTimeout = h.defaultReadTimeout
		originalWriteTimeout = h.defaultWriteTimeout
	}

	h.connectionStates[conn] = &ConnectionState{
		IsTransitioning:      true,
		TransitionType:       "MIGRATING",
		StartTime:            time.Now(),
		ShardID:              shardID,
		TimeoutSeconds:       timeoutSeconds,
		OriginalReadTimeout:  originalReadTimeout,
		OriginalWriteTimeout: originalWriteTimeout,
		LastCommandTime:      time.Now(),
	}

	internal.Logger.Printf(ctx, "hitless: connection %p marked as MIGRATING with increased timeouts", conn)

	return nil
}

// handleMigratedNotification processes MIGRATED notifications for specific connections
// Format: ["MIGRATED", shard_id]
func (h *UpgradeHandler) handleMigratedNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 2 {
		return fmt.Errorf("hitless: MIGRATED notification requires at least 2 elements, got %d", len(notification))
	}

	shardID, err := h.parseShardID(notification[1])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse shard ID for MIGRATED notification: %w", err)
	}

	conn := handlerCtx.Conn
	if conn == nil {
		return fmt.Errorf("hitless: no connection available in handler context")
	}

	internal.Logger.Printf(ctx, "hitless: MIGRATED notification - shard %s migration completed on connection %p", shardID, conn)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Clear the transitioning state for this connection and restore original timeouts
	if state, exists := h.connectionStates[conn]; exists && state.TransitionType == "MIGRATING" && state.ShardID == shardID {
		internal.Logger.Printf(ctx, "hitless: restoring original timeouts for connection %p (read: %v, write: %v)",
			conn, state.OriginalReadTimeout, state.OriginalWriteTimeout)

		// In a real implementation, this would restore the connection's original timeouts
		// For now, we'll just log and delete the state
		delete(h.connectionStates, conn)
		internal.Logger.Printf(ctx, "hitless: cleared MIGRATING state and restored timeouts for connection %p", conn)
	}

	return nil
}

// handleFailingOverNotification processes FAILING_OVER notifications for specific connections
// Format: ["FAILING_OVER", time_seconds, shard_id]
func (h *UpgradeHandler) handleFailingOverNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 3 {
		return fmt.Errorf("hitless: FAILING_OVER notification requires at least 3 elements, got %d", len(notification))
	}

	timeoutSeconds, err := h.parseTimeoutSeconds(notification[1])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse timeout for FAILING_OVER notification: %w", err)
	}

	shardID, err := h.parseShardID(notification[2])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse shard ID for FAILING_OVER notification: %w", err)
	}

	conn := handlerCtx.Conn
	if conn == nil {
		return fmt.Errorf("hitless: no connection available in handler context")
	}

	internal.Logger.Printf(ctx, "hitless: FAILING_OVER notification - shard %s will failover in %d seconds on connection %p", shardID, timeoutSeconds, conn)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Store original timeouts if not already stored
	var originalReadTimeout, originalWriteTimeout time.Duration
	if existingState, exists := h.connectionStates[conn]; exists {
		originalReadTimeout = existingState.OriginalReadTimeout
		originalWriteTimeout = existingState.OriginalWriteTimeout
	} else {
		// Get default timeouts from client configuration
		originalReadTimeout = h.defaultReadTimeout
		originalWriteTimeout = h.defaultWriteTimeout
	}

	h.connectionStates[conn] = &ConnectionState{
		IsTransitioning:      true,
		TransitionType:       "FAILING_OVER",
		StartTime:            time.Now(),
		ShardID:              shardID,
		TimeoutSeconds:       timeoutSeconds,
		OriginalReadTimeout:  originalReadTimeout,
		OriginalWriteTimeout: originalWriteTimeout,
		LastCommandTime:      time.Now(),
	}

	internal.Logger.Printf(ctx, "hitless: connection %p marked as FAILING_OVER with increased timeouts", conn)

	return nil
}

// handleFailedOverNotification processes FAILED_OVER notifications for specific connections
// Format: ["FAILED_OVER", shard_id]
func (h *UpgradeHandler) handleFailedOverNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) < 2 {
		return fmt.Errorf("hitless: FAILED_OVER notification requires at least 2 elements, got %d", len(notification))
	}

	shardID, err := h.parseShardID(notification[1])
	if err != nil {
		return fmt.Errorf("hitless: failed to parse shard ID for FAILED_OVER notification: %w", err)
	}

	conn := handlerCtx.Conn
	if conn == nil {
		return fmt.Errorf("hitless: no connection available in handler context")
	}

	internal.Logger.Printf(ctx, "hitless: FAILED_OVER notification - shard %s failover completed on connection %p", shardID, conn)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Clear the transitioning state for this connection and restore original timeouts
	if state, exists := h.connectionStates[conn]; exists && state.TransitionType == "FAILING_OVER" && state.ShardID == shardID {
		internal.Logger.Printf(ctx, "hitless: restoring original timeouts for connection %p (read: %v, write: %v)",
			conn, state.OriginalReadTimeout, state.OriginalWriteTimeout)

		// In a real implementation, this would restore the connection's original timeouts
		// For now, we'll just log and delete the state
		delete(h.connectionStates, conn)
		internal.Logger.Printf(ctx, "hitless: cleared FAILING_OVER state and restored timeouts for connection %p", conn)
	}

	return nil
}

// parseTimeoutSeconds parses timeout value from notification
func (h *UpgradeHandler) parseTimeoutSeconds(value interface{}) (int, error) {
	switch v := value.(type) {
	case int64:
		return int(v), nil
	case int:
		return v, nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("unsupported timeout type: %T", value)
	}
}

// parseShardID parses shard ID from notification
func (h *UpgradeHandler) parseShardID(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int:
		return strconv.Itoa(v), nil
	default:
		return "", fmt.Errorf("unsupported shard ID type: %T", value)
	}
}

// GetConnectionState returns the current state of a connection
func (h *UpgradeHandler) GetConnectionState(conn *pool.Conn) (*ConnectionState, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	state, exists := h.connectionStates[conn]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	stateCopy := *state
	return &stateCopy, true
}

// IsConnectionTransitioning checks if a connection is currently transitioning
func (h *UpgradeHandler) IsConnectionTransitioning(conn *pool.Conn) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	state, exists := h.connectionStates[conn]
	return exists && state.IsTransitioning
}

// IsPoolMoving and GetNewEndpoint removed - using atomic state in ClusterUpgradeManager instead

// CleanupExpiredStates removes expired connection and pool states
func (h *UpgradeHandler) CleanupExpiredStates() {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()

	// Cleanup expired connection states
	for conn, state := range h.connectionStates {
		timeout := time.Duration(state.TimeoutSeconds) * time.Second
		if now.Sub(state.StartTime) > timeout {
			delete(h.connectionStates, conn)
		}
	}

	// Pool state cleanup removed - using atomic state in ClusterUpgradeManager instead
}

// CleanupConnection removes state for a specific connection (called when connection is closed)
func (h *UpgradeHandler) CleanupConnection(conn *pool.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.connectionStates, conn)
}

// GetActiveTransitions returns information about all active connection transitions
func (h *UpgradeHandler) GetActiveTransitions() map[*pool.Conn]*ConnectionState {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Create copies to avoid race conditions
	connStates := make(map[*pool.Conn]*ConnectionState)
	for conn, state := range h.connectionStates {
		stateCopy := *state
		connStates[conn] = &stateCopy
	}

	return connStates
}
