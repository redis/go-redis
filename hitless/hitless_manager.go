package hitless

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/interfaces"
)

// MovingOperationKey provides a unique key for tracking MOVING operations
// that combines sequence ID with connection identifier to handle duplicate
// sequence IDs across multiple connections to the same node.
type MovingOperationKey struct {
	SeqID  int64  // Sequence ID from MOVING notification
	ConnID uint64 // Unique connection identifier
}

// String returns a string representation of the key for debugging
func (k MovingOperationKey) String() string {
	return fmt.Sprintf("seq:%d-conn:%d", k.SeqID, k.ConnID)
}

// HitlessManager provides a simplified hitless upgrade functionality.
type HitlessManager struct {
	mu sync.RWMutex

	client  interfaces.ClientInterface
	config  *Config
	options interfaces.OptionsInterface

	// MOVING operation tracking with composite keys
	activeMovingOps map[MovingOperationKey]*MovingOperation

	closed bool
}

// MovingOperation tracks an active MOVING operation.
type MovingOperation struct {
	SeqID       int64
	NewEndpoint string
	StartTime   time.Time
	Deadline    time.Time
}

// NewHitlessManager creates a new simplified hitless manager.
func NewHitlessManager(client interfaces.ClientInterface, config *Config) (*HitlessManager, error) {
	if client == nil {
		return nil, ErrInvalidClient
	}

	hm := &HitlessManager{
		client:          client,
		options:         client.GetOptions(),
		config:          config.Clone(),
		activeMovingOps: make(map[MovingOperationKey]*MovingOperation),
	}

	// Set up push notification handling
	if err := hm.setupPushNotifications(); err != nil {
		return nil, err
	}

	return hm, nil
}

// setupPushNotifications sets up push notification handling by registering with the client's processor.
func (hm *HitlessManager) setupPushNotifications() error {
	processor := hm.client.GetPushProcessor()
	if processor == nil {
		return ErrInvalidClient // Client doesn't support push notifications
	}

	// Create our notification handler
	handler := &NotificationHandler{manager: hm}

	// Register handlers for all hitless upgrade notifications with the client's processor
	if err := processor.RegisterHandler("MOVING", handler, true); err != nil {
		return err
	}
	if err := processor.RegisterHandler("MIGRATING", handler, true); err != nil {
		return err
	}
	if err := processor.RegisterHandler("MIGRATED", handler, true); err != nil {
		return err
	}
	if err := processor.RegisterHandler("FAILING_OVER", handler, true); err != nil {
		return err
	}
	if err := processor.RegisterHandler("FAILED_OVER", handler, true); err != nil {
		return err
	}

	return nil
}

// TrackMovingOperationWithConnID starts a new MOVING operation with a specific connection ID.
func (hm *HitlessManager) TrackMovingOperationWithConnID(ctx context.Context, newEndpoint string, deadline time.Time, seqID int64, connID uint64) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Create composite key
	key := MovingOperationKey{
		SeqID:  seqID,
		ConnID: connID,
	}

	// Check for duplicate operation
	if _, exists := hm.activeMovingOps[key]; exists {
		// Duplicate MOVING notification, ignore
		internal.Logger.Printf(ctx, "Duplicate MOVING operation ignored: %s", key.String())
		return nil
	}

	// Create MOVING operation record
	movingOp := &MovingOperation{
		SeqID:       seqID,
		NewEndpoint: newEndpoint,
		StartTime:   time.Now(),
		Deadline:    deadline,
	}
	hm.activeMovingOps[key] = movingOp

	return nil
}

// UntrackOperationWithConnID completes a MOVING operation with a specific connection ID.
func (hm *HitlessManager) UntrackOperationWithConnID(seqID int64, connID uint64) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Create composite key
	key := MovingOperationKey{
		SeqID:  seqID,
		ConnID: connID,
	}

	// Remove from active operations
	delete(hm.activeMovingOps, key)
}

// GetActiveMovingOperations returns active operations with composite keys.
func (hm *HitlessManager) GetActiveMovingOperations() map[MovingOperationKey]*MovingOperation {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	result := make(map[MovingOperationKey]*MovingOperation)
	for key, op := range hm.activeMovingOps {
		result[key] = &MovingOperation{
			SeqID:       op.SeqID,
			NewEndpoint: op.NewEndpoint,
			StartTime:   op.StartTime,
			Deadline:    op.Deadline,
		}
	}
	return result
}

// IsHandoffInProgress returns true if any handoff is in progress.
func (hm *HitlessManager) IsHandoffInProgress() bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return len(hm.activeMovingOps) > 0
}

// Close closes the hitless manager.
func (hm *HitlessManager) Close() error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.closed {
		return nil
	}

	hm.closed = true
	return nil
}

// GetState returns current state
func (hm *HitlessManager) GetState() State {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	if len(hm.activeMovingOps) > 0 {
		return StateMoving
	}
	return StateIdle
}

// GetConfig returns the hitless manager configuration.
func (hm *HitlessManager) GetConfig() *Config {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.config.Clone()
}

// CreateConnectionProcessor creates a connection processor with this manager already set.
// Returns the processor as the shared interface type.
func (hm *HitlessManager) CreateConnectionProcessor(protocol int, baseDialer func(context.Context, string, string) (net.Conn, error)) *RedisConnectionProcessor {
	// Get pool size from client options for better worker defaults
	poolSize := 0
	if hm.options != nil {
		poolSize = hm.options.GetPoolSize()
	}

	processor := NewRedisConnectionProcessorWithPoolSize(protocol, baseDialer, hm.config, hm, poolSize)

	return processor
}
