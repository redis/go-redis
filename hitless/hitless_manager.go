package hitless

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/interfaces"
	"github.com/redis/go-redis/v9/internal/pool"
)



// Push notification type constants for hitless upgrades
const (
	NotificationMoving      = "MOVING"
	NotificationMigrating   = "MIGRATING"
	NotificationMigrated    = "MIGRATED"
	NotificationFailingOver = "FAILING_OVER"
	NotificationFailedOver  = "FAILED_OVER"
)

// hitlessNotificationTypes contains all notification types that hitless upgrades handles
var hitlessNotificationTypes = []string{
	NotificationMoving,
	NotificationMigrating,
	NotificationMigrated,
	NotificationFailingOver,
	NotificationFailedOver,
}

// NotificationHook is called before and after notification processing
// PreHook can modify the notification and return false to skip processing
// PostHook is called after successful processing
type NotificationHook interface {
	PreHook(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool)
	PostHook(ctx context.Context, notificationType string, notification []interface{}, result error)
}

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

// HitlessManager provides a simplified hitless upgrade functionality with hooks and atomic state.
type HitlessManager struct {
	client  interfaces.ClientInterface
	config  *Config
	options interfaces.OptionsInterface
	pool    pool.Pooler

	// MOVING operation tracking - using sync.Map for better concurrent performance
	activeMovingOps sync.Map // map[MovingOperationKey]*MovingOperation

	// Atomic state tracking - no locks needed for state queries
	activeOperationCount atomic.Int64 // Number of active operations
	closed               atomic.Bool  // Manager closed state

	// Notification hooks for extensibility
	hooks        []NotificationHook
	hooksMu      sync.RWMutex // Protects hooks slice
	poolHooksRef *PoolHook
}

// MovingOperation tracks an active MOVING operation.
type MovingOperation struct {
	SeqID       int64
	NewEndpoint string
	StartTime   time.Time
	Deadline    time.Time
}

// NewHitlessManager creates a new simplified hitless manager.
func NewHitlessManager(client interfaces.ClientInterface, pool pool.Pooler, config *Config) (*HitlessManager, error) {
	if client == nil {
		return nil, ErrInvalidClient
	}

	hm := &HitlessManager{
		client:  client,
		pool:    pool,
		options: client.GetOptions(),
		config:  config.Clone(),
		hooks:   make([]NotificationHook, 0),
	}

	// Set up push notification handling
	if err := hm.setupPushNotifications(); err != nil {
		return nil, err
	}

	return hm, nil
}

// GetPoolHook creates a pool hook with a custom dialer.
func (hm *HitlessManager) InitPoolHook(baseDialer func(context.Context, string, string) (net.Conn, error)) {
	poolHook := hm.createPoolHook(baseDialer)
	hm.pool.AddPoolHook(poolHook)
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
	for _, notificationType := range hitlessNotificationTypes {
		if err := processor.RegisterHandler(notificationType, handler, true); err != nil {
			return fmt.Errorf("failed to register handler for %s: %w", notificationType, err)
		}
	}

	return nil
}

// TrackMovingOperationWithConnID starts a new MOVING operation with a specific connection ID.
func (hm *HitlessManager) TrackMovingOperationWithConnID(ctx context.Context, newEndpoint string, deadline time.Time, seqID int64, connID uint64) error {
	// Create composite key
	key := MovingOperationKey{
		SeqID:  seqID,
		ConnID: connID,
	}

	// Create MOVING operation record
	movingOp := &MovingOperation{
		SeqID:       seqID,
		NewEndpoint: newEndpoint,
		StartTime:   time.Now(),
		Deadline:    deadline,
	}

	// Use LoadOrStore for atomic check-and-set operation
	if _, loaded := hm.activeMovingOps.LoadOrStore(key, movingOp); loaded {
		// Duplicate MOVING notification, ignore
		internal.Logger.Printf(ctx, "Duplicate MOVING operation ignored: %s", key.String())
		return nil
	}

	// Increment active operation count atomically
	hm.activeOperationCount.Add(1)

	return nil
}

// UntrackOperationWithConnID completes a MOVING operation with a specific connection ID.
func (hm *HitlessManager) UntrackOperationWithConnID(seqID int64, connID uint64) {
	// Create composite key
	key := MovingOperationKey{
		SeqID:  seqID,
		ConnID: connID,
	}

	// Remove from active operations atomically
	if _, loaded := hm.activeMovingOps.LoadAndDelete(key); loaded {
		// Decrement active operation count only if operation existed
		hm.activeOperationCount.Add(-1)
	}
}

// GetActiveMovingOperations returns active operations with composite keys.
func (hm *HitlessManager) GetActiveMovingOperations() map[MovingOperationKey]*MovingOperation {
	result := make(map[MovingOperationKey]*MovingOperation)

	// Iterate over sync.Map to build result
	hm.activeMovingOps.Range(func(key, value interface{}) bool {
		k := key.(MovingOperationKey)
		op := value.(*MovingOperation)

		// Create a copy to avoid sharing references
		result[k] = &MovingOperation{
			SeqID:       op.SeqID,
			NewEndpoint: op.NewEndpoint,
			StartTime:   op.StartTime,
			Deadline:    op.Deadline,
		}
		return true // Continue iteration
	})

	return result
}

// IsHandoffInProgress returns true if any handoff is in progress.
// Uses atomic counter for lock-free operation.
func (hm *HitlessManager) IsHandoffInProgress() bool {
	return hm.activeOperationCount.Load() > 0
}

// GetActiveOperationCount returns the number of active operations.
// Uses atomic counter for lock-free operation.
func (hm *HitlessManager) GetActiveOperationCount() int64 {
	return hm.activeOperationCount.Load()
}

// Close closes the hitless manager.
func (hm *HitlessManager) Close() error {
	// Use atomic operation for thread-safe close check
	if !hm.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Shutdown the pool hook if it exists
	if hm.poolHooksRef != nil {
		// Use a timeout to prevent hanging indefinitely
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := hm.poolHooksRef.Shutdown(shutdownCtx)
		if err != nil {
			// was not able to close pool hook, keep closed state false
			hm.closed.Store(false)
			return err
		}
		// Remove the pool hook from the pool
		if hm.pool != nil {
			hm.pool.RemovePoolHook(hm.poolHooksRef)
		}
	}

	// Clear all active operations
	hm.activeMovingOps.Range(func(key, value interface{}) bool {
		hm.activeMovingOps.Delete(key)
		return true
	})

	// Reset counter
	hm.activeOperationCount.Store(0)

	return nil
}

// GetState returns current state using atomic counter for lock-free operation.
func (hm *HitlessManager) GetState() State {
	if hm.activeOperationCount.Load() > 0 {
		return StateMoving
	}
	return StateIdle
}

// processPreHooks calls all pre-hooks and returns the modified notification and whether to continue processing.
func (hm *HitlessManager) processPreHooks(ctx context.Context, notificationType string, notification []interface{}) ([]interface{}, bool) {
	hm.hooksMu.RLock()
	defer hm.hooksMu.RUnlock()

	currentNotification := notification

	for _, hook := range hm.hooks {
		modifiedNotification, shouldContinue := hook.PreHook(ctx, notificationType, currentNotification)
		if !shouldContinue {
			return modifiedNotification, false
		}
		currentNotification = modifiedNotification
	}

	return currentNotification, true
}

// processPostHooks calls all post-hooks with the processing result.
func (hm *HitlessManager) processPostHooks(ctx context.Context, notificationType string, notification []interface{}, result error) {
	hm.hooksMu.RLock()
	defer hm.hooksMu.RUnlock()

	for _, hook := range hm.hooks {
		hook.PostHook(ctx, notificationType, notification, result)
	}
}

// createPoolHook creates a pool hook with this manager already set.
func (hm *HitlessManager) createPoolHook(baseDialer func(context.Context, string, string) (net.Conn, error)) *PoolHook {
	if hm.poolHooksRef != nil {
		return hm.poolHooksRef
	}
	// Get pool size from client options for better worker defaults
	poolSize := 0
	if hm.options != nil {
		poolSize = hm.options.GetPoolSize()
	}

	hm.poolHooksRef = NewPoolHookWithPoolSize(baseDialer, hm.options.GetNetwork(), hm.config, hm, poolSize)
	hm.poolHooksRef.SetPool(hm.pool)

	return hm.poolHooksRef
}
