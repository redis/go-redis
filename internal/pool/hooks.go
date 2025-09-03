package pool

import (
	"context"
	"sync"
)

// PoolHook defines the interface for connection lifecycle hooks.
type PoolHook interface {
	// OnGet is called when a connection is retrieved from the pool.
	// It can modify the connection or return an error to prevent its use.
	// It has isNewConn flag to indicate if this is a new connection (rather than idle from the pool)
	// The flag can be used for gathering metrics on pool hit/miss ratio.
	OnGet(ctx context.Context, conn *Conn, isNewConn bool) error

	// OnPut is called when a connection is returned to the pool.
	// It returns whether the connection should be pooled and whether it should be removed.
	OnPut(ctx context.Context, conn *Conn) (shouldPool bool, shouldRemove bool, err error)
}

// PoolHookManager manages multiple pool hooks.
type PoolHookManager struct {
	hooks   []PoolHook
	hooksMu sync.RWMutex
}

// NewPoolHookManager creates a new pool hook manager.
func NewPoolHookManager() *PoolHookManager {
	return &PoolHookManager{
		hooks: make([]PoolHook, 0),
	}
}

// AddHook adds a pool hook to the manager.
// Hooks are called in the order they were added.
func (phm *PoolHookManager) AddHook(hook PoolHook) {
	phm.hooksMu.Lock()
	defer phm.hooksMu.Unlock()
	phm.hooks = append(phm.hooks, hook)
}

// RemoveHook removes a pool hook from the manager.
func (phm *PoolHookManager) RemoveHook(hook PoolHook) {
	phm.hooksMu.Lock()
	defer phm.hooksMu.Unlock()

	for i, h := range phm.hooks {
		if h == hook {
			// Remove hook by swapping with last element and truncating
			phm.hooks[i] = phm.hooks[len(phm.hooks)-1]
			phm.hooks = phm.hooks[:len(phm.hooks)-1]
			break
		}
	}
}

// ProcessOnGet calls all OnGet hooks in order.
// If any hook returns an error, processing stops and the error is returned.
func (phm *PoolHookManager) ProcessOnGet(ctx context.Context, conn *Conn, isNewConn bool) error {
	phm.hooksMu.RLock()
	defer phm.hooksMu.RUnlock()

	for _, hook := range phm.hooks {
		if err := hook.OnGet(ctx, conn, isNewConn); err != nil {
			return err
		}
	}
	return nil
}

// ProcessOnPut calls all OnPut hooks in order.
// The first hook that returns shouldRemove=true or shouldPool=false will stop processing.
func (phm *PoolHookManager) ProcessOnPut(ctx context.Context, conn *Conn) (shouldPool bool, shouldRemove bool, err error) {
	phm.hooksMu.RLock()
	defer phm.hooksMu.RUnlock()

	shouldPool = true // Default to pooling the connection

	for _, hook := range phm.hooks {
		hookShouldPool, hookShouldRemove, hookErr := hook.OnPut(ctx, conn)

		if hookErr != nil {
			return false, true, hookErr
		}

		// If any hook says to remove or not pool, respect that decision
		if hookShouldRemove {
			return false, true, nil
		}

		if !hookShouldPool {
			shouldPool = false
		}
	}

	return shouldPool, false, nil
}

// GetHookCount returns the number of registered hooks (for testing).
func (phm *PoolHookManager) GetHookCount() int {
	phm.hooksMu.RLock()
	defer phm.hooksMu.RUnlock()
	return len(phm.hooks)
}

// GetHooks returns a copy of all registered hooks.
func (phm *PoolHookManager) GetHooks() []PoolHook {
	phm.hooksMu.RLock()
	defer phm.hooksMu.RUnlock()

	hooks := make([]PoolHook, len(phm.hooks))
	copy(hooks, phm.hooks)
	return hooks
}
