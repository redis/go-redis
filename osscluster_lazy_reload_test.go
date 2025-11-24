package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestLazyReloadQueueBehavior tests that LazyReload properly queues reload requests
func TestLazyReloadQueueBehavior(t *testing.T) {
	t.Run("SingleReload", func(t *testing.T) {
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate reload work
			return &clusterState{}, nil
		})

		// Trigger one reload
		holder.LazyReload()

		// Wait for reload to complete
		time.Sleep(300 * time.Millisecond)

		if count := reloadCount.Load(); count != 1 {
			t.Errorf("Expected 1 reload, got %d", count)
		}
	})

	t.Run("ConcurrentReloadsDeduplication", func(t *testing.T) {
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate reload work
			return &clusterState{}, nil
		})

		// Trigger multiple reloads concurrently
		for i := 0; i < 10; i++ {
			go holder.LazyReload()
		}

		// Wait for all to complete
		time.Sleep(100 * time.Millisecond)

		// Should only reload once (all concurrent calls deduplicated)
		if count := reloadCount.Load(); count != 1 {
			t.Errorf("Expected 1 reload (deduplication), got %d", count)
		}
	})

	t.Run("PendingReloadDuringCooldown", func(t *testing.T) {
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(10 * time.Millisecond) // Simulate reload work
			return &clusterState{}, nil
		})

		// Trigger first reload
		holder.LazyReload()

		// Wait for reload to complete but still in cooldown
		time.Sleep(50 * time.Millisecond)

		// Trigger second reload during cooldown period
		holder.LazyReload()

		// Wait for second reload to complete
		time.Sleep(300 * time.Millisecond)

		// Should have reloaded twice (second request queued and executed)
		if count := reloadCount.Load(); count != 2 {
			t.Errorf("Expected 2 reloads (queued during cooldown), got %d", count)
		}
	})

	t.Run("MultiplePendingReloadsCollapsed", func(t *testing.T) {
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(10 * time.Millisecond) // Simulate reload work
			return &clusterState{}, nil
		})

		// Trigger first reload
		holder.LazyReload()

		// Wait for reload to start
		time.Sleep(5 * time.Millisecond)

		// Trigger multiple reloads during active reload + cooldown
		for i := 0; i < 10; i++ {
			holder.LazyReload()
			time.Sleep(5 * time.Millisecond)
		}

		// Wait for all to complete
		time.Sleep(400 * time.Millisecond)

		// Should have reloaded exactly twice:
		// 1. Initial reload
		// 2. One more reload for all the pending requests (collapsed into one)
		if count := reloadCount.Load(); count != 2 {
			t.Errorf("Expected 2 reloads (initial + collapsed pending), got %d", count)
		}
	})

	t.Run("ReloadAfterCooldownPeriod", func(t *testing.T) {
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(10 * time.Millisecond) // Simulate reload work
			return &clusterState{}, nil
		})

		// Trigger first reload
		holder.LazyReload()

		// Wait for reload + cooldown to complete
		time.Sleep(300 * time.Millisecond)

		// Trigger second reload after cooldown
		holder.LazyReload()

		// Wait for second reload to complete
		time.Sleep(300 * time.Millisecond)

		// Should have reloaded twice (separate reload cycles)
		if count := reloadCount.Load(); count != 2 {
			t.Errorf("Expected 2 reloads (separate cycles), got %d", count)
		}
	})

	t.Run("ErrorDuringReload", func(t *testing.T) {
		var reloadCount atomic.Int32
		var shouldFail atomic.Bool
		shouldFail.Store(true)

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			if shouldFail.Load() {
				return nil, context.DeadlineExceeded
			}
			return &clusterState{}, nil
		})

		// Trigger reload that will fail
		holder.LazyReload()

		// Wait for failed reload
		time.Sleep(50 * time.Millisecond)

		// Trigger another reload (should succeed now)
		shouldFail.Store(false)
		holder.LazyReload()

		// Wait for successful reload
		time.Sleep(300 * time.Millisecond)

		// Should have attempted reload twice (first failed, second succeeded)
		if count := reloadCount.Load(); count != 2 {
			t.Errorf("Expected 2 reload attempts, got %d", count)
		}
	})

	t.Run("CascadingSMigratedScenario", func(t *testing.T) {
		// Simulate the real-world scenario: multiple SMIGRATED notifications
		// arriving in quick succession from different node clients
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(20 * time.Millisecond) // Simulate realistic reload time
			return &clusterState{}, nil
		})

		// Simulate 5 SMIGRATED notifications arriving within 100ms
		for i := 0; i < 5; i++ {
			go holder.LazyReload()
			time.Sleep(20 * time.Millisecond)
		}

		// Wait for all reloads to complete
		time.Sleep(500 * time.Millisecond)

		// Should reload at most 2 times:
		// 1. First notification triggers reload
		// 2. Notifications 2-5 collapse into one pending reload
		count := reloadCount.Load()
		if count < 1 || count > 2 {
			t.Errorf("Expected 1-2 reloads for cascading scenario, got %d", count)
		}
	})
}
