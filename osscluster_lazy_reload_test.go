package redis

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestClusterStateReloadCoalesces(t *testing.T) {
	for _, loadErr := range []error{nil, errors.New("load failed"), context.DeadlineExceeded} {
		name := "success"
		if loadErr != nil {
			name = loadErr.Error()
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				previous := &clusterState{}
				next := &clusterState{slots: []*clusterSlot{{start: 1, end: 1}}}
				calls, before, after := 0, 0, 0
				holder := newClusterStateHolder(func(context.Context) (*clusterState, error) {
					calls++
					<-release
					return next, loadErr
				}, time.Second)
				holder.state.Store(previous)
				holder.beforeReload = func(current, old *clusterState) {
					before++
					if current != next || old != previous || holder.state.Load() != previous {
						t.Error("beforeReload did not precede topology publication")
					}
				}
				holder.onReload = func(current, _ *clusterState) {
					after++
					if current != next || holder.state.Load() != next {
						t.Error("onReload did not follow topology publication")
					}
				}
				var callers sync.WaitGroup
				for range 10 {
					callers.Go(func() {
						state, err := holder.Reload(context.Background())
						if err != loadErr || (err == nil && state != next) || (err != nil && state != nil) {
							t.Errorf("Reload = (%p, %v), want shared load result", state, err)
						}
					})
				}
				holder.LazyReload()
				synctest.Wait()
				if calls != 1 {
					t.Errorf("concurrent foreground/background loads = %d, want 1", calls)
				}
				close(release)
				callers.Wait()
				// Drain the background reload's cooldown using the fake clock.
				time.Sleep(200 * time.Millisecond)
				synctest.Wait()
				if loadErr == nil {
					if before != 1 || after != 1 {
						t.Errorf("publication callbacks = %d/%d, want 1/1", before, after)
					}
				} else if before != 0 || after != 0 || holder.state.Load() != previous {
					t.Error("failed load changed the published topology")
				}
				_, _ = holder.Reload(context.Background())
				if calls != 2 {
					t.Errorf("loads after subsequent Reload = %d, want 2", calls)
				}
			})
		})
	}
}

func TestClusterStateReloadCancellation(t *testing.T) {
	t.Run("waiter cancellation does not cancel load", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			release := make(chan struct{})
			holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
				<-release
				return &clusterState{}, ctx.Err()
			}, time.Second)
			leader := make(chan error, 1)
			go func() {
				_, err := holder.Reload(context.Background())
				leader <- err
			}()
			synctest.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waiter := make(chan error, 1)
			go func() {
				_, err := holder.Reload(ctx)
				waiter <- err
			}()
			synctest.Wait()
			cancel()
			if err := <-waiter; err != context.Canceled {
				t.Errorf("waiter error = %v, want context.Canceled", err)
			}
			close(release)
			if err := <-leader; err != nil {
				t.Fatalf("waiter cancellation affected the load: %v", err)
			}
		})
	})
	for _, test := range []struct {
		name    string
		timeout bool
		loadErr error
	}{
		{"leader cancellation", false, context.Canceled},
		{"leader I/O deadline", true, &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}},
	} {
		t.Run(test.name+" does not fail a live waiter", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
					calls++
					if calls == 1 {
						<-ctx.Done()
						return nil, test.loadErr
					}
					return &clusterState{}, nil
				}, time.Second)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				leader := make(chan error, 1)
				go func() {
					_, err := holder.Reload(ctx)
					leader <- err
				}()
				synctest.Wait()
				waiter := make(chan error, 1)
				go func() {
					_, err := holder.Reload(context.Background())
					waiter <- err
				}()
				synctest.Wait()
				if test.timeout {
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				if err := <-leader; err != test.loadErr {
					t.Errorf("leader error = %v, want %v", err, test.loadErr)
				}
				if err := <-waiter; err != nil || calls != 2 {
					t.Fatalf("waiter error = %v, loads = %d; want nil, 2", err, calls)
				}
			})
		})
	}
}

// TestLazyReloadQueueBehavior tests that LazyReload properly queues reload requests
func TestLazyReloadQueueBehavior(t *testing.T) {
	t.Run("SingleReload", func(t *testing.T) {
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate reload work
			return &clusterState{}, nil
		}, 10*time.Second)

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
		}, 10*time.Second)

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
		}, 10*time.Second)

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
		}, 10*time.Second)

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
		}, 10*time.Second)

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
		}, 10*time.Second)

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

	t.Run("CascadingSMIGRATEDScenario", func(t *testing.T) {
		// Simulate the real-world scenario: multiple SMIGRATED notifications
		// arriving in quick succession from different node clients
		var reloadCount atomic.Int32

		holder := newClusterStateHolder(func(ctx context.Context) (*clusterState, error) {
			reloadCount.Add(1)
			time.Sleep(20 * time.Millisecond) // Simulate realistic reload time
			return &clusterState{}, nil
		}, 10*time.Second)

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
