package maintnotifications

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// mockNetConn implements net.Conn for testing
type mockNetConn struct {
	addr           string
	shouldFailInit bool
}

func (m *mockNetConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockNetConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockNetConn) Close() error                       { return nil }
func (m *mockNetConn) LocalAddr() net.Addr                { return &mockAddr{m.addr} }
func (m *mockNetConn) RemoteAddr() net.Addr               { return &mockAddr{m.addr} }
func (m *mockNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConn) SetWriteDeadline(t time.Time) error { return nil }

type mockAddr struct {
	addr string
}

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return m.addr }

// createMockPoolConnection creates a mock pool connection for testing
func createMockPoolConnection() *pool.Conn {
	mockNetConn := &mockNetConn{addr: "test:6379"}
	conn := pool.NewConn(mockNetConn)
	conn.SetUsable(true) // Make connection usable for testing
	return conn
}

// mockPool implements pool.Pooler for testing
type mockPool struct {
	removedConnections map[uint64]bool
	mu                 sync.Mutex
}

func (mp *mockPool) NewConn(ctx context.Context) (*pool.Conn, error) {
	return nil, errors.New("not implemented")
}

func (mp *mockPool) CloseConn(conn *pool.Conn) error {
	return nil
}

func (mp *mockPool) Get(ctx context.Context) (*pool.Conn, error) {
	return nil, errors.New("not implemented")
}

func (mp *mockPool) Put(ctx context.Context, conn *pool.Conn) {
	// Not implemented for testing
}

func (mp *mockPool) Remove(ctx context.Context, conn *pool.Conn, reason error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Use pool.Conn directly - no adapter needed
	mp.removedConnections[conn.GetID()] = true
}

// WasRemoved safely checks if a connection was removed from the pool
func (mp *mockPool) WasRemoved(connID uint64) bool {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.removedConnections[connID]
}

func (mp *mockPool) Len() int {
	return 0
}

func (mp *mockPool) IdleLen() int {
	return 0
}

func (mp *mockPool) Stats() *pool.Stats {
	return &pool.Stats{}
}

func (mp *mockPool) Size() int {
	return 0
}

func (mp *mockPool) AddPoolHook(hook pool.PoolHook) {
	// Mock implementation - do nothing
}

func (mp *mockPool) RemovePoolHook(hook pool.PoolHook) {
	// Mock implementation - do nothing
}

func (mp *mockPool) Close() error {
	return nil
}

// TestConnectionHook tests the Redis connection processor functionality
func TestConnectionHook(t *testing.T) {
	// Create a base dialer for testing
	baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return &mockNetConn{addr: addr}, nil
	}

	t.Run("SuccessfulEventDrivenHandoff", func(t *testing.T) {
		config := &Config{
			Mode:              ModeAuto,
			EndpointType:      EndpointTypeAuto,
			MaxWorkers:        1,  // Use only 1 worker to ensure synchronization
			HandoffQueueSize:  10, // Explicit queue size to avoid 0-size queue
			MaxHandoffRetries: 3,
		}
		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 12345); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Verify connection is marked for handoff
		if !conn.ShouldHandoff() {
			t.Fatal("Connection should be marked for handoff")
		}
		// Set a mock initialization function with synchronization
		initConnCalled := make(chan bool, 1)
		proceedWithInit := make(chan bool, 1)
		initConnFunc := func(ctx context.Context, cn *pool.Conn) error {
			select {
			case initConnCalled <- true:
			default:
			}
			// Wait for test to proceed
			<-proceedWithInit
			return nil
		}
		conn.SetInitConnFunc(initConnFunc)

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should not error: %v", err)
		}

		// Should pool the connection immediately (handoff queued)
		if !shouldPool {
			t.Error("Connection should be pooled immediately with event-driven handoff")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when queuing handoff")
		}

		// Wait for initialization to be called (indicates handoff started)
		select {
		case <-initConnCalled:
			// Good, initialization was called
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for initialization function to be called")
		}

		// Connection should be in pending map while initialization is blocked
		if _, pending := processor.GetPendingMap().Load(conn.GetID()); !pending {
			t.Error("Connection should be in pending handoffs map")
		}

		// Allow initialization to proceed
		proceedWithInit <- true

		// Wait for handoff to complete with proper timeout and polling
		timeout := time.After(2 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		handoffCompleted := false
		for !handoffCompleted {
			select {
			case <-timeout:
				t.Fatal("Timeout waiting for handoff to complete")
			case <-ticker.C:
				if _, pending := processor.GetPendingMap().Load(conn); !pending {
					handoffCompleted = true
				}
			}
		}

		// Verify handoff completed (removed from pending map)
		if _, pending := processor.GetPendingMap().Load(conn); pending {
			t.Error("Connection should be removed from pending map after handoff")
		}

		// Verify connection is usable again
		if !conn.IsUsable() {
			t.Error("Connection should be usable after successful handoff")
		}

		// Verify handoff state is cleared
		if conn.ShouldHandoff() {
			t.Error("Connection should not be marked for handoff after completion")
		}
	})

	t.Run("HandoffNotNeeded", func(t *testing.T) {
		processor := NewPoolHook(baseDialer, "tcp", nil, nil)
		conn := createMockPoolConnection()
		// Don't mark for handoff

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should not error when handoff not needed: %v", err)
		}

		// Should pool the connection normally
		if !shouldPool {
			t.Error("Connection should be pooled when no handoff needed")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when no handoff needed")
		}
	})

	t.Run("EmptyEndpoint", func(t *testing.T) {
		processor := NewPoolHook(baseDialer, "tcp", nil, nil)
		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("", 12345); err != nil { // Empty endpoint
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should not error with empty endpoint: %v", err)
		}

		// Should pool the connection (empty endpoint clears state)
		if !shouldPool {
			t.Error("Connection should be pooled after clearing empty endpoint")
		}
		if shouldRemove {
			t.Error("Connection should not be removed after clearing empty endpoint")
		}

		// State should be cleared
		if conn.ShouldHandoff() {
			t.Error("Connection should not be marked for handoff after clearing empty endpoint")
		}
	})

	t.Run("EventDrivenHandoffDialerError", func(t *testing.T) {
		// Create a failing base dialer
		failingDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, errors.New("dial failed")
		}

		config := &Config{
			Mode:              ModeAuto,
			EndpointType:      EndpointTypeAuto,
			MaxWorkers:        2,
			HandoffQueueSize:  10,
			MaxHandoffRetries: 2,                      // Reduced retries for faster test
			HandoffTimeout:    500 * time.Millisecond, // Shorter timeout for faster test
		}
		processor := NewPoolHook(failingDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 12345); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should not return error to caller: %v", err)
		}

		// Should pool the connection initially (handoff queued)
		if !shouldPool {
			t.Error("Connection should be pooled initially with event-driven handoff")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when queuing handoff")
		}

		// Wait for handoff to complete and fail with proper timeout and polling
		timeout := time.After(3 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		// wait for handoff to start
		time.Sleep(50 * time.Millisecond)
		handoffCompleted := false
		for !handoffCompleted {
			select {
			case <-timeout:
				t.Fatal("Timeout waiting for failed handoff to complete")
			case <-ticker.C:
				if _, pending := processor.GetPendingMap().Load(conn.GetID()); !pending {
					handoffCompleted = true
				}
			}
		}

		// Connection should be removed from pending map after failed handoff
		if _, pending := processor.GetPendingMap().Load(conn.GetID()); pending {
			t.Error("Connection should be removed from pending map after failed handoff")
		}

		// Wait for retries to complete (with MaxHandoffRetries=2, it will retry twice then give up)
		// Each retry has a delay of handoffTimeout/2 = 250ms, so wait for all retries to complete
		time.Sleep(800 * time.Millisecond)

		// After max retries are reached, the connection should be removed from pool
		// and handoff state should be cleared
		if conn.ShouldHandoff() {
			t.Error("Connection should not be marked for handoff after max retries reached")
		}

		t.Logf("EventDrivenHandoffDialerError test completed successfully")
	})

	t.Run("BufferedDataRESP2", func(t *testing.T) {
		processor := NewPoolHook(baseDialer, "tcp", nil, nil)
		conn := createMockPoolConnection()

		// For this test, we'll just verify the logic works for connections without buffered data
		// The actual buffered data detection is handled by the pool's connection health check
		// which is outside the scope of the Redis connection processor

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should not error: %v", err)
		}

		// Should pool the connection normally (no buffered data in mock)
		if !shouldPool {
			t.Error("Connection should be pooled when no buffered data")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when no buffered data")
		}
	})

	t.Run("OnGet", func(t *testing.T) {
		processor := NewPoolHook(baseDialer, "tcp", nil, nil)
		conn := createMockPoolConnection()

		ctx := context.Background()
		err := processor.OnGet(ctx, conn, false)
		if err != nil {
			t.Errorf("OnGet should not error for normal connection: %v", err)
		}
	})

	t.Run("OnGetWithPendingHandoff", func(t *testing.T) {
		config := &Config{
			Mode:              ModeAuto,
			EndpointType:      EndpointTypeAuto,
			MaxWorkers:        2,
			HandoffQueueSize:  10,
			MaxHandoffRetries: 3, // Explicit queue size to avoid 0-size queue
		}
		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()

		// Simulate a pending handoff by marking for handoff and queuing
		conn.MarkForHandoff("new-endpoint:6379", 12345)
		processor.GetPendingMap().Store(conn.GetID(), int64(12345)) // Store connID -> seqID
		conn.MarkQueuedForHandoff()                                 // Mark as queued (sets usable=false)

		ctx := context.Background()
		err := processor.OnGet(ctx, conn, false)
		if err != ErrConnectionMarkedForHandoff {
			t.Errorf("Expected ErrConnectionMarkedForHandoff, got %v", err)
		}

		// Clean up
		processor.GetPendingMap().Delete(conn)
	})

	t.Run("EventDrivenStateManagement", func(t *testing.T) {
		processor := NewPoolHook(baseDialer, "tcp", nil, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()

		// Test initial state - no pending handoffs
		if _, pending := processor.GetPendingMap().Load(conn); pending {
			t.Error("New connection should not have pending handoffs")
		}

		// Test adding to pending map
		conn.MarkForHandoff("new-endpoint:6379", 12345)
		processor.GetPendingMap().Store(conn.GetID(), int64(12345)) // Store connID -> seqID
		conn.MarkQueuedForHandoff()                                 // Mark as queued (sets usable=false)

		if _, pending := processor.GetPendingMap().Load(conn.GetID()); !pending {
			t.Error("Connection should be in pending map")
		}

		// Test OnGet with pending handoff
		ctx := context.Background()
		err := processor.OnGet(ctx, conn, false)
		if err != ErrConnectionMarkedForHandoff {
			t.Error("Should return ErrConnectionMarkedForHandoff for pending connection")
		}

		// Test removing from pending map and clearing handoff state
		processor.GetPendingMap().Delete(conn)
		if _, pending := processor.GetPendingMap().Load(conn); pending {
			t.Error("Connection should be removed from pending map")
		}

		// Clear handoff state to simulate completed handoff
		conn.ClearHandoffState()
		conn.SetUsable(true) // Make connection usable again

		// Test OnGet without pending handoff
		err = processor.OnGet(ctx, conn, false)
		if err != nil {
			t.Errorf("Should not return error for non-pending connection: %v", err)
		}
	})

	t.Run("EventDrivenQueueOptimization", func(t *testing.T) {
		// Create processor with small queue to test optimization features
		config := &Config{
			MaxWorkers:        3,
			HandoffQueueSize:  2,
			MaxHandoffRetries: 3, // Small queue to trigger optimizations
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Add small delay to simulate network latency
			time.Sleep(10 * time.Millisecond)
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		// Create multiple connections that need handoff to fill the queue
		connections := make([]*pool.Conn, 5)
		for i := 0; i < 5; i++ {
			connections[i] = createMockPoolConnection()
			if err := connections[i].MarkForHandoff("new-endpoint:6379", int64(i)); err != nil {
				t.Fatalf("Failed to mark conn[%d] for handoff: %v", i, err)
			}
			// Set a mock initialization function
			connections[i].SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
				return nil
			})
		}

		ctx := context.Background()
		successCount := 0

		// Process connections - should trigger scaling and timeout logic
		for _, conn := range connections {
			shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
			if err != nil {
				t.Logf("OnPut returned error (expected with timeout): %v", err)
			}

			if shouldPool && !shouldRemove {
				successCount++
			}
		}

		// With timeout and scaling, most handoffs should eventually succeed
		if successCount == 0 {
			t.Error("Should have queued some handoffs with timeout and scaling")
		}

		t.Logf("Successfully queued %d handoffs with optimization features", successCount)

		// Give time for workers to process and scaling to occur
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("WorkerScalingBehavior", func(t *testing.T) {
		// Create processor with small queue to test scaling behavior
		config := &Config{
			MaxWorkers:        15, // Set to >= 10 to test explicit value preservation
			HandoffQueueSize:  1,
			MaxHandoffRetries: 3, // Very small queue to force scaling
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		// Verify initial worker count (should be 0 with on-demand workers)
		if processor.GetCurrentWorkers() != 0 {
			t.Errorf("Expected 0 initial workers with on-demand system, got %d", processor.GetCurrentWorkers())
		}
		if processor.GetMaxWorkers() != 15 {
			t.Errorf("Expected maxWorkers=15, got %d", processor.GetMaxWorkers())
		}

		// The on-demand worker behavior creates workers only when needed
		// This test just verifies the basic configuration is correct
		t.Logf("On-demand worker configuration verified - Max: %d, Current: %d",
			processor.GetMaxWorkers(), processor.GetCurrentWorkers())
	})

	t.Run("PassiveTimeoutRestoration", func(t *testing.T) {
		// Create processor with fast post-handoff duration for testing
		config := &Config{
			MaxWorkers:                 2,
			HandoffQueueSize:           10,
			MaxHandoffRetries:          3,                      // Allow retries for successful handoff
			PostHandoffRelaxedDuration: 100 * time.Millisecond, // Fast expiration for testing
			RelaxedTimeout:             5 * time.Second,
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		ctx := context.Background()

		// Create a connection and trigger handoff
		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a mock initialization function
		conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
			return nil
		})

		// Process the connection to trigger handoff
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("Handoff should succeed: %v", err)
		}
		if !shouldPool || shouldRemove {
			t.Error("Connection should be pooled after handoff")
		}

		// Wait for handoff to complete with proper timeout and polling
		timeout := time.After(1 * time.Second)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		handoffCompleted := false
		for !handoffCompleted {
			select {
			case <-timeout:
				t.Fatal("Timeout waiting for handoff to complete")
			case <-ticker.C:
				if _, pending := processor.GetPendingMap().Load(conn); !pending {
					handoffCompleted = true
				}
			}
		}

		// Verify relaxed timeout is set with deadline
		if !conn.HasRelaxedTimeout() {
			t.Error("Connection should have relaxed timeout after handoff")
		}

		// Test that timeout is still active before deadline
		// We'll use HasRelaxedTimeout which internally checks the deadline
		if !conn.HasRelaxedTimeout() {
			t.Error("Connection should still have active relaxed timeout before deadline")
		}

		// Wait for deadline to pass
		time.Sleep(150 * time.Millisecond) // 100ms deadline + buffer

		// Test that timeout is automatically restored after deadline
		// HasRelaxedTimeout should return false after deadline passes
		if conn.HasRelaxedTimeout() {
			t.Error("Connection should not have active relaxed timeout after deadline")
		}

		// Additional verification: calling HasRelaxedTimeout again should still return false
		// and should have cleared the internal timeout values
		if conn.HasRelaxedTimeout() {
			t.Error("Connection should not have relaxed timeout after deadline (second check)")
		}

		t.Logf("Passive timeout restoration test completed successfully")
	})

	t.Run("UsableFlagBehavior", func(t *testing.T) {
		config := &Config{
			MaxWorkers:        2,
			HandoffQueueSize:  10,
			MaxHandoffRetries: 3,
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		ctx := context.Background()

		// Create a new connection without setting it usable
		mockNetConn := &mockNetConn{addr: "test:6379"}
		conn := pool.NewConn(mockNetConn)

		// Initially, connection should not be usable (not initialized)
		if conn.IsUsable() {
			t.Error("New connection should not be usable before initialization")
		}

		// Simulate initialization by setting usable to true
		conn.SetUsable(true)
		if !conn.IsUsable() {
			t.Error("Connection should be usable after initialization")
		}

		// OnGet should succeed for usable connection
		err := processor.OnGet(ctx, conn, false)
		if err != nil {
			t.Errorf("OnGet should succeed for usable connection: %v", err)
		}

		// Mark connection for handoff
		if err := conn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a mock initialization function
		conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
			return nil
		})

		// Connection should still be usable until queued, but marked for handoff
		if !conn.IsUsable() {
			t.Error("Connection should still be usable after being marked for handoff (until queued)")
		}
		if !conn.ShouldHandoff() {
			t.Error("Connection should be marked for handoff")
		}

		// OnGet should fail for connection marked for handoff
		err = processor.OnGet(ctx, conn, false)
		if err == nil {
			t.Error("OnGet should fail for connection marked for handoff")
		}
		if err != ErrConnectionMarkedForHandoff {
			t.Errorf("Expected ErrConnectionMarkedForHandoff, got %v", err)
		}

		// Process the connection to trigger handoff
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should succeed: %v", err)
		}
		if !shouldPool || shouldRemove {
			t.Error("Connection should be pooled after handoff")
		}

		// Wait for handoff to complete
		time.Sleep(50 * time.Millisecond)

		// After handoff completion, connection should be usable again
		if !conn.IsUsable() {
			t.Error("Connection should be usable after handoff completion")
		}

		// OnGet should succeed again
		err = processor.OnGet(ctx, conn, false)
		if err != nil {
			t.Errorf("OnGet should succeed after handoff completion: %v", err)
		}

		t.Logf("Usable flag behavior test completed successfully")
	})

	t.Run("StaticQueueBehavior", func(t *testing.T) {
		config := &Config{
			MaxWorkers:        3,
			HandoffQueueSize:  50,
			MaxHandoffRetries: 3, // Explicit static queue size
		}

		processor := NewPoolHookWithPoolSize(baseDialer, "tcp", config, nil, 100) // Pool size: 100
		defer processor.Shutdown(context.Background())

		// Verify queue capacity matches configured size
		queueCapacity := cap(processor.GetHandoffQueue())
		if queueCapacity != 50 {
			t.Errorf("Expected queue capacity 50, got %d", queueCapacity)
		}

		// Test that queue size is static regardless of pool size
		// (No dynamic resizing should occur)

		ctx := context.Background()

		// Fill part of the queue
		for i := 0; i < 10; i++ {
			conn := createMockPoolConnection()
			if err := conn.MarkForHandoff("new-endpoint:6379", int64(i+1)); err != nil {
				t.Fatalf("Failed to mark conn[%d] for handoff: %v", i, err)
			}
			// Set a mock initialization function
			conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
				return nil
			})

			shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
			if err != nil {
				t.Errorf("Failed to queue handoff %d: %v", i, err)
			}

			if !shouldPool || shouldRemove {
				t.Errorf("conn[%d] should be pooled after handoff (shouldPool=%v, shouldRemove=%v)",
					i, shouldPool, shouldRemove)
			}
		}

		// Verify queue capacity remains static (the main purpose of this test)
		finalCapacity := cap(processor.GetHandoffQueue())

		if finalCapacity != 50 {
			t.Errorf("Queue capacity should remain static at 50, got %d", finalCapacity)
		}

		// Note: We don't check queue size here because workers process items quickly
		// The important thing is that the capacity remains static regardless of pool size
	})

	t.Run("ConnectionRemovalOnHandoffFailure", func(t *testing.T) {
		// Create a failing dialer that will cause handoff initialization to fail
		failingDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Return a connection that will fail during initialization
			return &mockNetConn{addr: addr, shouldFailInit: true}, nil
		}

		config := &Config{
			MaxWorkers:        2,
			HandoffQueueSize:  10,
			MaxHandoffRetries: 3,
		}

		processor := NewPoolHook(failingDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		// Create a mock pool that tracks removals
		mockPool := &mockPool{removedConnections: make(map[uint64]bool)}
		processor.SetPool(mockPool)

		ctx := context.Background()

		// Create a connection and mark it for handoff
		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a failing initialization function
		conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
			return fmt.Errorf("initialization failed")
		})

		// Process the connection - handoff should fail and connection should be removed
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)
		if err != nil {
			t.Errorf("OnPut should not error: %v", err)
		}
		if !shouldPool || shouldRemove {
			t.Error("Connection should be pooled after failed handoff attempt")
		}

		// Wait for handoff to be attempted and fail
		time.Sleep(100 * time.Millisecond)

		// Verify that the connection was removed from the pool
		if !mockPool.WasRemoved(conn.GetID()) {
			t.Errorf("conn[%d] should have been removed from pool after handoff failure", conn.GetID())
		}

		t.Logf("Connection removal on handoff failure test completed successfully")
	})

	t.Run("PostHandoffRelaxedTimeout", func(t *testing.T) {
		// Create config with short post-handoff duration for testing
		config := &Config{
			MaxWorkers:                 2,
			HandoffQueueSize:           10,
			MaxHandoffRetries:          3, // Allow retries for successful handoff
			RelaxedTimeout:             5 * time.Second,
			PostHandoffRelaxedDuration: 100 * time.Millisecond, // Short for testing
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 12345); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a mock initialization function
		conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
			return nil
		})

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.OnPut(ctx, conn)

		if err != nil {
			t.Fatalf("OnPut failed: %v", err)
		}

		if !shouldPool {
			t.Error("Connection should be pooled after successful handoff")
		}

		if shouldRemove {
			t.Error("Connection should not be removed after successful handoff")
		}

		// Wait for the handoff to complete (it happens asynchronously)
		timeout := time.After(1 * time.Second)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		handoffCompleted := false
		for !handoffCompleted {
			select {
			case <-timeout:
				t.Fatal("Timeout waiting for handoff to complete")
			case <-ticker.C:
				if _, pending := processor.GetPendingMap().Load(conn); !pending {
					handoffCompleted = true
				}
			}
		}

		// Verify that relaxed timeout was applied to the new connection
		if !conn.HasRelaxedTimeout() {
			t.Error("New connection should have relaxed timeout applied after handoff")
		}

		// Wait for the post-handoff duration to expire
		time.Sleep(150 * time.Millisecond) // Slightly longer than PostHandoffRelaxedDuration

		// Verify that relaxed timeout was automatically cleared
		if conn.HasRelaxedTimeout() {
			t.Error("Relaxed timeout should be automatically cleared after post-handoff duration")
		}
	})

	t.Run("MarkForHandoff returns error when already marked", func(t *testing.T) {
		conn := createMockPoolConnection()

		// First mark should succeed
		if err := conn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
			t.Fatalf("First MarkForHandoff should succeed: %v", err)
		}

		// Second mark should fail
		if err := conn.MarkForHandoff("another-endpoint:6379", 2); err == nil {
			t.Fatal("Second MarkForHandoff should return error")
		} else if err.Error() != "connection is already marked for handoff" {
			t.Fatalf("Expected specific error message, got: %v", err)
		}

		// Verify original handoff data is preserved
		if !conn.ShouldHandoff() {
			t.Fatal("Connection should still be marked for handoff")
		}
		if conn.GetHandoffEndpoint() != "new-endpoint:6379" {
			t.Fatalf("Expected original endpoint, got: %s", conn.GetHandoffEndpoint())
		}
		if conn.GetMovingSeqID() != 1 {
			t.Fatalf("Expected original sequence ID, got: %d", conn.GetMovingSeqID())
		}
	})

	t.Run("HandoffTimeoutConfiguration", func(t *testing.T) {
		// Test that HandoffTimeout from config is actually used
		customTimeout := 2 * time.Second
		config := &Config{
			MaxWorkers:        2,
			HandoffQueueSize:  10,
			HandoffTimeout:    customTimeout, // Custom timeout
			MaxHandoffRetries: 1,             // Single retry to speed up test
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		// Create a connection that will test the timeout
		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("test-endpoint:6379", 123); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a dialer that will check the context timeout
		var timeoutVerified int32 // Use atomic for thread safety
		conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
			// Check that the context has the expected timeout
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("Context should have a deadline")
				return errors.New("no deadline")
			}

			// The deadline should be approximately customTimeout from now
			expectedDeadline := time.Now().Add(customTimeout)
			timeDiff := deadline.Sub(expectedDeadline)
			if timeDiff < -500*time.Millisecond || timeDiff > 500*time.Millisecond {
				t.Errorf("Context deadline not as expected. Expected around %v, got %v (diff: %v)",
					expectedDeadline, deadline, timeDiff)
			} else {
				atomic.StoreInt32(&timeoutVerified, 1)
			}

			return nil // Successful handoff
		})

		// Trigger handoff
		shouldPool, shouldRemove, err := processor.OnPut(context.Background(), conn)
		if err != nil {
			t.Errorf("OnPut should not return error: %v", err)
		}

		// Connection should be queued for handoff
		if !shouldPool || shouldRemove {
			t.Errorf("Connection should be pooled for handoff processing")
		}

		// Wait for handoff to complete
		time.Sleep(500 * time.Millisecond)

		if atomic.LoadInt32(&timeoutVerified) == 0 {
			t.Error("HandoffTimeout was not properly applied to context")
		}

		t.Logf("HandoffTimeout configuration test completed successfully")
	})
}
