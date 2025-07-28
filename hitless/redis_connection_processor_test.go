package hitless

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
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

func (mp *mockPool) GetPubSub(ctx context.Context) (*pool.Conn, error) {
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

func (mp *mockPool) Close() error {
	return nil
}

// TestRedisConnectionProcessor tests the Redis connection processor functionality
func TestRedisConnectionProcessor(t *testing.T) {
	// Create a base dialer for testing
	baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return &mockNetConn{addr: addr}, nil
	}

	t.Run("SuccessfulEventDrivenHandoff", func(t *testing.T) {
		processor := NewRedisConnectionProcessor(3, baseDialer, nil, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 12345); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a mock initialization function
		initConnCalled := false
		initConnFunc := func(ctx context.Context, cn *pool.Conn) error {
			initConnCalled = true
			return nil
		}
		conn.SetInitConnFunc(initConnFunc)

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should not error: %v", err)
		}

		// Should pool the connection immediately (handoff queued)
		if !shouldPool {
			t.Error("Connection should be pooled immediately with event-driven handoff")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when queuing handoff")
		}

		// Connection should be in pending map
		if _, pending := processor.pending.Load(conn); !pending {
			t.Error("Connection should be in pending handoffs map")
		}

		// Wait for handoff to complete
		time.Sleep(100 * time.Millisecond)

		// Verify handoff completed (removed from pending map)
		if _, pending := processor.pending.Load(conn); pending {
			t.Error("Connection should be removed from pending map after handoff")
		}

		// Verify handoff state was cleared
		if conn.ShouldHandoff() {
			t.Error("Connection should not be marked for handoff after successful handoff")
		}

		// Verify initialization was called
		if !initConnCalled {
			t.Error("InitConn should have been called")
		}
	})

	t.Run("HandoffNotNeeded", func(t *testing.T) {
		processor := NewRedisConnectionProcessor(3, baseDialer, nil, nil)
		conn := createMockPoolConnection()
		// Don't mark for handoff

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should not error when handoff not needed: %v", err)
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
		processor := NewRedisConnectionProcessor(3, baseDialer, nil, nil)
		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("", 12345); err != nil { // Empty endpoint
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should not error with empty endpoint: %v", err)
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

		processor := NewRedisConnectionProcessor(3, failingDialer, nil, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 12345); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should not return error to caller: %v", err)
		}

		// Should pool the connection initially (handoff queued)
		if !shouldPool {
			t.Error("Connection should be pooled initially with event-driven handoff")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when queuing handoff")
		}

		// Wait for handoff to complete and fail
		time.Sleep(100 * time.Millisecond)

		// Connection should be removed from pending map after failed handoff
		if _, pending := processor.pending.Load(conn); pending {
			t.Error("Connection should be removed from pending map after failed handoff")
		}

		// Handoff state should still be set (since handoff failed)
		if !conn.ShouldHandoff() {
			t.Error("Connection should still be marked for handoff after failed handoff")
		}
	})

	t.Run("BufferedDataRESP2", func(t *testing.T) {
		processor := NewRedisConnectionProcessor(2, baseDialer, nil, nil)
		conn := createMockPoolConnection()

		// For this test, we'll just verify the logic works for connections without buffered data
		// The actual buffered data detection is handled by the pool's connection health check
		// which is outside the scope of the Redis connection processor

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should not error: %v", err)
		}

		// Should pool the connection normally (no buffered data in mock)
		if !shouldPool {
			t.Error("Connection should be pooled when no buffered data")
		}
		if shouldRemove {
			t.Error("Connection should not be removed when no buffered data")
		}
	})

	t.Run("ProcessConnectionOnGet", func(t *testing.T) {
		processor := NewRedisConnectionProcessor(3, baseDialer, nil, nil)
		conn := createMockPoolConnection()

		ctx := context.Background()
		err := processor.ProcessConnectionOnGet(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnGet should not error for normal connection: %v", err)
		}
	})

	t.Run("ProcessConnectionOnGetWithPendingHandoff", func(t *testing.T) {
		processor := NewRedisConnectionProcessor(3, baseDialer, nil, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()

		// Simulate a pending handoff by adding to pending map
		resultChan := make(chan HandoffResult, 1)
		processor.pending.Store(conn, resultChan)

		ctx := context.Background()
		err := processor.ProcessConnectionOnGet(ctx, conn)
		if err != ErrConnectionMarkedForHandoff {
			t.Errorf("Expected ErrConnectionMarkedForHandoff, got %v", err)
		}

		// Clean up
		processor.pending.Delete(conn)
	})

	t.Run("EventDrivenStateManagement", func(t *testing.T) {
		processor := NewRedisConnectionProcessor(3, baseDialer, nil, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()

		// Test initial state - no pending handoffs
		if _, pending := processor.pending.Load(conn); pending {
			t.Error("New connection should not have pending handoffs")
		}

		// Test adding to pending map
		resultChan := make(chan HandoffResult, 1)
		processor.pending.Store(conn, resultChan)

		if _, pending := processor.pending.Load(conn); !pending {
			t.Error("Connection should be in pending map")
		}

		// Test ProcessConnectionOnGet with pending handoff
		ctx := context.Background()
		err := processor.ProcessConnectionOnGet(ctx, conn)
		if err != ErrConnectionMarkedForHandoff {
			t.Error("Should return ErrConnectionMarkedForHandoff for pending connection")
		}

		// Test removing from pending map
		processor.pending.Delete(conn)
		if _, pending := processor.pending.Load(conn); pending {
			t.Error("Connection should be removed from pending map")
		}

		// Test ProcessConnectionOnGet without pending handoff
		err = processor.ProcessConnectionOnGet(ctx, conn)
		if err != nil {
			t.Errorf("Should not return error for non-pending connection: %v", err)
		}
	})

	t.Run("EventDrivenQueueOptimization", func(t *testing.T) {
		// Create processor with small queue to test optimization features
		config := &Config{
			MinWorkers:       1,
			MaxWorkers:       3,
			HandoffQueueSize: 2, // Small queue to trigger optimizations
			LogLevel:         3, // Debug level to see optimization logs
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Add small delay to simulate network latency
			time.Sleep(10 * time.Millisecond)
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewRedisConnectionProcessor(3, baseDialer, config, nil)
		defer processor.Shutdown(context.Background())

		// Create multiple connections that need handoff to fill the queue
		connections := make([]*pool.Conn, 5)
		for i := 0; i < 5; i++ {
			connections[i] = createMockPoolConnection()
			if err := connections[i].MarkForHandoff("new-endpoint:6379", int64(i)); err != nil {
				t.Fatalf("Failed to mark connection %d for handoff: %v", i, err)
			}
		}

		ctx := context.Background()
		successCount := 0

		// Process connections - should trigger scaling and timeout logic
		for _, conn := range connections {
			shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
			if err != nil {
				t.Logf("ProcessConnectionOnPut returned error (expected with timeout): %v", err)
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
			MinWorkers:       1,
			MaxWorkers:       4,
			HandoffQueueSize: 1, // Very small queue to force scaling
			LogLevel:         2, // Info level to see scaling logs
		}

		processor := NewRedisConnectionProcessor(3, baseDialer, config, nil)
		defer processor.Shutdown(context.Background())

		// Verify initial worker count and scaling level
		if processor.currentWorkers != 1 {
			t.Errorf("Expected 1 initial worker, got %d", processor.currentWorkers)
		}
		if processor.scaleLevel != 0 {
			t.Errorf("Processor should be at scale level 0 initially, got %d", processor.scaleLevel)
		}
		if processor.minWorkers != 1 {
			t.Errorf("Expected minWorkers=1, got %d", processor.minWorkers)
		}
		if processor.maxWorkers != 4 {
			t.Errorf("Expected maxWorkers=4, got %d", processor.maxWorkers)
		}

		// The scaling behavior is tested in other tests (ScaleDownDelayBehavior)
		// This test just verifies the basic configuration is correct
		t.Logf("Worker scaling configuration verified - Min: %d, Max: %d, Current: %d",
			processor.minWorkers, processor.maxWorkers, processor.currentWorkers)
	})

	t.Run("PassiveTimeoutRestoration", func(t *testing.T) {
		// Create processor with fast post-handoff duration for testing
		config := &Config{
			MinWorkers:                 1,
			MaxWorkers:                 2,
			HandoffQueueSize:           10,
			PostHandoffRelaxedDuration: 100 * time.Millisecond, // Fast expiration for testing
			RelaxedTimeout:             5 * time.Second,
			LogLevel:                   2,
		}

		processor := NewRedisConnectionProcessor(3, baseDialer, config, nil)
		defer processor.Shutdown(context.Background())

		ctx := context.Background()

		// Create a connection and trigger handoff
		conn := createMockPoolConnection()
		if err := conn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Process the connection to trigger handoff
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("Handoff should succeed: %v", err)
		}
		if !shouldPool || shouldRemove {
			t.Error("Connection should be pooled after handoff")
		}

		// Wait for handoff to complete
		time.Sleep(50 * time.Millisecond)

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
			MinWorkers:       1,
			MaxWorkers:       2,
			HandoffQueueSize: 10,
			LogLevel:         2,
		}

		processor := NewRedisConnectionProcessor(3, baseDialer, config, nil)
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

		// ProcessConnectionOnGet should succeed for usable connection
		err := processor.ProcessConnectionOnGet(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnGet should succeed for usable connection: %v", err)
		}

		// Mark connection for handoff
		if err := conn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Connection should no longer be usable
		if conn.IsUsable() {
			t.Error("Connection should not be usable after being marked for handoff")
		}

		// ProcessConnectionOnGet should fail for non-usable connection
		err = processor.ProcessConnectionOnGet(ctx, conn)
		if err == nil {
			t.Error("ProcessConnectionOnGet should fail for non-usable connection")
		}
		if err != ErrConnectionMarkedForHandoff {
			t.Errorf("Expected ErrConnectionMarkedForHandoff, got %v", err)
		}

		// Process the connection to trigger handoff
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should succeed: %v", err)
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

		// ProcessConnectionOnGet should succeed again
		err = processor.ProcessConnectionOnGet(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnGet should succeed after handoff completion: %v", err)
		}

		t.Logf("Usable flag behavior test completed successfully")
	})

	t.Run("StaticQueueBehavior", func(t *testing.T) {
		config := &Config{
			MinWorkers:       1,
			MaxWorkers:       3,
			HandoffQueueSize: 50, // Explicit static queue size
			LogLevel:         2,
		}

		processor := NewRedisConnectionProcessorWithPoolSize(3, baseDialer, config, nil, 100) // Pool size: 100
		defer processor.Shutdown(context.Background())

		// Verify queue capacity matches configured size
		queueCapacity := cap(processor.handoffQueue)
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
				t.Fatalf("Failed to mark connection %d for handoff: %v", i, err)
			}

			shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
			if err != nil {
				t.Errorf("Failed to queue handoff %d: %v", i, err)
			}
			if !shouldPool || shouldRemove {
				t.Errorf("Connection %d should be pooled after handoff", i)
			}
		}

		// Verify queue has items but capacity remains static
		currentQueueSize := len(processor.handoffQueue)
		if currentQueueSize == 0 {
			t.Error("Expected some items in queue after processing connections")
		}

		finalCapacity := cap(processor.handoffQueue)
		if finalCapacity != 50 {
			t.Errorf("Queue capacity should remain static at 50, got %d", finalCapacity)
		}

		t.Logf("Static queue test completed - Capacity: %d, Current size: %d",
			finalCapacity, currentQueueSize)
	})

	t.Run("ConnectionRemovalOnHandoffFailure", func(t *testing.T) {
		// Create a failing dialer that will cause handoff initialization to fail
		failingDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Return a connection that will fail during initialization
			return &mockNetConn{addr: addr, shouldFailInit: true}, nil
		}

		config := &Config{
			MinWorkers:       1,
			MaxWorkers:       2,
			HandoffQueueSize: 10,
			LogLevel:         2,
		}

		processor := NewRedisConnectionProcessor(3, failingDialer, config, nil)
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
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)
		if err != nil {
			t.Errorf("ProcessConnectionOnPut should not error: %v", err)
		}
		if !shouldPool || shouldRemove {
			t.Error("Connection should be pooled after failed handoff attempt")
		}

		// Wait for handoff to be attempted and fail
		time.Sleep(100 * time.Millisecond)

		// Verify that the connection was removed from the pool
		if !mockPool.WasRemoved(conn.GetID()) {
			t.Errorf("Connection %d should have been removed from pool after handoff failure", conn.GetID())
		}

		t.Logf("Connection removal on handoff failure test completed successfully")
	})

	t.Run("PostHandoffRelaxedTimeout", func(t *testing.T) {
		// Create config with short post-handoff duration for testing
		config := &Config{
			MinWorkers:                 1,
			MaxWorkers:                 2,
			HandoffQueueSize:           10,
			RelaxedTimeout:             5 * time.Second,
			PostHandoffRelaxedDuration: 100 * time.Millisecond, // Short for testing
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewRedisConnectionProcessor(3, baseDialer, config, nil)
		defer processor.Shutdown(context.Background())

		conn := createMockPoolConnection()
		conn.MarkForHandoff("new-endpoint:6379", 12345)

		ctx := context.Background()
		shouldPool, shouldRemove, err := processor.ProcessConnectionOnPut(ctx, conn)

		if err != nil {
			t.Fatalf("ProcessConnectionOnPut failed: %v", err)
		}

		if !shouldPool {
			t.Error("Connection should be pooled after successful handoff")
		}

		if shouldRemove {
			t.Error("Connection should not be removed after successful handoff")
		}

		// Wait for the handoff to complete (it happens asynchronously)
		time.Sleep(50 * time.Millisecond)

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
}
