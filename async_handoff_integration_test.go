package redis

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/logging"
)

// mockNetConn implements net.Conn for testing
type mockNetConn struct {
	addr string
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

// TestEventDrivenHandoffIntegration tests the complete event-driven handoff flow
func TestEventDrivenHandoffIntegration(t *testing.T) {
	t.Run("EventDrivenHandoffWithPoolSkipping", func(t *testing.T) {
		// Create a base dialer for testing
		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		// Create processor with event-driven handoff support
		processor := maintnotifications.NewPoolHook(baseDialer, "tcp", nil, nil)
		defer processor.Shutdown(context.Background())

		// Create a test pool with hooks
		hookManager := pool.NewPoolHookManager()
		hookManager.AddHook(processor)

		testPool := pool.NewConnPool(&pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				return &mockNetConn{addr: "original:6379"}, nil
			},
			PoolSize:    int32(5),
			PoolTimeout: time.Second,
		})

		// Add the hook to the pool after creation
		testPool.AddPoolHook(processor)
		defer testPool.Close()

		// Set the pool reference in the processor for connection removal on handoff failure
		processor.SetPool(testPool)

		ctx := context.Background()

		// Get a connection and mark it for handoff
		conn, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		// Set initialization function with a small delay to ensure handoff is pending
		initConnCalled := false
		initConnFunc := func(ctx context.Context, cn *pool.Conn) error {
			time.Sleep(50 * time.Millisecond) // Add delay to keep handoff pending
			initConnCalled = true
			return nil
		}
		conn.SetInitConnFunc(initConnFunc)

		// Mark connection for handoff
		err = conn.MarkForHandoff("new-endpoint:6379", 12345)
		if err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Return connection to pool - this should queue handoff
		testPool.Put(ctx, conn)

		// Give the on-demand worker a moment to start processing
		time.Sleep(10 * time.Millisecond)

		// Verify handoff was queued
		if !processor.IsHandoffPending(conn) {
			t.Error("Handoff should be queued in pending map")
		}

		// Try to get the same connection - should be skipped due to pending handoff
		conn2, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Failed to get second connection: %v", err)
		}

		// Should get a different connection (the pending one should be skipped)
		if conn == conn2 {
			t.Error("Should have gotten a different connection while handoff is pending")
		}

		// Return the second connection
		testPool.Put(ctx, conn2)

		// Wait for handoff to complete
		time.Sleep(200 * time.Millisecond)

		// Verify handoff completed (removed from pending map)
		if processor.IsHandoffPending(conn) {
			t.Error("Handoff should have completed and been removed from pending map")
		}

		if !initConnCalled {
			t.Error("InitConn should have been called during handoff")
		}

		// Now the original connection should be available again
		conn3, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Failed to get third connection: %v", err)
		}

		// Could be the original connection (now handed off) or a new one
		testPool.Put(ctx, conn3)
	})

	t.Run("ConcurrentHandoffs", func(t *testing.T) {
		// Create a base dialer that simulates slow handoffs
		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			time.Sleep(50 * time.Millisecond) // Simulate network delay
			return &mockNetConn{addr: addr}, nil
		}

		processor := maintnotifications.NewPoolHook(baseDialer, "tcp", nil, nil)
		defer processor.Shutdown(context.Background())

		// Create hooks manager and add processor as hook
		hookManager := pool.NewPoolHookManager()
		hookManager.AddHook(processor)

		testPool := pool.NewConnPool(&pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				return &mockNetConn{addr: "original:6379"}, nil
			},

			PoolSize:    int32(10),
			PoolTimeout: time.Second,
		})
		defer testPool.Close()

		// Add the hook to the pool after creation
		testPool.AddPoolHook(processor)

		// Set the pool reference in the processor
		processor.SetPool(testPool)

		ctx := context.Background()
		var wg sync.WaitGroup

		// Start multiple concurrent handoffs
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Get connection
				conn, err := testPool.Get(ctx)
				if err != nil {
					t.Errorf("Failed to get conn[%d]: %v", id, err)
					return
				}

				// Set initialization function
				initConnFunc := func(ctx context.Context, cn *pool.Conn) error {
					return nil
				}
				conn.SetInitConnFunc(initConnFunc)

				// Mark for handoff
				conn.MarkForHandoff("new-endpoint:6379", int64(id))

				// Return to pool (starts async handoff)
				testPool.Put(ctx, conn)
			}(i)
		}

		wg.Wait()

		// Wait for all handoffs to complete
		time.Sleep(300 * time.Millisecond)

		// Verify pool is still functional
		conn, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Pool should still be functional after concurrent handoffs: %v", err)
		}
		testPool.Put(ctx, conn)
	})

	t.Run("HandoffFailureRecovery", func(t *testing.T) {
		// Create a failing base dialer
		failingDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Err: &net.DNSError{Name: addr}}
		}

		processor := maintnotifications.NewPoolHook(failingDialer, "tcp", nil, nil)
		defer processor.Shutdown(context.Background())

		// Create hooks manager and add processor as hook
		hookManager := pool.NewPoolHookManager()
		hookManager.AddHook(processor)

		testPool := pool.NewConnPool(&pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				return &mockNetConn{addr: "original:6379"}, nil
			},

			PoolSize:    int32(3),
			PoolTimeout: time.Second,
		})
		defer testPool.Close()

		// Add the hook to the pool after creation
		testPool.AddPoolHook(processor)

		// Set the pool reference in the processor
		processor.SetPool(testPool)

		ctx := context.Background()

		// Get connection and mark for handoff
		conn, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		conn.MarkForHandoff("unreachable-endpoint:6379", 12345)

		// Return to pool (starts async handoff that will fail)
		testPool.Put(ctx, conn)

		// Wait for handoff to fail
		time.Sleep(200 * time.Millisecond)

		// Connection should be removed from pending map after failed handoff
		if processor.IsHandoffPending(conn) {
			t.Error("Connection should be removed from pending map after failed handoff")
		}

		// Pool should still be functional
		conn2, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Pool should still be functional: %v", err)
		}

		// In event-driven approach, the original connection remains in pool
		// even after failed handoff (it's still a valid connection)
		// We might get the same connection or a different one
		testPool.Put(ctx, conn2)
	})

	t.Run("GracefulShutdown", func(t *testing.T) {
		// Create a slow base dialer
		slowDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			time.Sleep(100 * time.Millisecond)
			return &mockNetConn{addr: addr}, nil
		}

		processor := maintnotifications.NewPoolHook(slowDialer, "tcp", nil, nil)
		defer processor.Shutdown(context.Background())

		// Create hooks manager and add processor as hook
		hookManager := pool.NewPoolHookManager()
		hookManager.AddHook(processor)

		testPool := pool.NewConnPool(&pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				return &mockNetConn{addr: "original:6379"}, nil
			},

			PoolSize:    int32(2),
			PoolTimeout: time.Second,
		})
		defer testPool.Close()

		// Add the hook to the pool after creation
		testPool.AddPoolHook(processor)

		// Set the pool reference in the processor
		processor.SetPool(testPool)

		ctx := context.Background()

		// Start a handoff
		conn, err := testPool.Get(ctx)
		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		if err := conn.MarkForHandoff("new-endpoint:6379", 12345); err != nil {
			t.Fatalf("Failed to mark connection for handoff: %v", err)
		}

		// Set a mock initialization function with delay to ensure handoff is pending
		conn.SetInitConnFunc(func(ctx context.Context, cn *pool.Conn) error {
			time.Sleep(50 * time.Millisecond) // Add delay to keep handoff pending
			return nil
		})

		testPool.Put(ctx, conn)

		// Give the on-demand worker a moment to start and begin processing
		// The handoff should be pending because the slowDialer takes 100ms
		time.Sleep(10 * time.Millisecond)

		// Verify handoff was queued and is being processed
		if !processor.IsHandoffPending(conn) {
			t.Error("Handoff should be queued in pending map")
		}

		// Give the handoff a moment to start processing
		time.Sleep(50 * time.Millisecond)

		// Shutdown processor gracefully
		// Use a longer timeout to account for slow dialer (100ms) plus processing overhead
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = processor.Shutdown(shutdownCtx)
		if err != nil {
			t.Errorf("Graceful shutdown should succeed: %v", err)
		}

		// Handoff should have completed (removed from pending map)
		if processor.IsHandoffPending(conn) {
			t.Error("Handoff should have completed and been removed from pending map after shutdown")
		}
	})
}

func init() {
	logging.Disable()
}
