package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

// Test that Listener returns the newly created listener, not nil
func TestManager_Listener_ReturnsNewListener(t *testing.T) {
	// Create a mock pool
	mockPool := &mockPooler{}

	// Create manager
	manager := NewManager(mockPool, time.Second)

	// Create a mock connection
	conn := &pool.Conn{}

	// Mock functions
	reAuth := func(cn *pool.Conn, creds auth.Credentials) error {
		return nil
	}

	onErr := func(cn *pool.Conn, err error) {
	}

	// Get listener - this should create a new one
	listener, err := manager.Listener(conn, reAuth, onErr)

	// Verify no error
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify listener is not nil (this was the bug!)
	if listener == nil {
		t.Fatal("Expected listener to be non-nil, but got nil")
	}

	// Verify it's the correct type
	if _, ok := listener.(*ConnReAuthCredentialsListener); !ok {
		t.Fatalf("Expected listener to be *ConnReAuthCredentialsListener, got %T", listener)
	}

	// Get the same listener again - should return the existing one
	listener2, err := manager.Listener(conn, reAuth, onErr)
	if err != nil {
		t.Fatalf("Expected no error on second call, got: %v", err)
	}

	if listener2 == nil {
		t.Fatal("Expected listener2 to be non-nil")
	}

	// Should be the same instance
	if listener != listener2 {
		t.Error("Expected to get the same listener instance on second call")
	}
}

// Test that Listener returns error when conn is nil
func TestManager_Listener_NilConn(t *testing.T) {
	mockPool := &mockPooler{}
	manager := NewManager(mockPool, time.Second)

	listener, err := manager.Listener(nil, nil, nil)

	if err == nil {
		t.Fatal("Expected error when conn is nil, got nil")
	}

	if listener != nil {
		t.Error("Expected listener to be nil when error occurs")
	}

	expectedErr := "poolCn cannot be nil"
	if err.Error() != expectedErr {
		t.Errorf("Expected error message %q, got %q", expectedErr, err.Error())
	}
}

// Mock pooler for testing
type mockPooler struct{}

func (m *mockPooler) NewConn(ctx context.Context) (*pool.Conn, error)                      { return nil, nil }
func (m *mockPooler) CloseConn(context.Context, *pool.Conn, string, string) error          { return nil }
func (m *mockPooler) Get(ctx context.Context) (*pool.Conn, error)                          { return nil, nil }
func (m *mockPooler) Put(ctx context.Context, conn *pool.Conn)                             {}
func (m *mockPooler) Remove(ctx context.Context, conn *pool.Conn, reason error)            {}
func (m *mockPooler) RemoveWithoutTurn(ctx context.Context, conn *pool.Conn, reason error) {}
func (m *mockPooler) Len() int                                                             { return 0 }
func (m *mockPooler) IdleLen() int                                                         { return 0 }
func (m *mockPooler) Stats() *pool.Stats                                                   { return &pool.Stats{} }
func (m *mockPooler) Size() int                                                            { return 10 }
func (m *mockPooler) AddPoolHook(hook pool.PoolHook)                                       {}
func (m *mockPooler) RemovePoolHook(hook pool.PoolHook)                                    {}
func (m *mockPooler) Close() error                                                         { return nil }

// RemoveConn is the cleanup for connections that never reach OnRemove, such
// as pub/sub connections: it must drop the listener and a pending re-auth.
func TestManager_RemoveConn_DropsListenerAndPendingReAuth(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	conn := pool.NewConn(nil)

	listener, err := manager.Listener(conn,
		func(*pool.Conn, auth.Credentials) error { return nil },
		func(*pool.Conn, error) {},
	)
	if err != nil {
		t.Fatalf("Listener: %v", err)
	}
	listener.OnNext(auth.NewBasicCredentials("user", "rotated"))
	if !manager.IsTracking(conn.GetID()) {
		t.Fatal("expected the manager to track the connection after a rotation")
	}

	manager.RemoveConn(conn.GetID())
	if manager.IsTracking(conn.GetID()) {
		t.Fatal("RemoveConn left re-auth state behind")
	}
	if _, ok := manager.credentialsListeners.Get(conn.GetID()); ok {
		t.Fatal("RemoveConn left the credentials listener behind")
	}
	if manager.poolHookRef.hasReAuth(conn.GetID()) {
		t.Fatal("RemoveConn left the pending re-auth behind")
	}
}

// A credential rotation can pass the listener's closed check just before the
// connection closes. Once RemoveConn ran for the closed connection, the
// rotation must not add a pending re-auth back.
func TestManager_MarkForReAuth_IgnoresClosedConn(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	conn := pool.NewConn(nil)
	if _, err := manager.Listener(conn,
		func(*pool.Conn, auth.Credentials) error { return nil },
		func(*pool.Conn, error) {},
	); err != nil {
		t.Fatalf("Listener: %v", err)
	}

	_ = conn.Close()
	manager.RemoveConn(conn.GetID())
	manager.MarkForReAuth(conn, func(error) {})
	if manager.IsTracking(conn.GetID()) {
		t.Fatal("MarkForReAuth added re-auth state for a closed connection")
	}
}
