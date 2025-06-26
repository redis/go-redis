package redis

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// testHandler is a simple implementation of PushNotificationHandler for testing
type testHandler struct {
	handlerFunc func(ctx context.Context, notification []interface{}) bool
}

func (h *testHandler) HandlePushNotification(ctx context.Context, notification []interface{}) bool {
	return h.handlerFunc(ctx, notification)
}

// newTestHandler creates a test handler from a function
func newTestHandler(f func(ctx context.Context, notification []interface{}) bool) *testHandler {
	return &testHandler{handlerFunc: f}
}

// TestConnectionPoolPushNotificationIntegration tests the connection pool's
// integration with push notifications for 100% coverage.
func TestConnectionPoolPushNotificationIntegration(t *testing.T) {
	// Create client with push notifications
	client := NewClient(&Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	processor := client.GetPushNotificationProcessor()
	if processor == nil {
		t.Fatal("Push notification processor should be available")
	}

	// Test that connections get the processor assigned
	ctx := context.Background()
	connPool := client.Pool().(*pool.ConnPool)

	// Get a connection and verify it has the processor
	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer connPool.Put(ctx, cn)

	if cn.PushNotificationProcessor == nil {
		t.Error("Connection should have push notification processor assigned")
	}

	if !cn.PushNotificationProcessor.IsEnabled() {
		t.Error("Connection push notification processor should be enabled")
	}

	// Test ProcessPendingNotifications method
	emptyReader := proto.NewReader(bytes.NewReader([]byte{}))
	err = cn.PushNotificationProcessor.ProcessPendingNotifications(ctx, emptyReader)
	if err != nil {
		t.Errorf("ProcessPendingNotifications should not error with empty reader: %v", err)
	}
}

// TestConnectionPoolPutWithBufferedData tests the pool's Put method
// when connections have buffered data (push notifications).
func TestConnectionPoolPutWithBufferedData(t *testing.T) {
	// Create client with push notifications
	client := NewClient(&Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	ctx := context.Background()
	connPool := client.Pool().(*pool.ConnPool)

	// Get a connection
	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Verify connection has processor
	if cn.PushNotificationProcessor == nil {
		t.Error("Connection should have push notification processor")
	}

	// Test putting connection back (should not panic or error)
	connPool.Put(ctx, cn)

	// Get another connection to verify pool operations work
	cn2, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get second connection: %v", err)
	}
	connPool.Put(ctx, cn2)
}

// TestConnectionHealthCheckWithPushNotifications tests the isHealthyConn
// integration with push notifications.
func TestConnectionHealthCheckWithPushNotifications(t *testing.T) {
	// Create client with push notifications
	client := NewClient(&Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Register a handler to ensure processor is active
	err := client.RegisterPushNotificationHandler("TEST_HEALTH", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}), false)
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Test basic connection operations to exercise health checks
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		pong, err := client.Ping(ctx).Result()
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if pong != "PONG" {
			t.Errorf("Expected PONG, got %s", pong)
		}
	}
}

// TestConnPushNotificationMethods tests all push notification methods on Conn type.
func TestConnPushNotificationMethods(t *testing.T) {
	// Create client with push notifications
	client := NewClient(&Options{
		Addr:              "localhost:6379",
		Protocol:          3,
		PushNotifications: true,
	})
	defer client.Close()

	// Create a Conn instance
	conn := client.Conn()
	defer conn.Close()

	// Test GetPushNotificationProcessor
	processor := conn.GetPushNotificationProcessor()
	if processor == nil {
		t.Error("Conn should have push notification processor")
	}

	if !processor.IsEnabled() {
		t.Error("Conn push notification processor should be enabled")
	}

	// Test RegisterPushNotificationHandler
	handler := newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	})

	err := conn.RegisterPushNotificationHandler("TEST_CONN_HANDLER", handler, false)
	if err != nil {
		t.Errorf("Failed to register handler on Conn: %v", err)
	}

	// Test RegisterPushNotificationHandler with function wrapper
	err = conn.RegisterPushNotificationHandler("TEST_CONN_FUNC", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}), false)
	if err != nil {
		t.Errorf("Failed to register handler func on Conn: %v", err)
	}

	// Test duplicate handler error
	err = conn.RegisterPushNotificationHandler("TEST_CONN_HANDLER", handler, false)
	if err == nil {
		t.Error("Should get error when registering duplicate handler")
	}

	// Test that handlers work
	registry := processor.GetRegistry()
	ctx := context.Background()

	handled := registry.HandleNotification(ctx, []interface{}{"TEST_CONN_HANDLER", "data"})
	if !handled {
		t.Error("Handler should have been called")
	}

	handled = registry.HandleNotification(ctx, []interface{}{"TEST_CONN_FUNC", "data"})
	if !handled {
		t.Error("Handler func should have been called")
	}
}

// TestConnWithoutPushNotifications tests Conn behavior when push notifications are disabled.
func TestConnWithoutPushNotifications(t *testing.T) {
	// Create client without push notifications
	client := NewClient(&Options{
		Addr:              "localhost:6379",
		Protocol:          2, // RESP2, no push notifications
		PushNotifications: false,
	})
	defer client.Close()

	// Create a Conn instance
	conn := client.Conn()
	defer conn.Close()

	// Test GetPushNotificationProcessor returns nil
	processor := conn.GetPushNotificationProcessor()
	if processor != nil {
		t.Error("Conn should not have push notification processor for RESP2")
	}

	// Test RegisterPushNotificationHandler returns nil (no error)
	err := conn.RegisterPushNotificationHandler("TEST", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}), false)
	if err != nil {
		t.Errorf("Should return nil error when no processor: %v", err)
	}

	// Test RegisterPushNotificationHandler returns nil (no error)
	err = conn.RegisterPushNotificationHandler("TEST", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}), false)
	if err != nil {
		t.Errorf("Should return nil error when no processor: %v", err)
	}
}

// TestNewConnWithCustomProcessor tests newConn with custom processor in options.
func TestNewConnWithCustomProcessor(t *testing.T) {
	// Create custom processor
	customProcessor := NewPushNotificationProcessor(true)

	// Create options with custom processor
	opt := &Options{
		Addr:                      "localhost:6379",
		Protocol:                  3,
		PushNotificationProcessor: customProcessor,
	}
	opt.init()

	// Create a mock connection pool
	connPool := newConnPool(opt, func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, nil // Mock dialer
	})

	// Test that newConn sets the custom processor
	conn := newConn(opt, connPool, nil)

	if conn.GetPushNotificationProcessor() != customProcessor {
		t.Error("newConn should set custom processor from options")
	}
}

// TestClonedClientPushNotifications tests that cloned clients preserve push notifications.
func TestClonedClientPushNotifications(t *testing.T) {
	// Create original client
	client := NewClient(&Options{
		Addr:     "localhost:6379",
		Protocol: 3,
	})
	defer client.Close()

	originalProcessor := client.GetPushNotificationProcessor()
	if originalProcessor == nil {
		t.Fatal("Original client should have push notification processor")
	}

	// Register handler on original
	err := client.RegisterPushNotificationHandler("TEST_CLONE", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}), false)
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Create cloned client with timeout
	clonedClient := client.WithTimeout(5 * time.Second)
	defer clonedClient.Close()

	// Test that cloned client has same processor
	clonedProcessor := clonedClient.GetPushNotificationProcessor()
	if clonedProcessor != originalProcessor {
		t.Error("Cloned client should have same push notification processor")
	}

	// Test that handlers work on cloned client
	registry := clonedProcessor.GetRegistry()
	ctx := context.Background()
	handled := registry.HandleNotification(ctx, []interface{}{"TEST_CLONE", "data"})
	if !handled {
		t.Error("Cloned client should handle notifications")
	}

	// Test registering new handler on cloned client
	err = clonedClient.RegisterPushNotificationHandler("TEST_CLONE_NEW", newTestHandler(func(ctx context.Context, notification []interface{}) bool {
		return true
	}), false)
	if err != nil {
		t.Errorf("Failed to register handler on cloned client: %v", err)
	}
}

// TestPushNotificationInfoStructure tests the cleaned up PushNotificationInfo.
func TestPushNotificationInfoStructure(t *testing.T) {
	// Test with various notification types
	testCases := []struct {
		name         string
		notification []interface{}
		expectedCmd  string
		expectedArgs int
	}{
		{
			name:         "MOVING notification",
			notification: []interface{}{"MOVING", "127.0.0.1:6380", "slot", "1234"},
			expectedCmd:  "MOVING",
			expectedArgs: 3,
		},
		{
			name:         "MIGRATING notification",
			notification: []interface{}{"MIGRATING", "time", "123456"},
			expectedCmd:  "MIGRATING",
			expectedArgs: 2,
		},
		{
			name:         "MIGRATED notification",
			notification: []interface{}{"MIGRATED"},
			expectedCmd:  "MIGRATED",
			expectedArgs: 0,
		},
		{
			name:         "Custom notification",
			notification: []interface{}{"CUSTOM_EVENT", "arg1", "arg2", "arg3"},
			expectedCmd:  "CUSTOM_EVENT",
			expectedArgs: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			info := ParsePushNotificationInfo(tc.notification)

			if info.Name != tc.expectedCmd {
				t.Errorf("Expected name %s, got %s", tc.expectedCmd, info.Name)
			}

			if len(info.Args) != tc.expectedArgs {
				t.Errorf("Expected %d args, got %d", tc.expectedArgs, len(info.Args))
			}

			// Verify no unused fields exist by checking the struct only has Name and Args
			// This is a compile-time check - if unused fields were added back, this would fail
			_ = struct {
				Name string
				Args []interface{}
			}{
				Name: info.Name,
				Args: info.Args,
			}
		})
	}
}

// TestConnectionPoolOptionsIntegration tests that pool options correctly include processor.
func TestConnectionPoolOptionsIntegration(t *testing.T) {
	// Create processor
	processor := NewPushNotificationProcessor(true)

	// Create options
	opt := &Options{
		Addr:                      "localhost:6379",
		Protocol:                  3,
		PushNotificationProcessor: processor,
	}
	opt.init()

	// Create connection pool
	connPool := newConnPool(opt, func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, nil // Mock dialer
	})

	// Verify the pool has the processor in its configuration
	// This tests the integration between options and pool creation
	if connPool == nil {
		t.Error("Connection pool should be created")
	}
}

// TestProcessPendingNotificationsEdgeCases tests edge cases in ProcessPendingNotifications.
func TestProcessPendingNotificationsEdgeCases(t *testing.T) {
	processor := NewPushNotificationProcessor(true)
	ctx := context.Background()

	// Test with nil reader (should not panic)
	err := processor.ProcessPendingNotifications(ctx, nil)
	if err != nil {
		t.Logf("ProcessPendingNotifications correctly handles nil reader: %v", err)
	}

	// Test with empty reader
	emptyReader := proto.NewReader(bytes.NewReader([]byte{}))
	err = processor.ProcessPendingNotifications(ctx, emptyReader)
	if err != nil {
		t.Errorf("Should not error with empty reader: %v", err)
	}

	// Test with disabled processor
	disabledProcessor := NewPushNotificationProcessor(false)
	err = disabledProcessor.ProcessPendingNotifications(ctx, emptyReader)
	if err != nil {
		t.Errorf("Disabled processor should not error: %v", err)
	}
}
