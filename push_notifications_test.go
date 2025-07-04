package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestHandler implements PushNotificationHandler interface for testing
type TestHandler struct {
	name        string
	handled     [][]interface{}
	returnValue bool
}

func NewTestHandler(name string, returnValue bool) *TestHandler {
	return &TestHandler{
		name:        name,
		handled:     make([][]interface{}, 0),
		returnValue: returnValue,
	}
}

func (h *TestHandler) HandlePushNotification(ctx context.Context, handlerCtx PushNotificationHandlerContext, notification []interface{}) bool {
	h.handled = append(h.handled, notification)
	return h.returnValue
}

func (h *TestHandler) GetHandledNotifications() [][]interface{} {
	return h.handled
}

func (h *TestHandler) Reset() {
	h.handled = make([][]interface{}, 0)
}

func TestPushNotificationRegistry(t *testing.T) {
	t.Run("NewRegistry", func(t *testing.T) {
		registry := NewRegistry()
		if registry == nil {
			t.Error("NewRegistry should not return nil")
		}
		
		if len(registry.GetRegisteredPushNotificationNames()) != 0 {
			t.Error("New registry should have no registered handlers")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test", true)
		
		err := registry.RegisterHandler("TEST", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}
		
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})

	t.Run("UnregisterHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test", true)
		
		registry.RegisterHandler("TEST", handler, false)
		
		err := registry.UnregisterHandler("TEST")
		if err != nil {
			t.Errorf("UnregisterHandler should not error: %v", err)
		}
		
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != nil {
			t.Error("GetHandler should return nil after unregistering")
		}
	})

	t.Run("ProtectedHandler", func(t *testing.T) {
		registry := NewRegistry()
		handler := NewTestHandler("test", true)
		
		// Register protected handler
		err := registry.RegisterHandler("TEST", handler, true)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}
		
		// Try to unregister protected handler
		err = registry.UnregisterHandler("TEST")
		if err == nil {
			t.Error("UnregisterHandler should error for protected handler")
		}
		
		// Handler should still be there
		retrievedHandler := registry.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("Protected handler should still be registered")
		}
	})
}

func TestPushNotificationProcessor(t *testing.T) {
	t.Run("NewProcessor", func(t *testing.T) {
		processor := NewProcessor()
		if processor == nil {
			t.Error("NewProcessor should not return nil")
		}
	})

	t.Run("RegisterAndGetHandler", func(t *testing.T) {
		processor := NewProcessor()
		handler := NewTestHandler("test", true)
		
		err := processor.RegisterHandler("TEST", handler, false)
		if err != nil {
			t.Errorf("RegisterHandler should not error: %v", err)
		}
		
		retrievedHandler := processor.GetHandler("TEST")
		if retrievedHandler != handler {
			t.Error("GetHandler should return the registered handler")
		}
	})
}

func TestVoidProcessor(t *testing.T) {
	t.Run("NewVoidProcessor", func(t *testing.T) {
		processor := NewVoidProcessor()
		if processor == nil {
			t.Error("NewVoidProcessor should not return nil")
		}
	})

	t.Run("GetHandler", func(t *testing.T) {
		processor := NewVoidProcessor()
		handler := processor.GetHandler("TEST")
		if handler != nil {
			t.Error("VoidProcessor GetHandler should always return nil")
		}
	})

	t.Run("RegisterHandler", func(t *testing.T) {
		processor := NewVoidProcessor()
		handler := NewTestHandler("test", true)
		
		err := processor.RegisterHandler("TEST", handler, false)
		if err == nil {
			t.Error("VoidProcessor RegisterHandler should return error")
		}
	})

	t.Run("ProcessPendingNotifications", func(t *testing.T) {
		processor := NewVoidProcessor()
		ctx := context.Background()
		handlerCtx := NewPushNotificationHandlerContext(nil, nil, nil, nil, false)
		
		// VoidProcessor should always succeed and do nothing
		err := processor.ProcessPendingNotifications(ctx, handlerCtx, nil)
		if err != nil {
			t.Errorf("VoidProcessor ProcessPendingNotifications should never error, got: %v", err)
		}
	})
}

func TestPushNotificationHandlerContext(t *testing.T) {
	t.Run("NewHandlerContext", func(t *testing.T) {
		client := &Client{}
		connPool := &pool.ConnPool{}
		pubSub := &PubSub{}
		conn := &pool.Conn{}
		
		ctx := NewPushNotificationHandlerContext(client, connPool, pubSub, conn, true)
		if ctx == nil {
			t.Error("NewPushNotificationHandlerContext should not return nil")
		}
		
		if ctx.GetClient() != client {
			t.Error("GetClient should return the provided client")
		}
		
		if ctx.GetConnPool() != connPool {
			t.Error("GetConnPool should return the provided connection pool")
		}
		
		if ctx.GetPubSub() != pubSub {
			t.Error("GetPubSub should return the provided PubSub")
		}
		
		if ctx.GetConn() != conn {
			t.Error("GetConn should return the provided connection")
		}
		
		if !ctx.IsBlocking() {
			t.Error("IsBlocking should return true")
		}
	})

	t.Run("TypedGetters", func(t *testing.T) {
		client := &Client{}
		ctx := NewPushNotificationHandlerContext(client, nil, nil, nil, false)
		
		// Test regular client getter
		regularClient := ctx.GetRegularClient()
		if regularClient != client {
			t.Error("GetRegularClient should return the client when it's a regular client")
		}
		
		// Test cluster client getter (should be nil for regular client)
		clusterClient := ctx.GetClusterClient()
		if clusterClient != nil {
			t.Error("GetClusterClient should return nil when client is not a cluster client")
		}
	})
}

func TestPushNotificationConstants(t *testing.T) {
	t.Run("Constants", func(t *testing.T) {
		if PushNotificationMoving != "MOVING" {
			t.Error("PushNotificationMoving should be 'MOVING'")
		}
		
		if PushNotificationMigrating != "MIGRATING" {
			t.Error("PushNotificationMigrating should be 'MIGRATING'")
		}
		
		if PushNotificationMigrated != "MIGRATED" {
			t.Error("PushNotificationMigrated should be 'MIGRATED'")
		}
		
		if PushNotificationFailingOver != "FAILING_OVER" {
			t.Error("PushNotificationFailingOver should be 'FAILING_OVER'")
		}
		
		if PushNotificationFailedOver != "FAILED_OVER" {
			t.Error("PushNotificationFailedOver should be 'FAILED_OVER'")
		}
	})
}
