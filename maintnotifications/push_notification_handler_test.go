package maintnotifications

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9/push"
)

// TestHandleSMigrated_CorrectFormat tests that handleSMigrated correctly parses the correct format
func TestHandleSMigrated_CorrectFormat(t *testing.T) {
	// Create a minimal manager for testing
	config := DefaultConfig()
	manager := &Manager{
		config: config,
	}

	handler := &NotificationHandler{
		manager: manager,
	}

	// Create notification in the correct format:
	// ["SMIGRATED", SeqID, [[host:port, slots], [host:port, slots], ...]]
	notification := []interface{}{
		"SMIGRATED",
		int64(12346),
		[]interface{}{
			[]interface{}{"127.0.0.1:6379", "123,456,789-1000"},
			[]interface{}{"127.0.0.1:6380", "124,457,300-500"},
		},
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil, // No connection needed for this test
	}

	// This should not return an error
	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated failed with correct format: %v", err)
	}
}

// TestHandleSMigrated_SingleEndpoint tests parsing with a single endpoint
func TestHandleSMigrated_SingleEndpoint(t *testing.T) {
	config := DefaultConfig()
	manager := &Manager{
		config: config,
	}

	handler := &NotificationHandler{
		manager: manager,
	}

	notification := []interface{}{
		"SMIGRATED",
		int64(100),
		[]interface{}{
			[]interface{}{"127.0.0.1:6380", "1000,2000-3000"},
		},
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated failed with single endpoint: %v", err)
	}
}
