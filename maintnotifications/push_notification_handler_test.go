package maintnotifications

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/interfaces"
	"github.com/redis/go-redis/v9/push"
)

// testOptions implements interfaces.OptionsInterface for testing SMIGRATED
type testOptions struct {
	originalEndpoint string
}

func (to *testOptions) GetReadTimeout() time.Duration       { return 5 * time.Second }
func (to *testOptions) GetWriteTimeout() time.Duration      { return 5 * time.Second }
func (to *testOptions) GetNetwork() string                  { return "tcp" }
func (to *testOptions) GetAddr() string                     { return "localhost:6379" }
func (to *testOptions) GetOriginalEndpoint() string         { return to.originalEndpoint }
func (to *testOptions) IsTLSEnabled() bool                  { return false }
func (to *testOptions) GetProtocol() int                    { return 3 }
func (to *testOptions) GetPoolSize() int                    { return 10 }
func (to *testOptions) NewDialer() func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) { return nil, nil }
}

func createTestManager(originalEndpoint string) *Manager {
	config := DefaultConfig()
	var opts interfaces.OptionsInterface
	if originalEndpoint != "" {
		opts = &testOptions{originalEndpoint: originalEndpoint}
	}
	return &Manager{
		config:  config,
		options: opts,
	}
}

// TestHandleSMigrated_CorrectFormat tests that handleSMigrated correctly parses the new triplet format
func TestHandleSMigrated_CorrectFormat(t *testing.T) {
	// Create a manager with matching original endpoint
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Create notification in the new triplet format:
	// ["SMIGRATED", SeqID, source1, target1, slots1, source2, target2, slots2, ...]
	notification := []interface{}{
		"SMIGRATED",
		int64(12346),
		"127.0.0.1:6379", "127.0.0.1:6380", "123,456,789-1000",
		"127.0.0.1:6379", "127.0.0.1:6381", "124,457,300-500",
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

// TestHandleSMigrated_SingleTriplet tests parsing with a single triplet
func TestHandleSMigrated_SingleTriplet(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Single triplet: source, target, slots
	notification := []interface{}{
		"SMIGRATED",
		int64(100),
		"127.0.0.1:6379", "127.0.0.1:6380", "1000,2000-3000",
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated failed with single triplet: %v", err)
	}
}

// TestHandleSMigrated_NoMatchingSource tests that notification is ignored when source doesn't match
func TestHandleSMigrated_NoMatchingSource(t *testing.T) {
	// Create a manager with a different original endpoint
	manager := createTestManager("127.0.0.1:9999")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Notification with source that doesn't match our endpoint
	notification := []interface{}{
		"SMIGRATED",
		int64(200),
		"127.0.0.1:6379", "127.0.0.1:6380", "1000,2000-3000",
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	// Should not return an error, just silently ignore
	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated should not error when source doesn't match: %v", err)
	}
}

// TestHandleSMigrated_IncompleteTriplets tests that incomplete triplets are rejected
func TestHandleSMigrated_IncompleteTriplets(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Incomplete triplet (missing slots)
	notification := []interface{}{
		"SMIGRATED",
		int64(300),
		"127.0.0.1:6379", "127.0.0.1:6380", // Missing slots
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err == nil {
		t.Error("handleSMigrated should error with incomplete triplets")
	}
}

// TestHandleSMigrated_TooFewElements tests that notifications with too few elements are rejected
func TestHandleSMigrated_TooFewElements(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Only SMIGRATED and SeqID, no triplets
	notification := []interface{}{
		"SMIGRATED",
		int64(400),
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err == nil {
		t.Error("handleSMigrated should error with too few elements")
	}
}
