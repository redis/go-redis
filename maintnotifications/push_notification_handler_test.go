package maintnotifications

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/interfaces"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// testOptions implements interfaces.OptionsInterface for testing SMIGRATED
type testOptions struct {
	nodeAddress string
}

func (to *testOptions) GetReadTimeout() time.Duration  { return 5 * time.Second }
func (to *testOptions) GetWriteTimeout() time.Duration { return 5 * time.Second }
func (to *testOptions) GetNetwork() string             { return "tcp" }
func (to *testOptions) GetAddr() string                { return "localhost:6379" }
func (to *testOptions) GetNodeAddress() string         { return to.nodeAddress }
func (to *testOptions) IsTLSEnabled() bool             { return false }
func (to *testOptions) GetProtocol() int               { return 3 }
func (to *testOptions) GetPoolSize() int               { return 10 }
func (to *testOptions) NewDialer() func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) { return nil, nil }
}

func createTestManager(nodeAddress string) *Manager {
	config := DefaultConfig()
	var opts interfaces.OptionsInterface
	if nodeAddress != "" {
		opts = &testOptions{nodeAddress: nodeAddress}
	}
	return &Manager{
		config:  config,
		options: opts,
	}
}

// TestHandleSMigrated_CorrectFormat tests that handleSMigrated correctly parses the nested array format
func TestHandleSMigrated_CorrectFormat(t *testing.T) {
	// Create a manager with matching original endpoint
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Create notification in the correct nested array format:
	// ["SMIGRATED", SeqID, [[source, target, slots], [source, target, slots], ...]]
	notification := []interface{}{
		"SMIGRATED",
		int64(12346),
		[]interface{}{
			[]interface{}{"127.0.0.1:6379", "127.0.0.1:6380", "123,456,789-1000"},
			[]interface{}{"127.0.0.1:6379", "127.0.0.1:6381", "124,457,300-500"},
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

// TestHandleSMigrated_SingleTriplet tests parsing with a single triplet
func TestHandleSMigrated_SingleTriplet(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Single triplet in nested array format
	notification := []interface{}{
		"SMIGRATED",
		int64(100),
		[]interface{}{
			[]interface{}{"127.0.0.1:6379", "127.0.0.1:6380", "1000,2000-3000"},
		},
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
		[]interface{}{
			[]interface{}{"127.0.0.1:6379", "127.0.0.1:6380", "1000,2000-3000"},
		},
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

// TestHandleSMigrated_IncompleteTriplet tests that incomplete triplets are skipped
func TestHandleSMigrated_IncompleteTriplet(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Triplet with only 2 elements (missing slots) - should be skipped
	notification := []interface{}{
		"SMIGRATED",
		int64(300),
		[]interface{}{
			[]interface{}{"127.0.0.1:6379", "127.0.0.1:6380"}, // Missing slots
		},
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	// Should not error, just skip the malformed triplet
	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err != nil {
		t.Errorf("handleSMigrated should not error with incomplete triplet (just skip it): %v", err)
	}
}

// TestHandleSMigrated_TooFewElements tests that notifications with too few elements are rejected
func TestHandleSMigrated_TooFewElements(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Only SMIGRATED and SeqID, no triplets array
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

// TestHandleSMigrated_EmptyTripletsArray tests that empty triplets array is rejected
func TestHandleSMigrated_EmptyTripletsArray(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Empty triplets array
	notification := []interface{}{
		"SMIGRATED",
		int64(500),
		[]interface{}{},
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err == nil {
		t.Error("handleSMigrated should error with empty triplets array")
	}
}

// TestHandleSMigrated_InvalidTripletsType tests that non-array triplets is rejected
func TestHandleSMigrated_InvalidTripletsType(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")

	handler := &NotificationHandler{
		manager: manager,
	}

	// Triplets is a string instead of array
	notification := []interface{}{
		"SMIGRATED",
		int64(600),
		"not an array",
	}

	ctx := context.Background()
	handlerCtx := push.NotificationHandlerContext{
		Conn: nil,
	}

	err := handler.handleSMigrated(ctx, handlerCtx, notification)
	if err == nil {
		t.Error("handleSMigrated should error with invalid triplets type")
	}
}

// TestHandleMoving_NullEndpointPubSubConn pins how a MOVING with no
// endpoint treats an unpooled pub/sub connection: the delayed mark names
// a null endpoint (its owner reconnects to wherever it is now, not to
// the configured address), and the connection — idle for its whole
// life — is not queued on the pool's handoff worker, which would race
// the owner reading from it.
func TestHandleMoving_NullEndpointPubSubConn(t *testing.T) {
	manager := createTestManager("127.0.0.1:6379")
	dialer := func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("handoff worker must not dial")
	}
	// Defaults applied: a zero-size handoff queue would stall a wrongly
	// queued handoff past the checks below.
	hookCfg := manager.config.ApplyDefaultsWithPoolSize(10)
	manager.poolHooksRef = NewPoolHook(dialer, "tcp", hookCfg, manager)
	t.Cleanup(func() { _ = manager.poolHooksRef.Shutdown(context.Background()) })
	handler := &NotificationHandler{manager: manager, operationsManager: manager}

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	cn := pool.NewConn(client)
	cn.MarkPubSub()
	cn.GetStateMachine().Transition(pool.StateIdle)

	// timeS of 1 schedules the mark at timeS/2 = 0s.
	notification := []interface{}{"MOVING", int64(1), int64(1), nil}
	handlerCtx := push.NotificationHandlerContext{Conn: cn}
	if err := handler.handleMoving(context.Background(), handlerCtx, notification); err != nil {
		t.Fatalf("handleMoving: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !cn.ShouldHandoff() {
		if time.Now().After(deadline) {
			t.Fatalf("connection not marked for handoff (usable=%v, state=%v)",
				cn.IsUsable(), cn.GetStateMachine().GetState())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ep := cn.GetHandoffEndpoint(); ep != internal.RedisNull {
		t.Fatalf("handoff endpoint = %q, want %q", ep, internal.RedisNull)
	}
	// Give a wrongly queued handoff time to land before checking.
	time.Sleep(50 * time.Millisecond)
	if !cn.IsUsable() || !cn.ShouldHandoff() {
		t.Fatalf("pub/sub connection was queued for handoff: usable=%v shouldHandoff=%v state=%v",
			cn.IsUsable(), cn.ShouldHandoff(), cn.GetStateMachine().GetState())
	}
}
