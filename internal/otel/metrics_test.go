package otel

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

func TestGenerateUniqueID(t *testing.T) {
	id := generateUniqueID()
	if len(id) != 8 {
		t.Errorf("generateUniqueID() = %q, want 8 hex chars", id)
	}
	if generateUniqueID() == id {
		t.Errorf("generateUniqueID() should produce different values")
	}
}

// fakeRecorder embeds noopRecorder for the methods we don't assert on, and
// implements PoolRegistrar to exercise the registrar code paths.
type fakeRecorder struct {
	noopRecorder
	opDuration       bool
	pipelineDuration bool
	registered       []string
	unregistered     int
}

func (f *fakeRecorder) RecordOperationDuration(context.Context, time.Duration, Cmder, int, error, *pool.Conn, int) {
	f.opDuration = true
}

func (f *fakeRecorder) RecordPipelineOperationDuration(context.Context, time.Duration, string, int, int, error, *pool.Conn, int) {
	f.pipelineDuration = true
}

func (f *fakeRecorder) RegisterPool(poolName string, _ pool.Pooler) {
	f.registered = append(f.registered, poolName)
}
func (f *fakeRecorder) UnregisterPool(pool.Pooler)             { f.unregistered++ }
func (f *fakeRecorder) RegisterPubSubPool(string, PubSubPooler) {}
func (f *fakeRecorder) UnregisterPubSubPool(PubSubPooler)       { f.unregistered++ }

type fakePool struct{ pool.Pooler }

type fakePubSubPool struct{}

func (fakePubSubPool) Stats() *pool.PubSubStats { return &pool.PubSubStats{} }

func TestNoopRecorderDefaults(t *testing.T) {
	// Default global recorder is a noop; package-level helpers must not panic.
	ctx := context.Background()
	RecordOperationDuration(ctx, time.Second, nil, 1, nil, nil, 0)
	RecordPipelineOperationDuration(ctx, time.Second, "PIPELINE", 2, 1, nil, nil, 0)
	RecordConnectionCreateTime(ctx, time.Second, nil)
	RecordPubSubMessage(ctx, nil, "sent", "ch", false)
	RecordStreamLag(ctx, time.Second, nil, "s", "g", "c")

	n := noopRecorder{}
	n.RecordConnectionRelaxedTimeout(ctx, 1, nil, "main", "MOVING")
	n.RecordConnectionHandoff(ctx, nil, "main")
	n.RecordError(ctx, "MOVED", nil, "MOVED", false, 0)
	n.RecordMaintenanceNotification(ctx, nil, "MOVING")
	n.RecordConnectionWaitTime(ctx, time.Second, nil)
	n.RecordConnectionClosed(ctx, nil, "idle", nil)
	n.RecordConnectionCount(ctx, 1, nil, "idle", false)
	n.RecordPendingRequests(ctx, 1, nil, "main")
}

func TestSetGlobalRecorderLifecycle(t *testing.T) {
	defer SetGlobalRecorder(nil)

	rec := &fakeRecorder{}
	SetGlobalRecorder(rec)

	if getRecorder() != rec {
		t.Fatalf("getRecorder did not return the set recorder")
	}

	opCb := GetOperationDurationCallback()
	if opCb == nil {
		t.Fatalf("operation duration callback should be set")
	}
	opCb(context.Background(), time.Second, nil, 1, nil, nil, 0)
	if !rec.opDuration {
		t.Errorf("operation duration callback did not reach recorder")
	}

	pipeCb := GetPipelineOperationDurationCallback()
	if pipeCb == nil {
		t.Fatalf("pipeline duration callback should be set")
	}
	pipeCb(context.Background(), time.Second, "PIPELINE", 1, 1, nil, nil, 0)
	if !rec.pipelineDuration {
		t.Errorf("pipeline duration callback did not reach recorder")
	}

	// Register / unregister pools through the registrar path.
	RegisterPools(fakePool{}, fakePubSubPool{}, "localhost:6379")
	if len(rec.registered) != 1 {
		t.Errorf("expected 1 registered pool, got %d", len(rec.registered))
	}
	UnregisterPools(fakePool{}, fakePubSubPool{})
	if rec.unregistered != 2 {
		t.Errorf("expected 2 unregister calls, got %d", rec.unregistered)
	}

	// Reset clears the callbacks back to noop.
	SetGlobalRecorder(nil)
	if GetOperationDurationCallback() != nil {
		t.Errorf("operation callback should be nil after reset")
	}
	if GetPipelineOperationDurationCallback() != nil {
		t.Errorf("pipeline callback should be nil after reset")
	}
}

func TestRegisterPoolsNoopRecorder(t *testing.T) {
	// With the default noop recorder (not a PoolRegistrar), these are no-ops.
	RegisterPools(fakePool{}, fakePubSubPool{}, "addr")
	UnregisterPools(fakePool{}, fakePubSubPool{})
}
