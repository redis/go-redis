package pool

import (
	"context"
	"testing"
	"time"
)

func TestConn_TimeAndNameAccessors(t *testing.T) {
	if GetCachedTimeNs() <= 0 {
		t.Error("GetCachedTimeNs should be positive")
	}
	cn := NewConn(nil)

	cn.SetLastPutAtNs(12345)
	if cn.LastPutAtNs() != 12345 {
		t.Errorf("LastPutAtNs = %d", cn.LastPutAtNs())
	}
	if cn.GetDialStartNs() != 0 {
		t.Errorf("GetDialStartNs default = %d, want 0", cn.GetDialStartNs())
	}
	cn.SetPoolName("mypool")
	if cn.PoolName() != "mypool" {
		t.Errorf("PoolName = %q", cn.PoolName())
	}
	if cn.IsPooled() {
		t.Error("IsPooled default should be false")
	}
	if cn.IsPubSub() {
		t.Error("IsPubSub default should be false")
	}
}

func TestConn_IsInited(t *testing.T) {
	cn := NewConn(nil)
	if cn.IsInited() {
		t.Error("fresh conn should not be inited")
	}
	cn.GetStateMachine().Transition(StateInitializing)
	cn.GetStateMachine().Transition(StateIdle)
	if !cn.IsInited() {
		t.Error("idle conn should be inited")
	}
}

func TestConn_HandoffAccessors(t *testing.T) {
	cn := NewConn(nil)

	// Defaults.
	if cn.GetHandoffEndpoint() != "" || cn.GetMovingSeqID() != 0 {
		t.Error("unexpected default handoff metadata")
	}
	if should, ep, seq := cn.GetHandoffInfo(); should || ep != "" || seq != 0 {
		t.Errorf("default GetHandoffInfo = %v, %q, %d", should, ep, seq)
	}
	if cn.HandoffRetries() != 0 {
		t.Errorf("HandoffRetries default = %d", cn.HandoffRetries())
	}
	if got := cn.IncrementAndGetHandoffRetries(2); got != 2 {
		t.Errorf("IncrementAndGetHandoffRetries = %d, want 2", got)
	}

	if err := cn.MarkForHandoff("newhost:6379", 42); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	if cn.GetHandoffEndpoint() != "newhost:6379" || cn.GetMovingSeqID() != 42 {
		t.Error("handoff metadata not stored")
	}
	should, ep, seq := cn.GetHandoffInfo()
	if !should || ep != "newhost:6379" || seq != 42 {
		t.Errorf("GetHandoffInfo = %v, %q, %d", should, ep, seq)
	}

	// MarkQueuedForHandoff transitions to UNUSABLE and clears ShouldHandoff.
	if err := cn.MarkQueuedForHandoff(); err != nil {
		t.Fatalf("MarkQueuedForHandoff: %v", err)
	}
	if cn.GetStateMachine().GetState() != StateUnusable {
		t.Errorf("state = %s, want UNUSABLE", cn.GetStateMachine().GetState())
	}

	// ClearHandoffState resets everything and returns to IDLE.
	cn.ClearHandoffState()
	if cn.ShouldHandoff() || cn.HandoffRetries() != 0 {
		t.Error("ClearHandoffState did not reset handoff metadata")
	}
	if cn.GetStateMachine().GetState() != StateIdle {
		t.Errorf("state after clear = %s, want IDLE", cn.GetStateMachine().GetState())
	}
}

func TestConn_RelaxedTimeoutWithDeadline(t *testing.T) {
	cn := NewConn(nil)
	if cn.HasRelaxedTimeout() {
		t.Error("fresh conn should not have relaxed timeout")
	}
	cn.SetRelaxedTimeoutWithDeadline(time.Second, 2*time.Second, time.Now().Add(time.Hour))
	if !cn.HasRelaxedTimeout() {
		t.Error("HasRelaxedTimeout should be true after set")
	}
}

func TestConn_NetConnAndInit(t *testing.T) {
	cn := NewConn(nil)

	// Callbacks.
	cn.SetOnClose(func() error { return nil })

	// ExecuteInitConn without a func returns an error.
	if err := cn.ExecuteInitConn(context.Background()); err == nil {
		t.Error("ExecuteInitConn without func should error")
	}

	called := false
	cn.SetInitConnFunc(func(context.Context, *Conn) error {
		called = true
		return nil
	})
	if err := cn.ExecuteInitConn(context.Background()); err != nil {
		t.Fatalf("ExecuteInitConn: %v", err)
	}
	if !called {
		t.Error("initConnFunc not called")
	}

	// SetNetConn / GetNetConn round-trip.
	fc := fakeNetConn{}
	cn.SetNetConn(fc)
	if cn.GetNetConn() != fc {
		t.Error("GetNetConn did not return the set conn")
	}

	// Write / RemoteAddr go through the underlying conn.
	if n, err := cn.Write([]byte("ping")); err != nil || n != 4 {
		t.Errorf("Write = %d, %v", n, err)
	}
	if cn.RemoteAddr() == nil {
		t.Error("RemoteAddr should not be nil")
	}
	// fakeNetConn does not implement syscall.Conn, so MaybeHasData is false.
	if cn.MaybeHasData() {
		t.Error("MaybeHasData should be false for non-syscall conn")
	}

	// SetNetConnAndInitConn runs the init func.
	fresh := NewConn(nil)
	fresh.SetInitConnFunc(func(context.Context, *Conn) error { return nil })
	if err := fresh.SetNetConnAndInitConn(context.Background(), fakeNetConn{}); err != nil {
		t.Fatalf("SetNetConnAndInitConn: %v", err)
	}
}

func TestConn_StateHelpers(t *testing.T) {
	if len(ValidFromIdle()) == 0 || len(ValidFromCreatedIdleOrUnusable()) == 0 {
		t.Error("predefined state slices should be non-empty")
	}
	cn := NewConn(nil)
	sm := cn.GetStateMachine()
	if !sm.TryTransitionFast(StateCreated, StateIdle) {
		t.Error("TryTransitionFast CREATED->IDLE should succeed")
	}
	if sm.TryTransitionFast(StateCreated, StateInUse) {
		t.Error("TryTransitionFast from wrong state should fail")
	}

	// PeekReplyTypeSafe with no buffered data returns an error.
	if _, err := cn.PeekReplyTypeSafe(); err == nil {
		t.Error("PeekReplyTypeSafe with no data should error")
	}
}
