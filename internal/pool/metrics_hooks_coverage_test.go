package pool

import (
	"context"
	"testing"
)

func TestSetAllMetricCallbacksAndGetters(t *testing.T) {
	// Restore to a clean state after the test.
	defer SetAllMetricCallbacks(nil)

	cbs := &MetricCallbacks{
		ConnectionRelaxedTimeout: func(context.Context, int, *Conn, string, string) {},
		ConnectionHandoff:        func(context.Context, *Conn, string) {},
		MaintenanceNotification:  func(context.Context, *Conn, string) {},
	}
	SetAllMetricCallbacks(cbs)

	if GetMetricConnectionRelaxedTimeoutCallback() == nil {
		t.Error("relaxed timeout callback should be set")
	}
	if GetMetricConnectionHandoffCallback() == nil {
		t.Error("handoff callback should be set")
	}
	if GetMetricMaintenanceNotificationCallback() == nil {
		t.Error("maintenance notification callback should be set")
	}

	// Passing nil clears every callback.
	SetAllMetricCallbacks(nil)
	if GetMetricConnectionRelaxedTimeoutCallback() != nil ||
		GetMetricConnectionHandoffCallback() != nil ||
		GetMetricMaintenanceNotificationCallback() != nil {
		t.Error("callbacks should be cleared after SetAllMetricCallbacks(nil)")
	}
}

func TestPoolHookManager_GetHooks(t *testing.T) {
	mgr := NewPoolHookManager()
	if len(mgr.GetHooks()) != 0 {
		t.Errorf("expected no hooks initially, got %d", len(mgr.GetHooks()))
	}

	hook := &countingHook{}
	mgr.AddHook(hook)

	hooks := mgr.GetHooks()
	if len(hooks) != 1 {
		t.Fatalf("GetHooks len = %d, want 1", len(hooks))
	}
	if hooks[0] != hook {
		t.Error("GetHooks returned an unexpected hook")
	}
	// Returned slice is a copy: mutating it must not affect the manager.
	hooks[0] = nil
	if mgr.GetHooks()[0] == nil {
		t.Error("GetHooks should return a defensive copy")
	}
}

// countingHook is a no-op PoolHook for hook-manager tests.
type countingHook struct{}

func (h *countingHook) OnGet(context.Context, *Conn, bool) (bool, error) { return true, nil }
func (h *countingHook) OnPut(context.Context, *Conn) (bool, bool, error) { return true, false, nil }
func (h *countingHook) OnRemove(context.Context, *Conn, error)           {}
