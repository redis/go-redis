package pool

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// TestHook for testing hook functionality
type TestHook struct {
	OnGetCalled  int
	OnPutCalled  int
	GetError     error
	PutError     error
	ShouldPool   bool
	ShouldRemove bool
}

func (th *TestHook) OnGet(ctx context.Context, conn *Conn, isNewConn bool) error {
	th.OnGetCalled++
	return th.GetError
}

func (th *TestHook) OnPut(ctx context.Context, conn *Conn) (shouldPool bool, shouldRemove bool, err error) {
	th.OnPutCalled++
	return th.ShouldPool, th.ShouldRemove, th.PutError
}

func TestPoolHookManager(t *testing.T) {
	manager := NewPoolHookManager()

	// Test initial state
	if manager.GetHookCount() != 0 {
		t.Errorf("Expected 0 hooks initially, got %d", manager.GetHookCount())
	}

	// Add hooks
	hook1 := &TestHook{ShouldPool: true}
	hook2 := &TestHook{ShouldPool: true}

	manager.AddHook(hook1)
	manager.AddHook(hook2)

	if manager.GetHookCount() != 2 {
		t.Errorf("Expected 2 hooks after adding, got %d", manager.GetHookCount())
	}

	// Test ProcessOnGet
	ctx := context.Background()
	conn := &Conn{} // Mock connection

	err := manager.ProcessOnGet(ctx, conn, false)
	if err != nil {
		t.Errorf("ProcessOnGet should not error: %v", err)
	}

	if hook1.OnGetCalled != 1 {
		t.Errorf("Expected hook1.OnGetCalled to be 1, got %d", hook1.OnGetCalled)
	}

	if hook2.OnGetCalled != 1 {
		t.Errorf("Expected hook2.OnGetCalled to be 1, got %d", hook2.OnGetCalled)
	}

	// Test ProcessOnPut
	shouldPool, shouldRemove, err := manager.ProcessOnPut(ctx, conn)
	if err != nil {
		t.Errorf("ProcessOnPut should not error: %v", err)
	}

	if !shouldPool {
		t.Error("Expected shouldPool to be true")
	}

	if shouldRemove {
		t.Error("Expected shouldRemove to be false")
	}

	if hook1.OnPutCalled != 1 {
		t.Errorf("Expected hook1.OnPutCalled to be 1, got %d", hook1.OnPutCalled)
	}

	if hook2.OnPutCalled != 1 {
		t.Errorf("Expected hook2.OnPutCalled to be 1, got %d", hook2.OnPutCalled)
	}

	// Remove a hook
	manager.RemoveHook(hook1)

	if manager.GetHookCount() != 1 {
		t.Errorf("Expected 1 hook after removing, got %d", manager.GetHookCount())
	}
}

func TestHookErrorHandling(t *testing.T) {
	manager := NewPoolHookManager()

	// Hook that returns error on Get
	errorHook := &TestHook{
		GetError:   errors.New("test error"),
		ShouldPool: true,
	}

	normalHook := &TestHook{ShouldPool: true}

	manager.AddHook(errorHook)
	manager.AddHook(normalHook)

	ctx := context.Background()
	conn := &Conn{}

	// Test that error stops processing
	err := manager.ProcessOnGet(ctx, conn, false)
	if err == nil {
		t.Error("Expected error from ProcessOnGet")
	}

	if errorHook.OnGetCalled != 1 {
		t.Errorf("Expected errorHook.OnGetCalled to be 1, got %d", errorHook.OnGetCalled)
	}

	// normalHook should not be called due to error
	if normalHook.OnGetCalled != 0 {
		t.Errorf("Expected normalHook.OnGetCalled to be 0, got %d", normalHook.OnGetCalled)
	}
}

func TestHookShouldRemove(t *testing.T) {
	manager := NewPoolHookManager()

	// Hook that says to remove connection
	removeHook := &TestHook{
		ShouldPool:   false,
		ShouldRemove: true,
	}

	normalHook := &TestHook{ShouldPool: true}

	manager.AddHook(removeHook)
	manager.AddHook(normalHook)

	ctx := context.Background()
	conn := &Conn{}

	shouldPool, shouldRemove, err := manager.ProcessOnPut(ctx, conn)
	if err != nil {
		t.Errorf("ProcessOnPut should not error: %v", err)
	}

	if shouldPool {
		t.Error("Expected shouldPool to be false")
	}

	if !shouldRemove {
		t.Error("Expected shouldRemove to be true")
	}

	if removeHook.OnPutCalled != 1 {
		t.Errorf("Expected removeHook.OnPutCalled to be 1, got %d", removeHook.OnPutCalled)
	}

	// normalHook should not be called due to early return
	if normalHook.OnPutCalled != 0 {
		t.Errorf("Expected normalHook.OnPutCalled to be 0, got %d", normalHook.OnPutCalled)
	}
}

func TestPoolWithHooks(t *testing.T) {
	// Create a pool with hooks
	hookManager := NewPoolHookManager()
	testHook := &TestHook{ShouldPool: true}
	hookManager.AddHook(testHook)

	opt := &Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			return &net.TCPConn{}, nil // Mock connection
		},
		PoolSize:    1,
		DialTimeout: time.Second,
	}

	pool := NewConnPool(opt)
	defer pool.Close()

	// Add hook to pool after creation
	pool.AddPoolHook(testHook)

	// Verify hooks are initialized
	if pool.hookManager == nil {
		t.Error("Expected hookManager to be initialized")
	}

	if pool.hookManager.GetHookCount() != 1 {
		t.Errorf("Expected 1 hook in pool, got %d", pool.hookManager.GetHookCount())
	}

	// Test adding hook to pool
	additionalHook := &TestHook{ShouldPool: true}
	pool.AddPoolHook(additionalHook)

	if pool.hookManager.GetHookCount() != 2 {
		t.Errorf("Expected 2 hooks after adding, got %d", pool.hookManager.GetHookCount())
	}

	// Test removing hook from pool
	pool.RemovePoolHook(additionalHook)

	if pool.hookManager.GetHookCount() != 1 {
		t.Errorf("Expected 1 hook after removing, got %d", pool.hookManager.GetHookCount())
	}
}
