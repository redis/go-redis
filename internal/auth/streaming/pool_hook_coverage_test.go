package streaming

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

func idleConn() *pool.Conn {
	cn := pool.NewConn(nil)
	cn.GetStateMachine().Transition(pool.StateInitializing)
	cn.GetStateMachine().Transition(pool.StateIdle)
	return cn
}

func TestReAuthPoolHook_OnGet(t *testing.T) {
	hook := NewReAuthPoolHook(4, time.Second)
	cn := idleConn()
	ctx := context.Background()

	// Not marked: accept.
	if accept, err := hook.OnGet(ctx, cn, false); err != nil || !accept {
		t.Fatalf("OnGet(unmarked) = %v, %v", accept, err)
	}

	// Marked for reauth: reject.
	hook.MarkForReAuth(cn.GetID(), func(error) {})
	if accept, _ := hook.OnGet(ctx, cn, false); accept {
		t.Error("OnGet(marked) should reject")
	}

	// Scheduled reauth: reject.
	hook.shouldReAuthLock.Lock()
	delete(hook.shouldReAuth, cn.GetID())
	hook.shouldReAuthLock.Unlock()
	hook.scheduledLock.Lock()
	hook.scheduledReAuth[cn.GetID()] = true
	hook.scheduledLock.Unlock()
	if accept, _ := hook.OnGet(ctx, cn, false); accept {
		t.Error("OnGet(scheduled) should reject")
	}
}

func TestReAuthPoolHook_OnPut(t *testing.T) {
	hook := NewReAuthPoolHook(4, time.Second)
	ctx := context.Background()

	// Nil conn is a no-op.
	if pooled, removed, err := hook.OnPut(ctx, nil); !pooled || removed || err != nil {
		t.Fatalf("OnPut(nil) = %v, %v, %v", pooled, removed, err)
	}

	// Conn not marked: no scheduling, pooled.
	cn := idleConn()
	if pooled, removed, err := hook.OnPut(ctx, cn); !pooled || removed || err != nil {
		t.Fatalf("OnPut(unmarked) = %v, %v, %v", pooled, removed, err)
	}

	// Marked conn: OnPut schedules background reauth which runs the callback.
	done := make(chan error, 1)
	hook.MarkForReAuth(cn.GetID(), func(err error) { done <- err })
	if pooled, removed, err := hook.OnPut(ctx, cn); !pooled || removed || err != nil {
		t.Fatalf("OnPut(marked) = %v, %v, %v", pooled, removed, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("reauth callback err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background reauth")
	}
}

func TestReAuthPoolHook_OnRemove(t *testing.T) {
	mockPool := &mockPooler{}
	manager := NewManager(mockPool, time.Second)
	hook := manager.poolHookRef

	cn := idleConn()
	// Seed a listener and reauth state for this connection.
	_, _ = manager.Listener(cn, func(*pool.Conn, auth.Credentials) error { return nil }, func(*pool.Conn, error) {})
	hook.MarkForReAuth(cn.GetID(), func(error) {})

	hook.OnRemove(context.Background(), cn, errors.New("removed"))

	hook.shouldReAuthLock.RLock()
	_, stillMarked := hook.shouldReAuth[cn.GetID()]
	hook.shouldReAuthLock.RUnlock()
	if stillMarked {
		t.Error("OnRemove should clear shouldReAuth entry")
	}
	if _, ok := manager.credentialsListeners.Get(cn.GetID()); ok {
		t.Error("OnRemove should remove the credentials listener")
	}
}

func TestManager_DelegatesAndPoolHook(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)

	if _, ok := manager.PoolHook().(*ReAuthPoolHook); !ok {
		t.Fatalf("PoolHook returned %T", manager.PoolHook())
	}

	cn := idleConn()
	manager.MarkForReAuth(cn, func(error) {})
	manager.poolHookRef.shouldReAuthLock.RLock()
	_, marked := manager.poolHookRef.shouldReAuth[cn.GetID()]
	manager.poolHookRef.shouldReAuthLock.RUnlock()
	if !marked {
		t.Error("Manager.MarkForReAuth should delegate to the pool hook")
	}

	// RemoveListener clears the registry entry.
	manager.credentialsListeners.Add(cn.GetID(), &ConnReAuthCredentialsListener{})
	manager.RemoveListener(cn.GetID())
	if _, ok := manager.credentialsListeners.Get(cn.GetID()); ok {
		t.Error("RemoveListener should remove the entry")
	}
}

func TestConnReAuthCredentialsListener_OnNextOnError(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	cn := idleConn()

	var gotErr error
	l, err := manager.Listener(cn,
		func(*pool.Conn, auth.Credentials) error { return nil },
		func(_ *pool.Conn, e error) { gotErr = e },
	)
	if err != nil {
		t.Fatalf("Listener error: %v", err)
	}
	cl := l.(*ConnReAuthCredentialsListener)

	// OnError delegates to onErr.
	sentinel := errors.New("stream error")
	cl.OnError(sentinel)
	if gotErr != sentinel {
		t.Errorf("OnError did not propagate; got %v", gotErr)
	}

	// OnNext on a live connection marks it for reauth.
	cl.OnNext(nil)
	manager.poolHookRef.shouldReAuthLock.RLock()
	_, marked := manager.poolHookRef.shouldReAuth[cn.GetID()]
	manager.poolHookRef.shouldReAuthLock.RUnlock()
	if !marked {
		t.Error("OnNext should mark the connection for reauth")
	}

	// Guard branches: nil conn / nil onErr are no-ops.
	(&ConnReAuthCredentialsListener{}).OnNext(nil)
	(&ConnReAuthCredentialsListener{}).OnError(sentinel)
}
