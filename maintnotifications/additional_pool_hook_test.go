package maintnotifications

import (
	"context"
	"net"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

// countingPool records how many pool hooks were added and removed so a test can
// assert a maintnotifications hook is attached to (and detached from) a pool.
type countingPool struct {
	pool.Pooler
	added   int
	removed int
}

func (p *countingPool) AddPoolHook(pool.PoolHook)    { p.added++ }
func (p *countingPool) RemovePoolHook(pool.PoolHook) { p.removed++ }

// TestInitPoolHookForPool verifies that a dedicated pool (e.g. a client's
// pipeline connection pool) receives an independent maintnotifications pool hook,
// and that the hook is detached on Close. This guards the regression where the
// dedicated pipeline pool never got the MOVING/MIGRATING handoff hook, so
// autopipelined/pipelined commands ran on connections without handoff handling.
func TestInitPoolHookForPool(t *testing.T) {
	client := &MockClient{options: &MockOptions{}}
	primary := &countingPool{}
	pipelinePool := &countingPool{}

	manager, err := NewManager(client, primary, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	dialer := func(context.Context, string, string) (net.Conn, error) { return nil, nil }

	manager.InitPoolHook(dialer)
	manager.InitPoolHookForPool(pipelinePool, dialer)

	if primary.added != 1 {
		t.Fatalf("primary pool: expected 1 hook added, got %d", primary.added)
	}
	if pipelinePool.added != 1 {
		t.Fatalf("pipeline pool: expected 1 hook added, got %d", pipelinePool.added)
	}

	// The additional hook must be a distinct instance bound to its own pool, so
	// failed-handoff removals target the pipeline pool, not the primary pool.
	if len(manager.additionalPoolHooks) != 1 {
		t.Fatalf("expected 1 additional pool hook tracked, got %d", len(manager.additionalPoolHooks))
	}
	if manager.additionalPoolHooks[0].hook == manager.poolHooksRef {
		t.Fatal("additional pool hook must be a separate instance from the primary hook")
	}
	if manager.additionalPoolHooks[0].pool != pipelinePool {
		t.Fatal("additional pool hook bound to the wrong pool")
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("manager close failed: %v", err)
	}
	if pipelinePool.removed != 1 {
		t.Fatalf("pipeline pool: expected hook removed on Close, got %d removals", pipelinePool.removed)
	}
}

// TestInitPoolHookForPoolNil is a no-op guard: a nil pool must not panic or
// register a hook.
func TestInitPoolHookForPoolNil(t *testing.T) {
	client := &MockClient{options: &MockOptions{}}
	manager, err := NewManager(client, &countingPool{}, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	manager.InitPoolHookForPool(nil, func(context.Context, string, string) (net.Conn, error) { return nil, nil })
	if len(manager.additionalPoolHooks) != 0 {
		t.Fatal("nil pool must not register an additional hook")
	}
}
