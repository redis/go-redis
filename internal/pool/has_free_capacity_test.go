package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestHasFreeCapacityHonorsMaxActiveConns: with MaxActiveConns < PoolSize and
// no idle conn, a Get would fail ErrPoolExhausted even though Len() < Size() —
// HasFreeCapacity must report false.
func TestHasFreeCapacityHonorsMaxActiveConns(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           4, // Len()<Size() alone would report "free"...
		MaxActiveConns:     1, // ...but only one active connection is allowed.
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })
	ctx := context.Background()

	// Fresh pool: nothing dialed yet, below PoolSize and MaxActiveConns — the
	// first Get can dial, so there is free capacity.
	if !connPool.HasFreeCapacity() {
		t.Fatal("fresh pool: HasFreeCapacity() = false, want true (first Get can dial)")
	}

	first, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// One active connection (== MaxActiveConns) and none idle: the next Get would
	// return ErrPoolExhausted even though Len()(=1) < Size()(=4). The old heuristic
	// would wrongly report free; HasFreeCapacity must report false.
	if connPool.HasFreeCapacity() {
		t.Fatal("at MaxActiveConns with no idle conn: HasFreeCapacity() = true, want false")
	}

	// Return it: an idle connection is now ready, so there is capacity again.
	connPool.Put(ctx, first)
	if !connPool.HasFreeCapacity() {
		t.Fatal("with an idle conn available: HasFreeCapacity() = false, want true")
	}
}

// TestHasFreeCapacityWithoutMaxActiveConns: with MaxActiveConns unset the gate
// reduces to the prior idle-or-under-size heuristic (fresh pool free, full pool
// with no idle not-free, idle conn free again). Conservative by design: it does
// not track non-pooled overflow Gets.
func TestHasFreeCapacityWithoutMaxActiveConns(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })
	ctx := context.Background()

	if !connPool.HasFreeCapacity() {
		t.Fatal("fresh pool: HasFreeCapacity() = false, want true")
	}

	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Grown to PoolSize (1), no idle conn and no free turn (the one turn is held):
	// the gate reports false.
	if connPool.HasFreeCapacity() {
		t.Fatal("at PoolSize with no idle conn / no free turn: HasFreeCapacity() = true, want false")
	}
	connPool.Put(ctx, cn)
	if !connPool.HasFreeCapacity() {
		t.Fatal("with a usable idle conn available: HasFreeCapacity() = false, want true")
	}
}

// TestHasFreeCapacityExcludesUnusableIdle: a not-usable idle conn (mid handoff
// or re-auth) must not count as capacity.
func TestHasFreeCapacityExcludesUnusableIdle(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })
	ctx := context.Background()

	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	connPool.Put(ctx, cn) // one usable idle conn
	if !connPool.HasFreeCapacity() {
		t.Fatal("usable idle conn: HasFreeCapacity() = false, want true")
	}

	// Mark the idle conn unusable (as a handoff / re-auth would). It is still in
	// idleConns, so IdleLen()>0, but it cannot serve a Get; at PoolSize there is
	// also nothing to dial, so capacity must read false.
	cn.SetUsable(false)
	if connPool.HasFreeCapacity() {
		t.Fatal("idle conn is UNUSABLE and pool is at PoolSize: HasFreeCapacity() = true, want false")
	}
}

// TestHasFreeCapacityExcludesHandoffIdle: a handoff-marked idle conn is still
// StateIdle/usable but an OnGet hook diverts it, so it must not count as
// capacity.
func TestHasFreeCapacityExcludesHandoffIdle(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Dialer:             dummyDialer,
		PoolSize:           1,
		MaxActiveConns:     1,
		MaxConcurrentDials: 1,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })
	ctx := context.Background()

	cn, err := connPool.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	if !cn.IsUsable() {
		t.Fatal("precondition: a handoff-marked conn should still be IsUsable (StateIdle)")
	}
	connPool.Put(ctx, cn)

	if connPool.HasFreeCapacity() {
		t.Fatal("HasFreeCapacity() = true with only a handoff-marked idle conn — an OnGet hook would divert it, so it must not count")
	}
}
