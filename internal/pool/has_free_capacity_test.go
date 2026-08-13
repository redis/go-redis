package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestHasFreeCapacityHonorsMaxActiveConns pins the branch the plain
// IdleLen()>0 || Len()<Size() heuristic missed: with MaxActiveConns < PoolSize
// and no idle connection, the pool cannot serve another Get (newConn returns
// ErrPoolExhausted once poolSize >= MaxActiveConns), yet Len() < Size() still
// holds. HasFreeCapacity must report false there — that is what makes the
// autopipeline straggler-hold gate stop shortening the hold into a pool the
// flush's Get would exhaust (#3962).
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

// TestHasFreeCapacityWithoutMaxActiveConns confirms that with MaxActiveConns
// unset HasFreeCapacity reduces EXACTLY to the prior `IdleLen()>0 || Len()<Size()`
// heuristic: a fresh pool is free, a pool grown to PoolSize with no idle conn is
// reported not-free, and an idle conn makes it free again. This is a conservative
// gate, not an admission check: a second Get on the PoolSize-full pool would in
// fact still succeed with a non-pooled connection (PoolSize does not hard-block
// dials) — HasFreeCapacity deliberately does not track that, it just keeps the
// straggler-hold conservative.
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
	// Grown to PoolSize (1), none idle: Len() >= Size() so the conservative gate
	// reports false (matching the old heuristic), even though a second Get would
	// still succeed non-pooled.
	if connPool.HasFreeCapacity() {
		t.Fatal("at PoolSize with no idle conn: HasFreeCapacity() = true, want false (conservative)")
	}
	connPool.Put(ctx, cn)
	if !connPool.HasFreeCapacity() {
		t.Fatal("with an idle conn available: HasFreeCapacity() = false, want true")
	}
}
