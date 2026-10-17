package otel

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

type pendingRecorder struct {
	noopRecorder
	mu      sync.Mutex
	pending int
}

func (r *pendingRecorder) RecordPendingRequests(_ context.Context, delta int, _ *pool.Conn, poolName string) {
	if poolName != "pending-requests" {
		return
	}
	r.mu.Lock()
	r.pending += delta
	r.mu.Unlock()
}

func (r *pendingRecorder) get() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending
}

func TestPendingRequestsBalancedWhenRecorderChanges(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Name: "pending-requests",
		Dialer: func(context.Context) (net.Conn, error) {
			c, _ := net.Pipe()
			return c, nil
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		PoolTimeout:        5 * time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })

	ctx := context.Background()
	// waitWhile runs change while a Get is waiting for the only connection.
	waitWhile := func(change func()) {
		t.Helper()
		held, err := connPool.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			cn, err := connPool.Get(ctx)
			if err == nil {
				connPool.Put(ctx, cn)
			}
			done <- err
		}()
		deadline := time.Now().Add(time.Second)
		for connPool.Stats().PendingRequests != 1 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if got := connPool.Stats().PendingRequests; got != 1 {
			t.Fatalf("pending requests in pool stats: got %d, want 1", got)
		}
		change()
		connPool.Put(ctx, held)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	r := &pendingRecorder{}
	waitWhile(func() { SetGlobalRecorder(r) })
	if got := r.get(); got != 0 {
		SetGlobalRecorder(nil)
		t.Fatalf("Init during wait: got %d, want 0", got)
	}

	waitWhile(func() { SetGlobalRecorder(nil) })
	if got := r.get(); got != 0 {
		t.Fatalf("Shutdown during wait: got %d, want 0", got)
	}
}
