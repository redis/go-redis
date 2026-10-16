package otel

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

type countRecorder struct {
	noopRecorder
	mu    sync.Mutex
	count int
}

func (r *countRecorder) RecordConnectionCount(_ context.Context, delta int, cn *pool.Conn, _ string, _ bool) {
	if cn.PoolName() != "shutdown-count" {
		return
	}
	r.mu.Lock()
	r.count += delta
	r.mu.Unlock()
}

func (r *countRecorder) get() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

func TestShutdownUncountsExistingConns(t *testing.T) {
	connPool := pool.NewConnPool(&pool.Options{
		Name: "shutdown-count",
		Dialer: func(context.Context) (net.Conn, error) {
			c, _ := net.Pipe()
			return c, nil
		},
		PoolSize:           2,
		MinIdleConns:       2,
		MaxConcurrentDials: 2,
		PoolTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	t.Cleanup(func() { _ = connPool.Close() })

	deadline := time.Now().Add(time.Second)
	for connPool.Len() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if got := connPool.Len(); got != 2 {
		t.Fatalf("MinIdleConns: got %d, want 2", got)
	}

	r := &countRecorder{}
	SetGlobalRecorder(r)
	if got := r.get(); got != 2 {
		SetGlobalRecorder(nil)
		t.Fatalf("after SetGlobalRecorder: got %d, want 2", got)
	}

	SetGlobalRecorder(nil)
	if got := r.get(); got != 0 {
		t.Fatalf("after SetGlobalRecorder(nil): got %d, want 0", got)
	}
}
