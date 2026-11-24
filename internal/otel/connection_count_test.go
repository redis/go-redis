package otel

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

type registryRecorder struct {
	noopRecorder
	mu     sync.Mutex
	pools  map[pool.Pooler]string
	pubSub map[PubSubPooler]string
}

func newRegistryRecorder() *registryRecorder {
	return &registryRecorder{pools: map[pool.Pooler]string{}, pubSub: map[PubSubPooler]string{}}
}

func (r *registryRecorder) RegisterPool(name string, p pool.Pooler) {
	r.mu.Lock()
	r.pools[p] = name
	r.mu.Unlock()
}

func (r *registryRecorder) UnregisterPool(p pool.Pooler) {
	r.mu.Lock()
	delete(r.pools, p)
	r.mu.Unlock()
}

func (r *registryRecorder) RegisterPubSubPool(name string, p PubSubPooler) {
	r.mu.Lock()
	r.pubSub[p] = name
	r.mu.Unlock()
}

func (r *registryRecorder) UnregisterPubSubPool(p PubSubPooler) {
	r.mu.Lock()
	delete(r.pubSub, p)
	r.mu.Unlock()
}

func (r *registryRecorder) poolName(p pool.Pooler) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name, ok := r.pools[p]
	return name, ok
}

func (r *registryRecorder) pubSubName(p PubSubPooler) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name, ok := r.pubSub[p]
	return name, ok
}

// A recorder installed after a pool was created is handed that pool, under
// the name its connections carry; a closed pool is not handed over.
func TestSetGlobalRecorderRegistersExistingPools(t *testing.T) {
	t.Cleanup(func() { SetGlobalRecorder(nil) })
	dialer := func(context.Context) (net.Conn, error) {
		c, _ := net.Pipe()
		return c, nil
	}
	connPool := pool.NewConnPool(&pool.Options{Name: "before-init", Dialer: dialer, PoolSize: 1, PoolTimeout: time.Second})
	t.Cleanup(func() { _ = connPool.Close() })
	pubSubPool := pool.NewPubSubPool(&pool.Options{Name: "before-init_pubsub"}, nil)
	t.Cleanup(func() { _ = pubSubPool.Close() })
	closedPool := pool.NewConnPool(&pool.Options{Name: "closed", Dialer: dialer, PoolSize: 1, PoolTimeout: time.Second})
	_ = closedPool.Close()

	r := newRegistryRecorder()
	SetGlobalRecorder(r)

	if name, ok := r.poolName(connPool); !ok || name != "before-init" {
		t.Fatalf("conn pool: registered=%v name=%q, want true and %q", ok, name, "before-init")
	}
	if name, ok := r.pubSubName(pubSubPool); !ok || name != "before-init_pubsub" {
		t.Fatalf("pubsub pool: registered=%v name=%q, want true and %q", ok, name, "before-init_pubsub")
	}
	if _, ok := r.poolName(closedPool); ok {
		t.Fatal("a closed pool was handed to the new recorder")
	}

	UnregisterPools(Scoped(nil), connPool, pubSubPool, nil)
	if _, ok := r.poolName(connPool); ok {
		t.Fatal("UnregisterPools left the conn pool registered")
	}
	if _, ok := r.pubSubName(pubSubPool); ok {
		t.Fatal("UnregisterPools left the pubsub pool registered")
	}
}
