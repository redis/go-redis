package streaming

import (
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// newTestPubSubConn returns a Pub/Sub connection backed by net.Pipe and a
// cleanup function. The connection never touches the network: the tests
// below only exercise re-auth scheduling, not the AUTH command itself.
func newTestPubSubConn(t *testing.T) (*pool.Conn, func()) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	psPool := pool.NewPubSubPool(&pool.Options{}, func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	})
	cn, err := psPool.NewConn(context.Background(), "tcp", "127.0.0.1:6379", nil)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	if !cn.IsPubSub() {
		t.Fatal("expected a Pub/Sub connection")
	}
	return cn, func() {
		_ = cn.Close()
		_ = serverConn.Close()
		_ = clientConn.Close()
	}
}

// Two rotations arriving while a re-auth is in flight must not race: the
// re-auths run one at a time, in arrival order, and only the latest
// pending rotation runs after the current one finishes (#4302 point 1).
func TestPubSubReAuthSerializesRotations(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	cn, cleanup := newTestPubSubConn(t)
	defer cleanup()

	var (
		mu      sync.Mutex
		order   []int
		running atomic.Int32
		maxSeen atomic.Int32
		done    = make(chan struct{})
	)
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once

	mkFn := func(n int) func(error) {
		return func(error) {
			cur := running.Add(1)
			for {
				max := maxSeen.Load()
				if cur <= max || maxSeen.CompareAndSwap(max, cur) {
					break
				}
			}
			mu.Lock()
			order = append(order, n)
			mu.Unlock()
			if n == 1 {
				once.Do(func() { close(started) })
				<-release
			}
			running.Add(-1)
			if n == 3 {
				close(done)
			}
		}
	}

	manager.MarkForReAuth(cn, mkFn(1))
	// Wait until the first re-auth is running, then queue two more
	// rotations while it is still in flight.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first re-auth did not start")
	}
	manager.MarkForReAuth(cn, mkFn(2))
	manager.MarkForReAuth(cn, mkFn(3))
	close(release)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-auths to finish")
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := order, []int{1, 3}; !slices.Equal(got, want) {
		t.Fatalf("re-auth order = %v, want %v (rotation 2 must be coalesced away)", got, want)
	}
	if max := maxSeen.Load(); max > 1 {
		t.Fatalf("max concurrent re-auths = %d, want 1", max)
	}
}

// A rotation for an idle connection runs right away, and once it finishes
// the connection is no longer tracked.
func TestPubSubReAuthRunsWhenIdle(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	cn, cleanup := newTestPubSubConn(t)
	defer cleanup()

	done := make(chan struct{})
	manager.MarkForReAuth(cn, func(error) { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("re-auth did not run")
	}
	// The callback runs before the worker clears its running flag, so
	// tracking may still read true right after done closes: poll until
	// the worker has finished instead of asserting immediately.
	deadline := time.Now().Add(2 * time.Second)
	for manager.IsTracking(cn.GetID()) {
		if time.Now().After(deadline) {
			t.Fatal("idle pub/sub connection still tracked after re-auth finished")
		}
		time.Sleep(time.Millisecond)
	}
}

// RemoveConn must drop a pending Pub/Sub re-auth: after the connection is
// closed, a queued rotation must never run.
func TestPubSubReAuthRemoveConnDropsPending(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	cn, cleanup := newTestPubSubConn(t)
	defer cleanup()

	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	ran := make(chan int, 3)

	mkFn := func(n int) func(error) {
		return func(error) {
			if n == 1 {
				once.Do(func() { close(started) })
				<-release
			}
			ran <- n
		}
	}

	manager.MarkForReAuth(cn, mkFn(1))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first re-auth did not start")
	}
	manager.MarkForReAuth(cn, mkFn(2))
	if !manager.IsTracking(cn.GetID()) {
		t.Fatal("expected the manager to track the connection while a re-auth is pending")
	}

	_ = cn.Close()
	manager.RemoveConn(cn.GetID())
	if manager.IsTracking(cn.GetID()) {
		t.Fatal("RemoveConn left pub/sub re-auth state behind")
	}
	close(release)

	// The in-flight re-auth finishes; the pending one must never run.
	select {
	case n := <-ran:
		if n != 1 {
			t.Fatalf("unexpected re-auth ran after RemoveConn: %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight re-auth did not finish")
	}
	select {
	case n := <-ran:
		t.Fatalf("pending re-auth %d ran after RemoveConn", n)
	case <-time.After(200 * time.Millisecond):
	}
}

// A rotation that arrives after the connection was removed must not
// resurrect tracking state for the closed connection: the entry created
// after RemoveConn would never be cleaned up again.
func TestPubSubReAuthNoStateAfterRemoveConn(t *testing.T) {
	manager := NewManager(&mockPooler{}, time.Second)
	cn, cleanup := newTestPubSubConn(t)
	defer cleanup()

	_ = cn.Close()
	manager.RemoveConn(cn.GetID())
	manager.poolHookRef.markPubSubForReAuth(cn, func(error) {
		t.Error("re-auth ran for a removed connection")
	})

	// Give a worker a chance to start, then check no state remains.
	time.Sleep(100 * time.Millisecond)
	manager.poolHookRef.pubSubReAuthLock.Lock()
	_, ok := manager.poolHookRef.pubSubReAuth[cn.GetID()]
	manager.poolHookRef.pubSubReAuthLock.Unlock()
	if ok {
		t.Fatal("mark after RemoveConn left pub/sub re-auth state behind")
	}
}
