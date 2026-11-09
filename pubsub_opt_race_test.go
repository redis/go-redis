package redis

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// Client.pubSub / SentinelClient.pubSub used to alias the client's *Options
// (opt: c.opt) instead of taking a private copy. PubSub.reconnect rewrites
// c.opt.Addr (pubsub.go) under the PubSub mutex when a connection is handed
// off, while the client's connection pool dialer reads opt.Addr (options.go,
// the Dialer closure) lock-free on every dial. Both point at the same Options,
// so the rewrite and the dial read race.
//
// Run with -race. Before the fix the shared Addr is read and written
// concurrently; after it the PubSub owns a private copy and the pointer
// identity check below also fails without -race.
func TestPubSubDoesNotAliasClientOptions(t *testing.T) {
	client := NewClient(&Options{Addr: "127.0.0.1:1"})
	defer client.Close()

	ps := client.pubSub()
	defer ps.Close()

	const iters = 100000

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: mirror PubSub.reconnect rewriting opt.Addr under the PubSub mutex.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			ps.mu.Lock()
			ps.opt.Addr = "127.0.0.1:2"
			ps.mu.Unlock()
		}
	}()

	// Reader: the pool dialer reads client.opt.Addr lock-free (options.go).
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = client.opt.Addr
		}
	}()

	wg.Wait()

	if ps.opt == client.opt {
		t.Fatal("PubSub aliases the client's *Options; reconnect races the pool dialer")
	}
}

// TestPubSubGetContextRace pins synchronization between PubSub.ReceiveTimeout
// writing c.cmd and concurrent readers of c.getContext() (such as sentinelFailover.listen).
func TestPubSubGetContextRace(t *testing.T) {
	pubsub := &PubSub{}
	pubsub.init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const iters = 100000

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: mirrors PubSub.ReceiveTimeout updating c.cmd under c.mu.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			cmd := NewCmd(ctx)
			pubsub.mu.Lock()
			pubsub.cmd = cmd
			pubsub.mu.Unlock()
		}
	}()

	// Reader: mirrors sentinelFailover.listen calling pubsub.getContext().
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = pubsub.getContext()
		}
	}()

	wg.Wait()
}

// TestPubSubReceiveTimeoutGetContextRace verifies that active execution of ReceiveTimeout
// synchronizes c.cmd updates with concurrent getContext reads.
func TestPubSubReceiveTimeoutGetContextRace(t *testing.T) {
	pubsub := &PubSub{
		opt: &Options{},
	}
	pubsub.init()

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	pubsub.SetNetConn(c1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const iters = 500

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: actively invokes ReceiveTimeout which writes c.cmd in its reader loop.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_, _ = pubsub.ReceiveTimeout(ctx, time.Microsecond)
		}
	}()

	// Reader: mirrors sentinelFailover.listen calling pubsub.getContext().
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = pubsub.getContext()
		}
	}()

	wg.Wait()
}
