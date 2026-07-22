package redis

import (
	"sync"
	"testing"
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
