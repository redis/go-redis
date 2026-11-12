package redis

import (
	"context"
	"errors"
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

// TestPubSubReceiveTimeoutReusesCmdPerConn verifies that ReceiveTimeout
// allocates c.cmd once per connection, reuses it across frames and AUTH-OK
// retries, resets it on closeTheCn, and remains race-free with getContext.
func TestPubSubReceiveTimeoutReusesCmdPerConn(t *testing.T) {
	client := NewClient(&Options{Addr: "127.0.0.1:1", Protocol: 3})
	defer client.Close()

	ps := client.pubSub()
	defer ps.Close()

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ps.SetNetConn(c1)
	ps.cn.SetResp3(true)

	done := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = ps.getContext()
			}
		}
	}()
	defer func() {
		close(done)
		readerWG.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Frame 1 initializes ps.cmd once for the active connection.
	go func() {
		_, _ = c2.Write([]byte(">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm1\r\n"))
	}()
	msg, err := ps.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatalf("first ReceiveTimeout failed: %v", err)
	}
	m, ok := msg.(*Message)
	if !ok || m.Payload != "m1" {
		t.Fatalf("unexpected first message: %#v", msg)
	}

	ps.mu.Lock()
	firstCmd := ps.cmd
	ps.mu.Unlock()
	if firstCmd == nil {
		t.Fatal("expected ps.cmd to be initialized after first ReceiveTimeout")
	}

	// Frame 2 + AUTH-OK internal retry reuse the exact same *Cmd instance.
	ps.mu.Lock()
	ps.authPending++
	ps.mu.Unlock()

	go func() {
		_, _ = c2.Write([]byte("+OK\r\n"))
		_, _ = c2.Write([]byte(">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm2\r\n"))
	}()
	msg, err = ps.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatalf("second ReceiveTimeout failed: %v", err)
	}
	m, ok = msg.(*Message)
	if !ok || m.Payload != "m2" {
		t.Fatalf("unexpected second message: %#v", msg)
	}

	ps.mu.Lock()
	reusedCmd := ps.cmd
	authPending := ps.authPending
	ps.mu.Unlock()
	if reusedCmd != firstCmd {
		t.Fatalf("expected ps.cmd pointer reuse (%p), got %p", firstCmd, reusedCmd)
	}
	if authPending != 0 {
		t.Fatalf("expected authPending decremented to 0, got %d", authPending)
	}

	// Closing the connection resets ps.cmd so a new connection allocates cleanly.
	ps.mu.Lock()
	if err := ps.closeTheCn(errors.New("reset")); err != nil {
		ps.mu.Unlock()
		t.Fatalf("closeTheCn failed: %v", err)
	}
	resetCmd := ps.cmd
	ps.mu.Unlock()
	if resetCmd != nil {
		t.Fatalf("expected ps.cmd reset to nil after closeTheCn, got %p", resetCmd)
	}

	c3, c4 := net.Pipe()
	defer c3.Close()
	defer c4.Close()
	ps.SetNetConn(c3)
	ps.cn.SetResp3(true)

	go func() {
		_, _ = c4.Write([]byte(">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm3\r\n"))
	}()
	msg, err = ps.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatalf("third ReceiveTimeout failed: %v", err)
	}
	m, ok = msg.(*Message)
	if !ok || m.Payload != "m3" {
		t.Fatalf("unexpected third message: %#v", msg)
	}

	ps.mu.Lock()
	secondCmd := ps.cmd
	ps.mu.Unlock()
	if secondCmd == nil || secondCmd == firstCmd {
		t.Fatalf("expected fresh ps.cmd after reconnect (first=%p, second=%p)", firstCmd, secondCmd)
	}
}
