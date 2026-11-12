package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// newWatchTestServer starts a RESP server on 127.0.0.1 and returns its
// address. newReply makes one reply function per connection, so a server can
// keep connection state such as the ASKING flag.
func newWatchTestServer(t *testing.T, newReply func() func(args []string) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			reply := newReply()
			wg.Add(1)
			go func() { defer wg.Done(); serveRedirectTestConn(conn, reply) }()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

func stateless(reply func(args []string) string) func() func(args []string) string {
	return func() func(args []string) string { return reply }
}

// clusterSlotsReply maps every slot to addr.
func clusterSlotsReply(addr string) string {
	host, port, _ := net.SplitHostPort(addr)
	return fmt.Sprintf("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$%d\r\n%s\r\n:%s\r\n", len(host), host, port)
}

func newWatchTestClusterClient(t *testing.T, addr string, maxRedirects int) *ClusterClient {
	t.Helper()
	client := NewClusterClient(&ClusterOptions{
		Addrs:                    []string{addr},
		Protocol:                 3,
		DisableIdentity:          true,
		MaxRedirects:             maxRedirects,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// The source of a slot in migration answers WATCH for a moved or new key with
// ASK. The target imports the slot and serves keyed commands only after
// ASKING; without it, the target answers MOVED back to the source.
// ClusterClient.Watch must send ASKING so the transaction runs on the target.
func TestClusterWatchAskSendsAsking(t *testing.T) {
	var source string
	var askings, execs atomic.Int32
	target := newWatchTestServer(t, func() func(args []string) string {
		var asking, multi bool
		return func(args []string) string {
			switch args[0] {
			case "hello":
				return "%0\r\n"
			case "asking":
				askings.Add(1)
				asking = true
				return "+OK\r\n"
			case "multi":
				// The ASKING flag holds inside MULTI until EXEC.
				multi = true
				return "+OK\r\n"
			case "unwatch":
				return "+OK\r\n"
			}
			if !asking {
				return "-MOVED 0 " + source + "\r\n"
			}
			if !multi {
				asking = false
			}
			switch args[0] {
			case "watch":
				return "+OK\r\n"
			case "get":
				return "_\r\n"
			case "set":
				return "+QUEUED\r\n"
			case "exec":
				multi, asking = false, false
				execs.Add(1)
				return "*1\r\n+OK\r\n"
			}
			return "-ERR unexpected command '" + args[0] + "'\r\n"
		}
	})
	source = newWatchTestServer(t, stateless(func(args []string) string {
		switch {
		case args[0] == "hello":
			return "%0\r\n"
		case args[0] == "watch":
			return "-ASK 0 " + target + "\r\n"
		case args[0] == "unwatch":
			return "+OK\r\n"
		case args[0] == "cluster" && args[1] == "shards":
			return "-ERR unknown subcommand\r\n"
		case args[0] == "cluster" && args[1] == "slots":
			return clusterSlotsReply(source)
		}
		return "-ERR unexpected command '" + args[0] + "'\r\n"
	}))

	client := newWatchTestClusterClient(t, source, 3)
	ctx := context.Background()
	called := false
	err := client.Watch(ctx, func(tx *Tx) error {
		called = true
		if err := tx.Get(ctx, "key").Err(); !errors.Is(err, Nil) {
			return fmt.Errorf("Get() = %v; want redis.Nil", err)
		}
		_, err := tx.TxPipelined(ctx, func(pipe Pipeliner) error {
			pipe.Set(ctx, "key", "value", 0)
			return nil
		})
		return err
	}, "key")
	if err != nil {
		t.Fatalf("Watch() = %v; want nil", err)
	}
	if !called {
		t.Fatal("Watch() did not run the callback")
	}
	if n := execs.Load(); n != 1 {
		t.Fatalf("target ran EXEC %d times; want 1", n)
	}
	if n := askings.Load(); n == 0 {
		t.Fatal("target received no ASKING")
	}
}

// When the attempts run out on redirects or READONLY replies, Watch must
// return the last error. It must not return nil without running the callback.
func TestClusterWatchReturnsErrorWhenRedirectsRunOut(t *testing.T) {
	t.Run("MOVED", func(t *testing.T) {
		var a, b string
		node := func(self, other *string) func(args []string) string {
			return func(args []string) string {
				switch {
				case args[0] == "hello":
					return "%0\r\n"
				case args[0] == "watch":
					return "-MOVED 0 " + *other + "\r\n"
				case args[0] == "cluster" && args[1] == "shards":
					return "-ERR unknown subcommand\r\n"
				case args[0] == "cluster" && args[1] == "slots":
					return clusterSlotsReply(*self)
				}
				return "+OK\r\n"
			}
		}
		a = newWatchTestServer(t, stateless(node(&a, &b)))
		b = newWatchTestServer(t, stateless(node(&b, &a)))

		client := newWatchTestClusterClient(t, a, 2)
		called := false
		err := client.Watch(context.Background(), func(*Tx) error { called = true; return nil }, "key")
		if err == nil {
			t.Fatal("Watch() = nil; want a MOVED error")
		}
		if moved, _, _ := isMovedError(err); !moved {
			t.Fatalf("Watch() = %v; want a MOVED error", err)
		}
		if called {
			t.Fatal("Watch() ran the callback")
		}
	})

	t.Run("READONLY", func(t *testing.T) {
		var a string
		a = newWatchTestServer(t, stateless(func(args []string) string {
			switch {
			case args[0] == "hello":
				return "%0\r\n"
			case args[0] == "watch":
				return "-READONLY You can't write against a read only replica.\r\n"
			case args[0] == "cluster" && args[1] == "shards":
				return "-ERR unknown subcommand\r\n"
			case args[0] == "cluster" && args[1] == "slots":
				return clusterSlotsReply(a)
			}
			return "+OK\r\n"
		}))

		client := newWatchTestClusterClient(t, a, 2)
		called := false
		err := client.Watch(context.Background(), func(*Tx) error { called = true; return nil }, "key")
		if err == nil {
			t.Fatal("Watch() = nil; want a READONLY error")
		}
		if !isReadOnlyError(err) {
			t.Fatalf("Watch() = %v; want a READONLY error", err)
		}
		if called {
			t.Fatal("Watch() ran the callback")
		}
	})
}

// Live check on a private cluster: while a slot migrates, Watch on a key that
// already moved and on a new key must run on the importing node and commit.
func TestClusterWatchLiveSlotMigration(t *testing.T) {
	_, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	c := NewClusterClient(&ClusterOptions{
		Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	t.Cleanup(func() { _ = c.Close() })

	moved, fresh := "{watch-ask}:moved", "{watch-ask}:new"
	if err := c.Set(ctx, moved, "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	source, err := c.MasterForKey(ctx, moved)
	if err != nil {
		t.Fatal(err)
	}
	var a, b *Client
	for _, n := range nodes[:3] {
		if n.opt.Addr == source.opt.Addr {
			a = n
		} else if b == nil {
			b = n
		}
	}
	if a == nil || b == nil {
		t.Fatal("migration nodes not found")
	}
	aid, err := a.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	bid, err := b.ClusterMyID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	slot := clusterKeySlot(moved)
	if err := b.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
		t.Fatal(err)
	}
	if err := a.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
		t.Fatal(err)
	}
	_, bport, _ := net.SplitHostPort(b.opt.Addr)
	if err := a.Do(ctx, "migrate", "127.0.0.1", bport, "", 0, 5000, "keys", moved).Err(); err != nil {
		t.Fatal(err)
	}

	incr := func(key string) error {
		return c.Watch(ctx, func(tx *Tx) error {
			n, err := tx.Get(ctx, key).Int()
			if err != nil && !errors.Is(err, Nil) {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe Pipeliner) error {
				pipe.Set(ctx, key, n+1, 0)
				return nil
			})
			return err
		}, key)
	}
	for _, tc := range []struct{ key, want string }{{moved, "2"}, {fresh, "1"}} {
		if err := incr(tc.key); err != nil {
			t.Fatalf("Watch(%s) = %v; want nil", tc.key, err)
		}
		conn := b.Conn()
		if err := conn.Do(ctx, "asking").Err(); err != nil {
			t.Fatal(err)
		}
		got, err := conn.Get(ctx, tc.key).Result()
		_ = conn.Close()
		if err != nil || got != tc.want {
			t.Fatalf("%s on the importing node = %q, %v; want %q", tc.key, got, err, tc.want)
		}
	}
}
