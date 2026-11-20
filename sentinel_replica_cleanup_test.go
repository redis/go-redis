package redis

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

type replicaLookupConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *replicaLookupConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestSentinelReplicaDiscoveryClosesUnusedClients(t *testing.T) {
	const usable = "*1\r\n*6\r\n+ip\r\n+127.0.0.1\r\n+port\r\n+6380\r\n+flags\r\n+slave\r\n"
	const disconnected = "*1\r\n*6\r\n+ip\r\n+127.0.0.1\r\n+port\r\n+6380\r\n+flags\r\n+slave,disconnected\r\n"
	for _, tc := range []struct {
		name       string
		firstReply string
		lastReply  string
		wantAddr   bool
	}{
		{name: "empty", firstReply: "*0\r\n", lastReply: "*0\r\n"},
		{name: "filtered", firstReply: disconnected, lastReply: disconnected},
		{name: "later sentinel", firstReply: "*0\r\n", lastReply: usable, wantAddr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			connections := make(map[string][]*replicaLookupConn)
			var servers sync.WaitGroup
			var returnEmpty atomic.Bool
			t.Cleanup(func() {
				mu.Lock()
				for _, conns := range connections {
					for _, conn := range conns {
						_ = conn.Close()
					}
				}
				mu.Unlock()
				servers.Wait()
			})
			dial := func(_ context.Context, _, addr string) (net.Conn, error) {
				client, server := net.Pipe()
				conn := &replicaLookupConn{Conn: client}
				mu.Lock()
				connections[addr] = append(connections[addr], conn)
				mu.Unlock()
				servers.Add(1)
				go func() {
					defer servers.Done()
					defer server.Close()
					reader := proto.NewReader(server)
					for {
						args, err := reader.ReadSlice()
						if err != nil {
							return
						}
						reply := "+OK\r\n"
						switch args[0] {
						case "hello":
							reply = "-ERR unknown command 'hello'\r\n"
						case "sentinel":
							reply = "*0\r\n"
							if args[1] == "replicas" && !returnEmpty.Load() {
								reply = tc.firstReply
								if addr == "second:26379" {
									reply = tc.lastReply
								}
							}
						case "subscribe":
							reply = fmt.Sprintf("*3\r\n+subscribe\r\n+%s\r\n:1\r\n", args[1])
						}
						if _, err := io.WriteString(server, reply); err != nil {
							return
						}
					}
				}()
				return conn, nil
			}
			f := &sentinelFailover{
				opt: &FailoverOptions{
					MasterName: "mymaster", Dialer: dial, PoolSize: 1,
					MaxRetries: -1, DisableIdentity: true,
					ReadTimeout: time.Second, WriteTimeout: time.Second,
				},
				sentinelAddrs: []string{"first:26379", "second:26379"},
			}
			t.Cleanup(func() { _ = f.Close() })
			addrs, err := f.replicaAddrs(context.Background(), false)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantAddr {
				if len(addrs) != 1 || addrs[0] != "127.0.0.1:6380" {
					t.Fatalf("unexpected replicas: %v", addrs)
				}
				// A cached sentinel must survive a later empty response; only
				// clients not selected during discovery are disposable.
				selected := f.sentinel
				returnEmpty.Store(true)
				addrs, err = f.replicaAddrs(context.Background(), false)
				if err != nil || len(addrs) != 0 || f.sentinel != selected {
					t.Fatalf("cached sentinel changed after empty replicas: addrs=%v err=%v", addrs, err)
				}
			} else if len(addrs) != 0 || f.sentinel != nil {
				t.Fatalf("unexpected selected sentinel or replicas: %v", addrs)
			}
			mu.Lock()
			for _, addr := range []string{"first:26379", "second:26379"} {
				if len(connections[addr]) == 0 {
					t.Errorf("sentinel %s was not queried", addr)
				}
				wantClosed := addr == "first:26379" || !tc.wantAddr
				for _, conn := range connections[addr] {
					if conn.closed.Load() != wantClosed {
						t.Errorf("sentinel %s: closed=%v, want %v", addr, conn.closed.Load(), wantClosed)
					}
				}
			}
			mu.Unlock()
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			for addr, conns := range connections {
				for _, conn := range conns {
					if !conn.closed.Load() {
						t.Errorf("sentinel %s still open after failover Close", addr)
					}
				}
			}
		})
	}
}
